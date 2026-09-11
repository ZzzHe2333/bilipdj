from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from . import update_client, update_network
from .update_manifest import (
    ManifestPathError,
    is_preserved_path,
    normalize_manifest_path,
    resolve_managed_path,
)

ProgressCallback = Callable[[int, int], None]
WINDOWS_PACKAGE_KEY = "windows-tk-x64"
FILE_MANIFEST_KIND = "bilipdj-file-manifest"


class IncrementalUpdateError(update_client.UpdateError):
    pass


class IncrementalUnavailable(IncrementalUpdateError):
    """Raised when the current local state cannot safely use incremental transport."""


@dataclass(frozen=True)
class IncrementalAssets:
    file_manifest: update_client.ReleaseAsset
    resource_pack: update_client.ReleaseAsset
    base_version: str


@dataclass(frozen=True)
class PreparedIncrementalUpdate:
    release: update_client.ReleaseInfo
    work_dir: Path
    zip_path: Path
    plan_path: Path
    file_manifest_path: Path
    delta_sha256: str
    replace_count: int
    remove_count: int
    unchanged_count: int
    scanned_count: int
    download_size: int


def _release_manifest_url(release: update_client.ReleaseInfo) -> str:
    tag = str(release.tag_name or "").strip()
    if not tag:
        raise IncrementalUnavailable("Release 缺少版本标签，无法定位增量更新清单")
    return f"https://github.com/{update_client.GITHUB_REPOSITORY}/releases/download/{tag}/update-manifest.json"


def _asset_from_object(raw: object, label: str) -> update_client.ReleaseAsset:
    if not isinstance(raw, dict):
        raise IncrementalUnavailable(f"更新清单缺少 {label}")
    name = str(raw.get("filename", "") or "").strip()
    url = str(raw.get("url", "") or "").strip()
    if not name or not url:
        raise IncrementalUnavailable(f"{label} 缺少 filename/url")
    size = update_client._asset_size(raw.get("size", 0), name)  # noqa: SLF001
    sha256 = update_client._normalize_sha256(raw.get("sha256", ""), name)  # noqa: SLF001
    if not sha256:
        raise IncrementalUnavailable(f"{label} 缺少 SHA-256")
    return update_client.ReleaseAsset(name=name, download_url=url, size=size, sha256=sha256)


def fetch_incremental_assets(
    release: update_client.ReleaseInfo,
    *,
    timeout: float = 15.0,
) -> IncrementalAssets:
    url = _release_manifest_url(release)
    try:
        with update_client._request(url, timeout=timeout) as response:  # noqa: SLF001
            payload = update_client._decode_json_response(response, "增量更新清单")  # noqa: SLF001
    except Exception as exc:  # noqa: BLE001
        raise IncrementalUnavailable(f"此 Release 没有可用的增量更新元数据：{exc}") from exc

    version = str(payload.get("version", "") or "").strip()
    if version != release.version:
        raise IncrementalUnavailable(
            f"增量更新清单版本不匹配：Release v{release.version}，清单 v{version or '?'}"
        )
    packages = payload.get("packages")
    if not isinstance(packages, dict):
        raise IncrementalUnavailable("增量更新清单缺少 packages")
    package = packages.get(WINDOWS_PACKAGE_KEY)
    if not isinstance(package, dict):
        raise IncrementalUnavailable("增量更新清单缺少 Windows Tk 包")

    raw_incremental = package.get("incremental")
    if not isinstance(raw_incremental, dict):
        raise IncrementalUnavailable("更新清单缺少增量资源包")
    transport = str(raw_incremental.get("transport", "") or "").strip().lower()
    if transport != "http-range":
        raise IncrementalUnavailable("此 Release 不支持按文件 Range 增量下载")
    return IncrementalAssets(
        file_manifest=_asset_from_object(package.get("file_manifest"), "逐文件清单"),
        resource_pack=_asset_from_object(raw_incremental, "增量资源包"),
        base_version=str(raw_incremental.get("base_version", "") or "").strip(),
    )


def _read_and_validate_file_manifest(
    path: Path,
    *,
    release: update_client.ReleaseInfo,
    pack_size: int,
) -> tuple[dict[str, dict[str, object]], list[str], str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IncrementalUpdateError("逐文件更新清单无法解析") from exc
    if not isinstance(payload, dict) or payload.get("kind") != FILE_MANIFEST_KIND:
        raise IncrementalUpdateError("逐文件更新清单格式无效")
    if int(payload.get("schema", 0) or 0) != 2:
        raise IncrementalUpdateError("逐文件更新清单 schema 不受支持")
    if str(payload.get("version", "") or "").strip() != release.version:
        raise IncrementalUpdateError("逐文件更新清单版本与 Release 不一致")
    if str(payload.get("package", "") or "").strip() != WINDOWS_PACKAGE_KEY:
        raise IncrementalUpdateError("逐文件更新清单不是 Windows Tk 包")

    package_sha = update_client._normalize_sha256(payload.get("package_sha256", ""), "完整包")  # noqa: SLF001
    release_sha = update_client._normalize_sha256(release.sha256 or release.zip_asset.sha256, release.zip_asset.name)  # noqa: SLF001
    if release_sha and package_sha and package_sha != release_sha:
        raise IncrementalUpdateError("逐文件清单记录的完整包 SHA-256 与 Release 不一致")

    raw_files = payload.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        raise IncrementalUpdateError("逐文件更新清单没有文件记录")
    files: dict[str, dict[str, object]] = {}
    seen_keys: set[str] = set()
    ranges: list[tuple[int, int, str]] = []
    for raw in raw_files:
        if not isinstance(raw, dict):
            raise IncrementalUpdateError("逐文件更新清单包含无效文件记录")
        try:
            relative = normalize_manifest_path(str(raw.get("path", "") or ""))
        except ManifestPathError as exc:
            raise IncrementalUpdateError(str(exc)) from exc
        if is_preserved_path(relative):
            raise IncrementalUpdateError(f"逐文件清单错误地包含用户数据路径：{relative}")
        key = relative.casefold()
        if key in seen_keys:
            raise IncrementalUpdateError(f"逐文件清单包含重复路径：{relative}")
        seen_keys.add(key)
        try:
            size = int(raw.get("size", -1))
            offset = int(raw.get("offset", -1))
            packed_size = int(raw.get("packed_size", -1))
        except (TypeError, ValueError) as exc:
            raise IncrementalUpdateError(f"文件大小/资源偏移无效：{relative}") from exc
        digest = str(raw.get("sha256", "") or "").strip().lower()
        packed_digest = str(raw.get("packed_sha256", "") or "").strip().lower()
        compression = str(raw.get("compression", "") or "").strip().lower()
        if (
            size < 0
            or offset < 0
            or packed_size < 0
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
            or not re.fullmatch(r"[0-9a-f]{64}", packed_digest)
            or compression not in {"store", "zlib"}
            or offset + packed_size > pack_size
        ):
            raise IncrementalUpdateError(f"文件清单元数据无效：{relative}")
        files[relative] = {
            "path": relative,
            "size": size,
            "sha256": digest,
            "offset": offset,
            "packed_size": packed_size,
            "packed_sha256": packed_digest,
            "compression": compression,
        }
        ranges.append((offset, offset + packed_size, relative))

    ranges.sort()
    for index in range(1, len(ranges)):
        if ranges[index][0] < ranges[index - 1][1]:
            raise IncrementalUpdateError(
                f"逐文件清单包含重叠资源范围：{ranges[index - 1][2]} / {ranges[index][2]}"
            )

    incremental = payload.get("incremental")
    if not isinstance(incremental, dict):
        raise IncrementalUpdateError("逐文件清单缺少 incremental 信息")
    removed: list[str] = []
    raw_removed = incremental.get("removed_from_base", [])
    if not isinstance(raw_removed, list):
        raise IncrementalUpdateError("incremental.removed_from_base 格式无效")
    for value in raw_removed:
        relative = normalize_manifest_path(str(value))
        if is_preserved_path(relative) or relative in files:
            raise IncrementalUpdateError(f"removed_from_base 包含无效路径：{relative}")
        removed.append(relative)

    return files, removed, str(incremental.get("base_version", "") or "").strip()


def _require_exact_base_version(app_dir: Path, base_version: str) -> str:
    base = str(base_version or "").strip()
    if not base:
        raise IncrementalUnavailable("本次 Release 没有声明增量基线版本，请使用全量更新。")
    version_path = Path(app_dir) / "VERSION"
    try:
        local = version_path.read_text(encoding="utf-8-sig").strip()
    except OSError as exc:
        raise IncrementalUnavailable("无法读取本地 VERSION，不能安全执行增量更新；请使用全量更新。") from exc
    try:
        local_key = update_client.normalize_version(local)
        base_key = update_client.normalize_version(base)
    except ValueError as exc:
        raise IncrementalUnavailable("本地版本或增量基线版本格式无效，请使用全量更新。") from exc
    if local_key != base_key:
        raise IncrementalUnavailable(
            f"本次增量包基于 v{base}，当前本地为 v{local or '?'}；跨版本增量可能遗留旧文件，请使用全量更新。"
        )
    return local


def _scan_local(
    app_dir: Path,
    files: dict[str, dict[str, object]],
) -> tuple[list[str], int]:
    needed: list[str] = []
    unchanged = 0
    for relative, metadata in files.items():
        target = resolve_managed_path(app_dir, relative)
        if target.is_symlink() or not target.is_file():
            needed.append(relative)
            continue
        expected_size = int(metadata["size"])
        try:
            if target.stat().st_size != expected_size:
                needed.append(relative)
                continue
            actual = update_client.calculate_sha256(target)
        except OSError:
            needed.append(relative)
            continue
        if actual.lower() != str(metadata["sha256"]).lower():
            needed.append(relative)
        else:
            unchanged += 1
    return needed, unchanged


def _download_range(url: str, *, offset: int, size: int, timeout: float = 45.0) -> bytes:
    if size < 0 or offset < 0:
        raise IncrementalUpdateError("增量资源范围无效")
    if size == 0:
        return b""
    end = offset + size - 1
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/octet-stream",
            "User-Agent": update_client.USER_AGENT,
            "Range": f"bytes={offset}-{end}",
        },
    )
    try:
        with update_network.open_url(request, timeout=timeout) as response:
            status = int(getattr(response, "status", 200) or 200)
            content_range = str(response.headers.get("Content-Range", "") or "")
            if status != 206:
                raise IncrementalUnavailable(
                    "当前 GitHub/CDN/代理未返回 HTTP 206 Range 响应；为避免偷偷下载完整资源包，请改用全量更新。"
                )
            expected_prefix = f"bytes {offset}-{end}/"
            if not content_range.lower().startswith(expected_prefix.lower()):
                raise IncrementalUpdateError(
                    f"增量资源 Content-Range 不匹配：期望 {expected_prefix}*，实际 {content_range or '?'}"
                )
            data = response.read(size + 1)
    except IncrementalUpdateError:
        raise
    except urllib.error.HTTPError as exc:
        raise IncrementalUnavailable(f"增量资源 Range 请求失败：HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise IncrementalUnavailable(f"无法下载增量资源：{exc.reason}") from exc
    except TimeoutError as exc:
        raise IncrementalUnavailable("下载增量资源超时") from exc
    if len(data) != size:
        raise IncrementalUpdateError(f"增量资源片段长度不一致：预期 {size}，实际 {len(data)}")
    return data


def _unpack_file(metadata: dict[str, object], packed: bytes) -> bytes:
    packed_digest = hashlib.sha256(packed).hexdigest()
    if packed_digest.lower() != str(metadata["packed_sha256"]).lower():
        raise IncrementalUpdateError(f"增量资源片段 SHA-256 校验失败：{metadata['path']}")
    compression = str(metadata["compression"])
    if compression == "store":
        raw = packed
    elif compression == "zlib":
        try:
            raw = zlib.decompress(packed)
        except zlib.error as exc:
            raise IncrementalUpdateError(f"增量资源解压失败：{metadata['path']}") from exc
    else:
        raise IncrementalUpdateError(f"不支持的增量压缩方式：{compression}")
    if len(raw) != int(metadata["size"]):
        raise IncrementalUpdateError(f"增量文件大小校验失败：{metadata['path']}")
    digest = hashlib.sha256(raw).hexdigest()
    if digest.lower() != str(metadata["sha256"]).lower():
        raise IncrementalUpdateError(f"增量文件 SHA-256 校验失败：{metadata['path']}")
    return raw


def cleanup_prepared_incremental(prepared: PreparedIncrementalUpdate | None) -> None:
    if prepared is None:
        return
    shutil.rmtree(Path(prepared.work_dir), ignore_errors=True)


def prepare_incremental_download(
    release: update_client.ReleaseInfo,
    *,
    app_dir: Path,
    progress: ProgressCallback | None = None,
    work_dir: Path | None = None,
) -> PreparedIncrementalUpdate:
    owns_work_dir = work_dir is None
    target_dir = Path(work_dir) if work_dir is not None else Path(tempfile.mkdtemp(prefix="bilipdj-incremental-"))
    target_dir.mkdir(parents=True, exist_ok=True)
    try:
        assets = fetch_incremental_assets(release)
        manifest_path = target_dir / assets.file_manifest.name
        patch_zip = target_dir / "incremental-patch.zip"
        update_client.download_file(
            assets.file_manifest.download_url,
            manifest_path,
            expected_size=assets.file_manifest.size,
        )
        update_client.verify_sha256(manifest_path, assets.file_manifest.sha256)
        files, removed, manifest_base_version = _read_and_validate_file_manifest(
            manifest_path,
            release=release,
            pack_size=assets.resource_pack.size,
        )
        base_version = manifest_base_version or assets.base_version
        if assets.base_version and manifest_base_version and update_client.normalize_version(assets.base_version) != update_client.normalize_version(manifest_base_version):
            raise IncrementalUpdateError("增量资源包与逐文件清单的基线版本不一致")
        _require_exact_base_version(Path(app_dir), base_version)

        needed, unchanged = _scan_local(Path(app_dir), files)
        remove_existing: list[str] = []
        for relative in removed:
            target = resolve_managed_path(Path(app_dir), relative)
            if target.exists() or target.is_symlink():
                remove_existing.append(relative)

        total_download = sum(int(files[path]["packed_size"]) for path in needed)
        downloaded = 0
        with zipfile.ZipFile(patch_zip, "w", compression=zipfile.ZIP_STORED) as patch:
            for relative in needed:
                metadata = files[relative]
                packed = _download_range(
                    assets.resource_pack.download_url,
                    offset=int(metadata["offset"]),
                    size=int(metadata["packed_size"]),
                )
                raw = _unpack_file(metadata, packed)
                patch.writestr(relative, raw)
                downloaded += len(packed)
                if progress is not None:
                    progress(downloaded, total_download)

        patch_digest = update_client.calculate_sha256(patch_zip)
        plan = {
            "schema": 1,
            "kind": "bilipdj-incremental-plan",
            "version": release.version,
            "base_version": base_version,
            "target_package_sha256": str(release.sha256 or release.zip_asset.sha256),
            "replace": [
                {"path": files[path]["path"], "size": files[path]["size"], "sha256": files[path]["sha256"]}
                for path in needed
            ],
            "remove": remove_existing,
        }
        plan_path = target_dir / "incremental-plan.json"
        plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return PreparedIncrementalUpdate(
            release=release,
            work_dir=target_dir,
            zip_path=patch_zip,
            plan_path=plan_path,
            file_manifest_path=manifest_path,
            delta_sha256=patch_digest,
            replace_count=len(needed),
            remove_count=len(remove_existing),
            unchanged_count=unchanged,
            scanned_count=len(files),
            download_size=total_download,
        )
    except Exception:
        if owns_work_dir:
            shutil.rmtree(target_dir, ignore_errors=True)
        raise


def launch_incremental_updater(
    prepared: PreparedIncrementalUpdate,
    *,
    updater_exe: Path,
    app_dir: Path,
    main_exe_name: str = "main.exe",
    current_pid: int | None = None,
) -> subprocess.Popen[bytes]:
    updater_copy = update_client.copy_updater_to_work_dir(updater_exe, prepared.work_dir)
    pid = int(current_pid or os.getpid())
    command = [
        str(updater_copy),
        "--pid",
        str(pid),
        "--app-dir",
        str(Path(app_dir).resolve()),
        "--zip",
        str(prepared.zip_path.resolve()),
        "--main-exe",
        str(main_exe_name),
        "--target-version",
        prepared.release.version,
        "--mode",
        "incremental",
        "--plan",
        str(prepared.plan_path.resolve()),
    ]
    creationflags = 0
    if sys.platform == "win32":
        creationflags = (
            getattr(subprocess, "DETACHED_PROCESS", 0)
            | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        )
    try:
        return subprocess.Popen(
            command,
            cwd=str(prepared.work_dir),
            close_fds=True,
            creationflags=creationflags,
        )
    except OSError as exc:
        raise IncrementalUpdateError(f"无法启动增量更新器：{exc}") from exc
