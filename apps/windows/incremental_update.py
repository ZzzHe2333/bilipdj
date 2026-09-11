from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from . import update_client
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
    """Raised when the current local state cannot be repaired by the delta bundle."""


@dataclass(frozen=True)
class IncrementalAssets:
    file_manifest: update_client.ReleaseAsset
    delta_zip: update_client.ReleaseAsset
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

    return IncrementalAssets(
        file_manifest=_asset_from_object(package.get("file_manifest"), "逐文件清单"),
        delta_zip=_asset_from_object(package.get("incremental"), "增量资源包"),
        base_version=str((package.get("incremental") or {}).get("base_version", "") if isinstance(package.get("incremental"), dict) else "").strip(),
    )


def _read_and_validate_file_manifest(
    path: Path,
    *,
    release: update_client.ReleaseInfo,
) -> tuple[dict[str, dict[str, object]], set[str], list[str], str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IncrementalUpdateError("逐文件更新清单无法解析") from exc
    if not isinstance(payload, dict) or payload.get("kind") != FILE_MANIFEST_KIND:
        raise IncrementalUpdateError("逐文件更新清单格式无效")
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
    for raw in raw_files:
        if not isinstance(raw, dict):
            raise IncrementalUpdateError("逐文件更新清单包含无效文件记录")
        try:
            relative = normalize_manifest_path(str(raw.get("path", "") or ""))
        except ManifestPathError as exc:
            raise IncrementalUpdateError(str(exc)) from exc
        if is_preserved_path(relative):
            raise IncrementalUpdateError(f"逐文件清单错误地包含用户数据路径：{relative}")
        if relative.casefold() in {key.casefold() for key in files}:
            raise IncrementalUpdateError(f"逐文件清单包含重复路径：{relative}")
        try:
            size = int(raw.get("size", -1))
        except (TypeError, ValueError) as exc:
            raise IncrementalUpdateError(f"文件大小无效：{relative}") from exc
        digest = str(raw.get("sha256", "") or "").strip().lower()
        if size < 0 or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise IncrementalUpdateError(f"文件清单元数据无效：{relative}")
        files[relative] = {"path": relative, "size": size, "sha256": digest}

    delta = payload.get("delta")
    if not isinstance(delta, dict):
        raise IncrementalUpdateError("逐文件清单缺少 delta 信息")
    delta_files: set[str] = set()
    raw_delta_files = delta.get("files", [])
    if not isinstance(raw_delta_files, list):
        raise IncrementalUpdateError("delta.files 格式无效")
    for value in raw_delta_files:
        relative = normalize_manifest_path(str(value))
        if is_preserved_path(relative) or relative not in files:
            raise IncrementalUpdateError(f"delta.files 包含无效路径：{relative}")
        delta_files.add(relative)

    removed: list[str] = []
    raw_removed = delta.get("removed", [])
    if not isinstance(raw_removed, list):
        raise IncrementalUpdateError("delta.removed 格式无效")
    for value in raw_removed:
        relative = normalize_manifest_path(str(value))
        if is_preserved_path(relative) or relative in files:
            raise IncrementalUpdateError(f"delta.removed 包含无效路径：{relative}")
        removed.append(relative)

    return files, delta_files, removed, str(delta.get("base_version", "") or "").strip()


def _sha256_stream(handle: object) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while True:
        chunk = handle.read(1024 * 1024)  # type: ignore[attr-defined]
        if not chunk:
            break
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


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


def _verify_delta_members(
    zip_path: Path,
    needed: list[str],
    files: dict[str, dict[str, object]],
) -> None:
    with zipfile.ZipFile(zip_path, "r") as archive:
        members: dict[str, zipfile.ZipInfo] = {}
        for info in archive.infolist():
            if info.is_dir():
                continue
            try:
                relative = normalize_manifest_path(info.filename)
            except ManifestPathError as exc:
                raise IncrementalUpdateError(f"增量包包含不安全路径：{info.filename}") from exc
            if is_preserved_path(relative):
                raise IncrementalUpdateError(f"增量包包含用户数据路径：{relative}")
            if relative in members:
                raise IncrementalUpdateError(f"增量包包含重复路径：{relative}")
            members[relative] = info

        for relative in needed:
            info = members.get(relative)
            if info is None:
                raise IncrementalUnavailable(f"增量包缺少本地需要更新的文件：{relative}，请使用全量更新")
            expected = files[relative]
            with archive.open(info, "r") as handle:
                digest, size = _sha256_stream(handle)
            if size != int(expected["size"]) or digest.lower() != str(expected["sha256"]).lower():
                raise IncrementalUpdateError(f"增量包文件校验失败：{relative}")


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
        delta_path = target_dir / assets.delta_zip.name
        update_client.download_file(
            assets.file_manifest.download_url,
            manifest_path,
            expected_size=assets.file_manifest.size,
        )
        update_client.verify_sha256(manifest_path, assets.file_manifest.sha256)
        files, delta_files, removed, manifest_base_version = _read_and_validate_file_manifest(
            manifest_path,
            release=release,
        )

        needed, unchanged = _scan_local(Path(app_dir), files)
        unavailable = [path for path in needed if path not in delta_files]
        if unavailable:
            preview = "、".join(unavailable[:3])
            suffix = "…" if len(unavailable) > 3 else ""
            raise IncrementalUnavailable(
                f"本地有 {len(unavailable)} 个差异文件不在本次增量包中（{preview}{suffix}），请使用全量更新。"
            )

        remove_existing: list[str] = []
        for relative in removed:
            target = resolve_managed_path(Path(app_dir), relative)
            if target.exists() or target.is_symlink():
                remove_existing.append(relative)

        update_client.download_file(
            assets.delta_zip.download_url,
            delta_path,
            expected_size=assets.delta_zip.size,
            progress=progress,
        )
        delta_digest = update_client.verify_sha256(delta_path, assets.delta_zip.sha256)
        _verify_delta_members(delta_path, needed, files)

        plan = {
            "schema": 1,
            "kind": "bilipdj-incremental-plan",
            "version": release.version,
            "base_version": manifest_base_version or assets.base_version,
            "target_package_sha256": str(release.sha256 or release.zip_asset.sha256),
            "replace": [files[path] for path in needed],
            "remove": remove_existing,
        }
        plan_path = target_dir / "incremental-plan.json"
        plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return PreparedIncrementalUpdate(
            release=release,
            work_dir=target_dir,
            zip_path=delta_path,
            plan_path=plan_path,
            file_manifest_path=manifest_path,
            delta_sha256=delta_digest,
            replace_count=len(needed),
            remove_count=len(remove_existing),
            unchanged_count=unchanged,
            scanned_count=len(files),
            download_size=assets.delta_zip.size,
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
