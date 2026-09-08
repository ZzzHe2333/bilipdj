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
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

GITHUB_REPOSITORY = "ZzzHe2333/bilipdj"
LATEST_RELEASE_API = (
    f"https://api.github.com/repos/{GITHUB_REPOSITORY}/releases/latest"
)
USER_AGENT = "bilipdj-auto-updater"
WINDOWS_ASSET_PREFIX = "bilibili-danmuji-windows-x64-"
UPDATER_EXE_NAME = "updater.exe"

ProgressCallback = Callable[[int, int], None]


class UpdateError(RuntimeError):
    """Raised when update discovery, download, or validation fails."""


@dataclass(frozen=True)
class ReleaseAsset:
    name: str
    download_url: str
    size: int


@dataclass(frozen=True)
class ReleaseInfo:
    version: str
    tag_name: str
    name: str
    body: str
    page_url: str
    zip_asset: ReleaseAsset
    checksum_asset: ReleaseAsset


@dataclass(frozen=True)
class PreparedUpdate:
    release: ReleaseInfo
    work_dir: Path
    zip_path: Path
    checksum_path: Path
    sha256: str


def normalize_version(value: str) -> tuple[int, ...]:
    text = str(value).strip()
    if text.lower().startswith("v"):
        text = text[1:]
    match = re.fullmatch(r"(\d+(?:\.\d+)*)(?:[-+].*)?", text)
    if not match:
        raise ValueError(f"无法识别版本号：{value}")
    parts = tuple(int(part) for part in match.group(1).split("."))
    return parts + (0,) * max(0, 3 - len(parts))


def is_newer_version(candidate: str, current: str) -> bool:
    return normalize_version(candidate) > normalize_version(current)


def _request(url: str, *, timeout: float = 15.0):
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": USER_AGENT,
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        return urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        raise UpdateError(f"GitHub 请求失败：HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise UpdateError(f"无法连接 GitHub：{exc.reason}") from exc
    except TimeoutError as exc:
        raise UpdateError("连接 GitHub 超时") from exc


def _asset_size(value: object, asset_name: str) -> int:
    try:
        size = int(value or 0)
    except (TypeError, ValueError) as exc:
        raise UpdateError(f"Release 附件大小无效：{asset_name}") from exc
    if size < 0:
        raise UpdateError(f"Release 附件大小不能为负数：{asset_name}")
    return size


def fetch_latest_release(*, timeout: float = 15.0) -> ReleaseInfo:
    with _request(LATEST_RELEASE_API, timeout=timeout) as response:
        try:
            payload = json.loads(response.read().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise UpdateError("GitHub Release 返回内容无法解析") from exc

    if not isinstance(payload, dict):
        raise UpdateError("GitHub Release 返回内容不是对象")

    tag_name = str(payload.get("tag_name", "")).strip()
    if not tag_name:
        raise UpdateError("最新 Release 缺少版本标签")
    version = tag_name[1:] if tag_name.lower().startswith("v") else tag_name
    try:
        normalize_version(version)
    except ValueError as exc:
        raise UpdateError(f"最新 Release 版本标签无效：{tag_name}") from exc

    assets = payload.get("assets", [])
    if not isinstance(assets, list):
        raise UpdateError("最新 Release 的附件列表无效")

    zip_name = f"{WINDOWS_ASSET_PREFIX}{tag_name}.zip"
    checksum_names = {f"{zip_name}.sha256", f"{zip_name}.sha256.txt"}
    zip_asset: ReleaseAsset | None = None
    checksum_asset: ReleaseAsset | None = None

    for raw_asset in assets:
        if not isinstance(raw_asset, dict):
            continue
        name = str(raw_asset.get("name", "")).strip()
        url = str(raw_asset.get("browser_download_url", "")).strip()
        if not name or not url:
            continue
        asset = ReleaseAsset(
            name=name,
            download_url=url,
            size=_asset_size(raw_asset.get("size", 0), name),
        )
        if name == zip_name:
            zip_asset = asset
        elif name in checksum_names:
            checksum_asset = asset

    if zip_asset is None:
        raise UpdateError(f"最新 Release 缺少 Windows 更新包：{zip_name}")
    if checksum_asset is None:
        raise UpdateError(
            f"最新 Release 缺少 SHA-256 校验文件：{zip_name}.sha256"
        )

    return ReleaseInfo(
        version=version,
        tag_name=tag_name,
        name=str(payload.get("name", "") or tag_name),
        body=str(payload.get("body", "") or ""),
        page_url=str(payload.get("html_url", "") or ""),
        zip_asset=zip_asset,
        checksum_asset=checksum_asset,
    )


def download_file(
    url: str,
    destination: Path,
    *,
    expected_size: int = 0,
    progress: ProgressCallback | None = None,
    timeout: float = 30.0,
) -> Path:
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + ".part")
    temp_path.unlink(missing_ok=True)

    try:
        with _request(url, timeout=timeout) as response, temp_path.open("wb") as output:
            try:
                header_size = int(response.headers.get("Content-Length", "0") or 0)
            except (TypeError, ValueError):
                header_size = 0
            declared_size = max(0, int(expected_size or header_size))
            downloaded = 0
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                output.write(chunk)
                downloaded += len(chunk)
                if progress is not None:
                    progress(downloaded, declared_size)

        actual_size = temp_path.stat().st_size
        if declared_size and actual_size != declared_size:
            raise UpdateError(
                f"下载文件大小不一致：预期 {declared_size} 字节，"
                f"实际 {actual_size} 字节"
            )
        temp_path.replace(destination)
        return destination
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def parse_checksum_file(path: Path, expected_filename: str) -> str:
    text = Path(path).read_text(encoding="utf-8-sig", errors="replace")
    expected_name = Path(expected_filename).name
    fallback: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = re.match(r"^([0-9a-fA-F]{64})(?:\s+[* ]?(.+))?$", line)
        if not match:
            continue
        digest = match.group(1).lower()
        filename = (match.group(2) or "").strip()
        if not filename:
            fallback = digest
        elif Path(filename).name == expected_name:
            return digest
    if fallback:
        return fallback
    raise UpdateError("SHA-256 校验文件格式无效")


def calculate_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_sha256(path: Path, expected_digest: str) -> str:
    actual = calculate_sha256(path)
    if actual.lower() != expected_digest.lower():
        raise UpdateError(
            f"更新包 SHA-256 校验失败：期望 {expected_digest}，实际 {actual}"
        )
    return actual


def cleanup_prepared_update(prepared: PreparedUpdate | None) -> None:
    """Remove downloaded update files that are no longer needed."""

    if prepared is None:
        return
    shutil.rmtree(Path(prepared.work_dir), ignore_errors=True)


def prepare_release_download(
    release: ReleaseInfo,
    *,
    progress: ProgressCallback | None = None,
    work_dir: Path | None = None,
) -> PreparedUpdate:
    owns_work_dir = work_dir is None
    target_dir = (
        Path(work_dir)
        if work_dir is not None
        else Path(tempfile.mkdtemp(prefix="bilipdj-update-"))
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    checksum_path = target_dir / release.checksum_asset.name
    zip_path = target_dir / release.zip_asset.name

    try:
        download_file(
            release.checksum_asset.download_url,
            checksum_path,
            expected_size=release.checksum_asset.size,
        )
        download_file(
            release.zip_asset.download_url,
            zip_path,
            expected_size=release.zip_asset.size,
            progress=progress,
        )
        expected_digest = parse_checksum_file(
            checksum_path,
            release.zip_asset.name,
        )
        actual_digest = verify_sha256(zip_path, expected_digest)
        return PreparedUpdate(
            release=release,
            work_dir=target_dir,
            zip_path=zip_path,
            checksum_path=checksum_path,
            sha256=actual_digest,
        )
    except Exception:
        if owns_work_dir:
            shutil.rmtree(target_dir, ignore_errors=True)
        raise


def copy_updater_to_work_dir(updater_exe: Path, work_dir: Path) -> Path:
    source = Path(updater_exe)
    if not source.is_file():
        raise UpdateError(f"找不到独立更新器：{source}")
    destination = Path(work_dir) / UPDATER_EXE_NAME
    shutil.copy2(source, destination)
    return destination


def launch_updater(
    prepared: PreparedUpdate,
    *,
    updater_exe: Path,
    app_dir: Path,
    main_exe_name: str = "main.exe",
    current_pid: int | None = None,
) -> subprocess.Popen[bytes]:
    updater_copy = copy_updater_to_work_dir(
        updater_exe,
        prepared.work_dir,
    )
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
        raise UpdateError(f"无法启动独立更新器：{exc}") from exc
