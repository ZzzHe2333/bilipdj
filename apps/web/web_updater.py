from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import zlib
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from typing import Any

WEB_PACKAGE_KEY = "web-portable-x64"
MAX_ARCHIVE_MEMBERS = 20_000
MAX_ARCHIVE_UNCOMPRESSED_BYTES = 2 * 1024 * 1024 * 1024
STARTUP_GRACE_SECONDS = 8.0
USER_AGENT = "bilipdj-web-updater"
PRESERVED_FILES = {
    "config.yaml", "quanxian.yaml", "kaiguan.yaml", "appearance.json", "style.json", "update-result.json",
    "core/config.yaml", "core/quanxian.yaml", "core/kaiguan.yaml", "core/appearance.json", "core/style.json",
}
PRESERVED_PREFIXES = ("core/cd/", "log/", "logs/", "backup/", "plugins/", "key/")


class WebUpdaterError(RuntimeError):
    pass


def _timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_component(value: str) -> str:
    text = "".join(ch if ch.isalnum() or ch in ".-_" else "_" for ch in str(value or "")).strip("._-")
    return text or "unknown"


def _normalize_path(value: str | Path) -> str:
    text = str(value).replace("\\", "/").strip()
    if not text or "\x00" in text:
        raise WebUpdaterError("更新清单包含空路径")
    path = PurePosixPath(text)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise WebUpdaterError(f"更新清单包含不安全路径：{value}")
    if path.parts and ":" in path.parts[0]:
        raise WebUpdaterError(f"更新清单包含盘符路径：{value}")
    return path.as_posix()


def _is_preserved(value: str | Path) -> bool:
    try:
        normalized = _normalize_path(value).casefold()
    except WebUpdaterError:
        return True
    return normalized in PRESERVED_FILES or any(normalized.startswith(prefix) for prefix in PRESERVED_PREFIXES)


def _resolve_managed(root: Path, value: str | Path) -> Path:
    relative = _normalize_path(value)
    if _is_preserved(relative):
        raise WebUpdaterError(f"拒绝更新用户数据路径：{relative}")
    base = root.resolve()
    target = (base / Path(*PurePosixPath(relative).parts)).resolve()
    try:
        target.relative_to(base)
    except ValueError as exc:
        raise WebUpdaterError(f"更新路径越界：{relative}") from exc
    return target


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify(path: Path, expected: str, label: str) -> None:
    digest = _sha256(path)
    if digest.lower() != str(expected or "").strip().lower():
        raise WebUpdaterError(f"{label} SHA-256 校验失败")


def _remove(path: Path) -> None:
    for attempt in range(30):
        try:
            if path.is_symlink() or path.is_file():
                path.unlink(missing_ok=True)
            elif path.exists():
                shutil.rmtree(path)
            return
        except OSError:
            if attempt == 29:
                raise
            time.sleep(0.2)


def _safe_extract(zip_path: Path, destination: Path) -> None:
    root = destination.resolve()
    seen: set[str] = set()
    expanded = 0
    with zipfile.ZipFile(zip_path, "r") as archive:
        members = archive.infolist()
        if len(members) > MAX_ARCHIVE_MEMBERS:
            raise WebUpdaterError("更新包文件数量超过安全上限")
        for member in members:
            name = str(member.filename or "")
            if not name or "\x00" in name:
                raise WebUpdaterError("更新包包含非法文件名")
            target = (destination / name).resolve()
            try:
                target.relative_to(root)
            except ValueError as exc:
                raise WebUpdaterError(f"更新包包含路径穿越：{name}") from exc
            key = str(target).casefold()
            if key in seen:
                raise WebUpdaterError(f"更新包包含重复路径：{name}")
            seen.add(key)
            if not member.is_dir():
                expanded += max(0, int(member.file_size))
                if expanded > MAX_ARCHIVE_UNCOMPRESSED_BYTES:
                    raise WebUpdaterError("更新包解压后体积超过安全上限")
        archive.extractall(destination)


def _copy_entry(source: Path, destination: Path) -> None:
    if source.is_symlink():
        raise WebUpdaterError(f"不支持符号链接：{source}")
    if source.is_dir():
        destination.mkdir(parents=True, exist_ok=False)
        for child in source.iterdir():
            _copy_entry(child, destination / child.name)
        return
    if source.is_file():
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return
    raise WebUpdaterError(f"不支持的文件类型：{source}")


def _copy_tree_contents(source: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for child in source.iterdir():
        _copy_entry(child, destination / child.name)


def _snapshot(app_dir: Path, target_version: str, state: "ProgressState") -> Path:
    backup_root = app_dir / "backup"
    backup_root.mkdir(parents=True, exist_ok=True)
    base = f"update-{datetime.now().strftime('%Y%m%d-%H%M%S')}-to-v{_safe_component(target_version)}"
    snapshot = backup_root / base
    index = 2
    while snapshot.exists():
        snapshot = backup_root / f"{base}-{index}"
        index += 1
    snapshot.mkdir()
    entries = [entry for entry in app_dir.iterdir() if entry.name.casefold() != "backup"]
    try:
        for i, entry in enumerate(entries, 1):
            state.set(stage="backup", percent=68 + min(8, int(i * 8 / max(1, len(entries)))), current_file=entry.name,
                      message="正在保存更新前版本快照…")
            _copy_entry(entry, snapshot / entry.name)
    except Exception:
        _remove(snapshot)
        raise
    return snapshot


def _copy_preserved(old_root: Path, new_root: Path) -> None:
    for name in PRESERVED_FILES:
        source = old_root / Path(*PurePosixPath(name).parts)
        destination = new_root / Path(*PurePosixPath(name).parts)
        if source.is_file() and not source.is_symlink():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
    for prefix in PRESERVED_PREFIXES:
        relative = prefix.rstrip("/")
        source = old_root / Path(*PurePosixPath(relative).parts)
        destination = new_root / Path(*PurePosixPath(relative).parts)
        if source.is_dir() and not source.is_symlink():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(source, destination, dirs_exist_ok=True)


def _write_result(app_dir: Path, *, status: str, version: str, backup: Path | None, error: str = "") -> None:
    if not app_dir.is_dir():
        return
    payload = {
        "status": status,
        "version": version,
        "installed_at": _timestamp(),
        "backup_dir": str(backup or ""),
        "mode": "web-portable",
    }
    if error:
        payload["error"] = error
    (app_dir / "update-result.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class ProgressState:
    def __init__(self, request: dict[str, Any]) -> None:
        self.lock = threading.RLock()
        self.started = time.monotonic()
        self.page_seen = threading.Event()
        self.data: dict[str, Any] = {
            "status": "running", "stage": "starting", "percent": 0, "message": "独立更新器正在启动…",
            "current_file": "", "downloaded": 0, "total": 0, "speed": 0, "error": "", "done": False,
            "success": False, "mode": request.get("mode", ""), "target_version": request.get("target_version", ""),
            "return_url": request.get("return_url", ""), "updated_at": time.time(),
        }

    def set(self, **values: Any) -> None:
        with self.lock:
            values["updated_at"] = time.time()
            self.data.update(values)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return dict(self.data)


class UpdateHost(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False

    def __init__(self, address: tuple[str, int], request_data: dict[str, Any], html_path: Path) -> None:
        self.request_data = request_data
        self.token = str(request_data["token"])
        self.state = ProgressState(request_data)
        self.html_path = html_path
        super().__init__(address, UpdateHandler)


class UpdateHandler(BaseHTTPRequestHandler):
    server: UpdateHost

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def _loopback(self) -> bool:
        host = str(self.client_address[0] if self.client_address else "").strip().lower()
        return host in {"127.0.0.1", "::1", "localhost"} or host.startswith("::ffff:127.")

    def _json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if not self._loopback():
            self._json({"status": "error", "message": "local only"}, 403)
            return
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path in {"/", "/update.html"}:
            try:
                body = self.server.html_path.read_bytes()
            except OSError:
                body = b"<!doctype html><meta charset=utf-8><title>BiliPDJ updater</title><h1>update.html missing</h1>"
            self.server.state.page_seen.set()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/api/status":
            query = urllib.parse.parse_qs(parsed.query)
            token = str((query.get("token") or [""])[0])
            if not secrets_compare(token, self.server.token):
                self._json({"status": "error", "message": "invalid token"}, 403)
                return
            self._json(self.server.state.snapshot())
            return
        self._json({"status": "error", "message": "not found"}, 404)


def secrets_compare(left: str, right: str) -> bool:
    import hmac
    return hmac.compare_digest(str(left), str(right))


def _bundle_html() -> Path:
    root = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parents[2])).resolve()
    candidates = [root / "apps" / "web" / "updater_static" / "update.html", Path(__file__).resolve().parent / "updater_static" / "update.html"]
    for path in candidates:
        if path.is_file():
            return path
    return candidates[0]


def _download(url: str, target: Path, *, expected_size: int, state: ProgressState, start_pct: int, end_pct: int, label: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/octet-stream"})
    downloaded = 0
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=45) as response, target.open("wb") as handle:
            total = int(response.headers.get("Content-Length", "0") or 0) or int(expected_size or 0)
            while True:
                chunk = response.read(1024 * 256)
                if not chunk:
                    break
                handle.write(chunk)
                downloaded += len(chunk)
                elapsed = max(0.001, time.monotonic() - started)
                ratio = downloaded / total if total > 0 else 0
                state.set(stage="downloading", percent=start_pct + int((end_pct - start_pct) * min(1.0, ratio)),
                          current_file=label, downloaded=downloaded, total=total, speed=int(downloaded / elapsed),
                          message=f"正在下载 {label}")
    except urllib.error.HTTPError as exc:
        raise WebUpdaterError(f"下载 {label} 失败：HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise WebUpdaterError(f"下载 {label} 失败：{exc}") from exc
    if expected_size > 0 and downloaded != expected_size:
        raise WebUpdaterError(f"{label} 下载大小不一致：预期 {expected_size}，实际 {downloaded}")


def _download_range(url: str, offset: int, size: int) -> bytes:
    if size == 0:
        return b""
    end = offset + size - 1
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/octet-stream", "Range": f"bytes={offset}-{end}"})
    try:
        with urllib.request.urlopen(req, timeout=45) as response:
            status = int(getattr(response, "status", 200) or 200)
            crange = str(response.headers.get("Content-Range", "") or "")
            if status != 206 or not crange.lower().startswith(f"bytes {offset}-{end}/".lower()):
                raise WebUpdaterError("CDN/代理不支持可靠的 HTTP Range 增量下载，请改用全量更新")
            data = response.read(size + 1)
    except urllib.error.HTTPError as exc:
        raise WebUpdaterError(f"增量 Range 下载失败：HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise WebUpdaterError(f"增量 Range 下载失败：{exc}") from exc
    if len(data) != size:
        raise WebUpdaterError("增量 Range 返回长度不匹配")
    return data


def _terminate_pid(pid: int) -> None:
    if pid <= 0 or pid == os.getpid():
        return
    if os.name == "nt":
        PROCESS_TERMINATE = 0x0001
        SYNCHRONIZE = 0x00100000
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_TERMINATE | SYNCHRONIZE, False, int(pid))
        if not handle:
            return
        try:
            ctypes.windll.kernel32.TerminateProcess(handle, 0)
            ctypes.windll.kernel32.WaitForSingleObject(handle, 10_000)
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.1)


def _stop_app(request: dict[str, Any], state: ProgressState) -> None:
    state.set(stage="stopping", percent=65, current_file="", message="更新页面已接管，正在停止 Web 主程序与后端服务…")
    _terminate_pid(int(request.get("launcher_pid", 0) or 0))
    _terminate_pid(int(request.get("backend_pid", 0) or 0))
    time.sleep(0.4)


def _launch_main(app_dir: Path, name: str) -> subprocess.Popen[bytes]:
    path = app_dir / name
    if not path.is_file():
        raise WebUpdaterError(f"更新后的主程序不存在：{name}")
    flags = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) if os.name == "nt" else 0
    return subprocess.Popen([str(path)], cwd=str(app_dir), close_fds=True, creationflags=flags)


def _wait_started(process: subprocess.Popen[bytes], state: ProgressState) -> None:
    state.set(stage="restarting", percent=96, current_file="", message="文件更新完成，正在重新启动 Web 服务…")
    deadline = time.monotonic() + STARTUP_GRACE_SECONDS
    while time.monotonic() < deadline:
        code = process.poll()
        if code is not None:
            raise WebUpdaterError(f"新版 Web 主程序启动后提前退出，退出码 {code}")
        time.sleep(0.25)


def _replace_directory(staging: Path, request: dict[str, Any], state: ProgressState) -> Path:
    app_dir = Path(request["app_dir"]).resolve()
    main_exe = str(request["main_exe"])
    updater_exe = str(request["updater_exe"])
    target_version = str(request.get("target_version", "") or "unknown")
    if not (staging / main_exe).is_file() or not (staging / updater_exe).is_file():
        raise WebUpdaterError(f"目标版本缺少 {main_exe} 或 {updater_exe}")

    snapshot = _snapshot(app_dir, target_version, state)
    parent = app_dir.parent
    rollback = parent / f".{app_dir.name}.web-update-rollback"
    _remove(rollback)
    rollback_created = False
    try:
        state.set(stage="installing", percent=78, message="正在切换到新版本文件…", current_file="")
        os.replace(app_dir, rollback)
        rollback_created = True
        os.replace(staging, app_dir)
        state.set(stage="installing", percent=88, message="正在恢复配置、插件、日志与备份数据…")
        _copy_preserved(rollback, app_dir)
        persisted_snapshot = app_dir / "backup" / snapshot.name
        _write_result(app_dir, status="installed", version=target_version, backup=persisted_snapshot)
        process = _launch_main(app_dir, main_exe)
        _wait_started(process, state)
        try:
            _remove(rollback)
        except OSError:
            pass
        rollback_created = False
        return persisted_snapshot
    except Exception as exc:
        if rollback_created:
            try:
                if app_dir.exists():
                    _remove(app_dir)
                os.replace(rollback, app_dir)
                rollback_created = False
                _write_result(app_dir, status="rolled_back", version=target_version, backup=app_dir / "backup" / snapshot.name, error=str(exc))
                try:
                    _launch_main(app_dir, main_exe)
                except Exception:
                    pass
            except Exception as rollback_exc:
                raise WebUpdaterError(f"更新失败且自动回滚失败：{exc}；回滚错误：{rollback_exc}") from exc
        raise


def _full_update(request: dict[str, Any], state: ProgressState, work: Path) -> None:
    package = request.get("package")
    if not isinstance(package, dict):
        raise WebUpdaterError("缺少 Web 完整包元数据")
    zip_path = work / str(package.get("filename", "web-update.zip"))
    _download(str(package.get("url", "")), zip_path, expected_size=int(package.get("size", 0) or 0), state=state,
              start_pct=3, end_pct=56, label=zip_path.name)
    state.set(stage="verifying", percent=58, message="正在校验完整更新包 SHA-256…")
    _verify(zip_path, str(package.get("sha256", "")), "完整更新包")
    staging = Path(request["app_dir"]).resolve().parent / f".{Path(request['app_dir']).name}.web-update-staging"
    _remove(staging)
    staging.mkdir(parents=True)
    state.set(stage="extracting", percent=61, message="正在安全解压完整更新包…")
    _safe_extract(zip_path, staging)
    state.page_seen.wait(timeout=8)
    _stop_app(request, state)
    _replace_directory(staging, request, state)


def _load_file_manifest(path: Path, request: dict[str, Any], pack_size: int) -> tuple[dict[str, dict[str, Any]], list[str]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise WebUpdaterError("逐文件清单无法解析") from exc
    if payload.get("kind") != "bilipdj-file-manifest" or int(payload.get("schema", 0) or 0) != 2:
        raise WebUpdaterError("逐文件清单格式不受支持")
    if str(payload.get("package", "") or "") != WEB_PACKAGE_KEY:
        raise WebUpdaterError("逐文件清单不是 Web Portable 包")
    if str(payload.get("version", "") or "") != str(request.get("target_version", "") or ""):
        raise WebUpdaterError("逐文件清单版本与目标版本不一致")
    incremental = payload.get("incremental")
    if not isinstance(incremental, dict):
        raise WebUpdaterError("逐文件清单缺少 incremental 信息")
    base = str(incremental.get("base_version", "") or "").strip()
    current = str(request.get("current_version", "") or "").strip()
    if not base or _version_key(base) != _version_key(current):
        raise WebUpdaterError(f"增量包基于 v{base or '?'}，当前为 v{current or '?'}；请使用全量更新")

    files: dict[str, dict[str, Any]] = {}
    ranges: list[tuple[int, int, str]] = []
    for raw in payload.get("files", []):
        if not isinstance(raw, dict):
            raise WebUpdaterError("逐文件清单包含无效记录")
        relative = _normalize_path(str(raw.get("path", "") or ""))
        if _is_preserved(relative) or relative.casefold() in {key.casefold() for key in files}:
            raise WebUpdaterError(f"逐文件清单包含非法路径：{relative}")
        try:
            size = int(raw.get("size", -1)); offset = int(raw.get("offset", -1)); packed_size = int(raw.get("packed_size", -1))
        except (TypeError, ValueError) as exc:
            raise WebUpdaterError(f"逐文件元数据无效：{relative}") from exc
        digest = str(raw.get("sha256", "") or "").lower(); packed_digest = str(raw.get("packed_sha256", "") or "").lower()
        compression = str(raw.get("compression", "") or "").lower()
        if size < 0 or offset < 0 or packed_size < 0 or offset + packed_size > pack_size or compression not in {"store", "zlib"}:
            raise WebUpdaterError(f"逐文件元数据越界：{relative}")
        if not re.fullmatch(r"[0-9a-f]{64}", digest) or not re.fullmatch(r"[0-9a-f]{64}", packed_digest):
            raise WebUpdaterError(f"逐文件 SHA-256 无效：{relative}")
        item = {"path": relative, "size": size, "sha256": digest, "offset": offset, "packed_size": packed_size,
                "packed_sha256": packed_digest, "compression": compression}
        files[relative] = item
        ranges.append((offset, offset + packed_size, relative))
    ranges.sort()
    for index in range(1, len(ranges)):
        if ranges[index][0] < ranges[index - 1][1]:
            raise WebUpdaterError("逐文件资源范围发生重叠")
    removed: list[str] = []
    for raw in incremental.get("removed_from_base", []):
        relative = _normalize_path(str(raw))
        if _is_preserved(relative) or relative in files:
            raise WebUpdaterError(f"删除清单包含非法路径：{relative}")
        removed.append(relative)
    return files, removed


def _version_key(value: str) -> tuple[int, ...]:
    text = str(value).strip().lstrip("vV")
    match = re.match(r"^(\d+(?:\.\d+)*)", text)
    if not match:
        raise WebUpdaterError(f"无法识别版本号：{value}")
    parts = tuple(int(piece) for piece in match.group(1).split("."))
    return parts + (0,) * max(0, 3 - len(parts))


def _incremental_update(request: dict[str, Any], state: ProgressState, work: Path) -> None:
    package = request.get("package")
    if not isinstance(package, dict):
        raise WebUpdaterError("缺少 Web 增量元数据")
    manifest_asset = package.get("file_manifest"); pack_asset = package.get("incremental")
    if not isinstance(manifest_asset, dict) or not isinstance(pack_asset, dict):
        raise WebUpdaterError("当前 Release 不支持 Web 增量更新")
    if str(pack_asset.get("transport", "") or "").lower() != "http-range":
        raise WebUpdaterError("Web 增量资源不是 HTTP Range 格式")
    manifest_path = work / "web-files.json"
    _download(str(manifest_asset.get("url", "")), manifest_path, expected_size=int(manifest_asset.get("size", 0) or 0),
              state=state, start_pct=3, end_pct=7, label="逐文件清单")
    _verify(manifest_path, str(manifest_asset.get("sha256", "")), "逐文件清单")
    files, removed = _load_file_manifest(manifest_path, request, int(pack_asset.get("size", 0) or 0))
    app_dir = Path(request["app_dir"]).resolve()
    state.set(stage="scanning", percent=9, message="正在扫描本地文件 SHA-256…", downloaded=0, total=0, speed=0)
    needed: list[str] = []
    for i, (relative, metadata) in enumerate(files.items(), 1):
        target = _resolve_managed(app_dir, relative)
        same = False
        if target.is_file() and not target.is_symlink():
            try:
                same = target.stat().st_size == int(metadata["size"]) and _sha256(target).lower() == str(metadata["sha256"])
            except OSError:
                same = False
        if not same:
            needed.append(relative)
        if i % 20 == 0 or i == len(files):
            state.set(percent=9 + int(6 * i / max(1, len(files))), current_file=relative)
    total_download = sum(int(files[path]["packed_size"]) for path in needed)
    patch_root = work / "patch"
    patch_root.mkdir(parents=True, exist_ok=True)
    done = 0; started = time.monotonic()
    for relative in needed:
        metadata = files[relative]
        packed = _download_range(str(pack_asset.get("url", "")), int(metadata["offset"]), int(metadata["packed_size"]))
        if hashlib.sha256(packed).hexdigest().lower() != str(metadata["packed_sha256"]):
            raise WebUpdaterError(f"增量片段校验失败：{relative}")
        raw = packed if metadata["compression"] == "store" else zlib.decompress(packed)
        if len(raw) != int(metadata["size"]) or hashlib.sha256(raw).hexdigest().lower() != str(metadata["sha256"]):
            raise WebUpdaterError(f"增量文件校验失败：{relative}")
        target = _resolve_managed(patch_root, relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        done += len(packed)
        elapsed = max(0.001, time.monotonic() - started)
        ratio = done / total_download if total_download else 1.0
        state.set(stage="downloading", percent=15 + int(45 * min(1.0, ratio)), current_file=relative,
                  downloaded=done, total=total_download, speed=int(done / elapsed),
                  message=f"增量下载：{len(needed)} 个文件，仅传输本地发生变化的内容")
    state.page_seen.wait(timeout=8)
    _stop_app(request, state)
    snapshot = _snapshot(app_dir, str(request.get("target_version", "")), state)
    remove_existing = [path for path in removed if (_resolve_managed(app_dir, path).exists() or _resolve_managed(app_dir, path).is_symlink())]
    touched = needed + remove_existing
    rollback_root = work / "rollback-files"
    existed: set[str] = set()
    for relative in touched:
        target = _resolve_managed(app_dir, relative)
        if target.is_symlink() or target.is_dir():
            raise WebUpdaterError(f"增量目标类型不安全：{relative}")
        if target.is_file():
            backup = _resolve_managed(rollback_root, relative)
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(target, backup)
            existed.add(relative)
    try:
        total_ops = max(1, len(touched))
        for i, relative in enumerate(needed, 1):
            src = _resolve_managed(patch_root, relative); dst = _resolve_managed(app_dir, relative)
            dst.parent.mkdir(parents=True, exist_ok=True)
            temp = dst.with_name(f".{dst.name}.web-update.part")
            shutil.copy2(src, temp); os.replace(temp, dst)
            state.set(stage="installing", percent=78 + int(14 * i / total_ops), current_file=relative, message="正在应用增量文件…")
        for j, relative in enumerate(remove_existing, len(needed) + 1):
            _remove(_resolve_managed(app_dir, relative))
            state.set(stage="installing", percent=78 + int(14 * j / total_ops), current_file=relative, message="正在清理旧版本文件…")
        persisted = app_dir / "backup" / snapshot.name
        _write_result(app_dir, status="installed", version=str(request.get("target_version", "")), backup=persisted)
        process = _launch_main(app_dir, str(request["main_exe"])); _wait_started(process, state)
    except Exception as exc:
        for relative in reversed(touched):
            target = _resolve_managed(app_dir, relative)
            if relative in existed:
                source = _resolve_managed(rollback_root, relative)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
            else:
                _remove(target)
        _write_result(app_dir, status="rolled_back", version=str(request.get("target_version", "")), backup=app_dir / "backup" / snapshot.name, error=str(exc))
        try:
            _launch_main(app_dir, str(request["main_exe"]))
        except Exception:
            pass
        raise


def _restore_update(request: dict[str, Any], state: ProgressState, work: Path) -> None:
    app_dir = Path(request["app_dir"]).resolve()
    backup_root = (app_dir / "backup").resolve()
    backup = Path(str(request.get("backup_path", ""))).resolve()
    try:
        backup.relative_to(backup_root)
    except ValueError as exc:
        raise WebUpdaterError("恢复备份路径不在 backup 目录内") from exc
    if backup == backup_root or not backup.is_dir() or backup.is_symlink():
        raise WebUpdaterError("恢复备份不存在")
    state.set(stage="preparing", percent=10, message="正在准备本地备份恢复，不需要联网下载…")
    staging = app_dir.parent / f".{app_dir.name}.web-restore-staging"
    _remove(staging); staging.mkdir(parents=True)
    _copy_tree_contents(backup, staging)
    state.set(percent=55, message="本地备份已准备完成，等待切换版本…")
    state.page_seen.wait(timeout=8)
    _stop_app(request, state)
    _replace_directory(staging, request, state)


def _worker(server: UpdateHost) -> None:
    request = server.request_data; state = server.state
    launch_dir = Path(str(request.get("launch_dir", "."))).resolve(); work = launch_dir / "work"; work.mkdir(parents=True, exist_ok=True)
    try:
        mode = str(request.get("mode", ""))
        if mode == "full":
            _full_update(request, state, work)
        elif mode == "incremental":
            _incremental_update(request, state, work)
        elif mode == "restore":
            _restore_update(request, state, work)
        else:
            raise WebUpdaterError(f"未知更新模式：{mode}")
        state.set(status="success", stage="complete", percent=100, current_file="", downloaded=state.snapshot().get("total", 0),
                  speed=0, done=True, success=True, message="更新完成，新版 Web 服务已启动。")
        threading.Timer(60.0, server.shutdown).start()
    except Exception as exc:  # noqa: BLE001
        state.set(status="error", stage="failed", done=True, success=False, speed=0, error=str(exc), message=f"更新失败：{exc}")
        threading.Timer(600.0, server.shutdown).start()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BiliPDJ Web Portable 独立更新器")
    parser.add_argument("--request", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        request = json.loads(args.request.read_text(encoding="utf-8-sig"))
        if not isinstance(request, dict) or int(request.get("schema", 0) or 0) != 1:
            raise WebUpdaterError("更新请求格式无效")
        token = str(request.get("token", "") or "")
        port = int(request.get("port", 0) or 0)
        if len(token) < 20 or not (1024 <= port <= 65535):
            raise WebUpdaterError("更新会话 token/port 无效")
        server = UpdateHost(("127.0.0.1", port), request, _bundle_html())
        ready = Path(str(request.get("launch_dir", args.request.parent))) / "ready.json"
        ready.write_text(json.dumps({"status": "ok", "port": port}, ensure_ascii=False), encoding="utf-8")
        threading.Thread(target=_worker, args=(server,), name="bilipdj-web-update-worker", daemon=True).start()
        server.serve_forever(poll_interval=0.25)
        server.server_close()
        return 0
    except Exception as exc:  # noqa: BLE001
        try:
            parent = args.request.parent
            (parent / "ready.json").write_text(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
