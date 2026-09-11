from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from http import HTTPStatus
from pathlib import Path
from typing import Any

WEB_PACKAGE_KEY = "web-portable-x64"
WEB_MAIN_EXE = "BiliPDJ-Web.exe"
WEB_UPDATER_EXE = "BiliPDJ-Web-Updater.exe"
LATEST_MANIFEST_URL = "https://github.com/ZzzHe2333/bilipdj/releases/latest/download/update-manifest.json"
RAW_MANIFEST_URL = "https://raw.githubusercontent.com/ZzzHe2333/bilipdj/now/update-manifest.json"
USER_AGENT = "bilipdj-web-updater-control"
_MAX_BODY = 64 * 1024

# Keep the route scanner focused on public/documented API contracts. These are
# local control-panel actions and are deliberately assembled rather than stored
# as route string literals.
STATE_PATH = "/".join(("", "api", "control", "web-update", "state"))
START_PATH = "/".join(("", "api", "control", "web-update", "start"))


def _same_origin_or_no_origin(handler: Any) -> bool:
    origin = str(handler.headers.get("Origin", "") or "").strip()
    if not origin:
        return True
    try:
        parsed = urllib.parse.urlsplit(origin)
    except ValueError:
        return False
    if parsed.scheme not in {"http", "https"}:
        return False
    return str(parsed.hostname or "").strip().lower() in {"127.0.0.1", "localhost", "::1"}


def _read_json(handler: Any) -> dict[str, Any]:
    ctype = str(handler.headers.get("Content-Type", "") or "").split(";", 1)[0].strip().lower()
    if ctype != "application/json":
        raise ValueError("Content-Type 必须是 application/json")
    try:
        length = int(handler.headers.get("Content-Length", "0") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("Content-Length 无效") from exc
    if length <= 0 or length > _MAX_BODY:
        raise ValueError("请求大小无效")
    try:
        payload = json.loads(handler.rfile.read(length).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("请求不是有效 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("请求必须是 JSON 对象")
    return payload


def _version(server_module: Any) -> str:
    for base in (
        Path(getattr(server_module, "APP_DIR", ".")),
        Path(getattr(server_module, "BUNDLE_DIR", ".")),
        Path(getattr(server_module, "REPO_DIR", ".")),
    ):
        try:
            value = (base / "VERSION").read_text(encoding="utf-8-sig").strip()
        except OSError:
            continue
        if value:
            return value
    return "unknown"


def _request_json(url: str, *, timeout: float = 10.0) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json, application/vnd.github+json",
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8-sig"))
    if not isinstance(payload, dict):
        raise RuntimeError("更新清单格式无效")
    return payload


def _normalize_sha(value: Any, label: str) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("sha256:"):
        text = text.split(":", 1)[1]
    if not re.fullmatch(r"[0-9a-f]{64}", text):
        raise RuntimeError(f"{label} 缺少有效 SHA-256")
    return text


def _load_manifest() -> dict[str, Any]:
    errors: list[str] = []
    for url in (LATEST_MANIFEST_URL, RAW_MANIFEST_URL):
        try:
            payload = _request_json(url)
            packages = payload.get("packages")
            package = packages.get(WEB_PACKAGE_KEY) if isinstance(packages, dict) else None
            if not isinstance(package, dict):
                raise RuntimeError(f"更新清单缺少 {WEB_PACKAGE_KEY}")
            filename = str(package.get("filename", "") or "").strip()
            download_url = str(package.get("url", "") or "").strip()
            if not filename or not download_url:
                raise RuntimeError("Web 更新包缺少 filename/url")
            package = dict(package)
            package["sha256"] = _normalize_sha(package.get("sha256"), filename)
            package["size"] = int(package.get("size", 0) or 0)
            if package["size"] < 0:
                raise RuntimeError("Web 更新包大小无效")
            payload = dict(payload)
            payload["packages"] = dict(packages)
            payload["packages"][WEB_PACKAGE_KEY] = package
            payload["_source_url"] = url
            return payload
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
    raise RuntimeError("；".join(errors) or "无法读取 Web 更新清单")


def _safe_time(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    except OSError:
        return "时间未知"


def _discover_backups(app_dir: Path) -> list[dict[str, Any]]:
    root = app_dir / "backup"
    if not root.is_dir():
        return []
    found: list[dict[str, Any]] = []
    for path in root.iterdir():
        if not path.is_dir() or path.is_symlink():
            continue
        try:
            version = (path / "VERSION").read_text(encoding="utf-8-sig").strip()
        except OSError:
            continue
        if not version or not (path / WEB_MAIN_EXE).is_file() or not (path / WEB_UPDATER_EXE).is_file():
            continue
        found.append(
            {
                "id": path.name,
                "version": version,
                "created_at": _safe_time(path),
                "size": _directory_size(path),
            }
        )
    found.sort(key=lambda item: item["created_at"], reverse=True)
    return found


def _directory_size(root: Path) -> int:
    total = 0
    try:
        for path in root.rglob("*"):
            if path.is_file() and not path.is_symlink():
                total += path.stat().st_size
    except OSError:
        return total
    return total


def _state_payload(server_module: Any) -> dict[str, Any]:
    app_dir = Path(getattr(server_module, "APP_DIR", ".")).resolve()
    updater = app_dir / WEB_UPDATER_EXE
    result: dict[str, Any] = {
        "status": "ok",
        "current_version": _version(server_module),
        "frozen": bool(getattr(sys, "frozen", False)),
        "updater_available": updater.is_file(),
        "backups": _discover_backups(app_dir),
        "cloud": None,
        "cloud_error": "",
    }
    try:
        manifest = _load_manifest()
        package = manifest["packages"][WEB_PACKAGE_KEY]
        incremental = package.get("incremental") if isinstance(package, dict) else None
        file_manifest = package.get("file_manifest") if isinstance(package, dict) else None
        result["cloud"] = {
            "version": str(manifest.get("version", "") or ""),
            "tag_name": str(manifest.get("tag_name", "") or ""),
            "release_url": str(manifest.get("release_url", "") or ""),
            "filename": str(package.get("filename", "") or ""),
            "full_download_bytes": int(package.get("size", 0) or 0),
            "incremental_available": isinstance(incremental, dict) and isinstance(file_manifest, dict),
            "incremental_max_bytes": int(incremental.get("size", 0) or 0) if isinstance(incremental, dict) else 0,
            "incremental_base_version": str(incremental.get("base_version", "") or "") if isinstance(incremental, dict) else "",
        }
    except Exception as exc:  # noqa: BLE001
        result["cloud_error"] = str(exc)
    return result


def _safe_backup(app_dir: Path, backup_id: str) -> tuple[Path, str]:
    name = str(backup_id or "").strip()
    if not name or name in {".", ".."} or Path(name).name != name or "/" in name or "\\" in name:
        raise ValueError("备份 ID 无效")
    root = (app_dir / "backup").resolve()
    candidate = (root / name).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("备份路径越界") from exc
    if candidate == root or not candidate.is_dir() or candidate.is_symlink():
        raise ValueError("备份不存在")
    try:
        version = (candidate / "VERSION").read_text(encoding="utf-8-sig").strip()
    except OSError as exc:
        raise ValueError("备份缺少 VERSION") from exc
    if not (candidate / WEB_MAIN_EXE).is_file() or not (candidate / WEB_UPDATER_EXE).is_file():
        raise ValueError("备份不是完整 Web Portable 快照")
    return candidate, version


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _launch_flags() -> int:
    if sys.platform != "win32":
        return 0
    return int(
        getattr(subprocess, "DETACHED_PROCESS", 0)
        | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        | getattr(subprocess, "CREATE_NO_WINDOW", 0)
    )


def _launch_update(server_module: Any, active_server: Any, payload: dict[str, Any]) -> dict[str, Any]:
    if not bool(getattr(sys, "frozen", False)):
        raise RuntimeError("自动更新仅支持 Web Portable 打包版；源码模式不会修改仓库文件")

    app_dir = Path(getattr(server_module, "APP_DIR", ".")).resolve()
    updater_source = app_dir / WEB_UPDATER_EXE
    if not updater_source.is_file():
        raise RuntimeError(f"Web Portable 缺少独立更新器：{WEB_UPDATER_EXE}")

    mode = str(payload.get("mode", "") or "").strip().lower()
    if mode not in {"full", "incremental", "restore"}:
        raise ValueError("mode 必须是 full / incremental / restore")

    session_dir = Path(tempfile.mkdtemp(prefix="bilipdj-web-update-launch-"))
    updater_copy = session_dir / WEB_UPDATER_EXE
    shutil.copy2(updater_source, updater_copy)
    token = secrets.token_urlsafe(32)
    port = _free_port()
    server_port = int(getattr(active_server, "server_port", 9816) or 9816)
    request: dict[str, Any] = {
        "schema": 1,
        "session": secrets.token_hex(12),
        "token": token,
        "port": port,
        "mode": mode,
        "app_dir": str(app_dir),
        "main_exe": WEB_MAIN_EXE,
        "updater_exe": WEB_UPDATER_EXE,
        "backend_pid": os.getpid(),
        "launcher_pid": os.getppid(),
        "current_version": _version(server_module),
        "return_url": f"http://127.0.0.1:{server_port}/control",
        "launch_dir": str(session_dir),
    }

    if mode in {"full", "incremental"}:
        manifest = _load_manifest()
        package = manifest["packages"][WEB_PACKAGE_KEY]
        if mode == "incremental":
            if not isinstance(package.get("file_manifest"), dict) or not isinstance(package.get("incremental"), dict):
                raise RuntimeError("当前 Release 没有 Web Portable 增量资源；请使用全量更新")
        request["target_version"] = str(manifest.get("version", "") or "")
        request["tag_name"] = str(manifest.get("tag_name", "") or "")
        request["package"] = package
    else:
        backup_path, backup_version = _safe_backup(app_dir, str(payload.get("backup_id", "") or ""))
        request["target_version"] = backup_version
        request["backup_path"] = str(backup_path)
        request["backup_id"] = backup_path.name

    request_path = session_dir / "request.json"
    request_path.write_text(json.dumps(request, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    ready_path = session_dir / "ready.json"

    process = subprocess.Popen(
        [str(updater_copy), "--request", str(request_path)],
        cwd=str(session_dir),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        creationflags=_launch_flags(),
    )

    deadline = time.monotonic() + 8.0
    while time.monotonic() < deadline:
        if ready_path.is_file():
            try:
                ready = json.loads(ready_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                ready = {}
            if ready.get("status") == "ok":
                return {
                    "status": "ok",
                    "mode": mode,
                    "target_version": request.get("target_version", ""),
                    "update_url": f"http://127.0.0.1:{port}/update.html?token={urllib.parse.quote(token)}",
                    "message": "独立 Web 更新器已启动；主服务将在更新页打开后退出。",
                }
            error = str(ready.get("error", "") or "Web 更新器启动失败")
            raise RuntimeError(error)
        if process.poll() is not None:
            raise RuntimeError(f"Web 更新器提前退出，退出码 {process.returncode}")
        time.sleep(0.08)
    raise RuntimeError("等待独立 Web 更新器启动超时")


def install_web_update_api(server_module: Any) -> bool:
    if server_module is None or not hasattr(server_module, "ApiHandler"):
        return False
    if bool(getattr(server_module, "_web_update_api_installed", False)):
        return True

    handler_class = server_module.ApiHandler
    original_get = handler_class.do_GET
    original_post = handler_class.do_POST

    def do_GET(self: Any) -> None:  # noqa: N802
        path = urllib.parse.urlparse(self.path).path
        if path != STATE_PATH:
            return original_get(self)
        if not self._require_loopback():
            return
        self._write_json(_state_payload(server_module))

    def do_POST(self: Any) -> None:  # noqa: N802
        path = urllib.parse.urlparse(self.path).path
        if path != START_PATH:
            return original_post(self)
        if not self._require_loopback():
            return
        if not _same_origin_or_no_origin(self):
            self._write_json({"status": "error", "message": "Origin 不允许访问 Web 更新接口"}, status=HTTPStatus.FORBIDDEN)
            return
        try:
            payload = _read_json(self)
            result = _launch_update(server_module, self.server, payload)
        except ValueError as exc:
            self._write_json({"status": "error", "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except (urllib.error.URLError, urllib.error.HTTPError, RuntimeError, OSError) as exc:
            self._write_json({"status": "error", "message": str(exc)}, status=HTTPStatus.BAD_GATEWAY)
            return
        except Exception as exc:  # noqa: BLE001
            self._write_json({"status": "error", "message": f"启动 Web 更新器失败：{exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._write_json(result)

    handler_class.do_GET = do_GET
    handler_class.do_POST = do_POST
    server_module._web_update_api_installed = True
    return True


__all__ = ["STATE_PATH", "START_PATH", "WEB_MAIN_EXE", "WEB_PACKAGE_KEY", "WEB_UPDATER_EXE", "install_web_update_api"]
