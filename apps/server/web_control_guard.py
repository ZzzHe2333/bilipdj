from __future__ import annotations

import json
import os
import platform
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http import HTTPStatus
from pathlib import Path
from typing import Any

_PATCH_LOCK = threading.RLock()
_CONTROL_GET_PATHS = {
    "/api/control/meta",
    "/api/control/logs",
    "/api/control/performance",
    "/api/control/update",
}
_CONTROL_SHUTDOWN_PATH = "/api/control/shutdown"
_MAX_CONTROL_BODY_BYTES = 4096


def _read_version(server_module: Any) -> str:
    candidates = [
        Path(getattr(server_module, "APP_DIR", ".")) / "VERSION",
        Path(getattr(server_module, "REPO_DIR", ".")) / "VERSION",
    ]
    bundle = Path(getattr(server_module, "BUNDLE_DIR", ".")) / "VERSION"
    candidates.insert(0, bundle)
    for path in candidates:
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if value:
            return value
    return "unknown"


def _tail_text(path: Path, max_bytes: int = 512 * 1024) -> list[str]:
    try:
        size = path.stat().st_size
        with path.open("rb") as handle:
            if size > max_bytes:
                handle.seek(-max_bytes, os.SEEK_END)
                handle.readline()
            raw = handle.read()
    except OSError:
        return []
    return raw.decode("utf-8", errors="replace").splitlines()


def _log_payload(server_module: Any, query: dict[str, list[str]]) -> dict[str, Any]:
    kind = str((query.get("kind") or ["all"])[0] or "all").strip().lower()
    try:
        limit = int((query.get("limit") or ["400"])[0])
    except (TypeError, ValueError):
        limit = 400
    limit = max(20, min(2000, limit))
    log_dir = Path(getattr(server_module, "LOG_DIR", Path("log")))
    prefixes = {
        "common": ("common_",),
        "error": ("error_",),
        "update": ("update_",),
        "all": ("common_", "error_", "update_"),
    }
    selected = prefixes.get(kind, prefixes["all"])
    try:
        files = [
            path for path in log_dir.glob("*.log")
            if path.is_file() and path.name.startswith(selected)
        ]
    except OSError:
        files = []
    files.sort(key=lambda path: path.stat().st_mtime if path.exists() else 0.0, reverse=True)
    files = files[:6]
    merged: list[tuple[float, str]] = []
    sanitizer = getattr(server_module, "sanitize_log_message", None)
    for path in files:
        try:
            mtime = float(path.stat().st_mtime)
        except OSError:
            mtime = 0.0
        for line in _tail_text(path):
            text = sanitizer(line) if callable(sanitizer) else line
            merged.append((mtime, str(text)))
    lines = [text for _mtime, text in merged[-limit:]]
    return {
        "status": "ok",
        "kind": kind,
        "files": [path.name for path in files],
        "lines": lines,
        "count": len(lines),
    }


def _performance_payload(server_module: Any, active_server: Any) -> dict[str, Any]:
    try:
        import psutil
    except Exception:  # noqa: BLE001
        psutil = None

    queue_manager = getattr(active_server, "queue_manager", None)
    websocket_hub = getattr(active_server, "ws_hub", None)
    relay = getattr(active_server, "danmu_relay", None)
    relay_status = relay.get_runtime_status() if relay is not None and hasattr(relay, "get_runtime_status") else {}
    queue = queue_manager.get_queue() if queue_manager is not None and hasattr(queue_manager, "get_queue") else []

    payload: dict[str, Any] = {
        "status": "ok",
        "timestamp": time.time(),
        "platform": str(getattr(active_server, "runtime_config", {}).get("platform", "bilibili")),
        "queue_size": len(queue),
        "websocket_clients": int(getattr(websocket_hub, "client_count", 0) or 0),
        "relay": relay_status if isinstance(relay_status, dict) else {},
        "system": {
            "os": platform.platform(),
            "python": platform.python_version(),
        },
    }
    if psutil is None:
        payload["system"]["psutil_available"] = False
        return payload

    try:
        process = psutil.Process()
        vm = psutil.virtual_memory()
        disk_root = Path(getattr(server_module, "APP_DIR", ".")).anchor or str(Path(getattr(server_module, "APP_DIR", ".")).resolve())
        disk = psutil.disk_usage(disk_root)
        memory_info = process.memory_info()
        payload["system"].update(
            {
                "psutil_available": True,
                "cpu_percent": float(psutil.cpu_percent(interval=None)),
                "memory_percent": float(vm.percent),
                "memory_used": int(vm.used),
                "memory_total": int(vm.total),
                "disk_percent": float(disk.percent),
                "disk_used": int(disk.used),
                "disk_total": int(disk.total),
            }
        )
        payload["process"] = {
            "pid": process.pid,
            "cpu_percent": float(process.cpu_percent(interval=None)),
            "memory_rss": int(memory_info.rss),
            "threads": int(process.num_threads()),
            "started_at": float(process.create_time()),
        }
    except Exception as exc:  # noqa: BLE001
        payload["system"]["psutil_error"] = str(exc)
    return payload


def _update_payload(server_module: Any) -> dict[str, Any]:
    current = _read_version(server_module)
    request = urllib.request.Request(
        "https://api.github.com/repos/ZzzHe2333/bilipdj/releases/latest",
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "bilipdj-web-control",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        return {"status": "error", "message": f"GitHub HTTP {exc.code}", "current_version": current}
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        return {"status": "error", "message": f"检查更新失败：{exc}", "current_version": current}
    return {
        "status": "ok",
        "current_version": current,
        "latest_version": str(payload.get("tag_name", "") or "").lstrip("v"),
        "tag_name": str(payload.get("tag_name", "") or ""),
        "name": str(payload.get("name", "") or ""),
        "published_at": str(payload.get("published_at", "") or ""),
        "html_url": str(payload.get("html_url", "") or ""),
        "body": str(payload.get("body", "") or "")[:12000],
    }


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


def _read_control_json(handler: Any) -> dict[str, Any]:
    content_type = str(handler.headers.get("Content-Type", "") or "").split(";", 1)[0].strip().lower()
    if content_type != "application/json":
        raise ValueError("Content-Type 必须是 application/json")
    try:
        length = int(handler.headers.get("Content-Length", "0") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("Content-Length 无效") from exc
    if length <= 0 or length > _MAX_CONTROL_BODY_BYTES:
        raise ValueError("请求大小无效")
    try:
        payload = json.loads(handler.rfile.read(length).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("请求不是有效 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("请求必须是 JSON 对象")
    return payload


def _schedule_server_shutdown(active_server: Any) -> None:
    logger = getattr(active_server, "logger", None)
    if logger is not None:
        logger.info("[Web控制台] 收到本地关闭服务器请求")

    def stop() -> None:
        time.sleep(0.15)
        try:
            active_server.shutdown()
        except Exception as exc:  # noqa: BLE001
            if logger is not None:
                logger.error("[Web控制台] 关闭服务器失败: %s", exc)

    threading.Thread(target=stop, name="bilipdj-web-shutdown", daemon=True).start()


def install_web_control_guard(server_module: Any) -> bool:
    """Add local-only Web control panel endpoints without changing queue logic."""
    if server_module is None or not hasattr(server_module, "ApiHandler"):
        return False
    with _PATCH_LOCK:
        if bool(getattr(server_module, "_web_control_guard_installed", False)):
            return True
        handler_class = server_module.ApiHandler
        original_get = handler_class.do_GET
        original_post = handler_class.do_POST

        def do_GET(self: Any) -> None:  # noqa: N802
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            if path in {"/", "/control", "/control/", "/control.html"}:
                if not self._require_loopback():
                    return
                file_path = Path(server_module.UI_DIR) / "control.html"
                if not file_path.is_file():
                    self._write_json({"status": "error", "message": "Web control panel is missing"}, status=404)
                    return
                self._serve_static_file(file_path)
                return
            if path not in _CONTROL_GET_PATHS:
                return original_get(self)
            if not self._require_loopback():
                return
            query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            if path == "/api/control/meta":
                runtime = getattr(self.server, "runtime_config", {})
                server_cfg = runtime.get("server", {}) if isinstance(runtime, dict) else {}
                self._write_json(
                    {
                        "status": "ok",
                        "version": _read_version(server_module),
                        "service": "bilipdj",
                        "platform": str(runtime.get("platform", "bilibili")) if isinstance(runtime, dict) else "bilibili",
                        "port": int(server_cfg.get("port", getattr(self.server, "server_port", 9816)) or 9816),
                        "local_only": True,
                    }
                )
                return
            if path == "/api/control/logs":
                self._write_json(_log_payload(server_module, query))
                return
            if path == "/api/control/performance":
                self._write_json(_performance_payload(server_module, self.server))
                return
            self._write_json(_update_payload(server_module))

        def do_POST(self: Any) -> None:  # noqa: N802
            path = urllib.parse.urlparse(self.path).path
            if path != _CONTROL_SHUTDOWN_PATH:
                return original_post(self)
            if not self._require_loopback():
                return
            if not _same_origin_or_no_origin(self):
                self._write_json(
                    {"status": "error", "message": "Origin 不允许访问本地关闭接口"},
                    status=HTTPStatus.FORBIDDEN,
                )
                return
            try:
                payload = _read_control_json(self)
            except ValueError as exc:
                self._write_json({"status": "error", "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return
            if str(payload.get("confirm", "") or "").strip().lower() != "shutdown":
                self._write_json(
                    {"status": "error", "message": "缺少关闭确认"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            self._write_json({"status": "ok", "message": "后端服务器正在关闭"})
            _schedule_server_shutdown(self.server)

        handler_class.do_GET = do_GET
        handler_class.do_POST = do_POST
        server_module._web_control_guard_installed = True
        return True


__all__ = ["install_web_control_guard"]
