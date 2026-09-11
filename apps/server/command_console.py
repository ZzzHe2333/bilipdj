from __future__ import annotations

import json
import threading
import urllib.parse
from http import HTTPStatus
from typing import Any

_PATCH_LOCK = threading.RLock()
COMMAND_PATH = "/api/control/command"
MAX_COMMAND_BYTES = 4096
MAX_COMMAND_CHARS = 500


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
    host = str(parsed.hostname or "").strip().lower()
    return host in {"127.0.0.1", "localhost", "::1"}


def _read_command(handler: Any) -> str:
    content_type = str(handler.headers.get("Content-Type", "") or "").split(";", 1)[0].strip().lower()
    if content_type != "application/json":
        raise ValueError("Content-Type 必须是 application/json")
    try:
        length = int(handler.headers.get("Content-Length", "0") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("Content-Length 无效") from exc
    if length <= 0 or length > MAX_COMMAND_BYTES:
        raise ValueError("指令请求大小无效")
    try:
        payload = json.loads(handler.rfile.read(length).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("指令请求不是有效 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("指令请求必须是 JSON 对象")
    command = str(payload.get("command", "") or "").strip()
    if not command:
        raise ValueError("请输入后端指令")
    if len(command) > MAX_COMMAND_CHARS or "\x00" in command:
        raise ValueError(f"指令长度不能超过 {MAX_COMMAND_CHARS} 个字符")
    return command


def _execute_command(server_module: Any, active_server: Any, command: str) -> dict[str, Any]:
    queue_manager = getattr(active_server, "queue_manager", None)
    if queue_manager is None or not hasattr(queue_manager, "process_danmu_event"):
        raise RuntimeError("后端队列管理器尚未就绪")

    before = list(queue_manager.get_queue()) if hasattr(queue_manager, "get_queue") else []
    recognized = False
    detector = getattr(queue_manager, "_is_command_like", None)
    if callable(detector):
        try:
            recognized = bool(detector(command))
        except Exception:
            recognized = False

    event_type = getattr(server_module, "DanmuEvent", None)
    if event_type is None:
        raise RuntimeError("后端 DanmuEvent 不可用")
    event = event_type(
        platform="console",
        user_id="local-console",
        username="本地控制台",
        content=command,
        is_anchor=True,
        metadata={"source": "local-control-console"},
    )
    queue_manager.process_danmu_event(event)
    after = list(queue_manager.get_queue()) if hasattr(queue_manager, "get_queue") else []
    logger = getattr(active_server, "logger", None)
    if logger is not None:
        logger.info("[本地指令] command=%r recognized=%s queue_changed=%s", command, recognized, before != after)
    return {
        "status": "ok",
        "accepted": True,
        "recognized": recognized,
        "queue_changed": before != after,
        "queue_size": len(after),
        "message": "指令已送入后端弹幕流。" if recognized else "内容已送入后端弹幕流；未识别为内置队列指令。",
    }


def install_command_console(server_module: Any) -> bool:
    """Install a loopback-only command input that reuses the DanmuEvent pipeline.

    This endpoint intentionally never invokes a shell, PowerShell, subprocess,
    eval, or exec. It only creates a synthetic local DanmuEvent with anchor
    privileges and lets the existing queue/permission command parser handle it.
    """

    if server_module is None or not hasattr(server_module, "ApiHandler"):
        return False
    with _PATCH_LOCK:
        if bool(getattr(server_module, "_command_console_installed", False)):
            return True
        handler_class = server_module.ApiHandler
        original_post = handler_class.do_POST

        def do_POST(self: Any) -> None:  # noqa: N802
            path = urllib.parse.urlparse(self.path).path
            if path != COMMAND_PATH:
                return original_post(self)
            if not self._require_loopback():
                return
            if not _same_origin_or_no_origin(self):
                self._write_json(
                    {"status": "error", "message": "Origin 不允许访问本地指令接口"},
                    status=HTTPStatus.FORBIDDEN,
                )
                return
            try:
                command = _read_command(self)
                result = _execute_command(server_module, self.server, command)
            except ValueError as exc:
                self._write_json({"status": "error", "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return
            except Exception as exc:  # noqa: BLE001
                self._write_json(
                    {"status": "error", "message": f"后端指令执行失败：{exc}"},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
                return
            self._write_json(result)

        handler_class.do_POST = do_POST
        server_module._command_console_installed = True
        return True


__all__ = ["COMMAND_PATH", "install_command_console"]
