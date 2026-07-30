from __future__ import annotations

import datetime as dt
import functools
import re
import threading
from pathlib import Path
from typing import Any

from . import log_manager

_PATCH_LOCK = threading.RLock()
_BACKEND_PANEL_LINE = re.compile(
    r"^\d{2}:\d{2}:\d{2}\s+\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]\s+",
    re.IGNORECASE,
)


def _room_token(panel: Any) -> str:
    platform = str(getattr(getattr(panel, "platform_var", None), "get", lambda: "")()).strip()
    if "抖音" in platform:
        for attribute in ("douyin_live_id_var", "douyin_room_id_var", "douyin_user_unique_id_var"):
            var = getattr(panel, attribute, None)
            value = str(var.get() if var is not None else "").strip()
            if value:
                return log_manager._safe_room_token(value)  # noqa: SLF001
    room_var = getattr(panel, "roomid_var", None)
    value = str(room_var.get() if room_var is not None else "").strip()
    return log_manager._safe_room_token(value)  # noqa: SLF001


def _is_error_message(message: str, warn: bool) -> bool:
    if warn:
        return True
    text = str(message or "")
    return any(marker in text for marker in ("失败", "错误", "异常", "Traceback", "CRITICAL", "[ERROR]"))


def _is_backend_forwarded_line(message: str) -> bool:
    text = str(message or "").strip()
    return bool(
        _BACKEND_PANEL_LINE.match(text)
        or text.startswith("[STDOUT]")
        or text.startswith("[STDERR]")
    )


def _append_file(panel: Any, module: Any, message: str, warn: bool) -> None:
    kind = "error" if _is_error_message(message, warn) else "common"
    path = log_manager.daily_log_path(kind, _room_token(panel), module.APP_DIR)
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {message}\n")


def patch_control_panel_logging(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    if Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False
    with _PATCH_LOCK:
        current = getattr(panel_class, "_append_log", None)
        if not callable(current):
            return False
        if bool(getattr(current, "_bilipdj_gui_log_sink", False)):
            return True

        @functools.wraps(current)
        def append_log_with_file(self: Any, message: str, warn: bool = False) -> None:
            sanitizer = getattr(module, "sanitize_log_message", None)
            safe_message = sanitizer(str(message)) if callable(sanitizer) else str(message)
            if not _is_backend_forwarded_line(safe_message):
                try:
                    _append_file(self, module, safe_message, bool(warn))
                except Exception:
                    pass
            return current(self, safe_message, warn=warn)

        setattr(append_log_with_file, "_bilipdj_gui_log_sink", True)
        setattr(panel_class, "_append_log", append_log_with_file)
        return True


__all__ = ["patch_control_panel_logging"]
