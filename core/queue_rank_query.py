"""Read-only personal queue-rank query interface.

``core.server`` imports :mod:`bilibili_gifts` before defining ``QueueManager``.
For compatibility with that order, this module can install a short-lived class
construction hook while the server module is being imported. The hook is only
installed from a server import frame, restores itself immediately after
``QueueManager`` is created, and has a timeout fallback if the import aborts.
"""
from __future__ import annotations

import builtins
import functools
import inspect
import os
import sys
import threading
from typing import Any, Callable

QUEUE_RANK_QUERY_COMMANDS = frozenset({"我的排队", "我的名次"})
_SERVER_MODULE_NAMES = frozenset({"core.server", "server", "__main__"})
_PATCH_LOCK = threading.RLock()
_PATCHED_CLASS_IDS: set[int] = set()
_HOOK_TIMEOUT_SECONDS = 10.0
_HOOK_ORIGINAL: Callable[..., Any] | None = None
_HOOK_WRAPPER: Callable[..., Any] | None = None
_HOOK_TIMER: threading.Timer | None = None


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value if value is not None else default)
    except (TypeError, ValueError):
        return default


def _format_query_log(uname: str, uid: int, command: str, message: str) -> str:
    return f"[智能查询] {uname}({uid}) 发送“{command}”：{message}"


def attach_queue_rank_query(queue_manager_cls: type[Any]) -> bool:
    """Attach the query interface to one QueueManager-compatible class."""

    with _PATCH_LOCK:
        class_id = id(queue_manager_cls)
        if class_id in _PATCHED_CLASS_IDS or bool(
            getattr(queue_manager_cls, "_queue_rank_query_installed", False)
        ):
            return True

        original_process = getattr(queue_manager_cls, "_process", None)
        find_index = getattr(queue_manager_cls, "_find_index", None)
        if not callable(original_process) or not callable(find_index):
            return False

        def query_queue_rank(
            self: Any,
            uid: int,
            uname: str,
            msg: str,
        ) -> dict[str, Any] | None:
            """Return and log the user's current queue position without mutation."""

            command = str(msg or "").strip()
            if command not in QUEUE_RANK_QUERY_COMMANDS:
                return None

            user_name = str(uname or "").strip()
            numeric_uid = _to_int(uid)
            lock = getattr(self, "_lock")
            with lock:
                index = _to_int(self._find_index(user_name), -1)
                total = len(getattr(self, "_persons", []))

            queued = index >= 0
            rank = index + 1 if queued else None
            if queued:
                message = f"当前排在第 {rank} 位，队列共 {total} 人"
            else:
                message = f"当前不在排队列表中，队列共 {total} 人"

            result: dict[str, Any] = {
                "handled": True,
                "command": command,
                "uid": numeric_uid,
                "uname": user_name,
                "queued": queued,
                "rank": rank,
                "total": total,
                "message": message,
            }
            logger = getattr(self, "_logger", None)
            if logger is not None:
                logger.info(_format_query_log(user_name, numeric_uid, command, message))
            return result

        @functools.wraps(original_process)
        def process_with_rank_query(
            self: Any,
            uid: int,
            uname: str,
            msg: str,
            is_anchor: bool,
            is_admin: bool,
            is_guard: bool,
            guard_level: int,
        ) -> tuple[bool, str | None]:
            if self.query_queue_rank(uid, uname, msg) is not None:
                return False, None
            return original_process(
                self,
                uid,
                uname,
                msg,
                is_anchor,
                is_admin,
                is_guard,
                guard_level,
            )

        setattr(queue_manager_cls, "query_queue_rank", query_queue_rank)
        setattr(queue_manager_cls, "_process", process_with_rank_query)
        setattr(queue_manager_cls, "_queue_rank_query_installed", True)
        setattr(queue_manager_cls, "_queue_rank_query_commands", QUEUE_RANK_QUERY_COMMANDS)
        _PATCHED_CLASS_IDS.add(class_id)
        return True


def _patch_existing_queue_manager() -> bool:
    for module_name in _SERVER_MODULE_NAMES:
        module = sys.modules.get(module_name)
        if module is None:
            continue
        if module_name == "__main__" and os.path.basename(
            str(getattr(module, "__file__", ""))
        ) != "server.py":
            continue
        candidate = getattr(module, "QueueManager", None)
        if isinstance(candidate, type) and attach_queue_rank_query(candidate):
            return True
    return False


def _is_server_frame(frame: Any) -> bool:
    module_name = str(frame.f_globals.get("__name__", ""))
    if module_name in {"core.server", "server"}:
        return True
    if module_name != "__main__":
        return False
    return os.path.basename(str(frame.f_globals.get("__file__", ""))) == "server.py"


def _called_from_server_import() -> bool:
    """Return true only when installation is requested from server import code."""

    frame = inspect.currentframe()
    try:
        frame = frame.f_back if frame is not None else None
        while frame is not None:
            if _is_server_frame(frame):
                return True
            frame = frame.f_back
        return False
    finally:
        del frame


def _restore_build_class_hook() -> None:
    global _HOOK_ORIGINAL, _HOOK_WRAPPER, _HOOK_TIMER
    with _PATCH_LOCK:
        wrapper = _HOOK_WRAPPER
        original = _HOOK_ORIGINAL
        if wrapper is not None and original is not None and builtins.__build_class__ is wrapper:
            builtins.__build_class__ = original
        timer = _HOOK_TIMER
        _HOOK_ORIGINAL = None
        _HOOK_WRAPPER = None
        _HOOK_TIMER = None
        if timer is not None and timer is not threading.current_thread():
            timer.cancel()


def install_queue_rank_query_hook() -> bool:
    """Install the interface now or briefly watch the server class definition.

    Returns ``True`` when an existing class was patched or a short-lived hook was
    installed. Standalone imports of :mod:`bilibili_gifts` return ``False`` and
    leave ``builtins.__build_class__`` untouched.
    """

    global _HOOK_ORIGINAL, _HOOK_WRAPPER, _HOOK_TIMER
    with _PATCH_LOCK:
        if _patch_existing_queue_manager():
            return True
        if not _called_from_server_import():
            return False
        if _HOOK_WRAPPER is not None:
            return True

        current_builder: Callable[..., Any] = builtins.__build_class__

        @functools.wraps(current_builder)
        def hooked_builder(
            func: Callable[..., Any],
            name: str,
            *bases: type[Any],
            **kwargs: Any,
        ) -> Any:
            created = current_builder(func, name, *bases, **kwargs)
            module_name = str(getattr(created, "__module__", "") or "")
            server_class = module_name in {"core.server", "server"}
            if module_name == "__main__":
                server_class = (
                    os.path.basename(str(func.__globals__.get("__file__", "")))
                    == "server.py"
                )
            if (
                name == "QueueManager"
                and server_class
                and isinstance(created, type)
                and attach_queue_rank_query(created)
            ):
                _restore_build_class_hook()
            return created

        _HOOK_ORIGINAL = current_builder
        _HOOK_WRAPPER = hooked_builder
        builtins.__build_class__ = hooked_builder
        timer = threading.Timer(_HOOK_TIMEOUT_SECONDS, _restore_build_class_hook)
        timer.daemon = True
        _HOOK_TIMER = timer
        timer.start()
        return True


__all__ = [
    "QUEUE_RANK_QUERY_COMMANDS",
    "attach_queue_rank_query",
    "install_queue_rank_query_hook",
]
