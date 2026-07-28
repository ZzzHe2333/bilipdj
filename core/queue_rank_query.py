"""Read-only personal queue-rank query interface.

The backend imports :mod:`bilibili_gifts` before defining ``QueueManager``.
This module installs a one-time class-construction hook, attaches the public
``query_queue_rank`` method when ``QueueManager`` is created, and immediately
restores Python's original class builder.

This keeps the feature available when ``core/server.py`` is imported as a
package or executed directly, without sending any automatic live-room reply.
"""
from __future__ import annotations

import builtins
import functools
import sys
import threading
from typing import Any, Callable

QUEUE_RANK_QUERY_COMMANDS = frozenset({"我的排队", "我的名次"})
_PATCH_LOCK = threading.RLock()
_PATCHED_CLASS_IDS: set[int] = set()


def _format_query_log(uname: str, uid: int, command: str, message: str) -> str:
    return f"[智能查询] {uname}({uid}) 发送“{command}”：{message}"


def attach_queue_rank_query(queue_manager_cls: type[Any]) -> bool:
    """Attach the query interface to one QueueManager-compatible class.

    Returns ``True`` when the class is compatible. Calling it repeatedly is
    safe and does not wrap ``_process`` more than once.
    """
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
            """Return and log the user's current queue position.

            ``None`` means the message is not one of the supported query
            phrases. A handled result is read-only and is intentionally not
            broadcast to WebSocket clients or sent back as a danmu message.
            """
            command = str(msg or "").strip()
            if command not in QUEUE_RANK_QUERY_COMMANDS:
                return None

            user_name = str(uname or "").strip()
            numeric_uid = int(uid or 0)
            lock = getattr(self, "_lock")
            with lock:
                index = int(self._find_index(user_name))
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
    for module_name in ("core.server", "server", "__main__"):
        module = sys.modules.get(module_name)
        if module is None:
            continue
        candidate = getattr(module, "QueueManager", None)
        if isinstance(candidate, type) and attach_queue_rank_query(candidate):
            return True
    return False


def install_queue_rank_query_hook() -> None:
    """Install the interface now or patch the next QueueManager definition."""
    with _PATCH_LOCK:
        if _patch_existing_queue_manager():
            return

        current_builder: Callable[..., Any] = builtins.__build_class__
        if bool(getattr(current_builder, "_bilipdj_queue_rank_hook", False)):
            return

        @functools.wraps(current_builder)
        def hooked_builder(func: Callable[..., Any], name: str, *bases: type[Any], **kwargs: Any) -> Any:
            created = current_builder(func, name, *bases, **kwargs)
            if name == "QueueManager" and isinstance(created, type):
                if attach_queue_rank_query(created):
                    if builtins.__build_class__ is hooked_builder:
                        builtins.__build_class__ = current_builder
            return created

        setattr(hooked_builder, "_bilipdj_queue_rank_hook", True)
        builtins.__build_class__ = hooked_builder


__all__ = [
    "QUEUE_RANK_QUERY_COMMANDS",
    "attach_queue_rank_query",
    "install_queue_rank_query_hook",
]
