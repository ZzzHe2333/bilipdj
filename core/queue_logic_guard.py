"""Targeted queue-logic compatibility fixes."""
from __future__ import annotations

import functools
import re
import threading
from typing import Any

_PATCH_LOCK = threading.RLock()
_SUPER_QUEUE_PATTERN = re.compile(r"^\s*<([^<>]+)>([^<>]*)\s*$", re.DOTALL)
_WAITING_MARKERS = re.compile(r"⏳待确认|等待确认")


def patch_queue_manager(queue_manager_cls: type[Any]) -> bool:
    """Preserve the internal ``<user>`` super-queue representation.

    The legacy sanitizer treated that representation as an HTML tag and reduced
    it to an empty string. Ordinary HTML still goes through the original
    sanitizer; only one complete, non-nested super-queue marker is preserved.
    """

    if not isinstance(queue_manager_cls, type):
        return False
    with _PATCH_LOCK:
        if bool(getattr(queue_manager_cls, "_bilipdj_queue_logic_guard_installed", False)):
            return True
        original_strip = getattr(queue_manager_cls, "_strip_html", None)
        if not callable(original_strip):
            return False

        @functools.wraps(original_strip)
        def strip_preserving_super_marker(text: Any) -> str:
            raw = str(text or "")
            match = _SUPER_QUEUE_PATTERN.fullmatch(raw)
            if match:
                item_id = _WAITING_MARKERS.sub("", match.group(1)).strip()
                extra = _WAITING_MARKERS.sub("", match.group(2)).strip()
                if item_id:
                    return f"<{item_id}>{extra}" if extra else f"<{item_id}>"
            return str(original_strip(raw) or "").strip()

        setattr(queue_manager_cls, "_strip_html", staticmethod(strip_preserving_super_marker))
        setattr(queue_manager_cls, "_bilipdj_queue_logic_guard_installed", True)
        return True


__all__ = ["patch_queue_manager"]
