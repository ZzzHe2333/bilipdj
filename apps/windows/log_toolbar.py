from __future__ import annotations

import functools
import sys
import threading
from pathlib import Path
from typing import Any

_PATCH_LOCK = threading.RLock()
_COMPACT_BUTTON_TEXTS = {"清空", "复制"}
_COMPACT_BUTTON_WIDTH = 5
_COMPACT_BUTTON_PADDING = (5, 5)
_COMPACT_BUTTON_PADX = (2, 1)


def _iter_descendants(widget: Any):
    try:
        stack = list(widget.winfo_children())
    except Exception:
        return
    while stack:
        current = stack.pop()
        yield current
        try:
            stack.extend(current.winfo_children())
        except Exception:
            pass


def compact_log_toolbar(frame: Any) -> tuple[str, ...]:
    """Shrink only the log-page clear/copy buttons so the toolbar fits 1180px."""

    changed: list[str] = []
    for widget in _iter_descendants(frame):
        try:
            if str(widget.winfo_class()) not in {"TButton", "Button"}:
                continue
            text = str(widget.cget("text") or "").strip()
        except Exception:
            continue
        if text not in _COMPACT_BUTTON_TEXTS:
            continue
        try:
            widget.configure(width=_COMPACT_BUTTON_WIDTH, padding=_COMPACT_BUTTON_PADDING)
        except Exception:
            try:
                widget.configure(width=_COMPACT_BUTTON_WIDTH)
            except Exception:
                continue
        try:
            if widget.grid_info():
                widget.grid_configure(padx=_COMPACT_BUTTON_PADX)
        except Exception:
            pass
        changed.append(text)
    return tuple(changed)


# Compatibility alias for older source-mode callers/tests.
_compact_log_toolbar = compact_log_toolbar


def patch_control_panel_issue196(panel_class: type[Any]) -> bool:
    """Legacy compatibility adapter.

    Production desktop startup no longer installs this method wrapper; the log
    page component calls ``compact_log_toolbar`` directly after construction.
    """

    if not isinstance(panel_class, type):
        return False
    module = sys.modules.get(str(getattr(panel_class, "__module__", "") or ""))
    if module is None or Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_issue196_log_toolbar_installed", False)):
            return True
        original = getattr(panel_class, "_build_log_tab", None)
        if not callable(original):
            return False

        @functools.wraps(original)
        def build_log_tab_compact(self: Any, frame: Any, *args: Any, **kwargs: Any) -> Any:
            result = original(self, frame, *args, **kwargs)
            self._issue196_compact_log_buttons = compact_log_toolbar(frame)
            return result

        panel_class._build_log_tab = build_log_tab_compact
        panel_class._issue196_log_toolbar_installed = True
        return True


__all__ = ["compact_log_toolbar", "patch_control_panel_issue196"]
