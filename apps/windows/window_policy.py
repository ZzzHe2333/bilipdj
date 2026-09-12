from __future__ import annotations

import functools
import threading
from pathlib import Path
from typing import Any

WINDOW_WIDTH = 1180
WINDOW_HEIGHT = 720
WINDOW_GEOMETRY = f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}"
_PATCH_LOCK = threading.RLock()


def _lock_root_size(root: Any, *, force_geometry: bool = True) -> None:
    """Keep the desktop control panel at one stable client size."""

    if root is None:
        return
    try:
        root.minsize(WINDOW_WIDTH, WINDOW_HEIGHT)
        root.maxsize(WINDOW_WIDTH, WINDOW_HEIGHT)
        root.resizable(False, False)
    except Exception:
        return

    if not force_geometry:
        try:
            if int(root.winfo_width()) == WINDOW_WIDTH and int(root.winfo_height()) == WINDOW_HEIGHT:
                return
        except Exception:
            pass
    try:
        root.geometry(WINDOW_GEOMETRY)
    except Exception:
        pass


def _schedule_root_size_lock(panel: Any) -> None:
    root = getattr(panel, "root", None)
    if root is None:
        return

    def enforce() -> None:
        _lock_root_size(root, force_geometry=False)

    try:
        root.after_idle(enforce)
    except Exception:
        enforce()


def patch_control_panel_issue194(panel_class: type[Any]) -> bool:
    """Lock the customer-facing Windows control panel to 1180x720."""

    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    if Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_issue194_fixed_window_installed", False)):
            return True

        original_init = getattr(panel_class, "__init__", None)
        if not callable(original_init):
            return False

        @functools.wraps(original_init)
        def init_fixed_window(self: Any, *args: Any, **kwargs: Any) -> None:
            root = args[0] if args else kwargs.get("root")
            _lock_root_size(root)
            original_init(self, *args, **kwargs)
            _lock_root_size(getattr(self, "root", root))
            _schedule_root_size_lock(self)
            self._issue194_fixed_window_size = (WINDOW_WIDTH, WINDOW_HEIGHT)

        setattr(panel_class, "__init__", init_fixed_window)
        setattr(panel_class, "_issue194_fixed_window_installed", True)
        return True


__all__ = [
    "WINDOW_WIDTH",
    "WINDOW_HEIGHT",
    "WINDOW_GEOMETRY",
    "patch_control_panel_issue194",
]
