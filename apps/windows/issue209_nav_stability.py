from __future__ import annotations

import functools
import threading
from pathlib import Path
from typing import Any

_PATCH_LOCK = threading.RLock()
_NAV_WIDTH_HEADROOM_PX = 12


def _freeze_navigation_width(panel: Any) -> int | None:
    """Freeze the left navigation at its final natural width.

    The navigation rows are managed with ``pack``. If pack propagation remains
    enabled, switching the selected button between normal/bold font weights can
    change the frame's requested width and push the right content column.
    """
    nav = getattr(panel, "_nav_frame", None)
    items = list(getattr(panel, "_nav_items", []) or [])
    if nav is None or not items:
        return None

    try:
        # Measure once after all navigation labels/theme patches have finished.
        # The root is still hidden during normal startup, so this settling pass
        # is not visible to the user.
        nav.pack_propagate(True)
    except Exception:
        pass
    try:
        panel.root.update_idletasks()
    except Exception:
        try:
            nav.update_idletasks()
        except Exception:
            pass

    requested: list[int] = []
    for row, _button, _indicator in items:
        try:
            requested.append(max(1, int(row.winfo_reqwidth())))
        except Exception:
            continue

    if requested:
        width = max(requested) + _NAV_WIDTH_HEADROOM_PX
    else:
        try:
            width = max(1, int(nav.winfo_width()))
        except Exception:
            return None

    shell = getattr(nav, "master", None)
    try:
        nav.configure(width=width)
        # nav's direct children use pack(), so pack propagation is the switch
        # that actually prevents text/font requested-width changes from
        # resizing the sidebar. grid_propagate(False) alone does not do this.
        nav.pack_propagate(False)
    except Exception:
        return None

    if shell is not None:
        try:
            shell.columnconfigure(0, weight=0, minsize=width)
        except Exception:
            pass

    # Configuring ``width`` and the shell column's ``minsize`` only changes Tk's
    # requested geometry. Without one final idle-layout pass the old actual width
    # can remain visible until the next <Configure> event, which makes the sidebar
    # jump right after the window has already appeared. Settle that geometry while
    # startup is still hidden so the first painted frame already has its final size.
    try:
        panel.root.update_idletasks()
    except Exception:
        try:
            nav.update_idletasks()
        except Exception:
            pass

    panel._issue209_nav_width = width
    return width


def patch_control_panel_issue209(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    if Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_issue209_nav_stability_installed", False)):
            return True

        original_build_ui = getattr(panel_class, "_build_ui", None)
        if not callable(original_build_ui):
            return False

        @functools.wraps(original_build_ui)
        def build_ui_with_stable_nav(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_build_ui(self, *args, **kwargs)
            _freeze_navigation_width(self)
            return result

        setattr(panel_class, "_build_ui", build_ui_with_stable_nav)
        setattr(panel_class, "_issue209_nav_stability_installed", True)
        return True


__all__ = ["patch_control_panel_issue209"]
