from __future__ import annotations

import functools
import threading
from typing import Any

from .version import APP_VERSION

_PATCH_LOCK = threading.RLock()


def _find_canvas(widget: Any, tk_module: Any) -> Any | None:
    try:
        if isinstance(widget, tk_module.Canvas):
            return widget
        children = widget.winfo_children()
    except Exception:
        return None
    for child in children:
        found = _find_canvas(child, tk_module)
        if found is not None:
            return found
    return None


def patch_control_panel_ui_finish(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    with _PATCH_LOCK:
        current = getattr(panel_class, "_build_ui", None)
        if not callable(current):
            return False
        if bool(getattr(current, "_bilipdj_ui_finish", False)):
            return True

        @functools.wraps(current)
        def build_ui_finalized(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = current(self, *args, **kwargs)
            try:
                self.root.title(f"{module.APP_NAME} 控制台 v{APP_VERSION}")
            except Exception:
                pass
            if bool(getattr(self, "_bilipdj_update_page_inserted", False)) and len(self._nav_items) >= 2:
                update_index = len(self._nav_items) - 2
                about_index = len(self._nav_items) - 1
                try:
                    self._nav_items[update_index][1].configure(
                        text=self._left_nav_label(update_index + 1, "更新软件")
                    )
                    self._nav_items[about_index][1].configure(
                        text=self._left_nav_label(about_index + 1, "关于")
                    )
                except Exception:
                    pass
                try:
                    update_page = self._content_pages[update_index]
                    canvas = _find_canvas(update_page, module.tk)
                    if canvas is not None and canvas not in self._settings_canvases:
                        self._settings_canvases.append(canvas)
                except Exception:
                    pass
            self._apply_theme(self._dark_mode)
            self._show_page(self._active_page)
            return result

        setattr(build_ui_finalized, "_bilipdj_ui_finish", True)
        setattr(panel_class, "_build_ui", build_ui_finalized)
        return True


__all__ = ["patch_control_panel_ui_finish"]
