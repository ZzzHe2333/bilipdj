from __future__ import annotations

import functools
import threading
from typing import Any

_PATCH_LOCK = threading.RLock()


def _nonzero_spacing(style: Any) -> bool:
    if not isinstance(style, dict):
        return False
    for key in ("queue_letter_spacing", "queue_word_spacing"):
        try:
            if float(str(style.get(key, 0)).strip().removesuffix("px")) != 0:
                return True
        except (TypeError, ValueError):
            continue
    return False


def _patch_overlay_class(module: Any) -> bool:
    app_class = getattr(module, "OverlayHostApp", None)
    if not isinstance(app_class, type):
        return False
    current = getattr(app_class, "_redraw", None)
    if not callable(current):
        return False
    if bool(getattr(current, "_bilipdj_spacing_performance_guard", False)):
        return True

    @functools.wraps(current)
    def redraw_with_bounded_cost(self: Any) -> Any:
        style = getattr(self, "style", None)
        if not _nonzero_spacing(style) or not bool(
            str(style.get("text_stroke_enabled", True)).strip().lower()
            not in {"0", "false", "off", "no"}
        ):
            return current(self)
        original_style = style
        temporary = dict(style)
        temporary["text_stroke_enabled"] = False
        self.style = temporary
        try:
            return current(self)
        finally:
            self.style = original_style

    setattr(redraw_with_bounded_cost, "_bilipdj_spacing_performance_guard", True)
    setattr(app_class, "_redraw", redraw_with_bounded_cost)
    return True


def install_overlay_performance_guard(refresh_module: Any | None = None) -> bool:
    if refresh_module is None:
        from . import overlay_refresh_guard as refresh_module
    with _PATCH_LOCK:
        current = getattr(refresh_module, "patch_overlay_module", None)
        if not callable(current):
            return False
        if bool(getattr(current, "_bilipdj_performance_wrapper", False)):
            return True

        @functools.wraps(current)
        def patch_with_performance_guard(module: Any) -> bool:
            result = bool(current(module))
            if result:
                _patch_overlay_class(module)
            return result

        setattr(patch_with_performance_guard, "_bilipdj_performance_wrapper", True)
        setattr(refresh_module, "patch_overlay_module", patch_with_performance_guard)
        return True


__all__ = ["install_overlay_performance_guard"]
