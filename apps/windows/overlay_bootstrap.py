from __future__ import annotations

import builtins
import functools
import os
import sys
import threading
from typing import Any, Callable

_LOCK = threading.RLock()
_ORIGINAL_BUILD_CLASS: Callable[..., Any] | None = None
_WRAPPER: Callable[..., Any] | None = None
_TIMER: threading.Timer | None = None


def _restore() -> None:
    global _ORIGINAL_BUILD_CLASS, _WRAPPER, _TIMER
    with _LOCK:
        if _ORIGINAL_BUILD_CLASS is not None and builtins.__build_class__ is _WRAPPER:
            builtins.__build_class__ = _ORIGINAL_BUILD_CLASS
        timer = _TIMER
        _ORIGINAL_BUILD_CLASS = None
        _WRAPPER = None
        _TIMER = None
        if timer is not None and timer is not threading.current_thread():
            timer.cancel()


def install_overlay_class_hook(*, timeout: float = 120.0) -> bool:
    global _ORIGINAL_BUILD_CLASS, _WRAPPER, _TIMER
    with _LOCK:
        if _WRAPPER is not None:
            return True
        current = builtins.__build_class__

        @functools.wraps(current)
        def guarded_build_class(func: Any, name: str, *bases: Any, **kwargs: Any) -> Any:
            cls = current(func, name, *bases, **kwargs)
            if name != "OverlayHostApp":
                return cls
            module = sys.modules.get(str(getattr(cls, "__module__", "") or ""))
            if os.path.basename(str(getattr(module, "__file__", ""))) != "overlay_host.py":
                return cls
            try:
                from .overlay_performance_guard import install_overlay_performance_guard
                from . import overlay_refresh_guard

                install_overlay_performance_guard(overlay_refresh_guard)
                overlay_refresh_guard.patch_overlay_module(module)
            finally:
                _restore()
            return cls

        _ORIGINAL_BUILD_CLASS = current
        _WRAPPER = guarded_build_class
        builtins.__build_class__ = guarded_build_class
        timer = threading.Timer(max(1.0, float(timeout)), _restore)
        timer.daemon = True
        _TIMER = timer
        timer.start()
        return True


__all__ = ["install_overlay_class_hook"]
