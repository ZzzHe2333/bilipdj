"""Install control-panel patches during the packaged entry script."""
from __future__ import annotations

import builtins
import functools
import os
import sys
import threading
from pathlib import Path
from typing import Any, Callable

_LOCK = threading.RLock()
_ORIGINAL_BUILD_CLASS: Callable[..., Any] | None = None
_BUILD_CLASS_WRAPPER: Callable[..., Any] | None = None
_RESTORE_TIMER: threading.Timer | None = None


def _control_panel_entry_expected() -> bool:
    try:
        argv0 = Path(str(sys.argv[0] or "")).name.casefold()
    except Exception:
        argv0 = ""
    if argv0 in {"control_panel.py", "main.exe", "main"}:
        return True

    frame = sys._getframe()
    while frame is not None:
        if os.path.basename(str(frame.f_code.co_filename)).casefold() == "control_panel.py":
            return True
        frame = frame.f_back
    return False


def _restore_hook() -> None:
    global _ORIGINAL_BUILD_CLASS, _BUILD_CLASS_WRAPPER, _RESTORE_TIMER
    with _LOCK:
        original = _ORIGINAL_BUILD_CLASS
        wrapper = _BUILD_CLASS_WRAPPER
        if original is not None and wrapper is not None and builtins.__build_class__ is wrapper:
            builtins.__build_class__ = original
        timer = _RESTORE_TIMER
        _ORIGINAL_BUILD_CLASS = None
        _BUILD_CLASS_WRAPPER = None
        _RESTORE_TIMER = None
        if timer is not None and timer is not threading.current_thread():
            timer.cancel()


def install_control_panel_class_hook(*, timeout: float = 120.0) -> bool:
    """Patch ``ControlPanelApp`` immediately after Python creates the class."""

    global _ORIGINAL_BUILD_CLASS, _BUILD_CLASS_WRAPPER, _RESTORE_TIMER
    if not _control_panel_entry_expected():
        return False

    with _LOCK:
        if _BUILD_CLASS_WRAPPER is not None:
            return True
        current = builtins.__build_class__

        @functools.wraps(current)
        def guarded_build_class(func: Any, name: str, *bases: Any, **kwargs: Any) -> Any:
            cls = current(func, name, *bases, **kwargs)
            if name != "ControlPanelApp":
                return cls
            module = sys.modules.get(str(getattr(cls, "__module__", "") or ""))
            if os.path.basename(str(getattr(module, "__file__", ""))) != "control_panel.py":
                return cls
            try:
                from .control_panel_guard import patch_control_panel_class
                from .control_panel_features import patch_control_panel_features
                from .control_panel_ui_finish import patch_control_panel_ui_finish
                from .gui_log_sink import patch_control_panel_logging

                patch_control_panel_class(cls)
                patch_control_panel_features(cls)
                patch_control_panel_ui_finish(cls)
                patch_control_panel_logging(cls)
            finally:
                _restore_hook()
            return cls

        _ORIGINAL_BUILD_CLASS = current
        _BUILD_CLASS_WRAPPER = guarded_build_class
        builtins.__build_class__ = guarded_build_class
        timer = threading.Timer(max(1.0, float(timeout)), _restore_hook)
        timer.daemon = True
        _RESTORE_TIMER = timer
        timer.start()
        return True


__all__ = ["install_control_panel_class_hook"]
