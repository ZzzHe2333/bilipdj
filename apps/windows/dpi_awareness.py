"""Opt in to system DPI awareness before Win32/Tk windows are created.

System-aware mode is deliberately chosen over per-monitor-v2: existing fixed
Tk/ttk geometry and older Tk versions have not yet been validated for
cross-monitor DPI change handling.
"""
from __future__ import annotations

import os


def enable_system_dpi_awareness() -> bool:
    if os.name != "nt" or os.environ.get("BILIPDJ_DISABLE_DPI_AWARENESS") == "1":
        return False
    try:
        import ctypes
        shcore = ctypes.WinDLL("shcore", use_last_error=True)
        shcore.SetProcessDpiAwareness.argtypes = (ctypes.c_int,)
        shcore.SetProcessDpiAwareness.restype = ctypes.c_long
        # S_OK or E_ACCESSDENIED: a manifest has already selected an aware mode.
        result = shcore.SetProcessDpiAwareness(1)  # PROCESS_SYSTEM_DPI_AWARE
        if result in (0, -2147024891):
            return True
    except (OSError, AttributeError, ValueError):
        pass
    try:
        import ctypes
        user32 = ctypes.WinDLL("user32", use_last_error=True)
        return bool(user32.SetProcessDPIAware())
    except (OSError, AttributeError, ValueError):
        return False


__all__ = ["enable_system_dpi_awareness"]
