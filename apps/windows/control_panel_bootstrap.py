"""Explicit compatibility helpers for Windows control-panel initialization.

Production startup is owned by ``apps.windows.main`` and calls
``install_desktop_runtime(ControlPanelApp)`` directly.  This module intentionally
contains no global class-construction hook and performs no import-time mutation.
"""
from __future__ import annotations

from typing import Any


def install_control_panel_runtime(panel_class: type[Any]) -> bool:
    """Install the production desktop runtime on an already-created panel class."""

    from .desktop_runtime import install_desktop_runtime

    return bool(install_desktop_runtime(panel_class))


def install_control_panel_class_hook(*_args: Any, **_kwargs: Any) -> bool:
    """Deprecated compatibility shim.

    Older callers may still import this name.  It no longer installs a Python
    class-construction hook; callers must pass the concrete class to
    ``install_control_panel_runtime`` instead.
    """

    return False


__all__ = ["install_control_panel_runtime"]
