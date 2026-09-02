"""Shared package bootstrap.

Desktop switch styling is optional so backend and model-interface imports keep
working on headless/minimal Python installations where ``tkinter`` is absent.
The remaining runtime guards use only the Python standard library.
"""
from __future__ import annotations

from typing import Any

try:
    from .slider_switches import SliderCheckbutton, install_slider_switches
except ImportError:  # tkinter is optional for backend-only/headless usage
    SliderCheckbutton: Any = None

    def install_slider_switches() -> None:
        return None

from .control_panel_bootstrap import install_control_panel_class_hook
from .multi_platform_danmu import install_control_panel_feature_patch
from .overlay_performance_guard import install_overlay_performance_guard
from .runtime_guards import install_runtime_guards
from .update_network import install_update_client_network_guard
from .web_queue_layout import install_web_queue_layout_guard

# Extend the existing control-panel feature pack before its class hook fires.
# This avoids adding a competing __build_class__ hook.
try:
    install_control_panel_feature_patch()
except Exception:
    pass

# The packaged entry point executes core/control_panel.py as __main__. Install
# the narrowly scoped class hook before that script reaches ControlPanelApp.
install_control_panel_class_hook()
install_web_queue_layout_guard()
try:
    install_overlay_performance_guard()
except Exception:
    # Real overlay startup will still use the thread-safe refresh guard when the
    # optional performance wrapper cannot be initialized in a headless runtime.
    pass
install_runtime_guards()
try:
    install_update_client_network_guard()
except Exception:
    # Update discovery remains available through the SDK's original request
    # path if the optional network preference layer cannot initialize.
    pass

__all__ = [
    "SliderCheckbutton",
    "install_control_panel_class_hook",
    "install_control_panel_feature_patch",
    "install_overlay_performance_guard",
    "install_runtime_guards",
    "install_slider_switches",
    "install_update_client_network_guard",
    "install_web_queue_layout_guard",
]
