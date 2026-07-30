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

from .runtime_guards import install_runtime_guards

install_runtime_guards()

__all__ = [
    "SliderCheckbutton",
    "install_runtime_guards",
    "install_slider_switches",
]
