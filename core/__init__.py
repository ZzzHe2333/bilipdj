"""Shared package bootstrap.

Desktop switch styling is optional so backend and model-interface imports keep
working on headless/minimal Python installations where ``tkinter`` is absent.
"""
from __future__ import annotations

from typing import Any

try:
    from .slider_switches import SliderCheckbutton, install_slider_switches
except ImportError:  # tkinter is optional for backend-only/headless usage
    SliderCheckbutton: Any = None

    def install_slider_switches() -> None:
        return None


__all__ = ["SliderCheckbutton", "install_slider_switches"]
