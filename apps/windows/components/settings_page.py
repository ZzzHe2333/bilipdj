from __future__ import annotations

from typing import Any, Callable

from ..windows_ui import _build_plugin_manager_tab
from .base import PageComponent


class SettingsPageComponent(PageComponent):
    def __init__(self, builder: Callable[..., Any]) -> None:
        super().__init__(key="settings", title="设置", builder=builder)

    def build(self, panel: Any, parent: Any, *args: Any, **kwargs: Any) -> Any:
        result = super().build(panel, parent, *args, **kwargs)
        module = __import__(str(panel.__class__.__module__), fromlist=["*"])
        _build_plugin_manager_tab(panel, module)
        return result


__all__ = ["SettingsPageComponent"]
