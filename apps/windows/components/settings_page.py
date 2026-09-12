from __future__ import annotations

from typing import Any, Callable

from .base import PageComponent


class SettingsPageComponent(PageComponent):
    def __init__(self, builder: Callable[[Any, Any], Any]) -> None:
        super().__init__(key="settings", title="设置", builder=builder)


__all__ = ["SettingsPageComponent"]
