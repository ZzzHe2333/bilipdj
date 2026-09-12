from __future__ import annotations

from typing import Any, Callable

from ..log_toolbar import compact_log_toolbar
from .base import PageComponent


class LogPageComponent(PageComponent):
    def __init__(self, builder: Callable[..., Any]) -> None:
        super().__init__(key="log", title="日志", builder=builder)

    def build(self, panel: Any, parent: Any, *args: Any, **kwargs: Any) -> Any:
        result = super().build(panel, parent, *args, **kwargs)
        host = getattr(panel, "_component_log_host", parent)
        panel._issue196_compact_log_buttons = compact_log_toolbar(host)
        return result


__all__ = ["LogPageComponent"]
