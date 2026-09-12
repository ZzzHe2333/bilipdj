from __future__ import annotations

from typing import Any, Callable

from .base import PageComponent


class LogPageComponent(PageComponent):
    def __init__(self, builder: Callable[[Any, Any], Any]) -> None:
        super().__init__(key="log", title="日志", builder=builder)


__all__ = ["LogPageComponent"]
