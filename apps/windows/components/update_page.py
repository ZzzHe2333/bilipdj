from __future__ import annotations

from typing import Any, Callable

from .base import PageComponent


class UpdatePageComponent(PageComponent):
    def __init__(self, builder: Callable[[Any, Any], Any]) -> None:
        super().__init__(key="update", title="更新软件", builder=builder)


__all__ = ["UpdatePageComponent"]
