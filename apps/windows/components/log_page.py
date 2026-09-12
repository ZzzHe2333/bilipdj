from __future__ import annotations

from typing import Any, Callable

from ..command_console_ui import _install_console_row
from ..log_toolbar import compact_log_toolbar
from .base import PageComponent


class LogPageComponent(PageComponent):
    def __init__(self, builder: Callable[..., Any]) -> None:
        super().__init__(key="log", title="日志", builder=builder)

    def build(self, panel: Any, parent: Any, *args: Any, **kwargs: Any) -> Any:
        result = super().build(panel, parent, *args, **kwargs)
        host = getattr(panel, "_component_log_host", parent)
        panel._issue196_compact_log_buttons = compact_log_toolbar(host)
        # The old production runtime installed command_console_ui through a
        # ControlPanel __init__ wrapper. During the component migration that
        # wrapper was intentionally removed, but the console was not reattached,
        # so the "后端指令" row silently disappeared. Keep page ownership explicit:
        # once the log widgets exist, install the command console directly from
        # the LogPageComponent.
        _install_console_row(panel)
        return result


__all__ = ["LogPageComponent"]
