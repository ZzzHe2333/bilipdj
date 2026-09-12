from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..customtk_ui import CompatFrame

PageBuilder = Callable[[Any, Any], Any]


@dataclass(frozen=True)
class PageComponent:
    """Explicit production page component hosted by a CustomTkinter frame.

    The wrapped builder is captured once after legacy compatibility features have
    been installed.  Runtime navigation calls this component instead of stacking
    additional page-level monkeypatches on ControlPanelApp.
    """

    key: str
    title: str
    builder: PageBuilder

    def build(self, panel: Any, parent: Any) -> Any:
        host = CompatFrame(parent, fg_color="transparent", corner_radius=0)
        host.grid(row=0, column=0, sticky="nsew")
        host.grid_columnconfigure(0, weight=1)
        host.grid_rowconfigure(0, weight=1)
        setattr(panel, f"_component_{self.key}_host", host)
        return self.builder(panel, host)

    def panel_method(self) -> PageBuilder:
        component = self

        def build(panel: Any, parent: Any) -> Any:
            return component.build(panel, parent)

        build.__name__ = f"build_{self.key}_component"
        setattr(build, "_bilipdj_page_component", self.key)
        return build

    def module_builder(self) -> PageBuilder:
        component = self

        def build(panel: Any, parent: Any) -> Any:
            return component.build(panel, parent)

        build.__name__ = f"build_{self.key}_component"
        setattr(build, "_bilipdj_page_component", self.key)
        return build


__all__ = ["PageBuilder", "PageComponent"]
