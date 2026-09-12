from __future__ import annotations

from typing import Any

from .base import PageComponent
from .log_page import LogPageComponent
from .settings_page import SettingsPageComponent
from .update_page import UpdatePageComponent


def _control_panel_module(panel: Any) -> Any:
    return __import__(str(panel.__class__.__module__), fromlist=["*"])


def _permissions_builder(panel: Any, frame: Any, *_args: Any, **_kwargs: Any) -> None:
    from ..windows_ui import _build_permissions_page

    _build_permissions_page(panel, frame, _control_panel_module(panel))


def _performance_builder(panel: Any, frame: Any, *_args: Any, **_kwargs: Any) -> None:
    from ..windows_ui import _build_performance_page

    _build_performance_page(panel, frame, _control_panel_module(panel))


def install_page_components(panel_class: type[Any]) -> bool:
    """Finalize production pages behind explicit component boundaries.

    Compatibility modules may adjust shared behavior before this function runs,
    but page construction is finalized here exactly once. Log and settings keep
    the last compatible base builder, while permissions/performance are owned
    directly by their production component functions. The update page is also
    exposed through the component host after its stable layout policy is applied.
    """

    if not isinstance(panel_class, type):
        return False
    if bool(getattr(panel_class, "_bilipdj_page_components_installed", False)):
        return True

    log_builder = getattr(panel_class, "_build_log_tab", None)
    settings_builder = getattr(panel_class, "_build_settings_tab", None)
    if not callable(log_builder) or not callable(settings_builder):
        return False

    from .. import update_page as update_page_module
    from ..windows_ui import _install_stable_update_layout

    # Keep update layout/version-selection normalization, but do not install the
    # old multi-page ControlPanel method patch from windows_ui.
    _install_stable_update_layout()
    update_builder = getattr(update_page_module, "build_update_tab", None)
    if not callable(update_builder):
        return False

    log_component = LogPageComponent(log_builder)
    settings_component = SettingsPageComponent(settings_builder)
    update_component = UpdatePageComponent(update_builder)
    permissions_component = PageComponent(
        key="permissions",
        title="权限",
        builder=_permissions_builder,
    )
    performance_component = PageComponent(
        key="performance",
        title="性能",
        builder=_performance_builder,
    )

    setattr(panel_class, "_build_log_tab", log_component.panel_method())
    setattr(panel_class, "_build_settings_tab", settings_component.panel_method())
    setattr(panel_class, "_build_quanxian_tab", permissions_component.panel_method())
    setattr(panel_class, "_build_perf_tab", performance_component.panel_method())
    update_page_module.build_update_tab = update_component.module_builder()

    components = {
        "log": log_component,
        "settings": settings_component,
        "update": update_component,
        "permissions": permissions_component,
        "performance": performance_component,
    }
    setattr(panel_class, "_bilipdj_page_components", components)
    setattr(panel_class, "_bilipdj_page_components_installed", True)
    return True


__all__ = ["install_page_components"]
