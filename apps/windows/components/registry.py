from __future__ import annotations

from typing import Any

from .log_page import LogPageComponent
from .settings_page import SettingsPageComponent
from .update_page import UpdatePageComponent


def install_page_components(panel_class: type[Any]) -> bool:
    """Finalize key production pages behind explicit component boundaries.

    Historical compatibility modules are allowed to adjust the class before this
    function runs.  Their final builders are captured once here.  From this point
    onward the desktop shell invokes stable component adapters for logs/settings,
    while the update module exposes the same component boundary for update UI.
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

    update_builder = getattr(update_page_module, "build_update_tab", None)
    if not callable(update_builder):
        return False

    log_component = LogPageComponent(log_builder)
    settings_component = SettingsPageComponent(settings_builder)
    update_component = UpdatePageComponent(update_builder)

    setattr(panel_class, "_build_log_tab", log_component.panel_method())
    setattr(panel_class, "_build_settings_tab", settings_component.panel_method())
    update_page_module.build_update_tab = update_component.module_builder()

    components = {
        "log": log_component,
        "settings": settings_component,
        "update": update_component,
    }
    setattr(panel_class, "_bilipdj_page_components", components)
    setattr(panel_class, "_bilipdj_page_components_installed", True)
    return True


__all__ = ["install_page_components"]
