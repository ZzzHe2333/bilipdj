from .log_page import LogPageComponent
from .registry import install_page_components
from .settings_page import SettingsPageComponent
from .update_page import UpdatePageComponent

__all__ = [
    "LogPageComponent",
    "SettingsPageComponent",
    "UpdatePageComponent",
    "install_page_components",
]
