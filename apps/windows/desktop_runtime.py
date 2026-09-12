from __future__ import annotations

import functools
from typing import Any

from .about_page import patch_control_panel_about
from .bilibili_qr_dialog import patch_control_panel_qr_login
from .control_panel_features import patch_control_panel_features
from .control_panel_guard import patch_control_panel_class
from .control_panel_ui_finish import patch_control_panel_ui_finish
from .customtk_ui import patch_control_panel_customtkinter
from .gui_log_sink import patch_control_panel_logging
from .gui_stability import patch_control_panel_issue185
from .huya_control_guard import patch_control_panel_huya
from .log_toolbar import patch_control_panel_issue196
from .navigation_layout import patch_control_panel_issue209
from .platform_features import install_platform_features
from .portable_autostart import patch_control_panel_portable_autostart
from .purple_mouse_control_guard import patch_control_panel_purple_mouse
from .redtv_control_guard import patch_control_panel_redtv
from .release_selector import install_release_selector
from .style_save_transport import install_style_save_transport
from .support_us import _render_support_content, patch_control_panel_support_us
from .unified_theme import patch_control_panel_unified_theme
from .update_channel import install_update_channel_guard
from .update_stability import patch_control_panel_issue187
from .webdav_backup_ui import patch_control_panel_webdav_backup
from .window_policy import patch_control_panel_issue194
from .windows_ui import patch_control_panel_issue180


def _install_final_support_renderer(panel_class: type[Any], module: Any) -> None:
    current = getattr(panel_class, "_build_ui", None)
    if not callable(current) or bool(getattr(current, "_bilipdj_support_final", False)):
        return

    @functools.wraps(current)
    def build_ui_with_final_support(self: Any, *args: Any, **kwargs: Any) -> Any:
        result = current(self, *args, **kwargs)
        items = list(getattr(self, "_nav_items", []) or [])
        pages = list(getattr(self, "_content_pages", []) or [])
        for index, item in enumerate(items):
            try:
                label = str(item[1].cget("text") or "").strip()
            except Exception:
                continue
            if label == "支持我们" and index < len(pages):
                try:
                    _render_support_content(self, pages[index], module)
                except Exception:
                    pass
                break
        return result

    setattr(build_ui_with_final_support, "_bilipdj_support_final", True)
    setattr(panel_class, "_build_ui", build_ui_with_final_support)


def install_desktop_runtime(panel_class: type[Any]) -> bool:
    """Install the production Windows GUI feature stack exactly once."""

    if not isinstance(panel_class, type):
        return False
    if bool(getattr(panel_class, "_bilipdj_desktop_runtime_installed", False)):
        return True

    module = __import__(str(panel_class.__module__), fromlist=["*"])

    install_style_save_transport()
    install_update_channel_guard()
    install_release_selector()

    patch_control_panel_customtkinter(panel_class)
    patch_control_panel_class(panel_class)
    patch_control_panel_features(panel_class)
    patch_control_panel_about(panel_class)
    patch_control_panel_portable_autostart(panel_class)
    patch_control_panel_qr_login(panel_class)
    patch_control_panel_webdav_backup(panel_class)
    patch_control_panel_support_us(panel_class)
    patch_control_panel_ui_finish(panel_class)
    patch_control_panel_logging(panel_class)
    install_platform_features(panel_class)
    patch_control_panel_huya(panel_class)
    patch_control_panel_redtv(panel_class)
    patch_control_panel_purple_mouse(panel_class)
    patch_control_panel_issue180(panel_class)
    patch_control_panel_issue185(panel_class)
    patch_control_panel_issue187(panel_class)
    patch_control_panel_issue194(panel_class)
    patch_control_panel_issue196(panel_class)
    patch_control_panel_unified_theme(panel_class)
    patch_control_panel_issue209(panel_class)
    _install_final_support_renderer(panel_class, module)

    setattr(panel_class, "_bilipdj_desktop_runtime_installed", True)
    return True


__all__ = ["install_desktop_runtime"]
