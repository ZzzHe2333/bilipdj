"""Preserve and extend queue display options for every style editor."""
from __future__ import annotations

import functools
import sys
import threading
from typing import Any

STYLE_OPTION_DEFAULTS: dict[str, Any] = {
    "auto_scroll": False,
    "show_sequence": False,
    "queue_font_family": "Microsoft YaHei, Noto Sans SC, PingFang SC, sans-serif",
    "queue_letter_spacing": 0,
    "queue_word_spacing": 0,
    "queue_line_height": "1.20",
    "queue_item_gap": 10,
    "queue_text_align": "left",
    "queue_text_opacity": 100,
    "queue_item_padding_x": 14,
    "queue_item_padding_y": 8,
}
DISPLAY_STYLE_DEFAULTS = STYLE_OPTION_DEFAULTS
STYLE_CSS_VARIABLES: dict[str, str] = {
    "queue_font_family": "--queue-font-family",
    "queue_letter_spacing": "--queue-letter-spacing",
    "queue_word_spacing": "--queue-word-spacing",
    "queue_line_height": "--queue-line-height",
    "queue_item_gap": "--queue-item-gap",
    "queue_text_align": "--queue-text-align",
    "queue_text_opacity": "--queue-text-opacity",
    "queue_item_padding_x": "--queue-item-padding-x",
    "queue_item_padding_y": "--queue-item-padding-y",
}
_PATCH_LOCK = threading.RLock()


def _patch_logging(module: Any) -> None:
    try:
        from .log_manager import patch_server_logging
    except ImportError:
        try:
            from log_manager import patch_server_logging
        except ImportError:
            return
    patch_server_logging(module)


def patch_style_module(module: Any) -> bool:
    """Add style defaults and make ``save_style`` merge current full state."""

    if module is None:
        return False
    defaults = getattr(module, "DEFAULT_STYLE", None)
    default_config = getattr(module, "DEFAULT_CONFIG", None)
    css_map = getattr(module, "STYLE_CSS_VAR_MAP", None)
    load_style = getattr(module, "load_style", None)
    save_style = getattr(module, "save_style", None)
    if not isinstance(defaults, dict) or not callable(load_style) or not callable(save_style):
        return False

    with _PATCH_LOCK:
        for key, value in STYLE_OPTION_DEFAULTS.items():
            defaults.setdefault(key, value)
        if isinstance(default_config, dict):
            style_defaults = default_config.setdefault("style", {})
            if isinstance(style_defaults, dict):
                for key, value in STYLE_OPTION_DEFAULTS.items():
                    style_defaults.setdefault(key, value)
        if isinstance(css_map, dict):
            for key, value in STYLE_CSS_VARIABLES.items():
                css_map.setdefault(key, value)
        _patch_logging(module)

        if bool(getattr(save_style, "_bilipdj_preserves_style_options", False)):
            return True

        original_save = save_style

        @functools.wraps(original_save)
        def preserving_save_style(data: dict[str, Any]) -> Any:
            current = load_style()
            merged = dict(current) if isinstance(current, dict) else {}
            if isinstance(data, dict):
                merged.update(data)
            for key, value in STYLE_OPTION_DEFAULTS.items():
                merged.setdefault(key, value)
            return original_save(merged)

        setattr(preserving_save_style, "_bilipdj_preserves_style_options", True)
        setattr(module, "save_style", preserving_save_style)
        return True


def _schedule_server_guards(module_name: str) -> None:
    try:
        from .server_runtime_guard import schedule_server_runtime_guards
    except ImportError:
        try:
            from server_runtime_guard import schedule_server_runtime_guards
        except ImportError:
            return
    schedule_server_runtime_guards(module_name)


def _patch_queue_manager_class(queue_manager_cls: type[Any]) -> None:
    try:
        from .queue_logic_guard import patch_queue_manager
    except ImportError:
        try:
            from queue_logic_guard import patch_queue_manager
        except ImportError:
            return
    patch_queue_manager(queue_manager_cls)


def _patch_douyin_module(server_module: Any) -> None:
    protocol_module = getattr(server_module, "douyin_protocol", None)
    if protocol_module is None:
        return
    try:
        from .douyin_fallback_guard import patch_douyin_module
    except ImportError:
        try:
            from douyin_fallback_guard import patch_douyin_module
        except ImportError:
            return
    patch_douyin_module(protocol_module)


def _patch_login_callback(server_module: Any) -> None:
    try:
        from .login_callback_guard import patch_login_callback
    except ImportError:
        try:
            from login_callback_guard import patch_login_callback
        except ImportError:
            return
    patch_login_callback(server_module)


def _patch_complete_server_module(module: Any) -> None:
    patch_style_module(module)
    try:
        from .server_runtime_guard import patch_api_handler
    except ImportError:
        try:
            from server_runtime_guard import patch_api_handler
        except ImportError:
            patch_api_handler = None
    if callable(patch_api_handler):
        patch_api_handler(module)
    _patch_douyin_module(module)
    _patch_login_callback(module)


def install_style_persistence_guard(queue_manager_cls: type[Any]) -> bool:
    """Patch the owning server module during import and again at construction."""

    module_name = str(getattr(queue_manager_cls, "__module__", "") or "")
    _patch_queue_manager_class(queue_manager_cls)
    _schedule_server_guards(module_name)

    with _PATCH_LOCK:
        if bool(getattr(queue_manager_cls, "_style_persistence_guard_installed", False)):
            return True
        original_init = getattr(queue_manager_cls, "__init__", None)
        if not callable(original_init):
            return False

        @functools.wraps(original_init)
        def init_with_style_guard(self: Any, *args: Any, **kwargs: Any) -> None:
            original_init(self, *args, **kwargs)
            module = sys.modules.get(module_name)
            _patch_complete_server_module(module)

        setattr(queue_manager_cls, "__init__", init_with_style_guard)
        setattr(queue_manager_cls, "_style_persistence_guard_installed", True)
        return True


__all__ = [
    "DISPLAY_STYLE_DEFAULTS",
    "STYLE_CSS_VARIABLES",
    "STYLE_OPTION_DEFAULTS",
    "install_style_persistence_guard",
    "patch_style_module",
]
