"""Preserve and extend queue display options for every style editor."""
from __future__ import annotations

import functools
import re
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
_PATCH_LOCK = threading.RLock()


def _clamp_int(value: Any, default: int, low: int, high: int) -> int:
    try:
        parsed = int(float(str(value).strip().removesuffix("px")))
    except (TypeError, ValueError):
        parsed = default
    return max(low, min(high, parsed))


def _safe_css_text(value: Any, default: str) -> str:
    text = str(value or default).replace("\r", " ").replace("\n", " ")
    text = re.sub(r"[;{}]", "", text).strip()
    return text or default


def _enhance_css(css: str, style: dict[str, Any]) -> str:
    font_family = _safe_css_text(style.get("queue_font_family"), str(STYLE_OPTION_DEFAULTS["queue_font_family"]))
    letter = _clamp_int(style.get("queue_letter_spacing"), 0, -20, 100)
    word = _clamp_int(style.get("queue_word_spacing"), 0, -20, 200)
    gap = _clamp_int(style.get("queue_item_gap"), 10, 0, 200)
    opacity = _clamp_int(style.get("queue_text_opacity"), 100, 0, 100)
    padding_x = _clamp_int(style.get("queue_item_padding_x"), 14, 0, 200)
    padding_y = _clamp_int(style.get("queue_item_padding_y"), 8, 0, 200)
    try:
        line_height = float(str(style.get("queue_line_height", "1.20")).strip())
    except (TypeError, ValueError):
        line_height = 1.2
    line_height = max(0.6, min(5.0, line_height))
    align = str(style.get("queue_text_align", "left") or "left").strip().lower()
    if align not in {"left", "center", "right"}:
        align = "left"

    variables = (
        f"    --queue-font-family: {font_family};\n"
        f"    --queue-letter-spacing: {letter}px;\n"
        f"    --queue-word-spacing: {word}px;\n"
        f"    --queue-line-height: {line_height:.2f};\n"
        f"    --queue-item-gap: {gap}px;\n"
        f"    --queue-text-align: {align};\n"
        f"    --queue-text-opacity: {opacity / 100:.2f};\n"
        f"    --queue-item-padding-x: {padding_x}px;\n"
        f"    --queue-item-padding-y: {padding_y}px;\n"
    )
    root_end = css.find("}")
    if root_end >= 0:
        css = css[:root_end] + variables + css[root_end:]
    else:
        css = ":root {\n" + variables + "}\n" + css
    return css + (
        "\n/* bilipdj advanced typography */\n"
        ".div { gap: var(--queue-item-gap); }\n"
        ".queue-item { padding: var(--queue-item-padding-y) var(--queue-item-padding-x); }\n"
        ".queue-content {\n"
        "  font-family: var(--queue-font-family);\n"
        "  letter-spacing: var(--queue-letter-spacing);\n"
        "  word-spacing: var(--queue-word-spacing);\n"
        "  line-height: var(--queue-line-height);\n"
        "  text-align: var(--queue-text-align);\n"
        "  opacity: var(--queue-text-opacity);\n"
        "}\n"
    )


def _patch_css_builder(module: Any) -> None:
    current = getattr(module, "build_index_css", None)
    if not callable(current) or bool(getattr(current, "_bilipdj_advanced_typography", False)):
        return

    @functools.wraps(current)
    def build_index_css(style: dict[str, Any] | None = None) -> str:
        merged = dict(STYLE_OPTION_DEFAULTS)
        if isinstance(style, dict):
            merged.update(style)
        return _enhance_css(current(merged), merged)

    setattr(build_index_css, "_bilipdj_advanced_typography", True)
    setattr(module, "build_index_css", build_index_css)


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
        _patch_css_builder(module)
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
    "STYLE_OPTION_DEFAULTS",
    "install_style_persistence_guard",
    "patch_style_module",
]
