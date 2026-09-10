"""Recursive secret redaction for PluginContext host configuration (Issue #130)."""
from __future__ import annotations

import copy
from typing import Any

_EXACT_SECRET_KEYS = frozenset(
    {
        "cookie",
        "cookies",
        "sessdata",
        "bili_jct",
        "dedeuserid",
        "dedeuserid__ckmd5",
        "buvid3",
        "auth_token",
        "access_token",
        "refresh_token",
        "token",
        "password",
        "passwd",
        "secret",
        "api_key",
        "authorization",
        "credential",
        "credentials",
    }
)
_SECRET_SUFFIXES = (
    "_token",
    "_password",
    "_passwd",
    "_secret",
    "_cookie",
    "_cookies",
    "_api_key",
    "_credential",
    "_credentials",
)


def _normalize_key(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def is_secret_key(value: Any) -> bool:
    key = _normalize_key(value)
    return key in _EXACT_SECRET_KEYS or any(key.endswith(suffix) for suffix in _SECRET_SUFFIXES)


def redact_secrets(value: Any) -> Any:
    """Deep-copy configuration while removing secret-looking dictionary fields."""
    if isinstance(value, dict):
        return {
            key: redact_secrets(item)
            for key, item in value.items()
            if not is_secret_key(key)
        }
    if isinstance(value, list):
        return [redact_secrets(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_secrets(item) for item in value)
    return copy.deepcopy(value)


def install_plugin_secret_guard(plugin_manager_module: Any) -> None:
    context_class = plugin_manager_module.PluginContext
    original = context_class.get_config
    if bool(getattr(original, "_issue130_recursive_secret_guard", False)):
        return

    def get_config(self: Any) -> dict[str, Any]:
        config = original(self)
        if "secrets" in getattr(self, "permissions", frozenset()):
            return config
        value = redact_secrets(config)
        return value if isinstance(value, dict) else {}

    get_config._issue130_recursive_secret_guard = True  # type: ignore[attr-defined]
    context_class.get_config = get_config
    plugin_manager_module._plugin_recursive_secret_guard_installed = True


__all__ = ["install_plugin_secret_guard", "is_secret_key", "redact_secrets"]
