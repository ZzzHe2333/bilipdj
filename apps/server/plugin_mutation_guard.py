"""Serialize PluginManager state mutations on its shared RLock (Issue #134)."""
from __future__ import annotations

from threading import RLock
from typing import Any, Callable

_PATCH_LOCK = RLock()
_MUTATING_METHODS = (
    "set_enabled",
    "uninstall",
    "add_trusted_key",
    "remove_trusted_key",
)


def _locked_method(original: Callable[..., Any], method_name: str) -> Callable[..., Any]:
    def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        lock = getattr(self, "_lock", None)
        if lock is None:
            raise RuntimeError("PluginManager mutation lock is unavailable")
        with lock:
            return original(self, *args, **kwargs)

    wrapped.__name__ = getattr(original, "__name__", method_name)
    wrapped.__doc__ = getattr(original, "__doc__", None)
    wrapped._issue134_plugin_mutation_lock = True  # type: ignore[attr-defined]
    wrapped._issue134_original = original  # type: ignore[attr-defined]
    return wrapped


def install_plugin_mutation_guard(plugin_manager_module: Any) -> bool:
    """Ensure every read-modify-write PluginManager operation uses one RLock."""
    with _PATCH_LOCK:
        if bool(getattr(plugin_manager_module, "_plugin_mutation_guard_installed", False)):
            return True
        manager_class = plugin_manager_module.PluginManager
        for method_name in _MUTATING_METHODS:
            original = getattr(manager_class, method_name)
            if bool(getattr(original, "_issue134_plugin_mutation_lock", False)):
                continue
            setattr(manager_class, method_name, _locked_method(original, method_name))
        plugin_manager_module._plugin_mutation_guard_installed = True
        return True


__all__ = ["install_plugin_mutation_guard"]
