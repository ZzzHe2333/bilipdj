"""Provide QueueManager with the active server config without per-danmu disk reads."""
from __future__ import annotations

import copy
import functools
import sys
import threading
from typing import Any

_RUNTIME_LOCAL = threading.local()


def patch_platform_runtime_context(server_module: Any, queue_manager_cls: type[Any]) -> bool:
    if server_module is None or not isinstance(queue_manager_cls, type):
        return False
    if bool(getattr(queue_manager_cls, "_bilipdj_platform_runtime_context_installed", False)):
        return True

    load_config = getattr(server_module, "load_config", None)
    ensure_relay = getattr(server_module, "_ensure_danmu_relay", None)
    process_danmu = getattr(queue_manager_cls, "process_danmu_json", None)
    if not all(callable(item) for item in (load_config, ensure_relay, process_danmu)):
        return False

    if not bool(getattr(load_config, "_bilipdj_runtime_context_aware", False)):
        @functools.wraps(load_config)
        def load_config_with_context(*args: Any, **kwargs: Any) -> dict[str, Any]:
            runtime = getattr(_RUNTIME_LOCAL, "config", None)
            if isinstance(runtime, dict):
                return copy.deepcopy(runtime)
            return load_config(*args, **kwargs)

        setattr(load_config_with_context, "_bilipdj_runtime_context_aware", True)
        setattr(server_module, "load_config", load_config_with_context)

    # Capture the possibly wrapped version after the load_config update.
    current_ensure = getattr(server_module, "_ensure_danmu_relay", ensure_relay)
    if not bool(getattr(current_ensure, "_bilipdj_runtime_context_aware", False)):
        @functools.wraps(current_ensure)
        def ensure_with_queue_server(server: Any, *args: Any, **kwargs: Any) -> Any:
            queue_manager = getattr(server, "queue_manager", None)
            if queue_manager is not None:
                queue_manager._pdj_server = server
            return current_ensure(server, *args, **kwargs)

        setattr(ensure_with_queue_server, "_bilipdj_runtime_context_aware", True)
        setattr(server_module, "_ensure_danmu_relay", ensure_with_queue_server)

    current_process = getattr(queue_manager_cls, "process_danmu_json", process_danmu)
    if not bool(getattr(current_process, "_bilipdj_runtime_context_aware", False)):
        @functools.wraps(current_process)
        def process_with_runtime_context(self: Any, payload: dict[str, Any]) -> Any:
            server = getattr(self, "_pdj_server", None)
            runtime = getattr(server, "runtime_config", None)
            previous = getattr(_RUNTIME_LOCAL, "config", None)
            if isinstance(runtime, dict):
                _RUNTIME_LOCAL.config = runtime
            try:
                return current_process(self, payload)
            finally:
                if previous is None:
                    try:
                        delattr(_RUNTIME_LOCAL, "config")
                    except AttributeError:
                        pass
                else:
                    _RUNTIME_LOCAL.config = previous

        setattr(process_with_runtime_context, "_bilipdj_runtime_context_aware", True)
        setattr(queue_manager_cls, "process_danmu_json", process_with_runtime_context)

    setattr(queue_manager_cls, "_bilipdj_platform_runtime_context_installed", True)
    return True


def install_queue_rank_runtime_context_integration(queue_rank_module: Any) -> bool:
    current = getattr(queue_rank_module, "attach_queue_rank_query", None)
    if not callable(current):
        return False
    if bool(getattr(current, "_bilipdj_platform_runtime_context_bridge", False)):
        return True

    @functools.wraps(current)
    def attach_with_runtime_context(queue_manager_cls: type[Any]) -> bool:
        result = bool(current(queue_manager_cls))
        server_module = sys.modules.get(str(getattr(queue_manager_cls, "__module__", "") or ""))
        patch_platform_runtime_context(server_module, queue_manager_cls)
        return result

    setattr(attach_with_runtime_context, "_bilipdj_platform_runtime_context_bridge", True)
    setattr(queue_rank_module, "attach_queue_rank_query", attach_with_runtime_context)
    return True


__all__ = [
    "install_queue_rank_runtime_context_integration",
    "patch_platform_runtime_context",
]
