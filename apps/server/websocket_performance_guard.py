"""Keep slow WebSocket display clients off the realtime danmu path."""
from __future__ import annotations

import builtins
import collections
import functools
import inspect
import json
import os
import sys
import threading
from typing import Any, Callable

_SERVER_MODULE_NAMES = frozenset({"core.server", "server", "__main__"})
_PATCH_LOCK = threading.RLock()
_PATCHED_CLASS_IDS: set[int] = set()
_HOOK_TIMEOUT_SECONDS = 10.0
_HOOK_ORIGINAL: Callable[..., Any] | None = None
_HOOK_WRAPPER: Callable[..., Any] | None = None
_HOOK_TIMER: threading.Timer | None = None
_MAX_EVENT_BACKLOG = 128


class _ClientSendState:
    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.events: collections.deque[str] = collections.deque()
        self.latest_queue_update: str | None = None
        self.closed = False

    def put(self, text: str, *, replaceable: bool) -> bool:
        with self.condition:
            if self.closed:
                return False
            if replaceable:
                self.latest_queue_update = text
            else:
                if len(self.events) >= _MAX_EVENT_BACKLOG:
                    self.events.popleft()
                self.events.append(text)
            self.condition.notify()
            return True

    def get(self) -> str | None:
        with self.condition:
            while not self.closed and not self.events and self.latest_queue_update is None:
                self.condition.wait()
            if self.closed:
                return None
            if self.events:
                return self.events.popleft()
            text = self.latest_queue_update
            self.latest_queue_update = None
            return text

    def close(self) -> None:
        with self.condition:
            self.closed = True
            self.events.clear()
            self.latest_queue_update = None
            self.condition.notify_all()


def _is_replaceable_queue_update(text: str) -> bool:
    sample = str(text or "")
    if "QUEUE_UPDATE" not in sample[:512]:
        return False
    try:
        payload = json.loads(sample)
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and payload.get("type") == "QUEUE_UPDATE"


def patch_websocket_hub(websocket_hub_cls: type[Any]) -> bool:
    """Make broadcasts non-blocking and coalesce stale queue snapshots."""

    if not isinstance(websocket_hub_cls, type):
        return False
    with _PATCH_LOCK:
        class_id = id(websocket_hub_cls)
        if class_id in _PATCHED_CLASS_IDS or bool(
            getattr(websocket_hub_cls, "_bilipdj_websocket_performance_guard_installed", False)
        ):
            return True

        original_init = getattr(websocket_hub_cls, "__init__", None)
        original_register = getattr(websocket_hub_cls, "register", None)
        original_unregister = getattr(websocket_hub_cls, "unregister", None)
        original_send_text = getattr(websocket_hub_cls, "send_text", None)
        original_broadcast_text = getattr(websocket_hub_cls, "broadcast_text", None)
        if not all(
            callable(candidate)
            for candidate in (
                original_init,
                original_register,
                original_unregister,
                original_send_text,
                original_broadcast_text,
            )
        ):
            return False

        def ensure_perf_state(self: Any) -> None:
            if not hasattr(self, "_bilipdj_ws_perf_lock"):
                self._bilipdj_ws_perf_lock = threading.Lock()
            if not hasattr(self, "_bilipdj_ws_perf_states"):
                self._bilipdj_ws_perf_states = {}

        def remove_sender_state(self: Any, conn: Any) -> _ClientSendState | None:
            ensure_perf_state(self)
            with self._bilipdj_ws_perf_lock:
                state = self._bilipdj_ws_perf_states.pop(conn, None)
            if state is not None:
                state.close()
            return state

        def client_is_registered(self: Any, conn: Any) -> bool:
            lock = getattr(self, "_lock", None)
            clients = getattr(self, "_clients", None)
            if lock is None or clients is None:
                return False
            with lock:
                return conn in clients

        def sender_worker(self: Any, conn: Any, state: _ClientSendState) -> None:
            while True:
                text = state.get()
                if text is None:
                    return
                try:
                    original_send_text(self, conn, text)
                except OSError:
                    unregister_with_sender(self, conn)
                    return
                except Exception:
                    unregister_with_sender(self, conn)
                    return

        def ensure_sender(self: Any, conn: Any) -> _ClientSendState:
            ensure_perf_state(self)
            with self._bilipdj_ws_perf_lock:
                state = self._bilipdj_ws_perf_states.get(conn)
                if state is not None and not state.closed:
                    return state
                state = _ClientSendState()
                self._bilipdj_ws_perf_states[conn] = state
                thread = threading.Thread(
                    target=sender_worker,
                    args=(self, conn, state),
                    name="bilipdj-ws-sender",
                    daemon=True,
                )
                thread.start()
                return state

        @functools.wraps(original_init)
        def init_with_sender_state(self: Any, *args: Any, **kwargs: Any) -> None:
            original_init(self, *args, **kwargs)
            self._bilipdj_ws_perf_lock = threading.Lock()
            self._bilipdj_ws_perf_states: dict[Any, _ClientSendState] = {}

        @functools.wraps(original_register)
        def register_with_sender(self: Any, conn: Any) -> None:
            original_register(self, conn)
            ensure_sender(self, conn)

        @functools.wraps(original_unregister)
        def unregister_with_sender(self: Any, conn: Any) -> None:
            remove_sender_state(self, conn)
            if client_is_registered(self, conn):
                original_unregister(self, conn)

        @functools.wraps(original_broadcast_text)
        def broadcast_text_nonblocking(self: Any, sender: Any, text: str) -> None:
            lock = getattr(self, "_lock", None)
            clients = getattr(self, "_clients", None)
            if lock is None or clients is None:
                return original_broadcast_text(self, sender, text)

            with lock:
                targets = list(clients)
            replaceable = _is_replaceable_queue_update(text)
            for conn in targets:
                if sender is not None and conn is sender:
                    continue
                state = ensure_sender(self, conn)
                state.put(str(text), replaceable=replaceable)

        setattr(websocket_hub_cls, "__init__", init_with_sender_state)
        setattr(websocket_hub_cls, "register", register_with_sender)
        setattr(websocket_hub_cls, "unregister", unregister_with_sender)
        setattr(websocket_hub_cls, "broadcast_text", broadcast_text_nonblocking)
        setattr(websocket_hub_cls, "_bilipdj_websocket_performance_guard_installed", True)
        _PATCHED_CLASS_IDS.add(class_id)
        return True


def _patch_existing_websocket_hub() -> bool:
    for module_name in _SERVER_MODULE_NAMES:
        module = sys.modules.get(module_name)
        if module is None:
            continue
        if module_name == "__main__" and os.path.basename(
            str(getattr(module, "__file__", ""))
        ) != "server.py":
            continue
        candidate = getattr(module, "WebSocketHub", None)
        if isinstance(candidate, type) and patch_websocket_hub(candidate):
            return True
    return False


def _is_server_frame(frame: Any) -> bool:
    module_name = str(frame.f_globals.get("__name__", ""))
    if module_name in {"core.server", "server"}:
        return True
    if module_name != "__main__":
        return False
    return os.path.basename(str(frame.f_globals.get("__file__", ""))) == "server.py"


def _called_from_server_import() -> bool:
    frame = inspect.currentframe()
    try:
        frame = frame.f_back if frame is not None else None
        while frame is not None:
            if _is_server_frame(frame):
                return True
            frame = frame.f_back
        return False
    finally:
        del frame


def _restore_build_class_hook() -> None:
    global _HOOK_ORIGINAL, _HOOK_WRAPPER, _HOOK_TIMER
    with _PATCH_LOCK:
        wrapper = _HOOK_WRAPPER
        original = _HOOK_ORIGINAL
        if wrapper is not None and original is not None and builtins.__build_class__ is wrapper:
            builtins.__build_class__ = original
        timer = _HOOK_TIMER
        _HOOK_ORIGINAL = None
        _HOOK_WRAPPER = None
        _HOOK_TIMER = None
        if timer is not None and timer is not threading.current_thread():
            timer.cancel()


def install_websocket_performance_guard() -> bool:
    """Patch WebSocketHub now or watch its definition during server import."""

    global _HOOK_ORIGINAL, _HOOK_WRAPPER, _HOOK_TIMER
    with _PATCH_LOCK:
        if _patch_existing_websocket_hub():
            return True
        if not _called_from_server_import():
            return False
        if _HOOK_WRAPPER is not None:
            return True

        current_builder: Callable[..., Any] = builtins.__build_class__

        @functools.wraps(current_builder)
        def hooked_builder(
            func: Callable[..., Any],
            name: str,
            *bases: type[Any],
            **kwargs: Any,
        ) -> Any:
            created = current_builder(func, name, *bases, **kwargs)
            module_name = str(getattr(created, "__module__", "") or "")
            server_class = module_name in {"core.server", "server"}
            if module_name == "__main__":
                server_class = (
                    os.path.basename(str(func.__globals__.get("__file__", "")))
                    == "server.py"
                )
            if (
                name == "WebSocketHub"
                and server_class
                and isinstance(created, type)
                and patch_websocket_hub(created)
            ):
                _restore_build_class_hook()
            return created

        _HOOK_ORIGINAL = current_builder
        _HOOK_WRAPPER = hooked_builder
        builtins.__build_class__ = hooked_builder
        timer = threading.Timer(_HOOK_TIMEOUT_SECONDS, _restore_build_class_hook)
        timer.daemon = True
        _HOOK_TIMER = timer
        timer.start()
        return True


__all__ = [
    "install_websocket_performance_guard",
    "patch_websocket_hub",
]
