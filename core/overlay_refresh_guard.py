"""Thread-safe refresh loop for the standalone Tk overlay."""
from __future__ import annotations

import functools
import queue
import threading
from typing import Any

_PATCH_LOCK = threading.RLock()
_RESULT_POLL_MS = 50


def _build_refresh_result(app: Any, style_snapshot: dict[str, Any]):
    queue_payload = app._request_json("/api/queue/state")
    style_payload = app._request_json("/api/style")

    entries = []
    if isinstance(queue_payload, dict):
        payload_entries = queue_payload.get("entries", [])
        if isinstance(payload_entries, list):
            entries = payload_entries
    items = [
        f"{str(entry.get('id', '')).strip()} {str(entry.get('content', '')).strip()}".rstrip()
        for entry in entries
        if isinstance(entry, dict)
        and str(entry.get("id", "") or entry.get("content", "")).strip()
    ]

    next_style = dict(style_snapshot)
    if isinstance(style_payload, dict):
        for key in (
            "text_color",
            "text_stroke_color",
            "text_stroke_enabled",
            "queue_font_size",
            "queue_font_weight",
            "queue_font_style",
            "auto_scroll",
            "show_sequence",
        ):
            if key in style_payload:
                next_style[key] = style_payload.get(key)
    return items, next_style


def patch_overlay_module(module: Any) -> bool:
    """Patch one loaded overlay module, idempotently."""

    app_class = getattr(module, "OverlayHostApp", None)
    if not isinstance(app_class, type):
        return False
    with _PATCH_LOCK:
        if bool(getattr(app_class, "_bilipdj_refresh_guard_installed", False)):
            return True
        original_close = getattr(app_class, "_close", None)
        if not callable(original_close):
            return False
        refresh_delay = max(100, int(getattr(module, "OVERLAY_REFRESH_MS", 1200)))

        def ensure_state(self: Any) -> None:
            if not hasattr(self, "_refresh_result_queue"):
                self._refresh_result_queue = queue.Queue()
            if not hasattr(self, "_refresh_poll_job"):
                self._refresh_poll_job = None
            if not hasattr(self, "_refresh_timer_job"):
                self._refresh_timer_job = None
            if not hasattr(self, "_refresh_closed"):
                self._refresh_closed = False

        def schedule_poll(self: Any) -> None:
            ensure_state(self)
            if self._refresh_closed or self._refresh_poll_job is not None:
                return
            try:
                self._refresh_poll_job = self.root.after(
                    _RESULT_POLL_MS,
                    lambda: poll_refresh_results(self),
                )
            except Exception:
                self._refresh_poll_job = None
                self._refresh_running = False

        def refresh_worker(self: Any, style_snapshot: dict[str, Any]) -> None:
            result = None
            try:
                result = _build_refresh_result(self, style_snapshot)
            except Exception:
                result = None
            finally:
                # Queue operations are thread-safe and do not enter Tcl/Tk.
                self._refresh_result_queue.put(result)

        def refresh_async(self: Any) -> None:
            ensure_state(self)
            if self._refresh_closed or self._refresh_running:
                return
            self._refresh_running = True
            style_snapshot = dict(getattr(self, "style", {}))
            threading.Thread(
                target=refresh_worker,
                args=(self, style_snapshot),
                name="bilipdj-overlay-refresh",
                daemon=True,
            ).start()
            schedule_poll(self)

        def poll_refresh_results(self: Any) -> None:
            ensure_state(self)
            self._refresh_poll_job = None
            if self._refresh_closed:
                self._refresh_running = False
                return

            latest = ...
            while True:
                try:
                    latest = self._refresh_result_queue.get_nowait()
                except queue.Empty:
                    break
            if latest is ...:
                schedule_poll(self)
                return

            self._refresh_running = False
            if latest is not None:
                items, next_style = latest
                changed = items != self.items or next_style != self.style
                self.items = list(items)
                self.style = dict(next_style)
                if changed:
                    self._reset_scroll()
                    self._redraw()

            if not self._refresh_closed:
                try:
                    self._refresh_timer_job = self.root.after(
                        refresh_delay,
                        lambda: refresh_timer_fired(self),
                    )
                except Exception:
                    self._refresh_timer_job = None

        def refresh_timer_fired(self: Any) -> None:
            self._refresh_timer_job = None
            refresh_async(self)

        @functools.wraps(original_close)
        def close_with_refresh_shutdown(self: Any) -> None:
            ensure_state(self)
            self._refresh_closed = True
            self._refresh_running = False
            for attribute in ("_refresh_poll_job", "_refresh_timer_job"):
                job = getattr(self, attribute, None)
                if job is not None:
                    try:
                        self.root.after_cancel(job)
                    except Exception:
                        pass
                    setattr(self, attribute, None)
            return original_close(self)

        setattr(app_class, "_refresh_async", refresh_async)
        setattr(app_class, "_refresh_worker", refresh_worker)
        setattr(app_class, "_poll_refresh_results", poll_refresh_results)
        setattr(app_class, "_close", close_with_refresh_shutdown)
        setattr(app_class, "_bilipdj_refresh_guard_installed", True)
        return True


__all__ = ["patch_overlay_module"]
