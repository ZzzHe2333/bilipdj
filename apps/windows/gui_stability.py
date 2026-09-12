from __future__ import annotations

import functools
import subprocess
import threading
from pathlib import Path
from typing import Any

_PATCH_LOCK = threading.RLock()


def _process_running(process: Any) -> bool:
    if process is None:
        return False
    try:
        return process.poll() is None
    except Exception:
        return False


def _safe_configure(widget: Any, **kwargs: Any) -> None:
    if widget is None:
        return
    try:
        widget.configure(**kwargs)
    except Exception:
        pass


def _finish_stopped_ui(panel: Any, process: Any, error: str = "") -> None:
    panel._issue185_backend_stop_busy = False
    running = _process_running(process)
    if not running and getattr(panel, "server_proc", None) is process:
        panel.server_proc = None

    if error and running:
        panel.status_var.set("后端停止失败")
        panel._append_log(f"[GUI] 后端停止失败：{error}", warn=True)
    else:
        panel.status_var.set("后端已停止")
        panel.ws_light_var.set("🔴")
        panel.ws_text_var.set("直播间链接状态：后端未启动")
        panel._append_log("[GUI] 后端已停止")

    _safe_configure(getattr(panel, "_stop_button", None), state="normal", text="停止", width=12)
    if not bool(getattr(panel, "_backend_starting", False)):
        _safe_configure(getattr(panel, "_start_button", None), state="normal", text="启动服务", width=12)
    try:
        panel.root.configure(cursor="")
    except Exception:
        pass
    refresh = getattr(panel, "_refresh_header_status", None)
    if callable(refresh):
        try:
            refresh()
        except Exception:
            pass


def _stop_server_nonblocking(panel: Any) -> None:
    """Stop the backend without waiting on the Tk main thread."""

    if bool(getattr(panel, "_issue185_backend_stop_busy", False)):
        return
    process = getattr(panel, "server_proc", None)
    if not _process_running(process):
        panel.status_var.set("后端未运行")
        panel.ws_light_var.set("🔴")
        panel.ws_text_var.set("直播间链接状态：后端未启动")
        _safe_configure(getattr(panel, "_start_button", None), state="normal", text="启动服务", width=12)
        _safe_configure(getattr(panel, "_stop_button", None), state="normal", text="停止", width=12)
        return

    panel._issue185_backend_stop_busy = True
    panel.status_var.set("正在停止后端…")
    _safe_configure(getattr(panel, "_start_button", None), state="disabled", width=12)
    _safe_configure(getattr(panel, "_stop_button", None), state="disabled", text="停止中…", width=12)
    try:
        panel.root.configure(cursor="watch")
    except Exception:
        pass
    panel._append_log("[GUI] 正在后台停止后端服务…")

    def worker() -> None:
        error = ""
        try:
            process.terminate()
            try:
                process.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2.0)
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
        try:
            panel.root.after(0, lambda: _finish_stopped_ui(panel, process, error))
        except Exception:
            pass

    threading.Thread(target=worker, name="bilipdj-desktop-stop", daemon=True).start()


def _stabilize_settings_canvas(panel: Any, inner: Any) -> None:
    try:
        canvas = inner.master
        candidates = list(canvas.find_all())
        window_id = next((item for item in candidates if str(canvas.type(item)) == "window"), None)
    except Exception:
        return
    if window_id is None:
        return

    state: dict[str, Any] = {
        "job": None,
        "pending_width": None,
        "width": -1,
        "bbox": None,
    }

    def flush() -> None:
        state["job"] = None
        try:
            pending_width = state.get("pending_width")
            state["pending_width"] = None
            if pending_width is not None:
                width = max(1, int(pending_width))
                if abs(width - int(state["width"])) > 1:
                    state["width"] = width
                    canvas.itemconfigure(window_id, width=width)
            bbox = canvas.bbox("all")
            if bbox is not None and bbox != state.get("bbox"):
                state["bbox"] = bbox
                canvas.configure(scrollregion=bbox)
        except Exception:
            return

    def schedule(_event: Any | None = None) -> None:
        if state["job"] is not None:
            return
        try:
            state["job"] = canvas.after_idle(flush)
        except Exception:
            state["job"] = None

    def resize(event: Any) -> None:
        state["pending_width"] = int(getattr(event, "width", 1) or 1)
        schedule()

    # Replace the eager bindings created by ControlPanelApp.  Recomputing the
    # scrollregion and window width for every Configure event can recursively
    # generate more geometry events and makes the settings controls visibly
    # shake while the page is settling.
    try:
        inner.bind("<Configure>", schedule)
        canvas.bind("<Configure>", resize)
        schedule()
        stores = getattr(panel, "_issue185_canvas_states", None)
        if not isinstance(stores, list):
            stores = []
            panel._issue185_canvas_states = stores
        stores.append(state)
    except Exception:
        pass


def _stabilize_style_preview(panel: Any) -> None:
    canvas = getattr(panel, "_style_preview_canvas", None)
    redraw = getattr(panel, "_redraw_style_preview", None)
    if canvas is None or not callable(redraw):
        return
    state = {"job": None, "size": None}

    def flush() -> None:
        state["job"] = None
        try:
            size = (int(canvas.winfo_width()), int(canvas.winfo_height()))
        except Exception:
            return
        if size == state.get("size"):
            return
        state["size"] = size
        try:
            redraw()
        except Exception:
            pass

    def schedule(_event: Any | None = None) -> None:
        if state["job"] is not None:
            return
        try:
            state["job"] = canvas.after(40, flush)
        except Exception:
            state["job"] = None

    try:
        canvas.bind("<Configure>", schedule)
        panel._issue185_preview_state = state
    except Exception:
        pass


def patch_control_panel_issue185(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    if Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_issue185_stability_installed", False)):
            return True
        original_build_ui = getattr(panel_class, "_build_ui", None)
        original_scroll_page = getattr(panel_class, "_add_scrollable_settings_page", None)
        if not callable(original_build_ui) or not callable(original_scroll_page):
            return False

        @functools.wraps(original_scroll_page)
        def add_scrollable_settings_page(self: Any, *args: Any, **kwargs: Any) -> Any:
            inner = original_scroll_page(self, *args, **kwargs)
            _stabilize_settings_canvas(self, inner)
            return inner

        @functools.wraps(original_build_ui)
        def build_ui_stable(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_build_ui(self, *args, **kwargs)
            _safe_configure(getattr(self, "_start_button", None), width=12)
            _safe_configure(
                getattr(self, "_stop_button", None),
                width=12,
                command=lambda: _stop_server_nonblocking(self),
            )
            _stabilize_style_preview(self)
            return result

        def stop_server_nonblocking(self: Any) -> None:
            _stop_server_nonblocking(self)

        setattr(panel_class, "_add_scrollable_settings_page", add_scrollable_settings_page)
        setattr(panel_class, "_build_ui", build_ui_stable)
        setattr(panel_class, "_stop_server_nonblocking", stop_server_nonblocking)
        setattr(panel_class, "_issue185_stability_installed", True)
        return True


__all__ = ["patch_control_panel_issue185"]
