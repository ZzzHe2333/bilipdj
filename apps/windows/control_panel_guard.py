"""Desktop control-panel fixes applied when ``ControlPanelApp`` is created.

The release entry point executes ``control_panel.py`` as ``__main__``.  Keeping
these focused compatibility changes in a small module avoids duplicating the
large control-panel source while still patching the class before its first
instance is constructed.
"""
from __future__ import annotations

import functools
import json
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

_PATCH_LOCK = threading.RLock()


def _module_for_class(panel_class: type[Any]) -> Any | None:
    return sys.modules.get(str(getattr(panel_class, "__module__", "") or ""))


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value or "").strip().casefold()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _load_config(module: Any) -> dict[str, Any]:
    try:
        backend = module.load_backend_server_module()
        payload = backend.load_config()
    except Exception:
        try:
            payload = module.load_simple_yaml(module.CONFIG_PATH)
        except Exception:
            payload = {}
    return payload if isinstance(payload, dict) else {}


def _load_style(module: Any) -> dict[str, Any]:
    try:
        backend = module.load_backend_server_module()
        payload = backend.load_style()
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _saved_dark_mode(module: Any) -> bool:
    config = _load_config(module)
    ui = config.get("ui", {})
    if not isinstance(ui, dict):
        return False
    value = str(ui.get("theme", "light") or "light").strip().casefold()
    return value in {"dark", "night", "dark_mode", "暗夜", "夜间"}


def _persist_theme(panel: Any, module: Any) -> bool:
    try:
        backend = module.load_backend_server_module()
        config = backend.load_config()
        if not isinstance(config, dict):
            config = {}
        updated = dict(config)
        ui = config.get("ui", {})
        ui = dict(ui) if isinstance(ui, dict) else {}
        ui["theme"] = "dark" if bool(getattr(panel, "_dark_mode", False)) else "light"
        updated["ui"] = ui
        backend.save_config(updated)
        return True
    except Exception as exc:
        logger = getattr(panel, "_append_log", None)
        if callable(logger):
            logger(f"[GUI] 主题设置保存失败：{exc}", warn=True)
        return False


def _load_overlay_display_options(panel: Any, module: Any) -> None:
    style = _load_style(module)
    panel.overlay_auto_scroll_var.set(_as_bool(style.get("auto_scroll"), False))
    panel.overlay_show_sequence_var.set(_as_bool(style.get("show_sequence"), False))
    status = getattr(panel, "overlay_display_status_var", None)
    if status is not None:
        status.set("显示设置已加载")


def _save_overlay_display_options(panel: Any, module: Any) -> bool:
    style = _load_style(module)
    style["auto_scroll"] = bool(panel.overlay_auto_scroll_var.get())
    style["show_sequence"] = bool(panel.overlay_show_sequence_var.get())
    status = getattr(panel, "overlay_display_status_var", None)
    try:
        backend = module.load_backend_server_module()
        backend.save_style(style)
    except Exception as exc:
        if status is not None:
            status.set("保存失败")
        logger = getattr(panel, "_append_log", None)
        if callable(logger):
            logger(f"[GUI] 透明窗口显示设置保存失败：{exc}", warn=True)
        return False

    port_var = getattr(panel, "port_var", None)
    port = str(port_var.get() if port_var is not None else "9816").strip() or "9816"
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/style",
        data=json.dumps(style, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=2):
            pass
    except (urllib.error.URLError, TimeoutError, OSError):
        # The file has already been saved.  A stopped backend will read it on
        # its next start; a running overlay also polls the style endpoint.
        pass

    if status is not None:
        status.set("已保存并生效")
    logger = getattr(panel, "_append_log", None)
    if callable(logger):
        logger(
            "[GUI] 透明窗口显示设置已保存："
            f"自动滚动={'开' if style['auto_scroll'] else '关'}，"
            f"序号={'开' if style['show_sequence'] else '关'}"
        )
    if getattr(panel, "_overlay_window_alive", lambda: False)():
        panel._overlay_style = dict(style)
        redraw = getattr(panel, "_redraw_overlay", None)
        if callable(redraw):
            redraw()
    return True


def _ensure_process_stopped(process: Any, *, timeout: float = 5.0) -> None:
    if process is None:
        return
    try:
        if process.poll() is not None:
            return
    except Exception:
        return
    try:
        process.wait(timeout=timeout)
        return
    except subprocess.TimeoutExpired:
        pass
    except Exception:
        return

    if sys.platform == "win32":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(int(process.pid)), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=8,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except Exception:
            pass
    try:
        process.kill()
    except Exception:
        pass
    try:
        process.wait(timeout=timeout)
    except Exception:
        pass


def patch_control_panel_class(panel_class: type[Any]) -> bool:
    """Patch one real ``ControlPanelApp`` class, idempotently."""

    if not isinstance(panel_class, type):
        return False
    module = _module_for_class(panel_class)
    if module is None or Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_bilipdj_preferences_guard_installed", False)):
            return True

        original_build_ui = getattr(panel_class, "_build_ui", None)
        original_build_overlay_tab = getattr(panel_class, "_build_overlay_tab", None)
        original_toggle_theme = getattr(panel_class, "_toggle_theme", None)
        original_load_from_file = getattr(panel_class, "load_from_file", None)
        original_gather_config = getattr(panel_class, "gather_config", None)
        original_stop_server = getattr(panel_class, "stop_server", None)
        original_stop_overlay = getattr(panel_class, "_stop_overlay_process", None)
        required = (
            original_build_ui,
            original_build_overlay_tab,
            original_toggle_theme,
            original_load_from_file,
            original_gather_config,
        )
        if not all(callable(item) for item in required):
            return False

        @functools.wraps(original_build_ui)
        def build_ui_with_saved_theme(self: Any, *args: Any, **kwargs: Any) -> Any:
            # First launch defaults to light.  Existing users get their stored
            # choice before any widgets are rendered, avoiding a dark flash.
            self._dark_mode = _saved_dark_mode(module)
            if not hasattr(self, "overlay_auto_scroll_var"):
                self.overlay_auto_scroll_var = module.tk.BooleanVar(master=self.root, value=False)
            if not hasattr(self, "overlay_show_sequence_var"):
                self.overlay_show_sequence_var = module.tk.BooleanVar(master=self.root, value=False)
            if not hasattr(self, "overlay_display_status_var"):
                self.overlay_display_status_var = module.tk.StringVar(master=self.root, value="")
            return original_build_ui(self, *args, **kwargs)

        @functools.wraps(original_build_overlay_tab)
        def build_overlay_tab_with_display_options(
            self: Any,
            frame: Any,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            result = original_build_overlay_tab(self, frame, *args, **kwargs)
            for child in frame.winfo_children():
                try:
                    if str(child.cget("text")) == "说明":
                        child.grid_configure(row=2)
                except Exception:
                    continue

            display = module.ttk.LabelFrame(frame, text="排队显示", padding=10)
            display.grid(row=1, column=0, sticky="ew", pady=(10, 0))
            display.columnconfigure(1, weight=1)
            module.ttk.Label(display, text="滚动设置", width=12, anchor="w").grid(
                row=0, column=0, sticky="w", pady=4
            )
            module.ttk.Checkbutton(
                display,
                text="自动滚动",
                variable=self.overlay_auto_scroll_var,
                command=lambda: self._save_overlay_display_options(),
            ).grid(row=0, column=1, sticky="w", pady=4)
            module.ttk.Label(display, text="序号", width=12, anchor="w").grid(
                row=1, column=0, sticky="w", pady=4
            )
            module.ttk.Checkbutton(
                display,
                text="显示序号",
                variable=self.overlay_show_sequence_var,
                command=lambda: self._save_overlay_display_options(),
            ).grid(row=1, column=1, sticky="w", pady=4)
            module.ttk.Label(
                display,
                textvariable=self.overlay_display_status_var,
                foreground="#0a0",
            ).grid(row=0, column=2, rowspan=2, sticky="e", padx=(12, 0))
            _load_overlay_display_options(self, module)
            return result

        @functools.wraps(original_toggle_theme)
        def toggle_theme_and_persist(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_toggle_theme(self, *args, **kwargs)
            _persist_theme(self, module)
            return result

        @functools.wraps(original_load_from_file)
        def load_from_file_with_preferences(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_load_from_file(self, *args, **kwargs)
            dark = _saved_dark_mode(module)
            if bool(getattr(self, "_dark_mode", False)) != dark:
                self._apply_theme(dark)
            _load_overlay_display_options(self, module)
            return result

        @functools.wraps(original_gather_config)
        def gather_config_with_theme(self: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
            payload = original_gather_config(self, *args, **kwargs)
            if isinstance(payload, dict):
                ui = payload.get("ui", {})
                ui = dict(ui) if isinstance(ui, dict) else {}
                ui["theme"] = "dark" if bool(getattr(self, "_dark_mode", False)) else "light"
                payload["ui"] = ui
            return payload

        def save_overlay_display_options(self: Any) -> bool:
            return _save_overlay_display_options(self, module)

        setattr(panel_class, "_build_ui", build_ui_with_saved_theme)
        setattr(panel_class, "_build_overlay_tab", build_overlay_tab_with_display_options)
        setattr(panel_class, "_toggle_theme", toggle_theme_and_persist)
        setattr(panel_class, "load_from_file", load_from_file_with_preferences)
        setattr(panel_class, "gather_config", gather_config_with_theme)
        setattr(panel_class, "_save_overlay_display_options", save_overlay_display_options)

        if callable(original_stop_server):
            @functools.wraps(original_stop_server)
            def stop_server_and_wait(self: Any, *args: Any, **kwargs: Any) -> Any:
                process = getattr(self, "server_proc", None)
                result = original_stop_server(self, *args, **kwargs)
                _ensure_process_stopped(process)
                return result

            setattr(panel_class, "stop_server", stop_server_and_wait)

        if callable(original_stop_overlay):
            @functools.wraps(original_stop_overlay)
            def stop_overlay_and_wait(self: Any, *args: Any, **kwargs: Any) -> Any:
                process = getattr(self, "overlay_proc", None)
                result = original_stop_overlay(self, *args, **kwargs)
                _ensure_process_stopped(process)
                return result

            setattr(panel_class, "_stop_overlay_process", stop_overlay_and_wait)

        setattr(panel_class, "_bilipdj_preferences_guard_installed", True)
        return True


__all__ = ["patch_control_panel_class"]
