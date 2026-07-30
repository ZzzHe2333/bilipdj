from __future__ import annotations

import functools
import json
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from . import log_manager, update_page
from .style_option_guard import STYLE_OPTION_DEFAULTS

_PATCH_LOCK = threading.RLock()

_ADVANCED_STYLE_FIELDS = (
    ("queue_font_family", "字体族", "entry", ()),
    ("queue_letter_spacing", "字间距(px)", "entry", ()),
    ("queue_word_spacing", "词间距(px)", "entry", ()),
    ("queue_line_height", "行高倍数", "entry", ()),
    ("queue_item_gap", "条目间距(px)", "entry", ()),
    ("queue_text_align", "文字对齐", "combo", ("left", "center", "right")),
    ("queue_text_opacity", "文字不透明度(0-100)", "entry", ()),
    ("queue_item_padding_x", "左右内边距(px)", "entry", ()),
    ("queue_item_padding_y", "上下内边距(px)", "entry", ()),
)
_INT_STYLE_KEYS = {
    "queue_font_size",
    "queue_letter_spacing",
    "queue_word_spacing",
    "queue_item_gap",
    "queue_text_opacity",
    "queue_item_padding_x",
    "queue_item_padding_y",
}


def _iter_widgets(widget: Any):
    yield widget
    try:
        children = widget.winfo_children()
    except Exception:
        children = []
    for child in children:
        yield from _iter_widgets(child)


def _insert_update_page(panel: Any, module: Any) -> None:
    if bool(getattr(panel, "_bilipdj_update_page_inserted", False)):
        return
    if not getattr(panel, "_content_pages", None) or not getattr(panel, "_nav_items", None):
        return
    nav = getattr(panel, "_nav_frame", None)
    if nav is None:
        return
    about_index = len(panel._content_pages) - 1
    about_row = panel._nav_items[about_index][0]
    content = panel._content_pages[0].master

    row = module.tk.Frame(nav, bd=0, highlightthickness=0)
    row.pack(fill="x", pady=1, before=about_row)
    indicator = module.tk.Frame(row, width=3, bd=0)
    indicator.pack(side="left", fill="y")
    button = module.tk.Button(
        row,
        text="更新软件",
        anchor="w",
        padx=13,
        pady=10,
        bd=0,
        relief="flat",
        highlightthickness=0,
        cursor="hand2",
    )
    button.pack(side="left", fill="x", expand=True)
    page = module.ttk.Frame(content, padding=2)
    page.grid(row=0, column=0, sticky="nsew")
    update_page.build_update_tab(panel, page, module.APP_NAME, module.APP_VERSION, module.APP_DIR)

    panel._nav_items.insert(about_index, (row, button, indicator))
    panel._content_pages.insert(about_index, page)
    for index, (_row, nav_button, _indicator) in enumerate(panel._nav_items):
        nav_button.configure(command=lambda i=index: panel._show_page(i))
    panel._bilipdj_update_page_inserted = True
    panel._apply_theme(panel._dark_mode)
    panel._show_page(panel._active_page)


def _build_static_about(panel: Any, frame: Any, module: Any) -> None:
    update_page.build_about_tab(panel, frame, module.APP_NAME, module.APP_VERSION)


def _add_advanced_style_controls(panel: Any, frame: Any, module: Any) -> None:
    if bool(getattr(panel, "_bilipdj_advanced_style_controls", False)):
        return
    style = dict(STYLE_OPTION_DEFAULTS)
    try:
        backend = module.load_backend_server_module()
        loaded = backend.load_style()
        if isinstance(loaded, dict):
            style.update(loaded)
    except Exception:
        pass

    advanced = module.ttk.LabelFrame(frame, text="高级字体与排版", padding=10)
    advanced.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(12, 0))
    advanced.columnconfigure(1, weight=1)
    advanced.columnconfigure(3, weight=1)
    for index, (key, label, field_type, values) in enumerate(_ADVANCED_STYLE_FIELDS):
        row = index // 2
        column = (index % 2) * 2
        module.ttk.Label(advanced, text=label).grid(
            row=row, column=column, sticky="w", padx=(0 if column == 0 else 16, 6), pady=4
        )
        var = module.tk.StringVar(value=str(style.get(key, STYLE_OPTION_DEFAULTS[key])))
        panel._style_vars[key] = var
        if field_type == "combo":
            widget = module.ttk.Combobox(
                advanced,
                textvariable=var,
                values=values,
                state="readonly",
                width=18,
            )
        else:
            widget = module.ttk.Entry(advanced, textvariable=var, width=24)
        widget.grid(row=row, column=column + 1, sticky="ew", pady=4)
    module.ttk.Label(
        advanced,
        text="字体族可填写系统字体名称并用逗号分隔；保存操作在后台执行，不会阻塞透明窗口。",
        wraplength=760,
    ).grid(row=(len(_ADVANCED_STYLE_FIELDS) + 1) // 2, column=0, columnspan=4, sticky="w", pady=(8, 0))
    panel._bilipdj_advanced_style_controls = True


def _clamp_int(value: Any, default: int, low: int, high: int) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        parsed = default
    return max(low, min(high, parsed))


def _collect_style_payload(panel: Any) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for key, var in panel._style_vars.items():
        value = str(var.get()).strip()
        if key == "queue_font_size":
            data[key] = _clamp_int(value, 50, 8, 300)
        elif key == "queue_font_style":
            data[key] = panel._style_font_style_to_css(value)
        elif key == "queue_font_weight":
            data[key] = panel._style_font_weight_to_css(value)
        elif key == "queue_letter_spacing":
            data[key] = _clamp_int(value, 0, -20, 100)
        elif key == "queue_word_spacing":
            data[key] = _clamp_int(value, 0, -20, 200)
        elif key == "queue_item_gap":
            data[key] = _clamp_int(value, 10, 0, 200)
        elif key == "queue_text_opacity":
            data[key] = _clamp_int(value, 100, 0, 100)
        elif key in {"queue_item_padding_x", "queue_item_padding_y"}:
            data[key] = _clamp_int(value, 8, 0, 200)
        elif key == "queue_line_height":
            try:
                line_height = float(value)
            except (TypeError, ValueError):
                line_height = 1.2
            data[key] = f"{max(0.6, min(5.0, line_height)):.2f}"
        elif key == "queue_text_align":
            data[key] = value if value in {"left", "center", "right"} else "left"
        elif key == "queue_font_family":
            data[key] = value or str(STYLE_OPTION_DEFAULTS[key])
        else:
            data[key] = value
    for key, var in getattr(panel, "_style_bool_vars", {}).items():
        data[key] = bool(var.get())
    for key, default in STYLE_OPTION_DEFAULTS.items():
        data.setdefault(key, default)
    return data


def _start_async_style_save(panel: Any, module: Any, payload: dict[str, Any]) -> None:
    if bool(getattr(panel, "_style_save_busy", False)):
        panel._style_save_pending = dict(payload)
        if hasattr(panel, "_style_save_status_var"):
            panel._style_save_status_var.set("等待保存……")
        return
    panel._style_save_busy = True
    panel._style_save_pending = None
    if hasattr(panel, "_style_save_status_var"):
        panel._style_save_status_var.set("正在后台保存……")

    port = str(panel.port_var.get()).strip() or "9816"

    def worker() -> None:
        error = ""
        try:
            backend = module.load_backend_server_module()
            backend.save_style(payload)
            request = urllib.request.Request(
                f"http://127.0.0.1:{port}/api/style",
                data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=1.0):
                    pass
            except (urllib.error.URLError, TimeoutError, OSError):
                pass
        except Exception as exc:
            error = str(exc)

        def finish() -> None:
            panel._style_save_busy = False
            if error:
                panel._style_save_status_var.set("保存失败")
                panel._append_log(f"[GUI] 样式写入失败：{error}", warn=True)
            else:
                import time

                panel._style_save_status_var.set(f"保存成功 {time.strftime('%H:%M:%S')}")
                panel._append_log("[GUI] 字体与排版样式已后台保存")
                if panel._overlay_window_alive():
                    panel._overlay_style = dict(panel._load_style_data())
                    panel._redraw_overlay()
            pending = getattr(panel, "_style_save_pending", None)
            panel._style_save_pending = None
            if isinstance(pending, dict) and pending != payload:
                _start_async_style_save(panel, module, pending)

        try:
            panel.root.after(0, finish)
        except Exception:
            pass

    threading.Thread(target=worker, name="bilipdj-style-save", daemon=True).start()


def patch_control_panel_features(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    if Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_bilipdj_feature_pack_installed", False)):
            return True
        original_build_ui = getattr(panel_class, "_build_ui", None)
        original_style_tab = getattr(panel_class, "_build_style_tab", None)
        original_settings_tab = getattr(panel_class, "_build_settings_tab", None)
        original_load = getattr(panel_class, "load_from_file", None)
        original_gather = getattr(panel_class, "gather_config", None)
        if not all(callable(item) for item in (original_build_ui, original_style_tab, original_settings_tab, original_load, original_gather)):
            return False

        def build_about_tab(self: Any, frame: Any) -> None:
            _build_static_about(self, frame, module)

        @functools.wraps(original_build_ui)
        def build_ui_with_update_page(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_build_ui(self, *args, **kwargs)
            _insert_update_page(self, module)
            return result

        @functools.wraps(original_style_tab)
        def build_style_with_advanced_controls(self: Any, frame: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_style_tab(self, frame, *args, **kwargs)
            _add_advanced_style_controls(self, frame, module)
            return result

        def save_style_async(self: Any) -> bool:
            _start_async_style_save(self, module, _collect_style_payload(self))
            return True

        @functools.wraps(original_settings_tab)
        def build_settings_with_log_retention(self: Any, frame: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_settings_tab(self, frame, *args, **kwargs)
            for widget in _iter_widgets(frame):
                try:
                    if str(widget.cget("text")) == "日志保留天数":
                        widget.configure(text="日志保存时间（天）")
                except Exception:
                    continue
            return result

        @functools.wraps(original_load)
        def load_with_cleanup(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_load(self, *args, **kwargs)
            try:
                days = _clamp_int(self.retention_days_var.get(), log_manager.DEFAULT_RETENTION_DAYS, 1, 3650)
                self.retention_days_var.set(str(days))
                deleted = log_manager.cleanup_logs(module.APP_DIR, days)
                if deleted:
                    self._append_log(f"[GUI] 已清理 {deleted} 个过期日志文件")
            except Exception as exc:
                self._append_log(f"[GUI] 清理过期日志失败：{exc}", warn=True)
            if hasattr(self, "update_bypass_proxy_var"):
                update_page.load_network_settings(self)
            return result

        @functools.wraps(original_gather)
        def gather_with_retention(self: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
            payload = original_gather(self, *args, **kwargs)
            if isinstance(payload, dict):
                logging_cfg = payload.get("logging", {})
                logging_cfg = dict(logging_cfg) if isinstance(logging_cfg, dict) else {}
                logging_cfg["retention_days"] = _clamp_int(
                    self.retention_days_var.get(),
                    log_manager.DEFAULT_RETENTION_DAYS,
                    1,
                    3650,
                )
                payload["logging"] = logging_cfg
            return payload

        setattr(panel_class, "_build_about_tab", build_about_tab)
        setattr(panel_class, "_build_ui", build_ui_with_update_page)
        setattr(panel_class, "_build_style_tab", build_style_with_advanced_controls)
        setattr(panel_class, "_save_style", save_style_async)
        setattr(panel_class, "_build_settings_tab", build_settings_with_log_retention)
        setattr(panel_class, "load_from_file", load_with_cleanup)
        setattr(panel_class, "gather_config", gather_with_retention)
        setattr(panel_class, "_bilipdj_feature_pack_installed", True)
        return True


__all__ = ["patch_control_panel_features"]
