"""Promote the Huya reserved slot in the Windows Tk control panel."""
from __future__ import annotations

import functools
import re
import threading
from typing import Any

_PATCH_LOCK = threading.RLock()


def _promote_huya_labels(module: Any) -> None:
    current = getattr(module, "PLATFORM_LABEL_TO_VALUE", {})
    if isinstance(current, dict):
        items: list[tuple[str, str]] = []
        found = False
        for label, value in current.items():
            if str(value) == "huya":
                if not found:
                    items.append(("虎牙", "huya"))
                    found = True
            else:
                items.append((str(label), str(value)))
        if not found:
            items.append(("虎牙", "huya"))
        module.PLATFORM_LABEL_TO_VALUE = dict(items)
        module.PLATFORM_VALUE_TO_LABEL = {value: key for key, value in module.PLATFORM_LABEL_TO_VALUE.items()}
        module.PLATFORM_LABELS = tuple(module.PLATFORM_LABEL_TO_VALUE.keys())

    meta = getattr(module, "RESERVED_PLATFORM_UI_META", None)
    if isinstance(meta, dict):
        huya = dict(meta.get("huya", {}) or {})
        huya.update({"title": "虎牙参数", "hint": "可填写虎牙直播间链接；房间号和主播 ID 可留空自动解析，也可手动填写作为兜底。"})
        meta["huya"] = huya


def _walk(widget: Any):
    yield widget
    try:
        children = list(widget.winfo_children())
    except Exception:
        children = []
    for child in children:
        yield from _walk(child)


def _find_stream_frame(panel: Any) -> Any | None:
    root = getattr(panel, "settings_notebook", None) or getattr(panel, "root", None)
    if root is None:
        return None
    for widget in _walk(root):
        try:
            if str(widget.cget("text") or "").strip() == "弹幕流":
                return widget
        except Exception:
            continue
    return None


def _sync_huya_var_from_status(panel: Any) -> None:
    vars_map = getattr(panel, "_platform_vars", None)
    status_var = getattr(panel, "_platform_status_var", None)
    if not isinstance(vars_map, dict) or "huya" not in vars_map or status_var is None:
        return
    try:
        text = str(status_var.get() or "")
    except Exception:
        return
    match = re.search(r"已激活：([^；;]+)", text)
    if not match:
        return
    raw = match.group(1).strip()
    active = set() if raw in {"", "无"} else {item.strip().lower() for item in re.split(r"[,，+]+", raw) if item.strip()}
    try:
        vars_map["huya"].set("huya" in active)
    except Exception:
        pass


def _install_huya_active_checkbox(panel: Any, module: Any) -> None:
    if bool(getattr(panel, "_huya_active_platform_ui_installed", False)):
        return
    vars_map = getattr(panel, "_platform_vars", None)
    if not isinstance(vars_map, dict):
        return
    if "huya" not in vars_map:
        vars_map["huya"] = module.tk.BooleanVar(value=False)

    frame = _find_stream_frame(panel)
    if frame is not None:
        for widget in list(_walk(frame)):
            try:
                text = str(widget.cget("text") or "")
            except Exception:
                continue
            if "虎牙" in text and "预留" in text:
                try:
                    widget.configure(text="快手 / 斗鱼 / 视频号：当前仅预留配置，尚未接入弹幕流。")
                    widget.grid_configure(row=3, column=0, sticky="w", pady=(8, 0))
                except Exception:
                    pass
        try:
            module.ttk.Checkbutton(frame, text="虎牙（一个直播间）", variable=vars_map["huya"]).grid(row=2, column=0, sticky="w", pady=5)
        except Exception:
            pass

    status_var = getattr(panel, "_platform_status_var", None)
    if status_var is not None:
        try:
            status_var.trace_add("write", lambda *_args: _sync_huya_var_from_status(panel))
        except Exception:
            pass
    _sync_huya_var_from_status(panel)
    panel._huya_active_platform_ui_installed = True


def patch_control_panel_huya(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_huya_control_guard_installed", False)):
            return True
        _promote_huya_labels(module)
        current_build_ui = getattr(panel_class, "_build_ui", None)
        if not callable(current_build_ui):
            return False

        @functools.wraps(current_build_ui)
        def build_ui_with_huya(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = current_build_ui(self, *args, **kwargs)
            _install_huya_active_checkbox(self, module)
            return result

        panel_class._build_ui = build_ui_with_huya
        panel_class._huya_control_guard_installed = True
        return True


__all__ = ["patch_control_panel_huya"]
