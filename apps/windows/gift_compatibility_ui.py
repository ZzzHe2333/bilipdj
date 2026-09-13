from __future__ import annotations

import functools
import math
import re
import threading
from typing import Any

_PATCH_LOCK = threading.RLock()
_PLATFORM_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,39}$")
_DEFAULT_PLATFORMS = ("bilibili", "douyin", "huya", "twitch", "youtube")


def _iter_widgets(widget: Any):
    yield widget
    try:
        children = list(widget.winfo_children())
    except Exception:
        children = []
    for child in children:
        yield from _iter_widgets(child)


def _plugin_frame(panel: Any) -> Any | None:
    notebook = getattr(panel, "settings_notebook", None)
    if notebook is None:
        return None
    try:
        for tab_id in notebook.tabs():
            if str(notebook.tab(tab_id, "text") or "").strip() != "插件管理":
                continue
            try:
                return notebook.nametowidget(tab_id)
            except Exception:
                return panel.root.nametowidget(tab_id)
    except Exception:
        return None
    return None


def _find_action_parent(frame: Any) -> Any | None:
    for widget in _iter_widgets(frame):
        try:
            if str(widget.cget("text") or "").strip() == "刷新状态":
                return widget.master
        except Exception:
            continue
    return None


def _open_gift_compatibility(panel: Any, module: Any, windows_ui: Any) -> None:
    existing = getattr(panel, "_gift_compatibility_window", None)
    try:
        if existing is not None and existing.winfo_exists():
            existing.deiconify()
            existing.lift()
            existing.focus_force()
            return
    except Exception:
        pass

    window = module.tk.Toplevel(panel.root)
    panel._gift_compatibility_window = window
    window.title("插件兼容性 - 礼物价值")
    window.geometry("820x560")
    window.minsize(720, 480)
    window.transient(panel.root)
    window.columnconfigure(0, weight=1)
    window.rowconfigure(1, weight=1)

    head = module.ttk.Frame(window, padding=(16, 14, 16, 8))
    head.grid(row=0, column=0, sticky="ew")
    head.columnconfigure(0, weight=1)
    module.ttk.Label(head, text="礼物兼容性", font=("Microsoft YaHei UI", 15, "bold")).grid(
        row=0, column=0, sticky="w"
    )
    module.ttk.Label(
        head,
        text="按平台维护礼物对应的排队价值。自定义规则优先于平台内置价值，可用于后续抖音等礼物插件。",
        wraplength=760,
        justify="left",
    ).grid(row=1, column=0, sticky="w", pady=(4, 0))

    body = module.ttk.Frame(window, padding=(16, 0, 16, 8))
    body.grid(row=1, column=0, sticky="nsew")
    body.columnconfigure(0, weight=1)
    body.rowconfigure(0, weight=1)

    columns = ("enabled", "platform", "gift_id", "gift_name", "value")
    tree_frame = module.ttk.Frame(body)
    tree_frame.grid(row=0, column=0, sticky="nsew")
    tree_frame.columnconfigure(0, weight=1)
    tree_frame.rowconfigure(0, weight=1)
    tree = module.ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse", height=12)
    headings = {
        "enabled": "启用",
        "platform": "平台",
        "gift_id": "礼物 ID",
        "gift_name": "礼物名称",
        "value": "单个价值",
    }
    widths = {"enabled": 62, "platform": 110, "gift_id": 150, "gift_name": 210, "value": 110}
    for key in columns:
        tree.heading(key, text=headings[key])
        tree.column(key, width=widths[key], anchor="center" if key in {"enabled", "platform", "value"} else "w")
    scroll = module.ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=scroll.set)
    tree.grid(row=0, column=0, sticky="nsew")
    scroll.grid(row=0, column=1, sticky="ns")

    form = module.ttk.LabelFrame(body, text="新增 / 编辑规则", padding=10)
    form.grid(row=1, column=0, sticky="ew", pady=(10, 0))
    form.columnconfigure(1, weight=1)
    form.columnconfigure(3, weight=1)

    platform_var = module.tk.StringVar(value="bilibili")
    gift_id_var = module.tk.StringVar(value="")
    gift_name_var = module.tk.StringVar(value="")
    value_var = module.tk.StringVar(value="")
    enabled_var = module.tk.BooleanVar(value=True)
    status_var = module.tk.StringVar(value="正在读取礼物兼容性配置……")
    rules: list[dict[str, Any]] = []
    selected_index: list[int | None] = [None]

    module.ttk.Label(form, text="平台").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
    platform_box = module.ttk.Combobox(form, textvariable=platform_var, values=_DEFAULT_PLATFORMS, state="normal")
    platform_box.grid(row=0, column=1, sticky="ew", padx=(0, 14), pady=4)
    module.ttk.Label(form, text="单个价值").grid(row=0, column=2, sticky="w", padx=(0, 8), pady=4)
    module.ttk.Entry(form, textvariable=value_var).grid(row=0, column=3, sticky="ew", pady=4)

    module.ttk.Label(form, text="礼物 ID").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
    module.ttk.Entry(form, textvariable=gift_id_var).grid(row=1, column=1, sticky="ew", padx=(0, 14), pady=4)
    module.ttk.Label(form, text="礼物名称").grid(row=1, column=2, sticky="w", padx=(0, 8), pady=4)
    module.ttk.Entry(form, textvariable=gift_name_var).grid(row=1, column=3, sticky="ew", pady=4)
    module.ttk.Checkbutton(form, text="启用此规则", variable=enabled_var).grid(
        row=2, column=0, columnspan=2, sticky="w", pady=(5, 2)
    )
    module.ttk.Label(
        form,
        text="礼物 ID 和礼物名称至少填写一项；同一平台优先按礼物 ID 匹配，再按名称匹配。价值按单个礼物填写，连送会自动乘数量。",
        wraplength=720,
        justify="left",
    ).grid(row=2, column=2, columnspan=2, sticky="w", pady=(5, 2))

    footer = module.ttk.Frame(window, padding=(16, 4, 16, 14))
    footer.grid(row=2, column=0, sticky="ew")
    footer.columnconfigure(0, weight=1)
    module.ttk.Label(footer, textvariable=status_var).grid(row=0, column=0, sticky="w")
    buttons = module.ttk.Frame(footer)
    buttons.grid(row=0, column=1, sticky="e")

    def clear_form() -> None:
        selected_index[0] = None
        gift_id_var.set("")
        gift_name_var.set("")
        value_var.set("")
        enabled_var.set(True)
        try:
            tree.selection_remove(tree.selection())
        except Exception:
            pass

    def render() -> None:
        for item in tree.get_children():
            tree.delete(item)
        for index, row in enumerate(rules):
            tree.insert(
                "",
                "end",
                iid=str(index),
                values=(
                    "是" if bool(row.get("enabled", True)) else "否",
                    str(row.get("platform", "")),
                    str(row.get("gift_id", "")),
                    str(row.get("gift_name", "")),
                    str(row.get("value", "")),
                ),
            )
        status_var.set(f"当前 {len(rules)} 条自定义礼物价值规则。")

    def parse_form() -> dict[str, Any] | None:
        platform = platform_var.get().strip().lower()
        gift_id = gift_id_var.get().strip()
        gift_name = gift_name_var.get().strip()
        if not _PLATFORM_RE.fullmatch(platform):
            module.messagebox.showwarning("礼物兼容性", "平台标识格式无效。", parent=window)
            return None
        if not gift_id and not gift_name:
            module.messagebox.showwarning("礼物兼容性", "礼物 ID 和礼物名称至少填写一项。", parent=window)
            return None
        try:
            value = float(value_var.get().strip())
        except ValueError:
            module.messagebox.showwarning("礼物兼容性", "礼物价值必须是数字。", parent=window)
            return None
        if not math.isfinite(value) or value < 0:
            module.messagebox.showwarning("礼物兼容性", "礼物价值必须大于等于 0。", parent=window)
            return None
        normalized_value: float | int = int(value) if value.is_integer() else value
        return {
            "platform": platform,
            "gift_id": gift_id,
            "gift_name": gift_name,
            "value": normalized_value,
            "enabled": bool(enabled_var.get()),
        }

    def add_or_update() -> None:
        row = parse_form()
        if row is None:
            return
        index = selected_index[0]
        if index is None:
            rules.append(row)
            status_var.set("已新增规则，点击“保存全部”写入后端。")
        elif 0 <= index < len(rules):
            rules[index] = row
            status_var.set("已更新规则，点击“保存全部”写入后端。")
        clear_form()
        render()

    def delete_selected() -> None:
        selection = tree.selection()
        if not selection:
            module.messagebox.showwarning("礼物兼容性", "请先选择要删除的规则。", parent=window)
            return
        try:
            index = int(selection[0])
        except (TypeError, ValueError):
            return
        if 0 <= index < len(rules):
            rules.pop(index)
            clear_form()
            render()
            status_var.set("已删除规则，点击“保存全部”写入后端。")

    def on_select(_event: Any | None = None) -> None:
        selection = tree.selection()
        if not selection:
            return
        try:
            index = int(selection[0])
        except (TypeError, ValueError):
            return
        if not (0 <= index < len(rules)):
            return
        selected_index[0] = index
        row = rules[index]
        platform_var.set(str(row.get("platform", "bilibili")))
        gift_id_var.set(str(row.get("gift_id", "")))
        gift_name_var.set(str(row.get("gift_name", "")))
        value_var.set(str(row.get("value", "")))
        enabled_var.set(bool(row.get("enabled", True)))

    def load() -> None:
        status_var.set("正在读取礼物兼容性配置……")
        try:
            payload = windows_ui._plugin_request(panel, "/api/gifts/state", timeout=8.0)
            rows = payload.get("compatibility_rules", [])
            if not isinstance(rows, list):
                raise RuntimeError("后端返回的 compatibility_rules 格式无效")
            rules.clear()
            rules.extend(dict(row) for row in rows if isinstance(row, dict))
            values = list(_DEFAULT_PLATFORMS)
            for row in rules:
                name = str(row.get("platform", "") or "").strip().lower()
                if name and name not in values:
                    values.append(name)
            observed = payload.get("observed_catalog", [])
            if isinstance(observed, list):
                for item in observed:
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("platform", "") or "").strip().lower()
                    if name and name not in values:
                        values.append(name)
            platform_box.configure(values=tuple(values))
            clear_form()
            render()
        except Exception as exc:  # noqa: BLE001
            status_var.set(f"读取失败：{exc}")
            module.messagebox.showerror("礼物兼容性", f"读取失败：{exc}", parent=window)

    def save() -> None:
        status_var.set("正在保存礼物兼容性配置……")
        try:
            payload = windows_ui._plugin_request(
                panel,
                "/api/gifts/state",
                method="POST",
                payload={"rules": rules},
                timeout=8.0,
            )
            rows = payload.get("rules", [])
            if isinstance(rows, list):
                rules.clear()
                rules.extend(dict(row) for row in rows if isinstance(row, dict))
            render()
            status_var.set("礼物兼容性配置已保存，后续礼物事件立即按新价值计算。")
        except Exception as exc:  # noqa: BLE001
            status_var.set(f"保存失败：{exc}")
            module.messagebox.showerror("礼物兼容性", f"保存失败：{exc}", parent=window)

    tree.bind("<<TreeviewSelect>>", on_select)
    module.ttk.Button(buttons, text="新增 / 更新", command=add_or_update).pack(side="left", padx=(0, 6))
    module.ttk.Button(buttons, text="删除", command=delete_selected).pack(side="left", padx=(0, 6))
    module.ttk.Button(buttons, text="清空表单", command=clear_form).pack(side="left", padx=(0, 6))
    module.ttk.Button(buttons, text="刷新", command=load).pack(side="left", padx=(0, 6))
    module.ttk.Button(buttons, text="保存全部", command=save).pack(side="left")
    window.protocol("WM_DELETE_WINDOW", window.destroy)
    load()


def install_gift_compatibility_ui() -> bool:
    """Add a gift-value compatibility dialog to the Windows plugin manager."""
    from . import windows_ui

    with _PATCH_LOCK:
        current = getattr(windows_ui, "_build_plugin_manager_tab", None)
        if not callable(current):
            return False
        if bool(getattr(current, "_issue248_gift_compatibility", False)):
            return True

        @functools.wraps(current)
        def build_plugin_manager_tab(panel: Any, module: Any) -> None:
            current(panel, module)
            frame = _plugin_frame(panel)
            if frame is None:
                return
            if bool(getattr(panel, "_gift_compatibility_button_installed", False)):
                return
            actions = _find_action_parent(frame)
            if actions is None:
                return
            button = module.ttk.Button(
                actions,
                text="礼物兼容性",
                command=lambda: _open_gift_compatibility(panel, module, windows_ui),
            )
            button.pack(side="left", padx=(8, 0))
            panel._gift_compatibility_button = button
            panel._gift_compatibility_button_installed = True

        setattr(build_plugin_manager_tab, "_issue248_gift_compatibility", True)
        windows_ui._build_plugin_manager_tab = build_plugin_manager_tab
        return True


__all__ = ["install_gift_compatibility_ui"]
