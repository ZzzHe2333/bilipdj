from __future__ import annotations

import base64
import functools
import json
import threading
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import replace
from pathlib import Path
from typing import Any

_PATCH_LOCK = threading.RLock()
_MAX_PLUGIN_PACKAGE_BYTES = 1024 * 1024


def _clear_children(widget: Any) -> None:
    try:
        children = list(widget.winfo_children())
    except Exception:
        children = []
    for child in children:
        try:
            child.destroy()
        except Exception:
            pass


def _iter_widgets(widget: Any):
    yield widget
    try:
        children = list(widget.winfo_children())
    except Exception:
        children = []
    for child in children:
        yield from _iter_widgets(child)


def _find_label_frame(root: Any, text: str) -> Any | None:
    for widget in _iter_widgets(root):
        try:
            if str(widget.cget("text") or "").strip() == text:
                return widget
        except Exception:
            continue
    return None


def _api_base(panel: Any) -> str:
    try:
        port = str(panel.port_var.get()).strip() or "9816"
    except Exception:
        port = "9816"
    return f"http://127.0.0.1:{port}"


def _plugin_request(
    panel: Any,
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = 5.0,
) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        _api_base(panel) + path,
        data=data,
        headers=headers,
        method=method,
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        result = json.loads(response.read().decode("utf-8", errors="replace"))
    if not isinstance(result, dict):
        raise RuntimeError("后端返回了无效的插件管理数据")
    if result.get("status") == "error":
        raise RuntimeError(str(result.get("message") or "插件管理操作失败"))
    return result


def _install_stable_update_layout() -> None:
    from . import update_page, update_version_selector

    current_bind = getattr(update_page, "_bind_stable_scroll_region", None)
    if callable(current_bind) and not bool(getattr(current_bind, "_issue180_stable", False)):
        def stable_bind(canvas: Any, inner: Any, window_id: int) -> None:
            state: dict[str, Any] = {
                "job": None,
                "pending_width": None,
                "width": -1,
                "bbox": None,
            }

            def flush_layout() -> None:
                state["job"] = None
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

            def schedule(_event: Any | None = None) -> None:
                if state["job"] is None:
                    state["job"] = canvas.after_idle(flush_layout)

            def resize(event: Any) -> None:
                state["pending_width"] = int(getattr(event, "width", 1) or 1)
                schedule()

            inner.bind("<Configure>", schedule)
            canvas.bind("<Configure>", resize)
            schedule()

        setattr(stable_bind, "_issue180_stable", True)
        update_page._bind_stable_scroll_region = stable_bind

    current_candidates = getattr(update_version_selector, "build_version_candidates", None)
    if callable(current_candidates) and not bool(getattr(current_candidates, "_issue180_labels", False)):
        @functools.wraps(current_candidates)
        def labeled_candidates(*args: Any, **kwargs: Any):
            candidates = list(current_candidates(*args, **kwargs))
            labeled = []
            for candidate in candidates:
                try:
                    if str(candidate.source) == "local" and candidate.backup is not None:
                        label = f"本地备份｜v{candidate.version}｜{candidate.backup.created_at}"
                    else:
                        label = f"云端最新｜v{candidate.version}"
                    candidate = replace(candidate, label=label)
                except Exception:
                    pass
                labeled.append(candidate)
            return labeled

        setattr(labeled_candidates, "_issue180_labels", True)
        update_version_selector.build_version_candidates = labeled_candidates

    current_build = getattr(update_page, "build_update_tab", None)
    if callable(current_build) and not bool(getattr(current_build, "_issue180_polish", False)):
        @functools.wraps(current_build)
        def build_update_tab_polished(app: Any, frame: Any, *args: Any, **kwargs: Any) -> Any:
            result = current_build(app, frame, *args, **kwargs)
            _polish_update_page(app, frame, update_page)
            return result

        setattr(build_update_tab_polished, "_issue180_polish", True)
        update_page.build_update_tab = build_update_tab_polished


def _polish_update_page(app: Any, frame: Any, module: Any) -> None:
    if bool(getattr(app, "_issue180_update_polished", False)):
        return
    update_frame = _find_label_frame(frame, "检测更新与更新内容")
    if update_frame is None:
        return

    # Reserve one fixed row for selected-version metadata. Moving all following
    # rows once prevents status text changes from shifting the action controls.
    direct_children = list(update_frame.winfo_children())
    for child in direct_children:
        try:
            info = child.grid_info()
            row = int(info.get("row", 0))
        except Exception:
            continue
        if row >= 2:
            try:
                child.grid_configure(row=row + 1)
            except Exception:
                pass

    app.update_selection_summary_var = module.tk.StringVar(value="来源：尚未检查 · 目标版本：-- · 可用操作：--")
    selection_slot = module.ttk.Frame(update_frame, height=30)
    selection_slot.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(3, 1))
    selection_slot.grid_propagate(False)
    module.ttk.Label(
        selection_slot,
        textvariable=app.update_selection_summary_var,
        anchor="w",
        justify="left",
    ).place(x=0, y=0, relwidth=1.0, relheight=1.0)

    progress = getattr(app, "_update_progress", None)
    if progress is not None:
        try:
            progress.grid_configure(column=0, columnspan=1, sticky="ew", padx=(0, 8))
        except Exception:
            pass
        progress_row = 3
        try:
            progress_row = int(progress.grid_info().get("row", 3))
        except Exception:
            pass
        app.update_progress_text_var = module.tk.StringVar(value="0%")
        module.ttk.Label(
            update_frame,
            textvariable=app.update_progress_text_var,
            width=7,
            anchor="e",
        ).grid(row=progress_row, column=1, sticky="e")

        def refresh_progress(*_args: Any) -> None:
            try:
                value = max(0.0, min(100.0, float(app.update_progress_var.get())))
            except Exception:
                value = 0.0
            app.update_progress_text_var.set(f"{value:.0f}%")

        app._issue180_progress_trace = app.update_progress_var.trace_add("write", refresh_progress)
        refresh_progress()

    for name in ("_update_check_button", "_update_full_button", "_update_incremental_button"):
        button = getattr(app, name, None)
        if button is not None:
            try:
                button.configure(width=12)
            except Exception:
                pass

    def refresh_selection(*_args: Any) -> None:
        mapping = getattr(app, "_update_version_candidates", {})
        label = str(app.update_version_var.get()) if hasattr(app, "update_version_var") else ""
        candidate = mapping.get(label) if isinstance(mapping, dict) else None
        if candidate is None:
            app.update_selection_summary_var.set("来源：尚未检查 · 目标版本：-- · 可用操作：--")
            return
        source = str(getattr(candidate, "source", ""))
        version = str(getattr(candidate, "version", "") or "--")
        if source == "local":
            backup = getattr(candidate, "backup", None)
            created = str(getattr(backup, "created_at", "") or "时间未知")
            app.update_selection_summary_var.set(
                f"来源：本地备份 · 目标版本：v{version} · 备份时间：{created} · 操作：恢复旧版"
            )
        else:
            app.update_selection_summary_var.set(
                f"来源：云端 Release · 目标版本：v{version} · 操作：全量更新 / 增量更新"
            )

    app._issue180_version_trace = app.update_version_var.trace_add("write", refresh_selection)
    app._issue180_refresh_update_selection = refresh_selection
    refresh_selection()
    app._issue180_update_polished = True


def _build_plugin_manager_tab(panel: Any, module: Any) -> None:
    notebook = getattr(panel, "settings_notebook", None)
    if notebook is None or bool(getattr(panel, "_issue180_plugin_manager_tab", False)):
        return
    try:
        existing = [str(notebook.tab(tab_id, "text") or "").strip() for tab_id in notebook.tabs()]
    except Exception:
        existing = []
    if "插件管理" in existing:
        panel._issue180_plugin_manager_tab = True
        return

    frame = module.ttk.Frame(notebook, padding=(20, 16))
    notebook.add(frame, text="插件管理")
    frame.columnconfigure(0, weight=1)
    frame.rowconfigure(3, weight=1)

    module.ttk.Label(frame, text="插件管理器", font=("Microsoft YaHei UI", 15, "bold")).grid(
        row=0, column=0, sticky="w"
    )
    module.ttk.Label(
        frame,
        text="管理 .bilipdj-plugin 本地插件。安装阶段不会执行插件代码；外部插件只有启用后才会加载。",
        wraplength=800,
        justify="left",
    ).grid(row=1, column=0, sticky="w", pady=(5, 10))

    summary_var = module.tk.StringVar(value="尚未读取插件状态")
    panel._issue180_plugin_status_var = summary_var
    module.ttk.Label(frame, textvariable=summary_var, wraplength=800, justify="left").grid(
        row=2, column=0, sticky="ew", pady=(0, 8)
    )

    list_frame = module.ttk.Frame(frame)
    list_frame.grid(row=3, column=0, sticky="nsew")
    list_frame.columnconfigure(0, weight=1)
    list_frame.rowconfigure(0, weight=1)
    columns = ("source", "status", "version", "runtime", "platform", "permissions", "trust")
    tree = module.ttk.Treeview(list_frame, columns=columns, show="tree headings", height=10, selectmode="browse")
    tree.heading("#0", text="插件")
    tree.heading("source", text="来源")
    tree.heading("status", text="状态")
    tree.heading("version", text="版本")
    tree.heading("runtime", text="运行时")
    tree.heading("platform", text="平台")
    tree.heading("permissions", text="权限")
    tree.heading("trust", text="校验/信任")
    tree.column("#0", width=190, minwidth=140, stretch=True)
    tree.column("source", width=60, anchor="center", stretch=False)
    tree.column("status", width=70, anchor="center", stretch=False)
    tree.column("version", width=80, anchor="center", stretch=False)
    tree.column("runtime", width=88, anchor="center", stretch=False)
    tree.column("platform", width=90, anchor="center", stretch=False)
    tree.column("permissions", width=180, minwidth=120, stretch=True)
    tree.column("trust", width=130, minwidth=100, stretch=True)
    scroll = module.ttk.Scrollbar(list_frame, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=scroll.set)
    tree.grid(row=0, column=0, sticky="nsew")
    scroll.grid(row=0, column=1, sticky="ns")
    panel._issue180_plugin_tree = tree
    panel._issue180_plugins_by_id = {}

    actions = module.ttk.Frame(frame)
    actions.grid(row=4, column=0, sticky="ew", pady=(10, 0))
    install_button = module.ttk.Button(actions, text="安装插件")
    refresh_button = module.ttk.Button(actions, text="刷新状态")
    toggle_button = module.ttk.Button(actions, text="启用 / 禁用", state="disabled")
    verify_button = module.ttk.Button(actions, text="校验完整性", state="disabled")
    config_button = module.ttk.Button(actions, text="打开配置", state="disabled")
    uninstall_button = module.ttk.Button(actions, text="卸载", state="disabled")
    for index, button in enumerate((install_button, refresh_button, toggle_button, verify_button, config_button, uninstall_button)):
        button.pack(side="left", padx=(0 if index == 0 else 8, 0))

    safety = module.ttk.LabelFrame(frame, text="安全说明", padding=10)
    safety.grid(row=5, column=0, sticky="ew", pady=(12, 0))
    module.ttk.Label(
        safety,
        text="JavaScript 插件由 Host 强制权限；第三方 Python 插件属于同进程全信任代码。未知来源或无法校验的插件不要启用。",
        wraplength=790,
        justify="left",
    ).grid(row=0, column=0, sticky="w")

    def selected_plugin() -> dict[str, Any] | None:
        selection = tree.selection()
        if not selection:
            return None
        plugin_id = str(selection[0])
        plugin = panel._issue180_plugins_by_id.get(plugin_id)
        return plugin if isinstance(plugin, dict) else None

    def update_action_state(_event: Any | None = None) -> None:
        plugin = selected_plugin()
        external = bool(plugin and str(plugin.get("source", "")) == "external")
        state = "normal" if external else "disabled"
        toggle_button.configure(state=state)
        verify_button.configure(state=state)
        uninstall_button.configure(state=state)
        config_button.configure(state="normal" if plugin else "disabled")
        if external and plugin:
            toggle_button.configure(text="禁用插件" if bool(plugin.get("enabled")) else "启用插件")
        else:
            toggle_button.configure(text="启用 / 禁用")

    def render(payload: dict[str, Any]) -> None:
        plugins = payload.get("plugins", [])
        plugins = plugins if isinstance(plugins, list) else []
        panel._issue180_plugins_by_id = {
            str(item.get("id", "")): item
            for item in plugins
            if isinstance(item, dict) and str(item.get("id", ""))
        }
        for item in tree.get_children():
            tree.delete(item)
        for plugin_id, plugin in panel._issue180_plugins_by_id.items():
            source = "内置" if str(plugin.get("source", "")) == "builtin" else "外部"
            status = "已启用" if bool(plugin.get("enabled")) else "已禁用"
            if plugin.get("available") is False:
                status = "不可用"
            permissions = plugin.get("permissions", [])
            permissions_text = ", ".join(str(value) for value in permissions) if isinstance(permissions, list) and permissions else "无"
            verified = "校验失败" if plugin.get("verified") is False else "已校验"
            signature = str(plugin.get("signature_status", "") or "")
            trust = f"{verified} / {signature}" if signature else verified
            name = str(plugin.get("name") or plugin_id)
            tree.insert(
                "",
                "end",
                iid=plugin_id,
                text=name,
                values=(
                    source,
                    status,
                    str(plugin.get("version") or "builtin"),
                    str(plugin.get("runtime") or "--"),
                    str(plugin.get("platform") or "--"),
                    permissions_text,
                    trust,
                ),
            )
        supported = payload.get("supported_permissions", [])
        supported_text = ", ".join(str(v) for v in supported) if isinstance(supported, list) and supported else "无"
        summary_var.set(
            f"BiliPDJ {payload.get('bilipdj_version', '')} · Plugin API v{payload.get('plugin_api', 1)} · "
            f"插件 {len(plugins)} 个 · 支持权限：{supported_text}"
        )
        update_action_state()

    def refresh() -> None:
        summary_var.set("正在读取插件状态……")

        def worker() -> None:
            try:
                payload = _plugin_request(panel, "/api/plugins/manage")
            except Exception as exc:  # noqa: BLE001
                panel.root.after(0, lambda error=str(exc): summary_var.set(f"读取失败：{error}"))
                return
            panel.root.after(0, lambda: render(payload))

        threading.Thread(target=worker, name="bilipdj-win-plugin-refresh", daemon=True).start()

    def run_action(path: str, plugin_id: str, pending_text: str) -> None:
        summary_var.set(pending_text)

        def worker() -> None:
            try:
                _plugin_request(panel, path, method="POST", payload={"id": plugin_id})
                payload = _plugin_request(panel, "/api/plugins/manage")
            except Exception as exc:  # noqa: BLE001
                panel.root.after(0, lambda error=str(exc): summary_var.set(f"操作失败：{error}"))
                return
            panel.root.after(0, lambda: render(payload))

        threading.Thread(target=worker, name="bilipdj-win-plugin-action", daemon=True).start()

    def toggle() -> None:
        plugin = selected_plugin()
        if not plugin:
            return
        plugin_id = str(plugin.get("id", ""))
        if bool(plugin.get("enabled")):
            run_action("/api/plugins/disable", plugin_id, f"正在禁用 {plugin_id}……")
        else:
            run_action("/api/plugins/enable", plugin_id, f"正在启用 {plugin_id}……")

    def verify() -> None:
        plugin = selected_plugin()
        if not plugin:
            return
        plugin_id = str(plugin.get("id", ""))
        run_action("/api/plugins/verify", plugin_id, f"正在校验 {plugin_id}……")

    def uninstall() -> None:
        plugin = selected_plugin()
        if not plugin:
            return
        plugin_id = str(plugin.get("id", ""))
        if not module.messagebox.askyesno(
            "卸载插件",
            f"确定卸载插件 {plugin.get('name') or plugin_id}？\n插件私有配置/数据的处理由当前插件管理器策略决定。",
            parent=panel.root,
        ):
            return
        run_action("/api/plugins/uninstall", plugin_id, f"正在卸载 {plugin_id}……")

    def install() -> None:
        filename = module.filedialog.askopenfilename(
            title="选择 BiliPDJ 插件包",
            filetypes=(("BiliPDJ 插件", "*.bilipdj-plugin"), ("所有文件", "*.*")),
        )
        if not filename:
            return
        path = Path(filename)
        if path.suffix.casefold() != ".bilipdj-plugin":
            module.messagebox.showerror("插件包无效", "文件扩展名必须为 .bilipdj-plugin。", parent=panel.root)
            return
        try:
            size = path.stat().st_size
        except OSError as exc:
            module.messagebox.showerror("读取失败", str(exc), parent=panel.root)
            return
        if size > _MAX_PLUGIN_PACKAGE_BYTES:
            module.messagebox.showerror("插件包过大", "插件包不能超过 1 MiB。", parent=panel.root)
            return
        allow_unsigned = module.messagebox.askyesno(
            "本次未签名插件授权",
            "如果这个插件没有可信 Ed25519 签名，是否允许本次安装？\n\n"
            "选择“是”只授权这一次安装。第三方 Python 插件属于全信任代码，仅安装来源可信的插件。",
            parent=panel.root,
        )
        summary_var.set(f"正在安装 {path.name}……")

        def worker() -> None:
            try:
                encoded = base64.b64encode(path.read_bytes()).decode("ascii")
                _plugin_request(
                    panel,
                    "/api/plugins/install",
                    method="POST",
                    payload={
                        "filename": path.name,
                        "data_base64": encoded,
                        "allow_unsigned": bool(allow_unsigned),
                    },
                    timeout=12.0,
                )
                payload = _plugin_request(panel, "/api/plugins/manage")
            except Exception as exc:  # noqa: BLE001
                panel.root.after(0, lambda error=str(exc): summary_var.set(f"安装失败：{error}"))
                return
            panel.root.after(0, lambda: render(payload))

        threading.Thread(target=worker, name="bilipdj-win-plugin-install", daemon=True).start()

    def open_config() -> None:
        plugin = selected_plugin()
        if not plugin:
            return
        plugin_id = urllib.parse.quote(str(plugin.get("id", "")), safe="")
        webbrowser.open(f"{_api_base(panel)}/plugin-config?id={plugin_id}")

    tree.bind("<<TreeviewSelect>>", update_action_state)
    install_button.configure(command=install)
    refresh_button.configure(command=refresh)
    toggle_button.configure(command=toggle)
    verify_button.configure(command=verify)
    config_button.configure(command=open_config)
    uninstall_button.configure(command=uninstall)
    panel._issue180_plugin_refresh = refresh
    panel._issue180_plugin_manager_tab = True
    panel.root.after(600, refresh)


def _build_permissions_page(panel: Any, frame: Any, module: Any) -> None:
    _clear_children(frame)
    frame.columnconfigure(0, weight=1)
    frame.columnconfigure(1, weight=1)
    panel._quanxian_text = {}

    module.ttk.Label(frame, text="权限与身份", font=("Microsoft YaHei UI", 15, "bold")).grid(
        row=0, column=0, columnspan=2, sticky="w", pady=(0, 3)
    )
    module.ttk.Label(
        frame,
        text="每行填写一个用户名。最高管理员/管理员用于管理命令；舰长与成员控制普通排队权限；黑名单优先阻止所有弹幕指令。",
        wraplength=800,
        justify="left",
    ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 10))

    definitions = [
        ("super_admin", "最高管理员", "可新增/删除管理员并执行全部管理命令"),
        ("admin", "管理员", "除新增/删除管理员外，可执行其他管理命令"),
        ("jianzhang", "舰长", "用于舰长插队等舰长级权限"),
        ("member", "成员", "普通观众的自助排队、取消和修改身份名单"),
        ("blacklist", "黑名单", "优先禁止触发任何弹幕指令，不能与管理员身份同时生效"),
    ]
    for index, (key, title, description) in enumerate(definitions):
        if key == "blacklist":
            row, column, columnspan = 4, 0, 2
        else:
            row, column, columnspan = 2 + index // 2, index % 2, 1
        card = module.ttk.LabelFrame(frame, text=title, padding=10)
        card.grid(
            row=row,
            column=column,
            columnspan=columnspan,
            sticky="nsew",
            padx=(0 if column == 0 else 6, 6 if column == 0 else 0),
            pady=5,
        )
        card.columnconfigure(0, weight=1)
        module.ttk.Label(card, text=description, wraplength=370 if columnspan == 1 else 790, justify="left").grid(
            row=0, column=0, sticky="w", pady=(0, 6)
        )
        editor = module.tk.Text(card, height=4 if key != "blacklist" else 3, wrap="word", undo=True)
        scrollbar = module.ttk.Scrollbar(card, orient="vertical", command=editor.yview)
        editor.configure(yscrollcommand=scrollbar.set)
        editor.grid(row=1, column=0, sticky="nsew")
        scrollbar.grid(row=1, column=1, sticky="ns")
        panel._quanxian_text[key] = editor
        text_widgets = getattr(panel, "_all_text_widgets", None)
        if isinstance(text_widgets, list):
            text_widgets.append(editor)

    status_var = module.tk.StringVar(value="修改后点击保存；刷新会重新读取当前权限配置。")
    panel._issue180_permission_status_var = status_var
    footer = module.ttk.Frame(frame)
    footer.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(10, 0))
    module.ttk.Label(footer, textvariable=status_var).pack(side="left", fill="x", expand=True)

    def save() -> None:
        status_var.set("正在保存权限……")
        try:
            panel._save_quanxian()
        except Exception as exc:
            status_var.set(f"保存失败：{exc}")
        else:
            status_var.set("权限保存请求已完成。")

    def refresh() -> None:
        status_var.set("正在刷新权限……")
        try:
            panel._load_quanxian()
        except Exception as exc:
            status_var.set(f"刷新失败：{exc}")
        else:
            status_var.set("已刷新当前权限配置。")

    module.ttk.Button(footer, text="保存权限", command=save, width=12).pack(side="right", padx=(8, 0))
    module.ttk.Button(footer, text="刷新权限", command=refresh, width=12).pack(side="right")
    panel._load_quanxian()


def _build_performance_page(panel: Any, frame: Any, module: Any) -> None:
    _clear_children(frame)
    frame.columnconfigure(0, weight=1)
    frame.columnconfigure(1, weight=1)
    panel._perf_vars = {}

    module.ttk.Label(frame, text="运行性能", font=("Microsoft YaHei UI", 15, "bold")).grid(
        row=0, column=0, columnspan=2, sticky="w", pady=(0, 3)
    )
    module.ttk.Label(
        frame,
        text="以下指标只用于观察本机运行状态，每 2 秒自动刷新；读取失败不会影响排队服务。",
        wraplength=800,
        justify="left",
    ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 12))

    metrics = [
        ("cpu", "CPU 使用率", "当前系统 CPU 负载"),
        ("mem", "本进程内存", "Windows 控制台进程占用"),
        ("sysmem", "系统内存", "整机内存使用情况"),
        ("disk", "程序目录", "BiliPDJ 所在磁盘空间"),
    ]
    for index, (key, title, hint) in enumerate(metrics):
        row = 2 + index // 2
        column = index % 2
        card = module.ttk.LabelFrame(frame, text=title, padding=(14, 10))
        card.grid(
            row=row,
            column=column,
            sticky="nsew",
            padx=(0 if column == 0 else 6, 6 if column == 0 else 0),
            pady=6,
        )
        card.columnconfigure(0, weight=1)
        value_var = module.tk.StringVar(value="读取中…")
        panel._perf_vars[key] = value_var
        module.ttk.Label(card, textvariable=value_var, font=("Microsoft YaHei UI", 16, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        module.ttk.Label(card, text=hint, wraplength=350, justify="left").grid(
            row=1, column=0, sticky="w", pady=(5, 0)
        )

    module.ttk.Label(
        frame,
        text="性能页刷新在后台线程中进行；切换页面不会启动额外的业务服务。",
        wraplength=800,
    ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(10, 0))
    panel.root.after(500, panel._refresh_perf)


def patch_control_panel_issue180(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    if Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_issue180_windows_ui_installed", False)):
            return True
        _install_stable_update_layout()

        original_settings = getattr(panel_class, "_build_settings_tab", None)
        original_permissions = getattr(panel_class, "_build_quanxian_tab", None)
        original_performance = getattr(panel_class, "_build_perf_tab", None)
        if not all(callable(item) for item in (original_settings, original_permissions, original_performance)):
            return False

        @functools.wraps(original_settings)
        def build_settings(self: Any, frame: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_settings(self, frame, *args, **kwargs)
            _build_plugin_manager_tab(self, module)
            return result

        def build_permissions(self: Any, frame: Any) -> None:
            _build_permissions_page(self, frame, module)

        def build_performance(self: Any, frame: Any) -> None:
            _build_performance_page(self, frame, module)

        setattr(panel_class, "_build_settings_tab", build_settings)
        setattr(panel_class, "_build_quanxian_tab", build_permissions)
        setattr(panel_class, "_build_perf_tab", build_performance)
        setattr(panel_class, "_issue180_windows_ui_installed", True)
        return True


__all__ = ["patch_control_panel_issue180"]
