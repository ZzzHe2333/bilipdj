from __future__ import annotations

import functools
import json
import re
import threading
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path
from typing import Any

REPOSITORY_URL = "https://github.com/ZzzHe2333/bilipdj"
RELEASES_URL = "https://github.com/ZzzHe2333/bilipdj/releases"
_PATCH_LOCK = threading.RLock()


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


def _build_about_project(panel: Any, frame: Any, module: Any) -> None:
    _clear_children(frame)
    frame.columnconfigure(0, weight=1)
    card = module.ttk.Frame(frame, padding=(30, 24))
    card.grid(row=0, column=0, sticky="nsew")
    card.columnconfigure(0, weight=1)
    module.ttk.Label(
        card,
        text="关于项目",
        font=("Microsoft YaHei UI", 18, "bold"),
    ).grid(row=0, column=0, sticky="w", pady=(0, 12))
    module.ttk.Label(
        card,
        text="弹幕排队姬是面向 Bilibili / 抖音直播间的弹幕排队管理工具。后端负责统一业务状态，Windows 与 Web 负责管理和展示。",
        wraplength=780,
        justify="left",
    ).grid(row=1, column=0, sticky="w", pady=(0, 16))
    module.ttk.Label(card, text=f"当前版本：v{module.APP_VERSION}").grid(row=2, column=0, sticky="w", pady=4)
    module.ttk.Label(card, text=f"GitHub：{REPOSITORY_URL}").grid(row=3, column=0, sticky="w", pady=4)
    module.ttk.Label(card, text=f"发行包：{RELEASES_URL}").grid(row=4, column=0, sticky="w", pady=4)
    actions = module.ttk.Frame(card)
    actions.grid(row=5, column=0, sticky="w", pady=(16, 0))
    module.ttk.Button(actions, text="打开 GitHub 仓库", command=lambda: webbrowser.open(REPOSITORY_URL)).pack(side="left", padx=(0, 8))
    module.ttk.Button(actions, text="打开发行包页面", command=lambda: webbrowser.open(RELEASES_URL)).pack(side="left")


def _build_support_page(panel: Any, frame: Any, module: Any) -> None:
    _clear_children(frame)
    frame.columnconfigure(0, weight=1)
    try:
        from .support_us import SUPPORT_COPY, SUPPORT_URL, _make_qr_photo
    except Exception:  # pragma: no cover - fallback only
        SUPPORT_COPY = "项目免费开源使用，感谢支持。"
        SUPPORT_URL = REPOSITORY_URL
        _make_qr_photo = None

    card = module.ttk.Frame(frame, padding=(36, 28))
    card.grid(row=0, column=0, sticky="nsew")
    card.columnconfigure(0, weight=1)
    module.ttk.Label(card, text="支持我们", font=("Microsoft YaHei UI", 18, "bold")).grid(row=0, column=0, pady=(0, 10))
    module.ttk.Label(card, text=SUPPORT_COPY, justify="center", wraplength=700).grid(row=1, column=0, pady=(0, 18))
    if callable(_make_qr_photo):
        try:
            photo = _make_qr_photo(panel.root, size=220)
            panel._issue79_support_qr = photo
            module.ttk.Label(card, image=photo).grid(row=2, column=0, pady=(0, 16))
        except Exception:
            pass
    module.ttk.Button(card, text="打开支持页面", command=lambda: webbrowser.open(SUPPORT_URL)).grid(row=3, column=0, ipadx=16, ipady=4)


def _clean_nav_label(text: Any) -> str:
    return re.sub(r"^\s*\d+\s*", "", str(text or "")).strip()


def _normalize_navigation(panel: Any, module: Any) -> None:
    items = list(getattr(panel, "_nav_items", []) or [])
    pages = list(getattr(panel, "_content_pages", []) or [])
    for _row, button, _indicator in items:
        try:
            button.configure(text=_clean_nav_label(button.cget("text")))
        except Exception:
            pass

    # The current layout contains exactly these last three logical pages:
    # updater, support page and about page. ui_finish previously renamed the
    # final two to “08 更新软件 / 09 关于”, which is why the screenshots show
    # duplicated update entries. Make the intended information architecture
    # explicit and rebuild the final two pages accordingly.
    if len(items) >= 9 and len(pages) >= 9:
        desired = {
            0: "日志",
            1: "当前排队",
            2: "设置",
            3: "透明窗口",
            4: "权限",
            5: "性能",
            6: "更新软件",
            7: "关于项目",
            8: "支持我们",
        }
        for index, label in desired.items():
            try:
                items[index][1].configure(text=label)
            except Exception:
                pass
        _build_about_project(panel, pages[7], module)
        _build_support_page(panel, pages[8], module)
        for index in range(9, len(items)):
            try:
                items[index][0].pack_forget()
            except Exception:
                pass


def _api_url(panel: Any, path: str) -> str:
    try:
        port = str(panel.port_var.get()).strip() or "9816"
    except Exception:
        port = "9816"
    return f"http://127.0.0.1:{port}{path}"


def _platform_request(panel: Any, method: str = "GET", payload: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(_api_url(panel, "/api/platforms/active"), data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=3.0) as response:
        result = json.loads(response.read().decode("utf-8", errors="replace"))
    return result if isinstance(result, dict) else {}


def _install_active_platform_tab(panel: Any, module: Any) -> None:
    notebook = getattr(panel, "settings_notebook", None)
    if notebook is None or bool(getattr(panel, "_issue79_active_platform_tab", False)):
        return
    try:
        existing = [str(notebook.tab(tab_id, "text")) for tab_id in notebook.tabs()]
    except Exception:
        existing = []
    if "激活平台" in existing:
        panel._issue79_active_platform_tab = True
        return

    frame = module.ttk.Frame(notebook, padding=(20, 18))
    try:
        notebook.insert(2, frame, text="激活平台")
    except Exception:
        notebook.add(frame, text="激活平台")
    frame.columnconfigure(0, weight=1)

    module.ttk.Label(frame, text="激活平台", font=("Microsoft YaHei UI", 15, "bold")).grid(row=0, column=0, sticky="w")
    module.ttk.Label(
        frame,
        text="可同时接收多个平台的弹幕并进入同一个排队系统。目前每个平台只允许配置一个直播间，不支持单个平台同时监听多个直播间。",
        wraplength=790,
        justify="left",
    ).grid(row=1, column=0, sticky="w", pady=(6, 16))

    vars_map = {
        "bilibili": module.tk.BooleanVar(value=True),
        "douyin": module.tk.BooleanVar(value=False),
    }
    panel._issue79_platform_vars = vars_map
    choices = module.ttk.LabelFrame(frame, text="弹幕流", padding=14)
    choices.grid(row=2, column=0, sticky="ew")
    module.ttk.Checkbutton(choices, text="Bilibili（一个直播间）", variable=vars_map["bilibili"]).grid(row=0, column=0, sticky="w", pady=5)
    module.ttk.Checkbutton(choices, text="抖音（一个直播间）", variable=vars_map["douyin"]).grid(row=1, column=0, sticky="w", pady=5)
    module.ttk.Label(choices, text="虎牙 / 快手 / 斗鱼 / 视频号：当前仅预留配置，尚未接入弹幕流。", wraplength=720).grid(row=2, column=0, sticky="w", pady=(8, 0))

    status_var = module.tk.StringVar(value="尚未读取后端状态")
    panel._issue79_platform_status_var = status_var
    module.ttk.Label(frame, textvariable=status_var, wraplength=780).grid(row=3, column=0, sticky="w", pady=(12, 8))
    actions = module.ttk.Frame(frame)
    actions.grid(row=4, column=0, sticky="w")

    def apply_payload(payload: dict[str, Any]) -> None:
        active = payload.get("active", []) if isinstance(payload, dict) else []
        active_set = {str(value) for value in active} if isinstance(active, list) else set()
        vars_map["bilibili"].set("bilibili" in active_set)
        vars_map["douyin"].set("douyin" in active_set)
        runtime = payload.get("runtime", {}) if isinstance(payload, dict) else {}
        platform_states = runtime.get("platforms", {}) if isinstance(runtime, dict) else {}
        connected = [name for name, state in platform_states.items() if isinstance(state, dict) and state.get("connected")]
        status_var.set(f"已激活：{', '.join(active) if active else '无'}；在线：{', '.join(connected) if connected else '暂无'}")

    def refresh() -> None:
        status_var.set("正在读取后端平台状态……")
        def worker() -> None:
            try:
                payload = _platform_request(panel)
            except Exception as exc:  # noqa: BLE001
                panel.root.after(0, lambda: status_var.set(f"读取失败：{exc}"))
                return
            panel.root.after(0, lambda: apply_payload(payload))
        threading.Thread(target=worker, name="bilipdj-platform-refresh", daemon=True).start()

    def save() -> None:
        active = [name for name, var in vars_map.items() if bool(var.get())]
        status_var.set("正在保存并重连弹幕流……")
        def worker() -> None:
            try:
                payload = _platform_request(panel, "POST", {"active": active})
            except Exception as exc:  # noqa: BLE001
                panel.root.after(0, lambda: status_var.set(f"保存失败：{exc}"))
                return
            panel.root.after(0, lambda: apply_payload(payload))
        threading.Thread(target=worker, name="bilipdj-platform-save", daemon=True).start()

    module.ttk.Button(actions, text="保存激活平台", command=save).pack(side="left", padx=(0, 8))
    module.ttk.Button(actions, text="刷新状态", command=refresh).pack(side="left")
    panel._issue79_active_platform_tab = True
    panel.root.after(500, refresh)


def _placeholder_entry(module: Any, parent: Any, placeholder: str, *, width: int = 44) -> tuple[Any, dict[str, bool]]:
    state = {"placeholder": True}
    entry = module.tk.Entry(
        parent,
        width=width,
        relief="flat",
        bd=0,
        highlightthickness=1,
        highlightbackground="#c9c1ef",
        highlightcolor="#6c5ce7",
        bg="#ffffff",
        fg="#9992a8",
        insertbackground="#312a46",
        font=("Microsoft YaHei UI", 10),
    )
    entry.insert(0, placeholder)

    def on_focus_in(_event: Any) -> None:
        if state["placeholder"]:
            entry.delete(0, "end")
            entry.configure(fg="#312a46")
            state["placeholder"] = False

    def on_focus_out(_event: Any) -> None:
        if not entry.get().strip():
            entry.delete(0, "end")
            entry.insert(0, placeholder)
            entry.configure(fg="#9992a8")
            state["placeholder"] = True

    entry.bind("<FocusIn>", on_focus_in)
    entry.bind("<FocusOut>", on_focus_out)
    return entry, state


def _queue_insert_dialog(panel: Any, module: Any) -> None:
    idx = panel._get_selected_index() or 0
    dialog = module.tk.Toplevel(panel.root)
    dialog.title("在下方新增")
    dialog.geometry("430x245")
    dialog.resizable(False, False)
    dialog.configure(bg="#f4f1ff")
    dialog.transient(panel.root)
    dialog.grab_set()

    header = module.tk.Frame(dialog, bg="#6c5ce7", height=48)
    header.pack(fill="x")
    header.pack_propagate(False)
    module.tk.Label(header, text="新增排队项", bg="#6c5ce7", fg="#ffffff", font=("Microsoft YaHei UI", 13, "bold")).pack(anchor="w", padx=18, pady=11)

    body = module.tk.Frame(dialog, bg="#f4f1ff")
    body.pack(fill="both", expand=True, padx=18, pady=14)
    module.tk.Label(body, text="用户名 *", bg="#f4f1ff", fg="#443b67", font=("Microsoft YaHei UI", 10, "bold")).grid(row=0, column=0, sticky="w", pady=(0, 5))
    username_entry, username_state = _placeholder_entry(module, body, "填写用户名")
    username_entry.grid(row=1, column=0, sticky="ew", ipady=7)
    module.tk.Label(body, text="排队内容", bg="#f4f1ff", fg="#443b67", font=("Microsoft YaHei UI", 10, "bold")).grid(row=2, column=0, sticky="w", pady=(12, 5))
    content_entry, content_state = _placeholder_entry(module, body, "填写排队内容（可选）")
    content_entry.grid(row=3, column=0, sticky="ew", ipady=7)
    body.columnconfigure(0, weight=1)

    error_var = module.tk.StringVar(value="")
    module.tk.Label(body, textvariable=error_var, bg="#f4f1ff", fg="#c0394b", font=("Microsoft YaHei UI", 9)).grid(row=4, column=0, sticky="w", pady=(7, 0))

    buttons = module.tk.Frame(dialog, bg="#f4f1ff")
    buttons.pack(fill="x", padx=18, pady=(0, 15))

    def value(entry: Any, state: dict[str, bool]) -> str:
        return "" if state["placeholder"] else str(entry.get() or "").strip()

    def submit() -> None:
        username = value(username_entry, username_state)
        content = value(content_entry, content_state)
        if not username:
            error_var.set("用户名为必填项。")
            username_entry.focus_set()
            return
        combined = username if not content else f"{username} {content}"
        payload = {"after": idx, "entry": combined, "username": username, "content": content}
        dialog.destroy()
        threading.Thread(
            target=panel._queue_backend_op,
            args=("/api/queue/insert", payload, f"已在第{idx}位后新增"),
            daemon=True,
        ).start()

    module.tk.Button(buttons, text="取消", command=dialog.destroy, bg="#e8e4f7", fg="#51486d", activebackground="#ddd7f3", relief="flat", padx=20, pady=7).pack(side="right")
    module.tk.Button(buttons, text="新增", command=submit, bg="#6c5ce7", fg="#ffffff", activebackground="#594bc7", activeforeground="#ffffff", relief="flat", padx=24, pady=7).pack(side="right", padx=(0, 8))
    dialog.bind("<Return>", lambda _event: submit())
    dialog.bind("<Escape>", lambda _event: dialog.destroy())
    username_entry.focus_set()


def patch_control_panel_issue79(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    if Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_issue79_features_installed", False)):
            return True
        current_build_ui = getattr(panel_class, "_build_ui", None)
        if not callable(current_build_ui):
            return False

        @functools.wraps(current_build_ui)
        def build_ui_issue79(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = current_build_ui(self, *args, **kwargs)
            _normalize_navigation(self, module)
            _install_active_platform_tab(self, module)
            try:
                self._apply_theme(self._dark_mode)
                self._show_page(self._active_page)
            except Exception:
                pass
            return result

        def queue_insert_issue79(self: Any) -> None:
            _queue_insert_dialog(self, module)

        setattr(panel_class, "_build_ui", build_ui_issue79)
        setattr(panel_class, "_queue_insert", queue_insert_issue79)
        setattr(panel_class, "_issue79_features_installed", True)
        return True


__all__ = ["patch_control_panel_issue79"]
