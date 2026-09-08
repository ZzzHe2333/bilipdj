from __future__ import annotations

import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any

from . import update_network, update_ui
from .version import APP_VERSION


def _set_notes(app: Any, text: str) -> None:
    update_ui._set_notes(app, text)  # noqa: SLF001 - shared internal UI helper


def _set_proxy_entry_state(app: Any) -> None:
    enabled = bool(app.update_third_party_proxy_var.get())
    state = "normal" if enabled else "disabled"
    for widget in getattr(app, "_update_proxy_entries", []):
        try:
            widget.configure(state=state)
        except Exception:
            pass


def _collect_network_settings(app: Any) -> dict[str, Any]:
    return update_network.normalize_update_network(
        {
            "bypass_system_proxy": bool(app.update_bypass_proxy_var.get()),
            "use_third_party_proxy": bool(app.update_third_party_proxy_var.get()),
            "proxy_host": app.update_proxy_host_var.get().strip(),
            "proxy_port": app.update_proxy_port_var.get().strip(),
            "use_mirrorchyan": False,
        }
    )


def load_network_settings(app: Any) -> dict[str, Any]:
    config = update_network.load_update_network(getattr(app, "_update_app_dir", None))
    app.update_bypass_proxy_var.set(bool(config["bypass_system_proxy"]))
    app.update_third_party_proxy_var.set(bool(config["use_third_party_proxy"]))
    app.update_proxy_host_var.set(str(config["proxy_host"]))
    app.update_proxy_port_var.set(str(config["proxy_port"]))
    app.update_mirrorchyan_var.set(False)
    _set_proxy_entry_state(app)
    return config


def save_network_settings(app: Any, *, show_status: bool = True) -> dict[str, Any] | None:
    try:
        config = update_network.save_update_network(
            _collect_network_settings(app),
            getattr(app, "_update_app_dir", None),
        )
    except Exception as exc:
        app.update_proxy_status_var.set(f"保存失败：{exc}")
        return None
    if show_status:
        app.update_proxy_status_var.set("更新网络设置已保存。")
    return config


def _toggle_bypass(app: Any) -> None:
    if app.update_bypass_proxy_var.get():
        app.update_third_party_proxy_var.set(False)
    _set_proxy_entry_state(app)
    save_network_settings(app)


def _toggle_third_party(app: Any) -> None:
    if app.update_third_party_proxy_var.get():
        app.update_bypass_proxy_var.set(False)
    _set_proxy_entry_state(app)
    save_network_settings(app)


def _toggle_mirrorchyan(app: Any) -> None:
    if app.update_mirrorchyan_var.get():
        messagebox.showinfo("Mirror酱更新", "暂未接入。", parent=app.root)
    app.update_mirrorchyan_var.set(False)
    save_network_settings(app, show_status=False)


def test_proxy_settings(app: Any) -> None:
    if getattr(app, "_update_proxy_test_busy", False):
        return
    config = save_network_settings(app, show_status=False)
    if config is None:
        return
    app._update_proxy_test_busy = True
    app._update_proxy_test_button.configure(state="disabled")
    app.update_proxy_status_var.set("正在检测 GitHub 更新连接……")

    def worker() -> None:
        try:
            result = update_network.test_update_connection(config)
        except Exception as exc:
            text = str(exc)
        else:
            text = (
                f"检测成功：{result['mode']}，HTTP {result['status']}，"
                f"延迟 {result['latency_ms']} ms。"
            )

        def finish() -> None:
            app._update_proxy_test_busy = False
            app._update_proxy_test_button.configure(state="normal")
            app.update_proxy_status_var.set(text)

        app.root.after(0, finish)

    threading.Thread(target=worker, name="bilipdj-update-proxy-test", daemon=True).start()


def build_update_tab(
    app: Any,
    frame: ttk.Frame,
    app_name: str,
    current_version: str,
    app_dir: Path,
) -> None:
    current_version = APP_VERSION
    app._update_current_version = current_version
    app._update_app_dir = Path(app_dir)
    app._update_busy = False
    app._available_update = None
    app._prepared_update = None
    app._update_launched = False
    app._update_proxy_test_busy = False
    app.update_status_var = tk.StringVar(value="尚未检查更新")
    app.update_progress_var = tk.DoubleVar(value=0.0)
    app.update_bypass_proxy_var = tk.BooleanVar(value=False)
    app.update_third_party_proxy_var = tk.BooleanVar(value=False)
    app.update_proxy_host_var = tk.StringVar(value="")
    app.update_proxy_port_var = tk.StringVar(value="")
    app.update_mirrorchyan_var = tk.BooleanVar(value=False)
    app.update_proxy_status_var = tk.StringVar(value="")
    app.root.bind("<Destroy>", lambda event: update_ui._on_root_destroy(app, event), add="+")  # noqa: SLF001

    frame.columnconfigure(0, weight=1)
    frame.rowconfigure(0, weight=1)
    canvas = tk.Canvas(frame, highlightthickness=0, bd=0)
    canvas.grid(row=0, column=0, sticky="nsew")
    scrollbar = ttk.Scrollbar(frame, orient="vertical", command=canvas.yview)
    scrollbar.grid(row=0, column=1, sticky="ns")
    canvas.configure(yscrollcommand=scrollbar.set)
    inner = ttk.Frame(canvas, padding=(10, 4, 10, 16))
    window_id = canvas.create_window((0, 0), window=inner, anchor="nw")
    inner.columnconfigure(0, weight=1)
    inner.bind("<Configure>", lambda _e: canvas.configure(scrollregion=canvas.bbox("all")))
    canvas.bind("<Configure>", lambda event: canvas.itemconfigure(window_id, width=event.width))

    ttk.Label(
        inner,
        text=f"{app_name} 软件更新",
        font=("Microsoft YaHei UI", 16, "bold") if sys.platform == "win32" else ("", 16, "bold"),
    ).grid(row=0, column=0, sticky="w", pady=(4, 3))
    ttk.Label(inner, text=f"当前版本：v{current_version}").grid(row=1, column=0, sticky="w", pady=(0, 12))

    update_frame = ttk.LabelFrame(inner, text="检测更新与更新内容", padding=12)
    update_frame.grid(row=2, column=0, sticky="ew")
    update_frame.columnconfigure(0, weight=1)
    ttk.Label(update_frame, textvariable=app.update_status_var, wraplength=760, justify="left").grid(
        row=0, column=0, columnspan=2, sticky="w"
    )
    app._update_progress = ttk.Progressbar(
        update_frame,
        mode="determinate",
        maximum=100,
        variable=app.update_progress_var,
    )
    app._update_progress.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(9, 8))
    button_row = ttk.Frame(update_frame)
    button_row.grid(row=2, column=0, columnspan=2, sticky="w")
    app._update_check_button = ttk.Button(
        button_row,
        text="检查更新",
        command=lambda: update_ui.check_for_updates(app, silent=False),
    )
    app._update_check_button.pack(side="left", padx=(0, 8))
    app._update_install_button = ttk.Button(
        button_row,
        text="下载并安装",
        command=lambda: update_ui.install_available_update(app),
        state="disabled",
    )
    app._update_install_button.pack(side="left")
    ttk.Label(update_frame, text="更新内容").grid(row=3, column=0, sticky="w", pady=(12, 4))
    notes_frame = ttk.Frame(update_frame)
    notes_frame.grid(row=4, column=0, columnspan=2, sticky="nsew")
    notes_frame.columnconfigure(0, weight=1)
    app._update_notes = tk.Text(notes_frame, height=10, wrap="word", state="disabled")
    app._all_text_widgets.append(app._update_notes)
    app._update_notes.grid(row=0, column=0, sticky="nsew")
    notes_scroll = ttk.Scrollbar(notes_frame, orient="vertical", command=app._update_notes.yview)
    notes_scroll.grid(row=0, column=1, sticky="ns")
    app._update_notes.configure(yscrollcommand=notes_scroll.set)
    _set_notes(app, "点击“检查更新”获取 GitHub 最新版本和更新内容。")

    network = ttk.LabelFrame(inner, text="更新网络", padding=12)
    network.grid(row=3, column=0, sticky="ew", pady=(12, 0))
    network.columnconfigure(1, weight=1)
    ttk.Checkbutton(
        network,
        text="绕过系统代理",
        variable=app.update_bypass_proxy_var,
        command=lambda: _toggle_bypass(app),
    ).grid(row=0, column=0, columnspan=2, sticky="w", pady=4)
    ttk.Label(
        network,
        text="开启后更新请求不读取 Windows/环境变量代理；只有 TUN 模式或其他网络层代理可以接管。",
        wraplength=760,
    ).grid(row=1, column=0, columnspan=3, sticky="w", padx=(12, 0), pady=(0, 7))
    ttk.Checkbutton(
        network,
        text="使用第三方代理",
        variable=app.update_third_party_proxy_var,
        command=lambda: _toggle_third_party(app),
    ).grid(row=2, column=0, columnspan=2, sticky="w", pady=4)
    ttk.Label(network, text="代理地址").grid(row=3, column=0, sticky="w", pady=4)
    host_entry = ttk.Entry(network, textvariable=app.update_proxy_host_var)
    host_entry.grid(row=3, column=1, sticky="ew", pady=4)
    ttk.Label(network, text="例如 127.0.0.1 或 http://127.0.0.1").grid(row=3, column=2, sticky="w", padx=(8, 0))
    ttk.Label(network, text="代理端口").grid(row=4, column=0, sticky="w", pady=4)
    port_entry = ttk.Entry(network, textvariable=app.update_proxy_port_var, width=12)
    port_entry.grid(row=4, column=1, sticky="w", pady=4)
    app._update_proxy_entries = [host_entry, port_entry]
    action_row = ttk.Frame(network)
    action_row.grid(row=5, column=0, columnspan=3, sticky="w", pady=(8, 3))
    ttk.Button(action_row, text="保存网络设置", command=lambda: save_network_settings(app)).pack(
        side="left", padx=(0, 8)
    )
    app._update_proxy_test_button = ttk.Button(
        action_row,
        text="检测连接",
        command=lambda: test_proxy_settings(app),
    )
    app._update_proxy_test_button.pack(side="left")
    ttk.Checkbutton(
        network,
        text="使用 Mirror酱更新",
        variable=app.update_mirrorchyan_var,
        command=lambda: _toggle_mirrorchyan(app),
    ).grid(row=6, column=0, columnspan=2, sticky="w", pady=(10, 2))
    ttk.Label(network, textvariable=app.update_proxy_status_var, wraplength=760).grid(
        row=7, column=0, columnspan=3, sticky="w", pady=(5, 0)
    )
    load_network_settings(app)


def build_about_tab(app: Any, frame: ttk.Frame, app_name: str, current_version: str) -> None:
    frame.columnconfigure(0, weight=1)
    ttk.Label(
        frame,
        text=f"{app_name} 控制台",
        font=("Microsoft YaHei UI", 15, "bold") if sys.platform == "win32" else ("", 15, "bold"),
    ).grid(row=0, column=0, pady=(16, 6))
    ttk.Label(frame, text=f"当前版本：v{APP_VERSION}").grid(row=1, column=0)
    ttk.Separator(frame, orient="horizontal").grid(row=2, column=0, sticky="ew", pady=12)
    ttk.Label(frame, text="Bilibili 直播弹幕排队管理工具").grid(row=3, column=0)
    ttk.Label(frame, text="排队逻辑由 Python 后端统一处理，前端仅负责显示。").grid(row=4, column=0, pady=(4, 0))
    ttk.Separator(frame, orient="horizontal").grid(row=5, column=0, sticky="ew", pady=12)
    lines = [
        "本软件完全免费，源码公开。",
        "若有人向你收费获取此软件（亲手上门帮安装调试除外），请立刻退款并举报！",
        "",
        "【侵权/倒卖责任】",
        "• 民事责任：侵权方须停止侵权、赔偿损失（含维权合理费用）。",
        "• 刑事责任：以营利为目的的侵权行为，情节严重时可能被追究刑事责任。",
    ]
    for index, line in enumerate(lines, start=6):
        ttk.Label(
            frame,
            text=line,
            foreground="#c00" if line.startswith(("若", "【", "•")) else "",
        ).grid(row=index, column=0, sticky="w", padx=20)


__all__ = [
    "build_about_tab",
    "build_update_tab",
    "load_network_settings",
    "save_network_settings",
    "test_proxy_settings",
]
