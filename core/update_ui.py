from __future__ import annotations

import os
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any

from . import update_client
from .version import APP_VERSION


def _set_notes(app: Any, text: str) -> None:
    widget = getattr(app, "_update_notes", None)
    if widget is None:
        return
    widget.configure(state="normal")
    widget.delete("1.0", "end")
    widget.insert("1.0", str(text).strip() or "此版本没有提供更新说明。")
    widget.configure(state="disabled")


def _set_busy(app: Any, busy: bool) -> None:
    app._update_busy = busy
    check_button = getattr(app, "_update_check_button", None)
    install_button = getattr(app, "_update_install_button", None)
    if check_button is not None:
        check_button.configure(state="disabled" if busy else "normal")
    if install_button is not None:
        enabled = not busy and getattr(app, "_available_update", None) is not None
        install_button.configure(state="normal" if enabled else "disabled")


def build_about_tab(app: Any, frame: ttk.Frame, app_name: str, current_version: str, app_dir: Path) -> None:
    current_version = APP_VERSION
    app._update_current_version = current_version
    app._update_app_dir = Path(app_dir)
    app._update_busy = False
    app._available_update = None
    app._prepared_update = None
    app.update_status_var = tk.StringVar(value="尚未检查更新")
    app.update_progress_var = tk.DoubleVar(value=0.0)

    ttk.Label(
        frame,
        text=f"{app_name} 控制台",
        font=("Microsoft YaHei UI", 15, "bold") if sys.platform == "win32" else ("", 15, "bold"),
    ).pack(pady=(16, 6))
    ttk.Label(frame, text=f"当前版本：v{current_version}").pack()

    update_frame = ttk.LabelFrame(frame, text="软件更新", padding=12)
    update_frame.pack(fill="x", padx=20, pady=(14, 8))
    ttk.Label(update_frame, textvariable=app.update_status_var, wraplength=720, justify="left").pack(anchor="w")
    app._update_progress = ttk.Progressbar(
        update_frame,
        mode="determinate",
        maximum=100,
        variable=app.update_progress_var,
    )
    app._update_progress.pack(fill="x", pady=(9, 8))

    button_row = ttk.Frame(update_frame)
    button_row.pack(fill="x")
    app._update_check_button = ttk.Button(
        button_row,
        text="检查更新",
        command=lambda: check_for_updates(app, silent=False),
    )
    app._update_check_button.pack(side="left", padx=(0, 8))
    app._update_install_button = ttk.Button(
        button_row,
        text="下载并安装",
        command=lambda: install_available_update(app),
        state="disabled",
    )
    app._update_install_button.pack(side="left")

    ttk.Label(update_frame, text="更新说明").pack(anchor="w", pady=(10, 4))
    notes_frame = ttk.Frame(update_frame)
    notes_frame.pack(fill="both", expand=True)
    app._update_notes = tk.Text(notes_frame, height=9, wrap="word", state="disabled")
    app._all_text_widgets.append(app._update_notes)
    app._update_notes.pack(side="left", fill="both", expand=True)
    notes_scroll = ttk.Scrollbar(notes_frame, orient="vertical", command=app._update_notes.yview)
    notes_scroll.pack(side="right", fill="y")
    app._update_notes.configure(yscrollcommand=notes_scroll.set)
    _set_notes(app, "点击“检查更新”获取 GitHub 最新版本和详细更新说明。")

    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=12)
    ttk.Label(frame, text="Bilibili 直播弹幕排队管理工具").pack()
    ttk.Label(frame, text="排队逻辑由 Python 后端统一处理，前端仅负责显示。").pack(pady=(4, 0))
    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=12)
    for line in [
        "本软件完全免费，源码公开。",
        "若有人向你收费获取此软件（亲手上门帮安装调试除外），请立刻退款并举报！",
        "",
        "【侵权/倒卖责任】",
        "• 民事责任：侵权方须停止侵权、赔偿损失（含维权合理费用）。",
        "• 刑事责任：以营利为目的的侵权行为，违法所得数额较大或",
        "  情节严重的，依《著作权法》第53条及相关司法解释，",
        "  可被追究刑事责任，最高判处3年有期徒刑并处罚金。",
    ]:
        ttk.Label(
            frame,
            text=line,
            foreground="#c00" if line.startswith(("若", "【", "•", " ")) else "",
        ).pack(anchor="w", padx=20)


def auto_check(app: Any) -> None:
    if not hasattr(app, "update_status_var"):
        return
    if getattr(sys, "frozen", False):
        check_for_updates(app, silent=True)
    else:
        app.update_status_var.set("开发模式：可检查版本，但自动安装仅在 Windows 打包版中启用。")


def check_for_updates(app: Any, *, silent: bool = False) -> None:
    if getattr(app, "_update_busy", False):
        return
    _set_busy(app, True)
    app.update_progress_var.set(0)
    app.update_status_var.set("正在检查 GitHub 最新版本…")

    def worker() -> None:
        try:
            release = update_client.fetch_latest_release()
        except Exception as exc:  # noqa: BLE001
            app.root.after(0, lambda error=str(exc): _check_failed(app, error, silent))
        else:
            app.root.after(0, lambda: _check_succeeded(app, release, silent))

    threading.Thread(target=worker, daemon=True).start()


def _check_succeeded(app: Any, release: update_client.ReleaseInfo, silent: bool) -> None:
    _set_notes(app, release.body)
    current_version = app._update_current_version
    if update_client.is_newer_version(release.version, current_version):
        app._available_update = release
        size_mb = release.zip_asset.size / 1024 ** 2
        app.update_status_var.set(f"发现新版本 v{release.version}，Windows 更新包约 {size_mb:.1f} MB。")
        _set_busy(app, False)
        if not silent:
            messagebox.showinfo("发现新版本", f"当前版本：v{current_version}\n最新版本：v{release.version}")
    else:
        app._available_update = None
        app.update_status_var.set(f"当前已是最新版本 v{current_version}。")
        _set_busy(app, False)


def _check_failed(app: Any, error: str, silent: bool) -> None:
    app._available_update = None
    app.update_status_var.set(f"检查更新失败：{error}")
    _set_busy(app, False)
    if not silent:
        messagebox.showerror("检查更新失败", error)


def install_available_update(app: Any) -> None:
    release = getattr(app, "_available_update", None)
    if release is None or getattr(app, "_update_busy", False):
        return
    if sys.platform != "win32" or not getattr(sys, "frozen", False):
        messagebox.showerror("无法自动安装", "自动安装仅支持 Windows 打包版。")
        return

    updater_exe = app._update_app_dir / update_client.UPDATER_EXE_NAME
    if not updater_exe.is_file():
        messagebox.showerror("缺少更新器", f"找不到独立更新器：\n{updater_exe}")
        return
    if not messagebox.askyesno(
        "下载并安装更新",
        f"将下载并安装 v{release.version}。\n\n"
        "安装时程序会自动关闭；配置、队列存档和日志将被保留。\n"
        "更新失败时会自动恢复上一版本。\n\n是否继续？",
    ):
        return

    _set_busy(app, True)
    app.update_progress_var.set(0)
    app.update_status_var.set("正在下载 SHA-256 校验文件…")

    def progress(downloaded: int, total: int) -> None:
        app.root.after(0, lambda: _show_download_progress(app, downloaded, total))

    def worker() -> None:
        try:
            prepared = update_client.prepare_release_download(release, progress=progress)
        except Exception as exc:  # noqa: BLE001
            app.root.after(0, lambda error=str(exc): _download_failed(app, error))
        else:
            app.root.after(0, lambda: _download_ready(app, prepared, updater_exe))

    threading.Thread(target=worker, daemon=True).start()


def _show_download_progress(app: Any, downloaded: int, total: int) -> None:
    if total > 0:
        percent = min(100.0, downloaded * 100.0 / total)
        app.update_progress_var.set(percent)
        app.update_status_var.set(
            f"正在下载更新包：{percent:.1f}%（{downloaded / 1024 ** 2:.1f} / {total / 1024 ** 2:.1f} MB）"
        )
    else:
        app.update_status_var.set(f"正在下载更新包：已下载 {downloaded / 1024 ** 2:.1f} MB")


def _download_failed(app: Any, error: str) -> None:
    app.update_status_var.set(f"更新下载失败：{error}")
    _set_busy(app, False)
    messagebox.showerror("更新失败", error)


def _download_ready(app: Any, prepared: update_client.PreparedUpdate, updater_exe: Path) -> None:
    app._prepared_update = prepared
    app.update_progress_var.set(100)
    app.update_status_var.set(f"更新包校验通过：SHA-256 {prepared.sha256[:12]}…")
    if not app.save_to_file(use_backend_api=False, switch_queue_slot=False):
        app.update_status_var.set("配置保存失败，已取消安装。")
        _set_busy(app, False)
        return

    try:
        if app.server_proc and app.server_proc.poll() is None:
            app.stop_server()
        app._stop_overlay_process()
        app._close_overlay_window()
        update_client.launch_updater(
            prepared,
            updater_exe=updater_exe,
            app_dir=app._update_app_dir,
            current_pid=os.getpid(),
        )
    except Exception as exc:  # noqa: BLE001
        app.update_status_var.set(f"启动更新器失败：{exc}")
        _set_busy(app, False)
        messagebox.showerror("更新失败", str(exc))
        return

    messagebox.showinfo("开始安装", "更新包已校验。程序将关闭，并由独立更新器完成替换和重启。")
    app.root.after(250, app.root.destroy)
