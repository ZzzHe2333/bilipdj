from __future__ import annotations

import os
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any

from . import incremental_update, update_client
from .version import APP_VERSION


def _set_notes(app: Any, text: str) -> None:
    widget = getattr(app, "_update_notes", None)
    if widget is None:
        return
    widget.configure(state="normal")
    widget.delete("1.0", "end")
    widget.insert("1.0", str(text).strip() or "此版本没有提供更新说明。")
    widget.configure(state="disabled")


def _selected_version_source(app: Any) -> str:
    mapping = getattr(app, "_update_version_candidates", None)
    variable = getattr(app, "update_version_var", None)
    if not isinstance(mapping, dict) or variable is None:
        return ""
    candidate = mapping.get(str(variable.get()))
    return str(getattr(candidate, "source", "")) if candidate is not None else ""


def _set_busy(app: Any, busy: bool) -> None:
    app._update_busy = busy
    check_button = getattr(app, "_update_check_button", None)
    full_button = getattr(app, "_update_full_button", getattr(app, "_update_install_button", None))
    incremental_button = getattr(app, "_update_incremental_button", None)
    version_combo = getattr(app, "_update_version_combo", None)
    if check_button is not None:
        check_button.configure(state="disabled" if busy else "normal")
    if version_combo is not None:
        values = tuple(version_combo.cget("values") or ())
        version_combo.configure(state="disabled" if busy or not values else "readonly")

    source = _selected_version_source(app)
    if source:
        enabled = not busy
        if full_button is not None:
            full_button.configure(
                text="恢复旧版" if source == "local" else "全量更新",
                state="normal" if enabled else "disabled",
            )
        if incremental_button is not None:
            incremental_button.configure(
                state="normal" if enabled and source == "cloud" else "disabled"
            )
        return

    enabled = not busy and getattr(app, "_available_update", None) is not None
    if full_button is not None:
        full_button.configure(state="normal" if enabled else "disabled")
    if incremental_button is not None:
        incremental_button.configure(state="normal" if enabled else "disabled")


def _discard_prepared_update(
    app: Any,
    prepared: update_client.PreparedUpdate | None = None,
) -> None:
    target = prepared or getattr(app, "_prepared_update", None)
    update_client.cleanup_prepared_update(target)
    if target is getattr(app, "_prepared_update", None):
        app._prepared_update = None


def _discard_prepared_incremental(
    app: Any,
    prepared: incremental_update.PreparedIncrementalUpdate | None = None,
) -> None:
    target = prepared or getattr(app, "_prepared_incremental_update", None)
    incremental_update.cleanup_prepared_incremental(target)
    if target is getattr(app, "_prepared_incremental_update", None):
        app._prepared_incremental_update = None


def _discard_all_prepared(app: Any) -> None:
    _discard_prepared_update(app)
    _discard_prepared_incremental(app)


def _on_root_destroy(app: Any, event: tk.Event[Any]) -> None:
    if event.widget is not app.root:
        return
    if not getattr(app, "_update_launched", False):
        _discard_all_prepared(app)


def _init_update_state(app: Any, current_version: str, app_dir: Path) -> None:
    app._update_current_version = current_version
    app._update_app_dir = Path(app_dir)
    app._update_busy = False
    app._available_update = None
    app._prepared_update = None
    app._prepared_incremental_update = None
    app._update_launched = False
    app.update_status_var = tk.StringVar(value="尚未检查更新")
    app.update_progress_var = tk.DoubleVar(value=0.0)
    app.root.bind("<Destroy>", lambda event: _on_root_destroy(app, event), add="+")


def build_about_tab(
    app: Any,
    frame: ttk.Frame,
    app_name: str,
    current_version: str,
    app_dir: Path,
) -> None:
    # VERSION is the single source of truth; keep the argument for API
    # compatibility with older callers.
    current_version = APP_VERSION
    app.root.title(f"{app_name} 控制台 v{current_version}")
    _init_update_state(app, current_version, app_dir)

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
    app._update_full_button = ttk.Button(
        button_row,
        text="全量更新",
        command=lambda: install_available_update(app),
        state="disabled",
    )
    app._update_full_button.pack(side="left", padx=(0, 8))
    app._update_install_button = app._update_full_button
    app._update_incremental_button = ttk.Button(
        button_row,
        text="增量更新",
        command=lambda: install_incremental_update(app),
        state="disabled",
    )
    app._update_incremental_button.pack(side="left")

    ttk.Label(update_frame, text="更新说明").pack(anchor="w", pady=(10, 4))
    notes_frame = ttk.Frame(update_frame)
    notes_frame.pack(fill="both", expand=True)
    app._update_notes = tk.Text(notes_frame, height=9, wrap="word", state="disabled")
    app._all_text_widgets.append(app._update_notes)
    app._update_notes.pack(side="left", fill="both", expand=True)
    notes_scroll = ttk.Scrollbar(notes_frame, orient="vertical", command=app._update_notes.yview)
    notes_scroll.pack(side="right", fill="y")
    app._update_notes.configure(yscrollcommand=notes_scroll.set)
    _set_notes(app, "点击“检查更新”后，可选择全量更新或增量更新。")

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
        if hasattr(app, "_update_version_combo"):
            from . import update_version_selector

            update_version_selector.check_for_versions(app, silent=True)
        else:
            check_for_updates(app, silent=True)
    else:
        app.update_status_var.set("开发模式：可检查版本，但自动安装仅在 Windows 打包版中启用。")


def check_for_updates(app: Any, *, silent: bool = False) -> None:
    if getattr(app, "_update_busy", False):
        return
    _discard_all_prepared(app)
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
        size_mb = release.zip_asset.size / 1024**2
        app.update_status_var.set(
            f"发现新版本 v{release.version}，完整包约 {size_mb:.1f} MB；可选择全量更新或增量更新。"
        )
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


def _validate_install_environment(app: Any) -> tuple[update_client.ReleaseInfo, Path] | None:
    release = getattr(app, "_available_update", None)
    if release is None or getattr(app, "_update_busy", False):
        return None
    if sys.platform != "win32" or not getattr(sys, "frozen", False):
        messagebox.showerror("无法自动安装", "自动安装仅支持 Windows 打包版。")
        return None
    updater_exe = app._update_app_dir / update_client.UPDATER_EXE_NAME
    if not updater_exe.is_file():
        messagebox.showerror("缺少更新器", f"找不到独立更新器：\n{updater_exe}")
        return None
    return release, updater_exe


def install_available_update(app: Any) -> None:
    validated = _validate_install_environment(app)
    if validated is None:
        return
    release, updater_exe = validated
    if not messagebox.askyesno(
        "全量更新",
        f"将下载完整安装包并更新到 v{release.version}。\n\n"
        "全量更新逻辑与原版本保持一致：完整下载 ZIP、校验 SHA-256 后替换程序目录。\n"
        "配置、队列存档和日志按现有更新器规则保留；失败时自动回滚。\n\n是否继续？",
    ):
        return

    _set_busy(app, True)
    app.update_progress_var.set(0)
    app.update_status_var.set("全量更新：正在准备 SHA-256 校验并下载完整包…")

    def progress(downloaded: int, total: int) -> None:
        app.root.after(0, lambda: _show_download_progress(app, downloaded, total, "全量更新"))

    def worker() -> None:
        try:
            prepared = update_client.prepare_release_download(release, progress=progress)
        except Exception as exc:  # noqa: BLE001
            app.root.after(0, lambda error=str(exc): _download_failed(app, error, "全量更新"))
        else:
            app.root.after(0, lambda: _download_ready(app, prepared, updater_exe))

    threading.Thread(target=worker, daemon=True).start()


def install_incremental_update(app: Any) -> None:
    validated = _validate_install_environment(app)
    if validated is None:
        return
    release, updater_exe = validated
    if not messagebox.askyesno(
        "增量更新",
        f"将扫描本地程序资源并与 v{release.version} 的逐文件 SHA-256 清单比较。\n\n"
        "只下载本次 Release 的差异资源包，并只替换本地缺失或哈希变化的程序文件。\n"
        "配置、存档、日志、backup 和 plugins 等用户数据不参与扫描，也不会被删除或覆盖。\n"
        "若本地差异超出本次增量包覆盖范围，会要求改用全量更新。\n\n是否继续？",
    ):
        return

    _discard_prepared_incremental(app)
    _set_busy(app, True)
    app.update_progress_var.set(0)
    app.update_status_var.set("增量更新：正在读取逐文件清单并扫描本地程序资源…")

    def progress(downloaded: int, total: int) -> None:
        app.root.after(0, lambda: _show_download_progress(app, downloaded, total, "增量更新"))

    def worker() -> None:
        try:
            prepared = incremental_update.prepare_incremental_download(
                release,
                app_dir=app._update_app_dir,
                progress=progress,
            )
        except incremental_update.IncrementalUnavailable as exc:
            app.root.after(0, lambda error=str(exc): _incremental_unavailable(app, error))
        except Exception as exc:  # noqa: BLE001
            app.root.after(0, lambda error=str(exc): _download_failed(app, error, "增量更新"))
        else:
            app.root.after(0, lambda: _incremental_ready(app, prepared, updater_exe))

    threading.Thread(target=worker, daemon=True).start()


def _show_download_progress(app: Any, downloaded: int, total: int, mode: str = "更新") -> None:
    if total > 0:
        percent = min(100.0, downloaded * 100.0 / total)
        app.update_progress_var.set(percent)
        app.update_status_var.set(
            f"{mode}：正在下载 {percent:.1f}%（{downloaded / 1024**2:.1f} / {total / 1024**2:.1f} MB）"
        )
    else:
        app.update_status_var.set(f"{mode}：已下载 {downloaded / 1024**2:.1f} MB")


def _download_failed(app: Any, error: str, mode: str = "更新") -> None:
    app.update_status_var.set(f"{mode}失败：{error}")
    _set_busy(app, False)
    messagebox.showerror(f"{mode}失败", error)


def _incremental_unavailable(app: Any, error: str) -> None:
    app.update_status_var.set(f"增量更新不可用：{error}")
    _set_busy(app, False)
    if messagebox.askyesno(
        "增量更新不可用",
        f"{error}\n\n是否改用全量更新？",
        parent=app.root,
    ):
        install_available_update(app)


def _prepare_for_updater_launch(app: Any) -> bool:
    if not app.save_to_file(use_backend_api=False, switch_queue_slot=False):
        app.update_status_var.set("配置保存失败，已取消安装。")
        return False
    return True


def _download_ready(app: Any, prepared: update_client.PreparedUpdate, updater_exe: Path) -> None:
    previous = getattr(app, "_prepared_update", None)
    if previous is not None and previous is not prepared:
        update_client.cleanup_prepared_update(previous)
    app._prepared_update = prepared
    app.update_progress_var.set(100)
    app.update_status_var.set(f"全量更新包校验通过：SHA-256 {prepared.sha256[:12]}…")

    if not _prepare_for_updater_launch(app):
        _discard_prepared_update(app, prepared)
        _set_busy(app, False)
        return
    _launch_prepared_full(app, prepared, updater_exe)


def _incremental_ready(
    app: Any,
    prepared: incremental_update.PreparedIncrementalUpdate,
    updater_exe: Path,
) -> None:
    previous = getattr(app, "_prepared_incremental_update", None)
    if previous is not None and previous is not prepared:
        incremental_update.cleanup_prepared_incremental(previous)
    app._prepared_incremental_update = prepared
    app.update_progress_var.set(100)
    app.update_status_var.set(
        "增量比对完成："
        f"扫描 {prepared.scanned_count} 个程序文件，"
        f"无需更新 {prepared.unchanged_count} 个，"
        f"替换 {prepared.replace_count} 个，删除旧文件 {prepared.remove_count} 个；"
        f"增量包 {prepared.download_size / 1024**2:.1f} MB。"
    )

    if not _prepare_for_updater_launch(app):
        _discard_prepared_incremental(app, prepared)
        _set_busy(app, False)
        return
    _launch_prepared_incremental(app, prepared, updater_exe)


def _stop_runtime_for_update(app: Any) -> bool:
    server_was_running = bool(app.server_proc and app.server_proc.poll() is None)
    if server_was_running:
        app.stop_server()
    app._stop_overlay_process()
    app._close_overlay_window()
    return server_was_running


def _restart_server_after_launch_failure(app: Any, server_was_running: bool) -> None:
    if not server_was_running:
        return
    try:
        app.start_server()
    except Exception:
        pass


def _launch_prepared_full(app: Any, prepared: update_client.PreparedUpdate, updater_exe: Path) -> None:
    server_was_running = False
    try:
        server_was_running = _stop_runtime_for_update(app)
        update_client.launch_updater(
            prepared,
            updater_exe=updater_exe,
            app_dir=app._update_app_dir,
            current_pid=os.getpid(),
        )
    except Exception as exc:  # noqa: BLE001
        _discard_prepared_update(app, prepared)
        app.update_status_var.set(f"启动全量更新器失败：{exc}")
        _set_busy(app, False)
        _restart_server_after_launch_failure(app, server_was_running)
        messagebox.showerror("全量更新失败", str(exc))
        return

    app._update_launched = True
    messagebox.showinfo("开始全量更新", "完整更新包已校验。程序将关闭，并由独立更新器完成替换和重启。")
    app.root.after(250, app.root.destroy)


def _launch_prepared_incremental(
    app: Any,
    prepared: incremental_update.PreparedIncrementalUpdate,
    updater_exe: Path,
) -> None:
    server_was_running = False
    try:
        server_was_running = _stop_runtime_for_update(app)
        incremental_update.launch_incremental_updater(
            prepared,
            updater_exe=updater_exe,
            app_dir=app._update_app_dir,
            current_pid=os.getpid(),
        )
    except Exception as exc:  # noqa: BLE001
        _discard_prepared_incremental(app, prepared)
        app.update_status_var.set(f"启动增量更新器失败：{exc}")
        _set_busy(app, False)
        _restart_server_after_launch_failure(app, server_was_running)
        messagebox.showerror("增量更新失败", str(exc))
        return

    app._update_launched = True
    messagebox.showinfo(
        "开始增量更新",
        f"将替换 {prepared.replace_count} 个程序文件，删除 {prepared.remove_count} 个旧程序文件。\n"
        "用户配置、存档、日志和插件数据不会被覆盖。程序将关闭并自动重启。",
    )
    app.root.after(250, app.root.destroy)
