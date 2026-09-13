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
    channel_combo = getattr(app, "_update_channel_combo", None)
    if check_button is not None:
        check_button.configure(state="disabled" if busy else "normal")
    if channel_combo is not None:
        channel_combo.configure(state="disabled" if busy else "readonly")
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
    current_version = APP_VERSION
    _init_update_state(app, current_version, app_dir)

    frame.columnconfigure(0, weight=1)
    ttk.Label(frame, text=f"{app_name} 软件更新", font=("Microsoft YaHei UI", 16, "bold")).grid(
        row=0, column=0, sticky="w", pady=(0, 8)
    )
    ttk.Label(frame, text=f"当前版本：v{current_version}").grid(row=1, column=0, sticky="w", pady=(0, 12))
    app.update_status_var.set("请使用“更新软件”页面检查和安装版本。")


def _cleanup_after_launch(app: Any) -> None:
    app._prepared_update = None
    app._prepared_incremental_update = None


def _ensure_updater_available(app: Any) -> Path:
    updater_exe = Path(app._update_app_dir) / update_client.UPDATER_EXE_NAME
    if sys.platform != "win32" or not getattr(sys, "frozen", False):
        raise update_client.UpdateError("自动更新仅支持 Windows 打包版")
    if not updater_exe.is_file():
        raise update_client.UpdateError(f"找不到独立更新器：{updater_exe}")
    return updater_exe


def _confirm_update(app: Any, release: update_client.ReleaseInfo, *, incremental: bool) -> bool:
    mode = "增量更新" if incremental else "全量更新"
    return messagebox.askyesno(
        mode,
        f"将安装 v{release.version}。\n\n更新前会自动备份当前程序；失败时会尝试恢复。是否继续？",
        parent=app.root,
    )


def install_available_update(app: Any) -> None:
    release = getattr(app, "_available_update", None)
    if release is None or getattr(app, "_update_busy", False):
        return
    if not _confirm_update(app, release, incremental=False):
        return
    try:
        updater_exe = _ensure_updater_available(app)
    except Exception as exc:
        messagebox.showerror("无法更新", str(exc), parent=app.root)
        return

    _discard_all_prepared(app)
    _set_busy(app, True)
    app.update_progress_var.set(0)
    app.update_status_var.set(f"正在准备 v{release.version} 完整更新…")

    def progress(done: int, total: int) -> None:
        def apply() -> None:
            if total > 0:
                app.update_progress_var.set(min(100.0, done * 100.0 / total))
            app.update_status_var.set(f"正在下载完整包：{done / 1024**2:.1f} / {total / 1024**2:.1f} MB")
        app.root.after(0, apply)

    def worker() -> None:
        prepared = None
        try:
            prepared = update_client.prepare_update(release, progress=progress)
            app._prepared_update = prepared
            update_client.launch_updater(
                updater_exe,
                app_dir=Path(app._update_app_dir),
                prepared=prepared,
                main_exe_name=Path(sys.executable).name,
            )
            app._update_launched = True
        except Exception as exc:  # noqa: BLE001
            if prepared is not None:
                update_client.cleanup_prepared_update(prepared)
            app.root.after(0, lambda: _finish_update_error(app, exc))
            return
        app.root.after(0, lambda: _finish_launch(app))

    threading.Thread(target=worker, name="bilipdj-full-update", daemon=True).start()


def _finish_update_error(app: Any, exc: Exception) -> None:
    _set_busy(app, False)
    app.update_progress_var.set(0)
    app.update_status_var.set(f"更新失败：{exc}")
    messagebox.showerror("更新失败", str(exc), parent=app.root)


def _finish_launch(app: Any) -> None:
    app.update_status_var.set("更新器已启动，正在退出当前程序…")
    _cleanup_after_launch(app)
    try:
        app.root.after(250, app.root.destroy)
    except Exception:
        app.root.destroy()


def check_for_update(app: Any, *, silent: bool = False) -> None:
    if getattr(sys, "frozen", False):
        if hasattr(app, "_update_version_combo"):
            from . import update_version_selector

            update_version_selector.check_for_versions(app, silent=silent)
            return
        # Legacy/fallback path for packed builds whose dedicated version picker
        # was not installed for some reason.
    if getattr(app, "_update_busy", False):
        return
    _set_busy(app, True)
    app.update_progress_var.set(0)
    app.update_status_var.set("正在检查更新…")

    def worker() -> None:
        try:
            release = update_client.fetch_latest_release()
        except Exception as exc:  # noqa: BLE001
            app.root.after(0, lambda: _finish_update_error(app, exc))
            return

        def finish() -> None:
            app._available_update = release
            _set_notes(app, release.body)
            _set_busy(app, False)
            if update_client.is_newer_version(release.version, app._update_current_version):
                app.update_status_var.set(f"发现新版本 v{release.version}")
                if not silent:
                    messagebox.showinfo(
                        "发现新版本",
                        f"当前版本：v{app._update_current_version}\n最新版本：v{release.version}",
                        parent=app.root,
                    )
            else:
                app.update_status_var.set(f"当前已是最新版本 v{app._update_current_version}")

        app.root.after(0, finish)

    threading.Thread(target=worker, name="bilipdj-update-check", daemon=True).start()
