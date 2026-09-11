from __future__ import annotations

import functools
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Callable

_PATCH_LOCK = threading.RLock()


def _process_running(process: Any) -> bool:
    if process is None:
        return False
    try:
        return process.poll() is None
    except Exception:
        return False


def _terminate_process(process: Any, *, timeout: float = 3.0) -> None:
    if not _process_running(process):
        return
    try:
        process.terminate()
    except Exception:
        pass
    try:
        process.wait(timeout=timeout)
        return
    except subprocess.TimeoutExpired:
        pass
    except Exception:
        if not _process_running(process):
            return

    if sys.platform == "win32":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(int(process.pid)), "/T", "/F"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=6,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except Exception:
            pass
    try:
        if _process_running(process):
            process.kill()
    except Exception:
        pass
    try:
        process.wait(timeout=2.0)
    except Exception:
        pass
    if _process_running(process):
        raise RuntimeError(f"进程 {getattr(process, 'pid', '?')} 未能停止")


def _safe_after(root: Any, callback: Callable[[], None]) -> None:
    try:
        root.after(0, callback)
    except Exception:
        pass


def _patch_style_redraw(panel_class: type[Any]) -> None:
    original = getattr(panel_class, "_redraw_style_preview", None)
    if not callable(original) or bool(getattr(original, "_issue187_debounced", False)):
        return

    @functools.wraps(original)
    def redraw_debounced(self: Any, *_args: Any, **_kwargs: Any) -> None:
        state = getattr(self, "_issue187_style_redraw_state", None)
        if not isinstance(state, dict):
            state = {"job": None}
            self._issue187_style_redraw_state = state
        if state.get("job") is not None:
            return

        def flush() -> None:
            state["job"] = None
            try:
                original(self)
            except Exception:
                pass

        try:
            state["job"] = self.root.after(45, flush)
        except Exception:
            state["job"] = None

    setattr(redraw_debounced, "_issue187_debounced", True)
    panel_class._redraw_style_preview = redraw_debounced


def _patch_update_launchers() -> None:
    from . import incremental_update, update_client, update_ui

    if bool(getattr(update_ui, "_issue187_async_update_launch_installed", False)):
        return

    def begin_runtime_stop(app: Any) -> tuple[Any, Any, bool]:
        server = getattr(app, "server_proc", None)
        overlay = getattr(app, "overlay_proc", None)
        server_was_running = _process_running(server)
        try:
            app._close_overlay_window()
        except Exception:
            pass
        try:
            app.update_status_var.set("正在后台停止运行时并启动独立更新器…")
        except Exception:
            pass
        return server, overlay, server_was_running

    def finalize_failure(app: Any, prepared: Any, error: str, *, incremental: bool, server_was_running: bool) -> None:
        app._update_launched = False
        if incremental:
            update_ui._discard_prepared_incremental(app, prepared)  # noqa: SLF001
            label = "增量更新"
        else:
            update_ui._discard_prepared_update(app, prepared)  # noqa: SLF001
            label = "全量更新"
        try:
            app.update_status_var.set(f"启动{label}器失败：{error}")
            update_ui._set_busy(app, False)  # noqa: SLF001
        except Exception:
            pass
        if server_was_running:
            try:
                app.start_server()
            except Exception:
                pass
        try:
            update_ui.messagebox.showerror(f"{label}失败", error, parent=app.root)
        except Exception:
            pass

    def finalize_success(app: Any, prepared: Any, *, incremental: bool) -> None:
        app._update_launched = True
        try:
            if incremental:
                update_ui.messagebox.showinfo(
                    "开始增量更新",
                    f"将替换 {prepared.replace_count} 个程序文件，删除 {prepared.remove_count} 个旧程序文件。\n"
                    "用户配置、存档、日志和插件数据不会被覆盖。程序将关闭并自动重启。",
                    parent=app.root,
                )
            else:
                update_ui.messagebox.showinfo(
                    "开始全量更新",
                    "完整更新包已校验。程序将关闭，并由独立更新器完成替换和重启。",
                    parent=app.root,
                )
            app.root.after(250, app.root.destroy)
        except Exception:
            try:
                app.root.destroy()
            except Exception:
                pass

    def launch_async(app: Any, prepared: Any, updater_exe: Path, *, incremental: bool) -> None:
        server, overlay, server_was_running = begin_runtime_stop(app)
        # Prevent the root Destroy cleanup hook from deleting the prepared files
        # while the background launcher is still consuming them.
        app._update_launched = True

        def worker() -> None:
            error = ""
            try:
                _terminate_process(overlay)
                _terminate_process(server)
                if getattr(app, "overlay_proc", None) is overlay:
                    app.overlay_proc = None
                if getattr(app, "server_proc", None) is server:
                    app.server_proc = None
                if incremental:
                    incremental_update.launch_incremental_updater(
                        prepared,
                        updater_exe=updater_exe,
                        app_dir=app._update_app_dir,
                        current_pid=update_ui.os.getpid(),
                    )
                else:
                    update_client.launch_updater(
                        prepared,
                        updater_exe=updater_exe,
                        app_dir=app._update_app_dir,
                        current_pid=update_ui.os.getpid(),
                    )
            except Exception as exc:  # noqa: BLE001
                error = str(exc)

            if error:
                _safe_after(
                    app.root,
                    lambda: finalize_failure(
                        app,
                        prepared,
                        error,
                        incremental=incremental,
                        server_was_running=server_was_running,
                    ),
                )
            else:
                _safe_after(app.root, lambda: finalize_success(app, prepared, incremental=incremental))

        threading.Thread(
            target=worker,
            name="bilipdj-update-launch",
            daemon=True,
        ).start()

    def launch_full(app: Any, prepared: Any, updater_exe: Path) -> None:
        launch_async(app, prepared, updater_exe, incremental=False)

    def launch_incremental(app: Any, prepared: Any, updater_exe: Path) -> None:
        launch_async(app, prepared, updater_exe, incremental=True)

    update_ui._launch_prepared_full = launch_full  # type: ignore[attr-defined]  # noqa: SLF001
    update_ui._launch_prepared_incremental = launch_incremental  # type: ignore[attr-defined]  # noqa: SLF001
    update_ui._issue187_async_update_launch_installed = True


def patch_control_panel_issue187(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = sys.modules.get(str(getattr(panel_class, "__module__", "") or ""))
    if module is None or Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_issue187_stability_installed", False)):
            return True

        _patch_style_redraw(panel_class)
        _patch_update_launchers()

        original_on_close = getattr(panel_class, "on_close", None)
        if not callable(original_on_close):
            return False

        def stop_server_async(self: Any) -> None:
            nonblocking = getattr(self, "_stop_server_nonblocking", None)
            if callable(nonblocking):
                nonblocking()
                return
            process = getattr(self, "server_proc", None)
            if not _process_running(process):
                return

            def worker() -> None:
                try:
                    _terminate_process(process)
                except Exception:
                    pass
                if getattr(self, "server_proc", None) is process:
                    self.server_proc = None

            threading.Thread(target=worker, name="bilipdj-stop-server", daemon=True).start()

        @functools.wraps(original_on_close)
        def on_close_async(self: Any) -> None:
            if bool(getattr(self, "_issue187_closing", False)):
                return
            self._issue187_closing = True
            try:
                self._close_overlay_window()
            except Exception:
                pass
            server = getattr(self, "server_proc", None)
            overlay = getattr(self, "overlay_proc", None)
            try:
                self.status_var.set("正在后台停止服务并退出…")
                self.root.configure(cursor="watch")
            except Exception:
                pass

            def worker() -> None:
                try:
                    _terminate_process(overlay)
                except Exception:
                    pass
                try:
                    _terminate_process(server)
                except Exception:
                    pass
                if getattr(self, "overlay_proc", None) is overlay:
                    self.overlay_proc = None
                if getattr(self, "server_proc", None) is server:
                    self.server_proc = None
                _safe_after(self.root, self.root.destroy)

            threading.Thread(target=worker, name="bilipdj-control-close", daemon=True).start()

        panel_class.stop_server = stop_server_async
        panel_class.on_close = on_close_async
        panel_class._issue187_stability_installed = True
        return True


__all__ = ["patch_control_panel_issue187"]
