from __future__ import annotations

import functools
import subprocess
import threading
from typing import Any


def _running(process: Any) -> bool:
    if process is None:
        return False
    try:
        return process.poll() is None
    except Exception:
        return False


def _terminate(process: Any, *, timeout: float = 4.0) -> None:
    if not _running(process):
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
        if not _running(process):
            return
    try:
        if _running(process):
            process.kill()
    except Exception:
        pass
    try:
        process.wait(timeout=2.0)
    except Exception:
        pass


def patch_portable_launcher(launcher_class: type[Any]) -> bool:
    if not isinstance(launcher_class, type):
        return False
    if bool(getattr(launcher_class, "_issue187_launcher_installed", False)):
        return True

    def stop_and_exit_nonblocking(self: Any) -> None:
        if bool(getattr(self, "_closing", False)):
            return
        self._closing = True
        try:
            self._stop_tray()
        except Exception:
            pass
        process = getattr(self, "backend_proc", None)
        owns = bool(getattr(self, "owns_backend", False))
        if not owns or not _running(process):
            self.backend_proc = None
            try:
                self.root.destroy()
            except Exception:
                pass
            return

        try:
            self.status_var.set("正在后台停止后端服务器……")
            self.detail_var.set("窗口保持响应；后端退出后将自动关闭。")
            self.progress.configure(mode="indeterminate")
            self.progress.start(12)
            for name in ("config_btn", "index_btn", "minimize_btn"):
                widget = getattr(self, name, None)
                if widget is not None:
                    widget.configure(state="disabled")
        except Exception:
            pass

        def worker() -> None:
            _terminate(process)
            self.backend_proc = None

            def finish() -> None:
                try:
                    self.progress.stop()
                except Exception:
                    pass
                try:
                    self.root.destroy()
                except Exception:
                    pass

            try:
                self.root.after(0, finish)
            except Exception:
                pass

        threading.Thread(target=worker, name="bilipdj-web-portable-stop", daemon=True).start()

    launcher_class.stop_and_exit = stop_and_exit_nonblocking
    launcher_class._issue187_launcher_installed = True
    return True


def patch_web_updater(module: Any) -> bool:
    if bool(getattr(module, "_issue187_page_handoff_installed", False)):
        return True

    def gate(function: Any) -> Any:
        @functools.wraps(function)
        def wrapped(request: dict[str, Any], state: Any, work: Any) -> Any:
            state.set(
                stage="waiting-page",
                percent=1,
                current_file="",
                message="等待独立更新页面接管；页面未打开时不会停止主服务…",
            )
            if not state.page_seen.wait(timeout=8):
                raise module.WebUpdaterError(
                    "更新页面未成功打开或接管，已取消本次更新；主 Web 服务保持运行"
                )
            return function(request, state, work)

        return wrapped

    for name in ("_full_update", "_incremental_update", "_restore_update"):
        function = getattr(module, name, None)
        if not callable(function):
            return False
        setattr(module, name, gate(function))

    module._issue187_page_handoff_installed = True
    return True


__all__ = ["patch_portable_launcher", "patch_web_updater"]
