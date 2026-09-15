from __future__ import annotations

import functools
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from . import update_stability

_PATCH_LOCK = threading.RLock()


def _process_running(process: Any) -> bool:
    if process is None:
        return False
    try:
        return process.poll() is None
    except Exception:
        return False


def _taskkill_tree(pid: int, *, force: bool) -> None:
    if sys.platform != "win32" or pid <= 0:
        return
    command = ["taskkill", "/PID", str(int(pid)), "/T"]
    if force:
        command.append("/F")
    try:
        subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception:
        pass


def terminate_process_tree(process: Any, *, timeout: float = 3.0) -> None:
    """Stop one child and, on Windows, every descendant it created."""

    if not _process_running(process):
        return

    if sys.platform == "win32":
        # Popen.terminate() only terminates the direct process on Windows.  The
        # backend can own helper descendants, so close the complete tree first.
        _taskkill_tree(int(process.pid), force=False)
        try:
            process.wait(timeout=min(max(timeout, 0.2), 2.0))
            return
        except subprocess.TimeoutExpired:
            pass
        except Exception:
            if not _process_running(process):
                return

        _taskkill_tree(int(process.pid), force=True)
        try:
            process.wait(timeout=max(timeout, 1.0))
            return
        except subprocess.TimeoutExpired:
            pass
        except Exception:
            if not _process_running(process):
                return
    else:
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


def _arm_exit_watchdog(panel: Any, processes: tuple[Any, ...], *, delay: float = 7.0) -> None:
    """Guarantee that a frozen Windows GUI cannot remain headless forever."""

    if sys.platform != "win32" or not bool(getattr(sys, "frozen", False)):
        return
    if bool(getattr(panel, "_issue271_exit_watchdog_armed", False)):
        return
    panel._issue271_exit_watchdog_armed = True

    def watchdog() -> None:
        time.sleep(max(delay, 1.0))
        for process in processes:
            try:
                terminate_process_tree(process, timeout=1.0)
            except Exception:
                pass
        # At this point the user explicitly requested application exit.  This
        # is a final safeguard for non-daemon/native threads that can otherwise
        # keep the PyInstaller process visible in Task Manager after Tk closes.
        os._exit(0)

    threading.Thread(
        target=watchdog,
        name="bilipdj-windows-exit-watchdog",
        daemon=True,
    ).start()


def install_shutdown_guard(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = sys.modules.get(str(getattr(panel_class, "__module__", "") or ""))
    if module is None or Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_issue271_shutdown_guard_installed", False)):
            return True

        # issue187's async close callback resolves this module-global function
        # at call time, so replacing it also upgrades update/close cleanup.
        update_stability._terminate_process = terminate_process_tree  # type: ignore[attr-defined]  # noqa: SLF001

        original_on_close = getattr(panel_class, "on_close", None)
        if not callable(original_on_close):
            return False

        @functools.wraps(original_on_close)
        def on_close_with_process_guard(self: Any) -> Any:
            if not bool(getattr(self, "_issue271_close_requested", False)):
                self._issue271_close_requested = True
                _arm_exit_watchdog(
                    self,
                    (
                        getattr(self, "overlay_proc", None),
                        getattr(self, "server_proc", None),
                    ),
                )
            return original_on_close(self)

        panel_class.on_close = on_close_with_process_guard
        panel_class._issue271_shutdown_guard_installed = True
        return True


__all__ = ["install_shutdown_guard", "terminate_process_tree"]
