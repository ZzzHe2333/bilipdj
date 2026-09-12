from __future__ import annotations

import multiprocessing as mp
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

if __name__ == "__main__":
    mp.freeze_support()

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

if getattr(sys, "frozen", False):
    os.environ.setdefault("BILIPDJ_PORTABLE_AUTO_BACKEND", "1")

from apps.server import server as backend  # noqa: E402
from apps.server.main import configure_web_assets  # noqa: E402
from apps.server.runtime_layout import ensure_runtime_layout  # noqa: E402

configure_web_assets()

from apps.windows import control_panel, update_ui  # noqa: E402
from apps.windows.about_page import patch_control_panel_about  # noqa: E402
from apps.windows.bilibili_qr_dialog import patch_control_panel_qr_login  # noqa: E402
from apps.windows.command_console_ui import patch_control_panel_command_console  # noqa: E402
from apps.windows.customtk_ui import (  # noqa: E402
    BiliPDJCTk,
    WINDOW_HEIGHT,
    WINDOW_WIDTH,
    patch_control_panel_customtkinter,
)
from apps.windows.log_toolbar import patch_control_panel_issue196  # noqa: E402
from apps.windows.navigation_layout import patch_control_panel_issue209  # noqa: E402
from apps.windows.platform_features import patch_control_panel_issue79  # noqa: E402
from apps.windows.update_estimate_ui import patch_update_ui  # noqa: E402
from apps.windows.update_workspace_runtime import install_windows_update_workspace  # noqa: E402
from apps.windows.window_policy import patch_control_panel_issue194  # noqa: E402

GUI_STARTUP_LOG_NAME = "gui-startup-error.log"


def _application_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return REPO_ROOT


def _startup_log_path() -> Path:
    return _application_dir() / "log" / GUI_STARTUP_LOG_NAME


def _write_startup_error(message: str, *, exc: BaseException | None = None, trace: str = "") -> Path:
    path = _startup_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not trace and exc is not None:
        trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    lines = [
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Windows GUI startup failure",
        f"message: {message}",
        f"frozen: {bool(getattr(sys, 'frozen', False))}",
        f"executable: {sys.executable}",
        f"cwd: {Path.cwd()}",
    ]
    if trace:
        lines.extend(("traceback:", trace.rstrip()))
    with path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n\n")
    return path


def _show_startup_error(message: str, log_path: Path) -> None:
    if os.name != "nt":
        return
    try:
        import ctypes

        ctypes.windll.user32.MessageBoxW(
            0,
            f"桌面端启动失败。\n\n{message}\n\n错误日志：\n{log_path}",
            "BiliPDJ 启动失败",
            0x10,
        )
    except Exception:
        pass


def _configure_control_panel_paths() -> None:
    frozen = bool(getattr(sys, "frozen", False))
    bundle_root = Path(getattr(sys, "_MEIPASS", REPO_ROOT)).resolve()
    app_dir = Path(sys.executable).resolve().parent if frozen else REPO_ROOT
    windows_dir = REPO_ROOT / "apps" / "windows"
    config_dir, key_dir = ensure_runtime_layout(app_dir)

    control_panel.REPO_DIR = REPO_ROOT
    control_panel.CORE_DIR = windows_dir
    control_panel.BUNDLE_DIR = bundle_root
    control_panel.APP_DIR = app_dir
    control_panel._YAML_DIR = config_dir
    control_panel.BUNDLE_CORE_DIR = bundle_root / "apps" / "windows"
    control_panel.RUNTIME_CORE_DIR = windows_dir
    control_panel.CONFIG_PATH = config_dir / "config.yaml"
    control_panel.QUANXIAN_PATH = config_dir / "quanxian.yaml"
    control_panel.KAIGUAN_PATH = config_dir / "kaiguan.yaml"
    control_panel.KEY_DIR = key_dir
    control_panel.SERVER_PATH = REPO_ROOT / "apps" / "server" / "main.py"
    control_panel.OVERLAY_HOST_SCRIPT = windows_dir / "overlay_host.py"
    control_panel._BACKEND_SERVER_MODULE = backend


_configure_control_panel_paths()
install_windows_update_workspace()
patch_update_ui(update_ui)
patch_control_panel_customtkinter(control_panel.ControlPanelApp)
patch_control_panel_qr_login(control_panel.ControlPanelApp)
patch_control_panel_about(control_panel.ControlPanelApp)
patch_control_panel_issue79(control_panel.ControlPanelApp)
patch_control_panel_command_console(control_panel.ControlPanelApp)
patch_control_panel_issue194(control_panel.ControlPanelApp)
patch_control_panel_issue196(control_panel.ControlPanelApp)
patch_control_panel_issue209(control_panel.ControlPanelApp)


def _install_callback_error_logger(root: Any) -> list[str]:
    errors: list[str] = []

    def report_callback_exception(exc_type: type[BaseException], exc: BaseException, tb: Any) -> None:
        trace = "".join(traceback.format_exception(exc_type, exc, tb))
        errors.append(trace)
        log_path = _write_startup_error("Tk callback exception", exc=exc, trace=trace)
        _show_startup_error(str(exc) or type(exc).__name__, log_path)

    root.report_callback_exception = report_callback_exception
    return errors


def _finish_root_show(root: Any) -> None:
    root.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}")
    root.minsize(WINDOW_WIDTH, WINDOW_HEIGHT)
    root.maxsize(WINDOW_WIDTH, WINDOW_HEIGHT)
    root.resizable(False, False)
    root.update_idletasks()
    root.deiconify()
    try:
        root.state("normal")
    except Exception:
        pass
    try:
        root.lift()
    except Exception:
        pass


def _create_desktop() -> tuple[Any, Any, list[str]]:
    root = BiliPDJCTk()
    root.withdraw()
    callback_errors = _install_callback_error_logger(root)
    app = control_panel.ControlPanelApp(root)
    root._bilipdj_control_panel = app  # type: ignore[attr-defined]
    _finish_root_show(root)
    return root, app, callback_errors


def _stop_probe_process(process: Any) -> None:
    if process is None:
        return
    try:
        if process.poll() is not None:
            return
    except Exception:
        return
    try:
        process.terminate()
        process.wait(timeout=4)
    except Exception:
        try:
            process.kill()
        except Exception:
            pass


def _run_gui_startup_self_test() -> None:
    root: Any | None = None
    app: Any | None = None
    callback_errors: list[str] = []
    started = time.monotonic()
    try:
        root, app, callback_errors = _create_desktop()
        root.update()
        deadline = time.monotonic() + 4.0
        while time.monotonic() < deadline:
            if callback_errors:
                raise RuntimeError("GUI startup callback raised an exception")
            try:
                exists = bool(root.winfo_exists())
            except Exception as exc:
                raise RuntimeError("GUI root became unavailable during startup") from exc
            if not exists:
                raise RuntimeError("GUI root was destroyed during startup")
            try:
                if str(root.state()) == "withdrawn":
                    raise RuntimeError("GUI root returned to withdrawn state after first show")
            except RuntimeError:
                raise
            except Exception:
                pass
            root.update()
            time.sleep(0.02)
        if time.monotonic() - started < 3.5:
            raise RuntimeError("GUI startup self-test ended prematurely")
    finally:
        if app is not None:
            _stop_probe_process(getattr(app, "overlay_proc", None))
            _stop_probe_process(getattr(app, "server_proc", None))
        if root is not None:
            try:
                if root.winfo_exists():
                    root.destroy()
            except Exception:
                pass


def _run_desktop() -> None:
    if "--backend" in sys.argv[1:] or "--overlay-host" in sys.argv[1:]:
        control_panel.main()
        return
    root, _app, _errors = _create_desktop()
    root.mainloop()


def _run_self_test_and_exit(test: Any, label: str) -> None:
    try:
        test()
    except BaseException as exc:
        _write_startup_error(f"{label} failed: {exc}", exc=exc)
        os._exit(1)
    os._exit(0)


def main() -> None:
    if "--plugin-runtime-self-test" in sys.argv[1:]:
        from apps.windows.frozen_plugin_probe import run_frozen_plugin_probe

        _run_self_test_and_exit(run_frozen_plugin_probe, "plugin runtime self-test")
    if "--gui-startup-self-test" in sys.argv[1:]:
        _run_self_test_and_exit(_run_gui_startup_self_test, "GUI startup self-test")
    _run_desktop()


if __name__ == "__main__":
    try:
        main()
    except BaseException as exc:
        log_path = _write_startup_error(str(exc) or type(exc).__name__, exc=exc)
        if "--gui-startup-self-test" not in sys.argv[1:]:
            _show_startup_error(str(exc) or type(exc).__name__, log_path)
        raise
