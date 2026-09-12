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
    # Required by PyInstaller when JavaScript plugins spawn their isolated worker.
    mp.freeze_support()

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Customer-facing frozen builds are portable bundles: launching the Tk frontend
# must always bring up its embedded backend automatically. Source/developer runs
# keep the existing configurable auto-start behavior.
if getattr(sys, "frozen", False):
    os.environ.setdefault("BILIPDJ_PORTABLE_AUTO_BACKEND", "1")

from apps.server import server as backend  # noqa: E402
from apps.server.main import configure_web_assets  # noqa: E402
from apps.server.runtime_layout import ensure_runtime_layout  # noqa: E402

configure_web_assets()

from apps.windows import control_panel, update_ui  # noqa: E402
from apps.windows.about_page import patch_control_panel_about  # noqa: E402
from apps.windows.bilibili_qr_dialog import patch_control_panel_qr_login  # noqa: E402
from apps.windows.customtk_ui import (  # noqa: E402
    BiliPDJCTk,
    WINDOW_HEIGHT,
    WINDOW_WIDTH,
    patch_control_panel_customtkinter,
    run_control_panel,
)
from apps.windows.issue79_features import patch_control_panel_issue79  # noqa: E402
from apps.windows.issue167_command_console import patch_control_panel_command_console  # noqa: E402
from apps.windows.issue167_update_estimate import patch_update_ui  # noqa: E402
from apps.windows.issue194_fixed_window import patch_control_panel_issue194  # noqa: E402
from apps.windows.issue196_log_toolbar import patch_control_panel_issue196  # noqa: E402
from apps.windows.issue209_nav_stability import patch_control_panel_issue209  # noqa: E402


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
patch_update_ui(update_ui)

# Install the CustomTkinter shell first.  The existing feature patches then wrap
# this stable shell, preserving business behavior while keeping navigation
# geometry independent from label requested widths.
patch_control_panel_customtkinter(control_panel.ControlPanelApp)

# Customer-facing entry points install critical UI patches explicitly instead of
# relying only on legacy class-construction hooks and import order.
patch_control_panel_qr_login(control_panel.ControlPanelApp)
patch_control_panel_about(control_panel.ControlPanelApp)
patch_control_panel_issue79(control_panel.ControlPanelApp)
patch_control_panel_command_console(control_panel.ControlPanelApp)
patch_control_panel_issue194(control_panel.ControlPanelApp)
patch_control_panel_issue196(control_panel.ControlPanelApp)
# Kept for compatibility with older entry points.  Issue #209 detects the CTk
# marker and becomes a no-op instead of re-measuring the navigation width.
patch_control_panel_issue209(control_panel.ControlPanelApp)


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
    """Exercise the real frozen GUI startup path, including first show events."""

    root = BiliPDJCTk()
    root.withdraw()
    try:
        root.wm_attributes("-alpha", 0)
    except Exception:
        pass
    callback_errors: list[str] = []

    def report_callback_exception(exc_type: type[BaseException], exc: BaseException, tb: Any) -> None:
        trace = "".join(traceback.format_exception(exc_type, exc, tb))
        callback_errors.append(trace)
        _write_startup_error("Tk callback exception during GUI startup self-test", exc=exc, trace=trace)

    root.report_callback_exception = report_callback_exception  # type: ignore[method-assign]
    app: Any | None = None
    started = time.monotonic()
    try:
        app = control_panel.ControlPanelApp(root)
        root.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}")
        root.minsize(WINDOW_WIDTH, WINDOW_HEIGHT)
        root.maxsize(WINDOW_WIDTH, WINDOW_HEIGHT)
        root.resizable(False, False)
        root.update_idletasks()
        root.deiconify()
        try:
            root.wm_attributes("-alpha", 1)
        except Exception:
            pass
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
            root.update()
            time.sleep(0.02)

        if time.monotonic() - started < 3.5:
            raise RuntimeError("GUI startup self-test ended prematurely")
    finally:
        if app is not None:
            _stop_probe_process(getattr(app, "overlay_proc", None))
            _stop_probe_process(getattr(app, "server_proc", None))
        try:
            if root.winfo_exists():
                root.destroy()
        except Exception:
            pass


def main() -> None:
    if "--plugin-runtime-self-test" in sys.argv[1:]:
        from apps.windows.frozen_plugin_probe import run_frozen_plugin_probe

        run_frozen_plugin_probe()
        return
    if "--gui-startup-self-test" in sys.argv[1:]:
        _run_gui_startup_self_test()
        return
    run_control_panel(control_panel)


if __name__ == "__main__":
    try:
        main()
    except BaseException as exc:
        log_path = _write_startup_error(str(exc) or type(exc).__name__, exc=exc)
        if "--gui-startup-self-test" not in sys.argv[1:]:
            _show_startup_error(str(exc) or type(exc).__name__, log_path)
        raise
