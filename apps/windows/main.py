from __future__ import annotations

import multiprocessing as mp
import os
import sys
from pathlib import Path

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
from apps.windows.issue79_features import patch_control_panel_issue79  # noqa: E402
from apps.windows.issue167_command_console import patch_control_panel_command_console  # noqa: E402
from apps.windows.issue167_update_estimate import patch_update_ui  # noqa: E402
from apps.windows.issue194_fixed_window import patch_control_panel_issue194  # noqa: E402
from apps.windows.issue196_log_toolbar import patch_control_panel_issue196  # noqa: E402
from apps.windows.issue209_nav_stability import patch_control_panel_issue209  # noqa: E402


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

# Customer-facing entry points install critical UI patches explicitly instead of
# relying only on legacy class-construction hooks and import order.
patch_control_panel_qr_login(control_panel.ControlPanelApp)
patch_control_panel_about(control_panel.ControlPanelApp)
patch_control_panel_issue79(control_panel.ControlPanelApp)
patch_control_panel_command_console(control_panel.ControlPanelApp)
patch_control_panel_issue194(control_panel.ControlPanelApp)
patch_control_panel_issue196(control_panel.ControlPanelApp)
patch_control_panel_issue209(control_panel.ControlPanelApp)


def main() -> None:
    if "--plugin-runtime-self-test" in sys.argv[1:]:
        from apps.windows.frozen_plugin_probe import run_frozen_plugin_probe

        run_frozen_plugin_probe()
        return
    control_panel.main()


if __name__ == "__main__":
    main()