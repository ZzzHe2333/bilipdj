from __future__ import annotations

import os
import sys
from pathlib import Path

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

configure_web_assets()

from apps.windows import control_panel  # noqa: E402
from apps.windows.about_page import patch_control_panel_about  # noqa: E402
from apps.windows.bilibili_qr_dialog import patch_control_panel_qr_login  # noqa: E402


def _configure_control_panel_paths() -> None:
    frozen = bool(getattr(sys, "frozen", False))
    bundle_root = Path(getattr(sys, "_MEIPASS", REPO_ROOT)).resolve()
    app_dir = Path(sys.executable).resolve().parent if frozen else REPO_ROOT
    windows_dir = REPO_ROOT / "apps" / "windows"
    config_dir = app_dir if frozen else REPO_ROOT / "core"

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
    control_panel.SERVER_PATH = REPO_ROOT / "apps" / "server" / "main.py"
    control_panel.OVERLAY_HOST_SCRIPT = windows_dir / "overlay_host.py"
    control_panel._BACKEND_SERVER_MODULE = backend


_configure_control_panel_paths()

# Customer-facing entry points install critical UI patches explicitly instead of
# relying only on legacy class-construction hooks and import order.
patch_control_panel_qr_login(control_panel.ControlPanelApp)
patch_control_panel_about(control_panel.ControlPanelApp)


def main() -> None:
    control_panel.main()


if __name__ == "__main__":
    main()
