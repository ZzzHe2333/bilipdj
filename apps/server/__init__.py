"""BiliPDJ backend package.

The implementation now lives in :mod:`apps.server`.  Runtime paths are kept
compatible with existing installs while code is migrated out of ``core``.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
_SERVER_MODULE_NAMES = frozenset({"apps.server.server", "core.server", "server", "__main__"})

# The legacy guards predate the monorepo layout and recognize ``core.server``.
# Patch only their module-name predicates before importing the real server so
# the same safety/performance hooks apply to ``apps.server.server``.
from . import queue_rank_query as _queue_rank_query
from . import server_runtime_guard as _server_runtime_guard
from . import style_option_guard as _style_option_guard
from . import web_queue_layout as _web_queue_layout
from . import websocket_performance_guard as _websocket_performance_guard


def _is_server_frame(frame: Any) -> bool:
    module_name = str(frame.f_globals.get("__name__", ""))
    if module_name in {"apps.server.server", "core.server", "server"}:
        return True
    if module_name != "__main__":
        return False
    return os.path.basename(str(frame.f_globals.get("__file__", ""))) == "server.py"


def _server_module_valid(module: Any) -> bool:
    if module is None:
        return False
    name = str(getattr(module, "__name__", "") or "")
    if name not in _SERVER_MODULE_NAMES:
        return False
    return name != "__main__" or os.path.basename(str(getattr(module, "__file__", ""))) == "server.py"


_queue_rank_query._SERVER_MODULE_NAMES = _SERVER_MODULE_NAMES
_queue_rank_query._is_server_frame = _is_server_frame
_websocket_performance_guard._SERVER_MODULE_NAMES = _SERVER_MODULE_NAMES
_websocket_performance_guard._is_server_frame = _is_server_frame
_server_runtime_guard._server_module_valid = _server_module_valid
_web_queue_layout.install_web_queue_layout_guard(_style_option_guard)

from . import server as server  # noqa: E402


def configure_runtime_paths(module: Any = server) -> Any:
    """Apply source/frozen runtime paths after the physical code migration.

    User configuration/data paths intentionally remain compatible in this
    migration phase.  Web assets, however, now have a single canonical source
    under ``apps/web/static``.
    """
    frozen = bool(getattr(sys, "frozen", False))
    bundle_root = Path(getattr(sys, "_MEIPASS", REPO_ROOT)).resolve()
    app_dir = Path(sys.executable).resolve().parent if frozen else REPO_ROOT
    compatibility_core_dir = REPO_ROOT / "core"
    yaml_dir = app_dir if frozen else compatibility_core_dir

    source_web = REPO_ROOT / "apps" / "web" / "static"
    bundled_web = bundle_root / "apps" / "web" / "static"
    if frozen and bundled_web.is_dir():
        ui_dir = bundled_web
    elif source_web.is_dir():
        ui_dir = source_web
    else:
        ui_dir = bundled_web

    module.REPO_DIR = REPO_ROOT
    module.CORE_DIR = compatibility_core_dir
    module.BUNDLE_DIR = bundle_root
    module.APP_DIR = app_dir
    module._YAML_DIR = yaml_dir
    module.BUNDLE_CORE_DIR = bundle_root / "apps" / "server"
    module.RUNTIME_CORE_DIR = app_dir / "core" if frozen else compatibility_core_dir
    module.BUNDLE_UI_DIR = bundled_web
    module.UI_DIR = ui_dir
    module.CONFIG_PATH = yaml_dir / "config.yaml"
    module.LOG_DIR = app_dir / "log"
    module.PD_DIR = app_dir / "core" / "cd"
    module.QUEUE_STATE_PATH = module.PD_DIR / "queue_archive_state.json"
    module.BLACKLIST_PATH = module.PD_DIR / "blacklist.csv"
    module.QUANXIAN_PATH = yaml_dir / "quanxian.yaml"
    module.KAIGUAN_PATH = yaml_dir / "kaiguan.yaml"
    module.STYLE_PATH = yaml_dir / "style.json"
    module.LIVE_STYLE_CSS_PATH = ui_dir / "moren.css"
    module._CONFIG_LOCK_PATH = yaml_dir / ".config.lock"
    return module


configure_runtime_paths()

__all__ = ["REPO_ROOT", "configure_runtime_paths", "server"]
