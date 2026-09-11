"""BiliPDJ backend package.

The implementation now lives in :mod:`apps.server`. Runtime paths are kept
compatible with existing installs while code is migrated out of ``core``.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
_SERVER_MODULE_NAMES = frozenset({"apps.server.server", "core.server", "server", "__main__"})

from . import queue_rank_query as _queue_rank_query
from . import server_runtime_guard as _server_runtime_guard
from . import style_option_guard as _style_option_guard
from . import web_queue_layout as _web_queue_layout
from . import websocket_performance_guard as _websocket_performance_guard
from .runtime_layout import data_dir_overridden as _data_dir_overridden
from .runtime_layout import ensure_runtime_layout as _ensure_runtime_layout
from .runtime_layout import resolve_data_dir as _resolve_data_dir


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

_queue_rank_query.attach_queue_rank_query(server.QueueManager)
_websocket_performance_guard.patch_websocket_hub(server.WebSocketHub)
_websocket_performance_guard._restore_build_class_hook()
_queue_rank_query._restore_build_class_hook()


def configure_runtime_paths(module: Any = server) -> Any:
    """Apply source/frozen runtime paths and optional external data storage."""
    frozen = bool(getattr(sys, "frozen", False))
    bundle_root = Path(getattr(sys, "_MEIPASS", REPO_ROOT)).resolve()
    app_dir = Path(sys.executable).resolve().parent if frozen else REPO_ROOT
    compatibility_core_dir = REPO_ROOT / "core"
    data_dir = _resolve_data_dir(app_dir)
    external_data_dir = _data_dir_overridden()
    runtime_core_dir, key_dir = _ensure_runtime_layout(
        app_dir,
        defaults_dir=compatibility_core_dir,
    )
    # Legacy source/portable layouts stay unchanged unless BILIPDJ_DATA_DIR is
    # explicitly configured. Docker uses the external data root for settings.
    misc_config_dir = data_dir if external_data_dir else (app_dir if frozen else compatibility_core_dir)

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
    module.DATA_DIR = data_dir
    module._YAML_DIR = misc_config_dir
    module.BUNDLE_CORE_DIR = bundle_root / "apps" / "server"
    module.RUNTIME_CORE_DIR = runtime_core_dir
    module.BUNDLE_UI_DIR = bundled_web
    module.UI_DIR = ui_dir
    module.CONFIG_PATH = runtime_core_dir / "config.yaml"
    module.LOG_DIR = (data_dir if external_data_dir else app_dir) / "log"
    module.PD_DIR = runtime_core_dir / "cd"
    module.QUEUE_STATE_PATH = module.PD_DIR / "queue_archive_state.json"
    module.BLACKLIST_PATH = module.PD_DIR / "blacklist.csv"
    module.QUANXIAN_PATH = runtime_core_dir / "quanxian.yaml"
    module.KAIGUAN_PATH = runtime_core_dir / "kaiguan.yaml"
    module.STYLE_PATH = misc_config_dir / "style.json"
    module.APPEARANCE_PATH = misc_config_dir / "appearance.json"
    module.KEY_DIR = key_dir
    module.UPDATE_RESULT_PATH = key_dir / "update-result.json"
    module.PLUGINS_DIR = (data_dir if external_data_dir else app_dir) / "plugins"
    module.BACKUP_DIR = (data_dir if external_data_dir else app_dir) / "backup"
    module.LIVE_STYLE_CSS_PATH = ui_dir / "moren.css"
    module._CONFIG_LOCK_PATH = runtime_core_dir / ".config.lock"
    return module


configure_runtime_paths()

from . import settings_backup as _settings_backup  # noqa: E402
from . import settings_backup_bugfix_guard as _settings_backup_bugfix_guard  # noqa: E402
from . import settings_mtime_guard as _settings_mtime_guard  # noqa: E402
from . import settings_storage_guard as _settings_storage_guard  # noqa: E402
from . import web_control_guard as _web_control_guard  # noqa: E402
from . import issue79_guard as _issue79_guard  # noqa: E402
from . import huya_runtime_guard as _huya_runtime_guard  # noqa: E402
from . import youtube_runtime_guard as _youtube_runtime_guard  # noqa: E402
from . import twitch_runtime_guard as _twitch_runtime_guard  # noqa: E402
from . import danmu_plugins as _danmu_plugins  # noqa: E402
from . import plugin_manager as _plugin_manager  # noqa: E402
from . import plugin_runtime_dual as _plugin_runtime_dual  # noqa: E402
from . import plugin_secret_guard as _plugin_secret_guard  # noqa: E402
from . import plugin_mutation_guard as _plugin_mutation_guard  # noqa: E402
from . import plugin_api_security_guard as _plugin_api_security_guard  # noqa: E402
from . import plugin_config_schema as _plugin_config_schema  # noqa: E402
from . import plugin_config_web as _plugin_config_web  # noqa: E402
from . import appearance_guard as _appearance_guard  # noqa: E402
from . import issue123_guard as _issue123_guard  # noqa: E402
from . import security_hardening_guard as _security_hardening_guard  # noqa: E402
from . import docker_runtime as _docker_runtime  # noqa: E402

# appearance.json is a first-class cross-client setting and participates in the
# same ZIP/WebDAV backup format as config.yaml/style.json.
if "appearance.json" not in _settings_backup.SETTINGS_FILES:
    _settings_backup.SETTINGS_FILES = tuple(_settings_backup.SETTINGS_FILES) + ("appearance.json",)
_original_settings_paths = _settings_backup.SettingsBackupService.settings_paths


def _settings_paths_with_appearance(self: Any) -> dict[str, Path]:
    paths = dict(_original_settings_paths(self))
    paths["appearance.json"] = Path(getattr(self.server, "APPEARANCE_PATH", Path(getattr(self.server, "_YAML_DIR")) / "appearance.json"))
    return paths


_settings_backup.SettingsBackupService.settings_paths = _settings_paths_with_appearance

_settings_backup.install_settings_backup(server)
_settings_mtime_guard.install_settings_mtime_guard(_settings_backup)
_settings_storage_guard.install_settings_storage_guard(_settings_backup, server)
_settings_backup_bugfix_guard.install_settings_backup_bugfix_guard(_settings_backup)
_web_control_guard.install_web_control_guard(server)

# Legacy platform guards still own protocol-specific compatibility fixes and UI
# fields. After they expose their Relay classes, Plugin API v1 becomes the only
# authoritative platform discovery/factory path.
_huya_runtime_guard.install_huya_runtime_guard(server, _issue79_guard)
_youtube_runtime_guard.install_youtube_runtime_guard(server, _issue79_guard)
_twitch_runtime_guard.install_twitch_runtime_guard(server, _issue79_guard)
_danmu_plugins.install_danmu_plugin_system(server, _issue79_guard)
_plugin_runtime_dual.install_dual_runtime_support()
_plugin_secret_guard.install_plugin_secret_guard(_plugin_manager)
_docker_runtime.install_plugin_data_root(_plugin_manager)
_plugin_manager.install_plugin_manager(server, _issue79_guard)
_issue79_guard.install_issue79_guard(server)

# Issue #136 defines the final config-aware PluginManager mutation methods first.
# Issue #134 then wraps those final methods with the shared RLock so config-aware
# enable/uninstall cannot bypass the existing mutation serialization guarantee.
_plugin_config_schema.install_plugin_config_schema(server, _plugin_manager, _plugin_api_security_guard)
_plugin_mutation_guard.install_plugin_mutation_guard(_plugin_manager)
_plugin_config_web.install_plugin_config_web(server, _plugin_manager)

_appearance_guard.install_appearance_guard(server)
_issue123_guard.install_issue123_guard(
    server,
    _issue79_guard,
    _youtube_runtime_guard.youtube_protocol,
    _settings_backup,
)
_security_hardening_guard.install_security_hardening(server)

# Keep this outermost: original plugin-management POST requests must pass the
# browser origin/content-type/body-size safety boundary before their handlers.
_plugin_api_security_guard.install_plugin_api_security_guard(server, _plugin_manager)
# Docker source-address translation is handled last so every existing local-only
# guard keeps its Host/Origin validation and only the transport address changes.
_docker_runtime.install_docker_local_access(server)

__all__ = ["REPO_ROOT", "configure_runtime_paths", "server"]
