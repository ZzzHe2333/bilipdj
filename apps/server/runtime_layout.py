from __future__ import annotations

import importlib.util
import os
import shutil
from pathlib import Path
from typing import Any

if __package__:
    from .user_data_paths import (preferred_root, preferred_log_root, preferred_state_root,
        preferred_archive_dir, preferred_backup_dir, migrate, migrate_explicit_root_files, migrate_legacy_local_state,
        seed_client_styles, promote_legacy_archive_settings)
else:
    # Runtime layout has standalone file-loader compatibility probes.
    import importlib.util as _importlib_util
    _spec = _importlib_util.spec_from_file_location(
        "bilipdj_user_data_paths_probe", Path(__file__).resolve().with_name("user_data_paths.py")
    )
    if _spec is None or _spec.loader is None:
        raise ImportError("Could not load user_data_paths")
    _paths = _importlib_util.module_from_spec(_spec)
    _spec.loader.exec_module(_paths)
    preferred_root = _paths.preferred_root
    preferred_log_root = _paths.preferred_log_root
    migrate = _paths.migrate
    migrate_explicit_root_files = _paths.migrate_explicit_root_files
    seed_client_styles = _paths.seed_client_styles
    promote_legacy_archive_settings = _paths.promote_legacy_archive_settings
    preferred_state_root = _paths.preferred_state_root
    preferred_archive_dir = _paths.preferred_archive_dir
    preferred_backup_dir = _paths.preferred_backup_dir
    migrate_legacy_local_state = _paths.migrate_legacy_local_state

CORE_CONFIG_FILES = ("config.yaml", "quanxian.yaml", "kaiguan.yaml")
LEGACY_UPDATE_METADATA_FILES = ("update-result.json",)
DEFAULT_DATA_FILES = ("style.json", "appearance.json")
DATA_DIR_ENV = "BILIPDJ_DATA_DIR"
KEY_DIR_NAME = "key"


def resolve_data_dir(app_dir: Path) -> Path:
    """Resolve small configuration directory (Roaming, Application Support, XDG)."""

    return preferred_root(Path(app_dir))


def data_dir_overridden() -> bool:
    return bool(str(os.getenv(DATA_DIR_ENV, "") or "").strip())


def _seed_default(source: Path, target: Path, *, logger: Any | None = None) -> bool:
    if target.exists() or not source.is_file():
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    if logger is not None:
        logger.info("Seeded runtime default: %s -> %s", source, target)
    return True


def _load_archive_sync():
    """Load the archive helper while keeping file-module CI probes working."""

    if __package__:
        from .local_data_archive import sync_local_data_archive

        return sync_local_data_archive

    helper_path = Path(__file__).resolve().with_name("local_data_archive.py")
    spec = importlib.util.spec_from_file_location("bilipdj_local_data_archive_probe", helper_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {helper_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.sync_local_data_archive


def _sync_windows_roaming_archive(app_root: Path, *, logger: Any | None = None) -> None:
    """Obsolete mirror. The per-user directory is the live data authority."""
    return


def ensure_runtime_layout(
    app_dir: Path,
    *,
    logger: Any | None = None,
    defaults_dir: Path | None = None,
) -> tuple[Path, Path]:
    """Create portable/Docker data folders and migrate legacy root-owned files.

    Program files stay under ``app_dir``. Only user-owned state is redirected to
    ``BILIPDJ_DATA_DIR`` when that environment variable is explicitly set.
    On Windows portable/source runs, user-owned state is additionally mirrored
    to ``%APPDATA%\\bilipdj`` for reinstall recovery while ``core`` remains the
    normal runtime authority.
    """

    app_root = Path(app_dir).resolve()
    data_root = resolve_data_dir(app_root)
    core_dir = data_root / "core"
    key_dir = data_root / KEY_DIR_NAME
    state_root = preferred_state_root(app_root)
    archive_dir = preferred_archive_dir(app_root)
    backup_dir = preferred_backup_dir(app_root)

    for path in (
        data_root,
        core_dir,
        key_dir,
        data_root / "plugins",
        state_root,
        state_root / "cache",
        archive_dir,
        backup_dir,
        preferred_log_root(data_root),
    ):
        path.mkdir(parents=True, exist_ok=True)

    # Recover older Roaming mirror settings before project defaults are copied.
    promote_legacy_archive_settings(data_root)

    if data_dir_overridden():
        # Explicit Docker/hosting mount: never sync with a host user's home.
        # Same safety semantics as desktop migration: copy, never delete,
        # and explicitly resolve every changed two-copy conflict (including
        # headless Docker mounts and user-chosen BILIPDJ_DATA_DIR paths).
        migrate_explicit_root_files(app_root, data_root)
        if defaults_dir is not None:
            for name in DEFAULT_DATA_FILES:
                _seed_default(Path(defaults_dir) / name, data_root / name, logger=logger)
    else:
        # Existing profiles may already have old queue data mirrored into
        # Roaming/core/cd. Bring that into Local FIRST, then check the program
        # directory separately. Divergent files require a deliberate choice.
        migrate_legacy_local_state(data_root, state_root)
        migrate(app_root, data_root, defaults_dir=defaults_dir, state_root=state_root)
    # The older archive code stored blacklist.csv with queue slots. It is
    # a small permission/configuration file and now stays with Roaming config.
    saved_blacklist = core_dir / "blacklist.csv"
    if not saved_blacklist.exists():
        for candidate in (core_dir / "cd" / "blacklist.csv", archive_dir / "blacklist.csv"):
            if candidate.is_file() and not candidate.is_symlink():
                shutil.copy2(candidate, saved_blacklist)
                break
    seed_client_styles(data_root)
    return core_dir, key_dir


def configure_server_runtime_layout(server_module: Any) -> tuple[Path, Path]:
    app_dir = Path(getattr(server_module, "APP_DIR", ".")).resolve()
    defaults_dir = Path(getattr(server_module, "CORE_DIR", app_dir / "core")).resolve()
    data_dir = resolve_data_dir(app_dir)
    core_dir, key_dir = ensure_runtime_layout(
        app_dir,
        logger=getattr(server_module, "LOGGER", None),
        defaults_dir=defaults_dir,
    )
    server_module.DATA_DIR = data_dir
    server_module.CONFIG_PATH = core_dir / "config.yaml"
    server_module.QUANXIAN_PATH = core_dir / "quanxian.yaml"
    server_module.KAIGUAN_PATH = core_dir / "kaiguan.yaml"
    server_module._CONFIG_LOCK_PATH = core_dir / ".config.lock"
    server_module.RUNTIME_CORE_DIR = core_dir
    server_module.KEY_DIR = key_dir
    server_module.UPDATE_RESULT_PATH = key_dir / "update-result.json"

    server_module._YAML_DIR = data_dir
    server_module.LOG_DIR = preferred_log_root(data_dir)
    server_module.PD_DIR = preferred_archive_dir(app_dir)
    server_module.QUEUE_STATE_PATH = server_module.PD_DIR / "queue_archive_state.json"
    server_module.BLACKLIST_PATH = core_dir / "blacklist.csv"
    server_module.STYLE_PATH = data_dir / "style-web.json"
    server_module.APPEARANCE_PATH = data_dir / "appearance-web.json"
    # Generated CSS belongs in persistent storage, not readonly Web assets.
    server_module.LIVE_STYLE_CSS_PATH = data_dir / "moren.css"
    server_module.PLUGINS_DIR = data_dir / "plugins"
    server_module.BACKUP_DIR = preferred_backup_dir(app_dir)
    return core_dir, key_dir


__all__ = [
    "CORE_CONFIG_FILES",
    "DATA_DIR_ENV",
    "DEFAULT_DATA_FILES",
    "KEY_DIR_NAME",
    "configure_server_runtime_layout",
    "data_dir_overridden",
    "ensure_runtime_layout",
    "resolve_data_dir",
]
