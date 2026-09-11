from __future__ import annotations

import hashlib
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

CORE_CONFIG_FILES = ("config.yaml", "quanxian.yaml", "kaiguan.yaml")
LEGACY_UPDATE_METADATA_FILES = ("update-result.json",)
DEFAULT_DATA_FILES = ("style.json", "appearance.json")
DATA_DIR_ENV = "BILIPDJ_DATA_DIR"
KEY_DIR_NAME = "key"


def resolve_data_dir(app_dir: Path) -> Path:
    """Resolve the user-data root without changing legacy defaults.

    When ``BILIPDJ_DATA_DIR`` is unset, runtime data stays under ``app_dir`` as
    before. Docker can point the variable at a mounted directory such as
    ``/data`` without moving program files there.
    """

    app_root = Path(app_dir).resolve()
    raw = str(os.getenv(DATA_DIR_ENV, "") or "").strip()
    if not raw:
        return app_root
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = app_root / candidate
    return candidate.resolve()


def data_dir_overridden() -> bool:
    return bool(str(os.getenv(DATA_DIR_ENV, "") or "").strip())


def _same_file_content(left: Path, right: Path) -> bool:
    try:
        if left.stat().st_size != right.stat().st_size:
            return False

        def digest(path: Path) -> str:
            h = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    h.update(chunk)
            return h.hexdigest()

        return digest(left) == digest(right)
    except OSError:
        return False


def _migration_backup_path(destination_dir: Path, source: Path) -> Path:
    backup_dir = destination_dir / "migration-backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    candidate = backup_dir / f"{source.name}.legacy-root-{stamp}"
    index = 2
    while candidate.exists():
        candidate = backup_dir / f"{source.name}.legacy-root-{stamp}-{index}"
        index += 1
    return candidate


def _migrate_one(source: Path, target: Path, *, logger: Any | None = None) -> bool:
    if not source.is_file():
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        try:
            source.replace(target)
        except OSError:
            shutil.copy2(source, target)
            source.unlink(missing_ok=True)
        if logger is not None:
            logger.info("Migrated legacy runtime file: %s -> %s", source, target)
        return True

    if _same_file_content(source, target):
        source.unlink(missing_ok=True)
        return True

    try:
        source_mtime = source.stat().st_mtime_ns
        target_mtime = target.stat().st_mtime_ns
    except OSError:
        source_mtime = target_mtime = 0

    if source_mtime > target_mtime:
        backup = _migration_backup_path(target.parent, target)
        shutil.copy2(target, backup)
        try:
            source.replace(target)
        except OSError:
            shutil.copy2(source, target)
            source.unlink(missing_ok=True)
        if logger is not None:
            logger.warning(
                "Legacy root file was newer; preserved previous target at %s and migrated %s",
                backup,
                source,
            )
    else:
        backup = _migration_backup_path(target.parent, source)
        shutil.copy2(source, backup)
        source.unlink(missing_ok=True)
        if logger is not None:
            logger.warning(
                "Both legacy and migrated runtime files existed; kept %s and preserved legacy copy at %s",
                target,
                backup,
            )
    return True


def _seed_default(source: Path, target: Path, *, logger: Any | None = None) -> bool:
    if target.exists() or not source.is_file():
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    if logger is not None:
        logger.info("Seeded runtime default: %s -> %s", source, target)
    return True


def ensure_runtime_layout(
    app_dir: Path,
    *,
    logger: Any | None = None,
    defaults_dir: Path | None = None,
) -> tuple[Path, Path]:
    """Create portable/Docker data folders and migrate legacy root-owned files.

    Program files stay under ``app_dir``. Only user-owned state is redirected to
    ``BILIPDJ_DATA_DIR`` when that environment variable is explicitly set.
    """

    app_root = Path(app_dir).resolve()
    data_root = resolve_data_dir(app_root)
    core_dir = data_root / "core"
    key_dir = data_root / KEY_DIR_NAME

    for path in (
        data_root,
        core_dir,
        key_dir,
        data_root / "log",
        data_root / "plugins",
        data_root / "backup",
        core_dir / "cd",
    ):
        path.mkdir(parents=True, exist_ok=True)

    for name in CORE_CONFIG_FILES:
        _migrate_one(app_root / name, core_dir / name, logger=logger)
    for name in LEGACY_UPDATE_METADATA_FILES:
        _migrate_one(app_root / name, key_dir / name, logger=logger)

    if data_dir_overridden() and defaults_dir is not None:
        source_root = Path(defaults_dir).resolve()
        for name in DEFAULT_DATA_FILES:
            _seed_default(source_root / name, data_root / name, logger=logger)

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

    if data_dir_overridden():
        server_module._YAML_DIR = data_dir
        server_module.LOG_DIR = data_dir / "log"
        server_module.PD_DIR = core_dir / "cd"
        server_module.QUEUE_STATE_PATH = server_module.PD_DIR / "queue_archive_state.json"
        server_module.BLACKLIST_PATH = server_module.PD_DIR / "blacklist.csv"
        server_module.STYLE_PATH = data_dir / "style.json"
        server_module.APPEARANCE_PATH = data_dir / "appearance.json"
        server_module.PLUGINS_DIR = data_dir / "plugins"
        server_module.BACKUP_DIR = data_dir / "backup"
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
