from __future__ import annotations

import hashlib
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

CORE_CONFIG_FILES = ("config.yaml", "quanxian.yaml", "kaiguan.yaml")
LEGACY_UPDATE_METADATA_FILES = ("update-result.json",)
KEY_DIR_NAME = "key"


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


def ensure_runtime_layout(app_dir: Path, *, logger: Any | None = None) -> tuple[Path, Path]:
    """Create portable data folders and migrate legacy root-owned files.

    Only user-owned runtime data moves here. Program files remain managed by the
    updater. The operation is intentionally idempotent so every launcher may call
    it before opening configuration files.
    """

    root = Path(app_dir).resolve()
    core_dir = root / "core"
    key_dir = root / KEY_DIR_NAME
    core_dir.mkdir(parents=True, exist_ok=True)
    key_dir.mkdir(parents=True, exist_ok=True)

    for name in CORE_CONFIG_FILES:
        _migrate_one(root / name, core_dir / name, logger=logger)
    for name in LEGACY_UPDATE_METADATA_FILES:
        _migrate_one(root / name, key_dir / name, logger=logger)
    return core_dir, key_dir


def configure_server_runtime_layout(server_module: Any) -> tuple[Path, Path]:
    app_dir = Path(getattr(server_module, "APP_DIR", ".")).resolve()
    core_dir, key_dir = ensure_runtime_layout(
        app_dir,
        logger=getattr(server_module, "LOGGER", None),
    )
    server_module.CONFIG_PATH = core_dir / "config.yaml"
    server_module.QUANXIAN_PATH = core_dir / "quanxian.yaml"
    server_module.KAIGUAN_PATH = core_dir / "kaiguan.yaml"
    server_module._CONFIG_LOCK_PATH = core_dir / ".config.lock"
    server_module.RUNTIME_CORE_DIR = core_dir
    server_module.KEY_DIR = key_dir
    server_module.UPDATE_RESULT_PATH = key_dir / "update-result.json"
    return core_dir, key_dir


__all__ = [
    "CORE_CONFIG_FILES",
    "KEY_DIR_NAME",
    "configure_server_runtime_layout",
    "ensure_runtime_layout",
]
