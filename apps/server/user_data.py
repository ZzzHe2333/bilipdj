"""Cross-platform persistent data placement and non-destructive legacy migration.

No import from apps.server: this module must work before backend initialization
and in source-level CI probes. Explicit BILIPDJ_DATA_DIR remains authoritative.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping

DATA_CHOICE_FILE = ".storage-choice.json"
CHOICE_ENV = "BILIPDJ_DATA_CHOICE"
DATA_ENV = "BILIPDJ_DATA_DIR"

# Only actual user data: never migrate executable scripts, bundled UI or caches.
MIGRATION_FILES = (
    "core/config.yaml", "core/quanxian.yaml", "core/kaiguan.yaml",
    "core/blacklist.csv", "core/webdav_backup.json",
    "core/gift_compatibility.json", "core/language.json",
    "config.yaml", "quanxian.yaml", "kaiguan.yaml",
    "blacklist.csv", "webdav_backup.json",
    "gift_compatibility.json", "language.json",
    "core/style.json", "core/appearance.json",
    "style.json", "appearance.json",
)
MIGRATION_DIRS = ("core/cd", "plugins", "key", "backup")


def user_data_root(
    *, platform: str | None = None, environment: Mapping[str, str] | None = None,
    home: Path | None = None
) -> Path:
    platform = sys.platform if platform is None else platform
    env = os.environ if environment is None else environment
    home_path = Path.home() if home is None else Path(home)
    if platform == "win32":
        return Path(env.get("APPDATA") or (home_path / "AppData" / "Roaming")) / "bilipdj"
    if platform == "darwin":
        return home_path / "Library" / "Application Support" / "bilipdj"
    raw = str(env.get("XDG_DATA_HOME", "") or "").strip()
    base = Path(raw).expanduser() if raw and Path(raw).expanduser().is_absolute() else home_path / ".local" / "share"
    return base / "bilipdj"


def local_log_root(
    *, platform: str | None = None, environment: Mapping[str, str] | None = None,
    home: Path | None = None,
) -> Path:
    platform = sys.platform if platform is None else platform
    env = os.environ if environment is None else environment
    h = Path.home() if home is None else Path(home)
    if platform == "win32":
        return Path(env.get("LOCALAPPDATA") or (h / "AppData" / "Local")) / "bilipdj" / "log"
    if platform == "darwin":
        return h / "Library" / "Logs" / "bilipdj"
    raw = str(env.get("XDG_STATE_HOME", "") or "").strip()
    base = Path(raw).expanduser() if raw and Path(raw).expanduser().is_absolute() else h / ".local" / "state"
    return base / "bilipdj" / "log"


def _is_safe_file(path: Path) -> bool:
    return path.is_file() and not path.is_symlink()


def _is_safe_dir(path: Path) -> bool:
    return path.is_dir() and not path.is_symlink()


def _user_content_exists(root: Path) -> bool:
    for rel in MIGRATION_FILES:
        if _is_safe_file(root / rel):
            return True
    for rel in MIGRATION_DIRS:
        directory = root / rel
        if not _is_safe_dir(directory):
            continue
        try:
            if any(p.name not in {".bilipdj-appdata-sync-v1", ".gitkeep"} for p in directory.iterdir() if not p.is_symlink()):
                return True
        except OSError:
            continue
    for name in ("style-win.json", "style-web.json", "appearance-win.json", "appearance-web.json"):
        if _is_safe_file(root / name):
            return True
    return False


def _load_decision(destination: Path) -> str:
    file = destination / DATA_CHOICE_FILE
    if not _is_safe_file(file):
        return ""
    try:
        data = json.loads(file.read_text(encoding="utf-8"))
        value = str(data.get("choice", "") or "").lower() if isinstance(data, dict) else ""
        return value if value in {"user", "legacy"} else ""
    except (OSError, ValueError):
        return ""


def _store_decision(destination: Path, choice: str) -> None:
    if choice not in {"user", "legacy"}:
        raise ValueError("choice must be user or legacy")
    destination.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".storage-choice-", suffix=".tmp", dir=str(destination))
    os.close(fd)
    temp = Path(tmp)
    try:
        temp.write_text(json.dumps({"choice": choice, "schema": 1}, ensure_ascii=False) + "\n", encoding="utf-8")
        os.replace(temp, destination / DATA_CHOICE_FILE)
    finally:
        temp.unlink(missing_ok=True)


def storage_plan(
    app_dir: Path, *, destination: Path | None = None, environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    env = os.environ if environment is None else environment
    legacy = Path(app_dir).expanduser().resolve()
    explicit = str(env.get(DATA_ENV, "") or "").strip()
    if explicit:
        target = Path(explicit).expanduser()
        if not target.is_absolute():
            target = legacy / target
        return {"mode": "explicit", "legacy": legacy, "user": target.resolve(),
                "active": target.resolve(), "conflict": False, "choice": "explicit", "migrate": False}
    target = (Path(destination) if destination is not None else user_data_root(environment=env)).expanduser().resolve()
    if target == legacy or target in legacy.parents or legacy in target.parents:
        raise ValueError("用户数据目录必须与程序目录分离，避免覆盖应用文件")
    old_has = _user_content_exists(legacy)
    new_has = _user_content_exists(target)
    stored = str(env.get(CHOICE_ENV, "") or "").strip().lower() or _load_decision(target)
    choice = stored if stored in {"legacy", "user"} else ""
    conflict = old_has and new_has and not choice
    active = legacy if (choice == "legacy" or conflict) else target
    return {
        "mode": "managed", "legacy": legacy, "user": target, "active": active,
        "conflict": conflict, "choice": choice, "migrate": old_has and not new_has and active == target,
        "old_has_data": old_has, "new_has_data": new_has,
    }


def _copy_missing(source: Path, destination: Path) -> int:
    count = 0
    for rel in MIGRATION_FILES:
        source_path = source / rel
        target = destination / rel
        if not _is_safe_file(source_path) or target.exists() or target.is_symlink():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target)
        count += 1
    for rel in MIGRATION_DIRS:
        base = source / rel
        if not _is_safe_dir(base):
            continue
        for child in base.rglob("*"):
            if child.is_symlink() or not child.is_file() or not all(not parent.is_symlink() for parent in child.parents if parent != base and base in parent.parents):
                continue
            rel_child = child.relative_to(base)
            target = destination / rel / rel_child
            if target.exists() or target.is_symlink():
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(child, target)
            count += 1
    return count


def migrate_if_needed(plan: dict[str, Any]) -> int:
    """Copy, never move or overwrite. Auto-migration pins the new root."""
    if plan.get("mode") != "managed" or not plan.get("migrate"):
        return 0
    destination = Path(plan["user"])
    copied = _copy_missing(Path(plan["legacy"]), destination)
    _store_decision(destination, "user")
    return copied


def choose_storage(app_dir: Path, choice: str, *, destination: Path | None = None) -> dict[str, Any]:
    """Save a user decision. Restart is required to rebind existing module paths."""
    if choice not in {"legacy", "user"}:
        raise ValueError("Invalid storage choice")
    plan = storage_plan(app_dir, destination=destination)
    if plan["mode"] == "explicit":
        raise ValueError("Explicit BILIPDJ_DATA_DIR cannot be changed via UI")
    _store_decision(Path(plan["user"]), choice)
    return {"choice": choice, "restart_required": True, "active": str(plan["active"])}


__all__ = [
    "DATA_CHOICE_FILE", "DATA_ENV", "CHOICE_ENV", "MIGRATION_FILES", "MIGRATION_DIRS",
    "user_data_root", "local_log_root", "storage_plan", "migrate_if_needed", "choose_storage",
]
