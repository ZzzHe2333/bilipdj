"""Portable user-data location and non-destructive legacy migration.

The explicit BILIPDJ_DATA_DIR contract (Docker, hosted deployments) takes
priority over platform defaults. User-controlled conflict choice is required
when two different copies exist; never decide by modification time.
"""
from __future__ import annotations

import os
import shutil
import sys
import hashlib
import json
from pathlib import Path

DATA_FILES = (
    "core/config.yaml", "core/quanxian.yaml", "core/kaiguan.yaml",
    "core/blacklist.csv", "core/cd", "key", "plugins", "backup",
    "config.yaml", "quanxian.yaml", "kaiguan.yaml", "blacklist.csv",
    "update-result.json",
    "style.json", "appearance.json", "webdav_backup.json",
    "gift_compatibility.json", "language.json",
    "core/style.json", "core/appearance.json", "core/webdav_backup.json",
    "core/gift_compatibility.json", "core/language.json",
)
STYLE_CLIENTS = ("win", "web")


class DataConflictError(RuntimeError):
    pass


def preferred_root(app_dir: Path, *, platform: str | None = None, environ: dict | None = None,
                   home: Path | None = None) -> Path:
    env = dict(os.environ if environ is None else environ)
    app = Path(app_dir).resolve()
    override = str(env.get("BILIPDJ_DATA_DIR", "") or "").strip()
    if override:
        candidate = Path(override).expanduser()
        return (candidate if candidate.is_absolute() else app / candidate).resolve()
    plat = platform if platform is not None else sys.platform
    user_home = Path(home) if home is not None else Path.home()
    if plat == "win32":
        base = Path(env["APPDATA"]) if env.get("APPDATA") else user_home / "AppData" / "Roaming"
    elif plat == "darwin":
        base = user_home / "Library" / "Application Support"
    else:
        custom = str(env.get("XDG_DATA_HOME", "") or "").strip()
        base = Path(custom).expanduser() if custom and Path(custom).expanduser().is_absolute() else user_home / ".local" / "share"
    return (base / "bilipdj").resolve()




def preferred_state_root(app_dir: Path, *, platform: str | None = None,
                         environ: dict | None = None, home: Path | None = None) -> Path:
    """Large mutable data: LocalAppData on Windows, XDG data/macOS Support elsewhere."""
    env = dict(os.environ if environ is None else environ)
    root = preferred_root(app_dir, platform=platform, environ=env, home=home)
    if str(env.get("BILIPDJ_DATA_DIR", "") or "").strip():
        return root
    plat = platform if platform is not None else sys.platform
    if plat == "win32":
        user_home = Path(home) if home is not None else Path.home()
        local = Path(env["LOCALAPPDATA"]) if env.get("LOCALAPPDATA") else user_home / "AppData" / "Local"
        return (local / "bilipdj").resolve()
    return root


def preferred_archive_dir(app_dir: Path, *, platform: str | None = None,
                          environ: dict | None = None, home: Path | None = None) -> Path:
    env = dict(os.environ if environ is None else environ)
    root = preferred_state_root(app_dir, platform=platform, environ=env, home=home)
    if str(env.get("BILIPDJ_DATA_DIR", "") or "").strip():
        return root / "core" / "cd"
    return root / "archives"


def preferred_backup_dir(app_dir: Path, *, platform: str | None = None,
                         environ: dict | None = None, home: Path | None = None) -> Path:
    env = dict(os.environ if environ is None else environ)
    root = preferred_state_root(app_dir, platform=platform, environ=env, home=home)
    if str(env.get("BILIPDJ_DATA_DIR", "") or "").strip():
        return root / "backup"
    return root / "backups"


def preferred_log_root(data_root: Path, *, platform: str | None = None, environ: dict | None = None,
                       home: Path | None = None) -> Path:
    """Large logs do not belong to Windows roaming profile synchronization."""
    env = dict(os.environ if environ is None else environ)
    if str(env.get("BILIPDJ_DATA_DIR", "") or "").strip():
        return Path(data_root) / "log"
    plat = platform if platform is not None else sys.platform
    user_home = Path(home) if home is not None else Path.home()
    if plat == "win32":
        base = Path(env["LOCALAPPDATA"]) if env.get("LOCALAPPDATA") else user_home / "AppData" / "Local"
        return (base / "bilipdj" / "log").resolve()
    if plat == "darwin":
        return (user_home / "Library" / "Logs" / "bilipdj").resolve()
    custom = str(env.get("XDG_STATE_HOME", "") or "").strip()
    base = Path(custom).expanduser() if custom and Path(custom).expanduser().is_absolute() else user_home / ".local" / "state"
    return (base / "bilipdj" / "log").resolve()

def _digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _files(root: Path, relative: Path):
    candidate = root / relative
    if candidate.is_symlink():
        return []
    if candidate.is_file():
        return [(relative, candidate)]
    if candidate.is_dir():
        return [(p.relative_to(root), p) for p in candidate.rglob("*") if p.is_file() and not p.is_symlink()]
    return []


def ask_preference(conflicts: list[tuple[Path, Path]]) -> str:
    """Prompt once for a migration conflict; cancel leaves all files intact."""
    explicit = str(os.getenv("BILIPDJ_MIGRATION_CHOICE", "") or "").strip().lower()
    if explicit:
        if explicit in ("user", "project"):
            return explicit
        raise DataConflictError("BILIPDJ_MIGRATION_CHOICE 只能为 user 或 project")
    title = "BiliPDJ 发现两处不同的数据"
    message = ("用户数据目录和项目目录均有数据，存在 %d 个冲突文件。\n"
               "是：使用用户目录；否：导入项目数据（项目原件不会删除）；取消：退出。\n"
               "用户目录：%s\n项目文件：%s" % (len(conflicts), conflicts[0][1], conflicts[0][0]))
    if sys.stdin and sys.stdin.isatty():
        while True:
            choice = input(f"{title}\n{message}\n选 1=用户目录，2=项目目录，q=取消: ").strip().lower()
            if choice in ("1", "2"):
                return "user" if choice == "1" else "project"
            if choice in ("q", ""):
                raise DataConflictError("已取消数据迁移，原数据未修改")
    if sys.platform == "win32" or (sys.platform == "darwin" or os.environ.get("DISPLAY")):
        try:
            import tkinter as tk
            from tkinter import messagebox
            dialog = tk.Tk()
            dialog.withdraw()
            try:
                result = messagebox.askyesnocancel(title, message, parent=dialog)
            finally:
                dialog.destroy()
            if result is not None:
                return "user" if result else "project"
        except Exception:
            # Headless Linux/macOS may have tkinter installed without a display.
            pass
    raise DataConflictError("存在两份不同的 BiliPDJ 数据；无交互界面时停止迁移，不会覆盖。请在终端设置 BILIPDJ_MIGRATION_CHOICE=user 或 project 后重试。")


def _merge_candidates(candidates, dst: Path, *, chooser=None, state_root: Path | None = None):
    # A source fingerprint is a durable *migration history*, not a live mirror:
    # editing the new config must not turn a previously migrated old copy into
    # a fresh conflict every time the application restarts.
    manifest_file = dst / ".migration-sources.json"
    try:
        saved_sources = json.loads(manifest_file.read_text(encoding="utf-8"))
        if not isinstance(saved_sources, dict):
            saved_sources = {}
    except (OSError, ValueError):
        saved_sources = {}
    current_sources = dict(saved_sources)
    manifest_keys = {
        (src, dest): str(src) + " -> " + str(dest.relative_to(dst)) if dest.is_relative_to(dst) else str(dest)
        for src, dest in candidates
    }
    source_hashes = {(src, dest): _digest(src) for src, dest in candidates}
    conflict = [
        (src, dest) for src, dest in candidates
        if dest.is_file() and _digest(dest) != source_hashes[(src, dest)]
        and saved_sources.get(manifest_keys[(src, dest)]) != source_hashes[(src, dest)]
    ]
    choice = chooser(conflict) if conflict and chooser else (ask_preference(conflict) if conflict else "user")
    if choice not in ("user", "project"):
        raise DataConflictError("用户未选择有效的迁移来源")
    # No overwrites are allowed before the conflict choice is settled.
    copied = 0
    for src, dest in candidates:
        if dest.is_symlink() or src.is_symlink():
            continue
        changed_source = saved_sources.get(manifest_keys[(src, dest)]) != source_hashes[(src, dest)]
        if dest.exists():
            if choice != "project" or not changed_source or _digest(src) == _digest(dest):
                current_sources[manifest_keys[(src, dest)]] = source_hashes[(src, dest)]
                continue
            # Preserve every replaced target; never overwrite a previous
            # migration backup with a later (different) configuration.
            backup_base = dst if dest.is_relative_to(dst) else Path(state_root or dst)
            backup_dir = backup_base / "migration-backup" / dest.relative_to(backup_base).parent
            backup_dir.mkdir(parents=True, exist_ok=True)
            suffix = 0
            while True:
                backup = backup_dir / (dest.name + f".before-import-{suffix:04d}")
                if not backup.exists():
                    break
                suffix += 1
            shutil.copy2(dest, backup)
        dest.parent.mkdir(parents=True, exist_ok=True)
        temp = dest.with_name("." + dest.name + ".migrating")
        try:
            shutil.copy2(src, temp)
            os.replace(temp, dest)
        finally:
            temp.unlink(missing_ok=True)
        copied += 1
        current_sources[manifest_keys[(src, dest)]] = source_hashes[(src, dest)]
    if candidates:
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        temp = manifest_file.with_suffix(".tmp")
        try:
            temp.write_text(json.dumps(current_sources, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temp, manifest_file)
        finally:
            temp.unlink(missing_ok=True)
    return {"copied": copied, "conflicts": len(conflict), "choice": choice}


def migrate(app_dir: Path, destination: Path, *, chooser=None, defaults_dir: Path | None = None,
            portable_layout: bool | None = None, state_root: Path | None = None):
    """Copy without deleting original data; stage all conflicts before writing."""
    app = Path(app_dir).resolve()
    dst = Path(destination).resolve()
    if app == dst:
        return {"copied": 0, "conflicts": 0, "choice": "same"}
    if app in dst.parents or dst in app.parents:
        raise ValueError("数据目录必须与程序目录分离")
    candidates = []
    # Source runs historically used core/* settings; PyInstaller portable
    # builds historically used the executable's own directory. When both
    # exist, choose the authoritative legacy location for that build type.
    frozen = bool(getattr(sys, "frozen", False)) if portable_layout is None else portable_layout
    sources = list(DATA_FILES)
    if frozen:
        portable_names = {"config.yaml", "quanxian.yaml", "kaiguan.yaml",
                          "blacklist.csv", "style.json", "appearance.json",
                          "webdav_backup.json", "gift_compatibility.json",
                          "language.json"}
        sources.sort(key=lambda name: (0 if name in portable_names else 1))
    for rel_text in sources:
        rel = Path(rel_text)
        for src_rel, src in _files(app, rel):
            dest_rel = src_rel
            if rel_text in ("config.yaml", "quanxian.yaml", "kaiguan.yaml", "blacklist.csv"):
                dest_rel = Path("core") / src_rel
            elif rel_text == "update-result.json":
                dest_rel = Path("key") / src_rel
            elif rel.parts[0] == "core" and len(rel.parts) == 2 and rel.parts[1].endswith(".json"):
                dest_rel = Path(rel.parts[1])
            target = dst / dest_rel
            if state_root is not None and rel_text == "core/cd":
                target = Path(state_root) / "archives" / src_rel.relative_to(Path("core/cd"))
            elif state_root is not None and rel_text == "backup":
                target = Path(state_root) / "backups" / src_rel.relative_to(Path("backup"))
            if not any(previous_target == target for _, previous_target in candidates):
                candidates.append((src, target))
    if defaults_dir:
        defaults = Path(defaults_dir).resolve()
        if defaults != app:
            for name in ("style.json", "appearance.json", "webdav_backup.json"):
                src = defaults / name
                target = dst / name
                if src.is_file() and not src.is_symlink() and not any(t == target for _, t in candidates):
                    candidates.append((src, target))
    return _merge_candidates(candidates, dst, chooser=chooser, state_root=state_root)


def migrate_legacy_local_state(data_root: Path, state_root: Path, *, chooser=None) -> dict:
    """Copy old Roaming/core/cd and Roaming/backup files into local storage.

    The live Local directory takes precedence in any two-copy conflict until
    the user explicitly selects the former roaming copy. No files are deleted.
    """
    root = Path(data_root).resolve()
    local = Path(state_root).resolve()
    candidates = []
    for old, current in (("core/cd", "archives"), ("archives", "archives"),
                         ("backup", "backups"), ("backups", "backups")):
        prefix = Path(old)
        for rel, source in _files(root, prefix):
            dest = local / current / rel.relative_to(prefix)
            if source == dest:
                continue
            if not any(other_dest == dest for _, other_dest in candidates):
                candidates.append((source, dest))
    return _merge_candidates(candidates, root, chooser=chooser, state_root=local)


def promote_legacy_archive_settings(data_root: Path) -> None:
    """Recover settings from the prior Roaming mirror's core/ subfolder.

    Issue #268 mirrored source-mode core/style.json into Roaming/core.
    New runtime paths use Roaming/style.json. Prefer existing user data over
    freshly bundled project defaults, without deleting either copy.
    """
    root = Path(data_root)
    for name in ("style.json", "appearance.json", "webdav_backup.json",
                 "gift_compatibility.json", "language.json"):
        source = root / "core" / name
        destination = root / name
        if destination.exists() or destination.is_symlink():
            continue
        if not source.is_file() or source.is_symlink():
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def seed_client_styles(data_root: Path) -> None:
    """Imported legacy style becomes a one-time baseline for each client."""
    root = Path(data_root)
    for base in ("appearance", "style"):
        legacy = root / (base + ".json")
        if not legacy.is_file():
            continue
        for client in STYLE_CLIENTS:
            target = root / f"{base}-{client}.json"
            if not target.exists():
                shutil.copy2(legacy, target)
