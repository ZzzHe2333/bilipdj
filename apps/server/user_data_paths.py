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
    "core/blacklist.csv", "core/cd", "key", "plugins",
    "config.yaml", "quanxian.yaml", "kaiguan.yaml", "blacklist.csv",
    "update-result.json",
    "style.json", "appearance.json", "webdav_backup.json",
    "gift_compatibility.json", "language.json",
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
    title = "BiliPDJ 发现两处不同的数据"
    message = ("用户数据目录和项目目录均有数据，存在 %d 个冲突文件。\\n"
               "是：使用用户目录；否：导入项目数据（项目原件不会删除）；取消：退出。\\n"
               "用户目录：%s\\n项目文件：%s" % (len(conflicts), conflicts[0][1], conflicts[0][0]))
    if sys.stdin and sys.stdin.isatty():
        while True:
            choice = input(f"{title}\\n{message}\\n选 1=用户目录，2=项目目录，q=取消: ").strip().lower()
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
        except (ImportError, RuntimeError, OSError):
            pass
    raise DataConflictError("存在两份不同的 BiliPDJ 数据；无交互界面时停止迁移，避免错误覆盖。请在桌面环境选择。")


def migrate(app_dir: Path, destination: Path, *, chooser=None, defaults_dir: Path | None = None):
    """Copy without deleting original data; stage all conflicts before writing."""
    app = Path(app_dir).resolve()
    dst = Path(destination).resolve()
    if app == dst:
        return {"copied": 0, "conflicts": 0, "choice": "same"}
    if app in dst.parents or dst in app.parents:
        raise ValueError("数据目录必须与程序目录分离")
    candidates = []
    # Source development data are under core/; frozen portable settings
    # also live at the application root.
    for rel_text in DATA_FILES:
        rel = Path(rel_text)
        for src_rel, src in _files(app, rel):
            dest_rel = src_rel
            if rel_text in ("config.yaml", "quanxian.yaml", "kaiguan.yaml", "blacklist.csv"):
                dest_rel = Path("core") / src_rel
            elif rel_text == "update-result.json":
                dest_rel = Path("key") / src_rel
            target = dst / dest_rel
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
    conflict = [(a, b) for a, b in candidates if b.exists() and a.is_file() and b.is_file() and _digest(a) != _digest(b)]
    # Remember an explicit decision for unchanged source/target fingerprints;
    # do not repeatedly interrupt startup while old portable files are retained.
    decision_file = dst / ".migration-decision.json"
    signature = {
        str(b.relative_to(dst)): [_digest(a), _digest(b)] for a, b in conflict
    }
    try:
        previous = json.loads(decision_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        previous = {}
    old_choice = previous.get("choice") if previous.get("signature") == signature else None
    choice = old_choice if old_choice in ("user", "project") else (
        chooser(conflict) if conflict and chooser else (ask_preference(conflict) if conflict else "user")
    )
    if choice not in ("user", "project"):
        raise DataConflictError("用户未选择有效的迁移来源")
    copied = 0
    for src, dest in candidates:
        if dest.is_symlink() or src.is_symlink():
            continue
        if dest.exists():
            if choice != "project" or _digest(src) == _digest(dest):
                continue
            # Never destroy the user-dir copy when choosing the project version.
            backup = dst / "migration-backup" / dest.relative_to(dst)
            backup.parent.mkdir(parents=True, exist_ok=True)
            if backup.exists() and _digest(backup) != _digest(dest):
                raise DataConflictError(f"冲突备份已存在且内容不同：{backup}")
            if not backup.exists():
                shutil.copy2(dest, backup)
        dest.parent.mkdir(parents=True, exist_ok=True)
        temp = dest.with_name("." + dest.name + ".migrating")
        shutil.copy2(src, temp)
        os.replace(temp, dest)
        copied += 1
    if conflict:
        # Save what was chosen, along with the CURRENT fingerprints so that
        # the decision remains valid on subsequent launches.
        current = {str(b.relative_to(dst)): [_digest(a), _digest(b)] for a, b in conflict}
        decision_file.parent.mkdir(parents=True, exist_ok=True)
        temp = decision_file.with_suffix(".tmp")
        temp.write_text(json.dumps({"choice": choice, "signature": current}, indent=2), encoding="utf-8")
        temp.replace(decision_file)
    return {"copied": copied, "conflicts": len(conflict), "choice": choice}


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
