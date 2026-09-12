from __future__ import annotations

import os
import shutil
import time
from pathlib import Path

UPDATE_DIR_NAME = "update"


def update_root(app_dir: Path, *, create: bool = True) -> Path:
    """Return the update workspace rooted inside the application directory."""

    app = Path(app_dir).resolve()
    if app == app.parent:
        raise ValueError("refusing to use a filesystem root as the application directory")
    root = (app / UPDATE_DIR_NAME).resolve(strict=False)
    try:
        root.relative_to(app)
    except ValueError as exc:
        raise ValueError("update workspace escapes the application directory") from exc
    if create:
        root.mkdir(parents=True, exist_ok=True)
    return root


def allocate_update_session(app_dir: Path, purpose: str) -> Path:
    """Allocate a unique session below ``<app>/update`` without OS temp."""

    root = update_root(app_dir)
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in str(purpose or "session")).strip("-_")
    safe = safe or "session"
    stamp = time.strftime("%Y%m%d-%H%M%S")
    base = f"{safe}-{stamp}-{os.getpid()}"
    for index in range(1, 10_000):
        name = base if index == 1 else f"{base}-{index}"
        candidate = root / name
        try:
            candidate.mkdir(parents=False, exist_ok=False)
        except FileExistsError:
            continue
        return candidate
    raise RuntimeError("unable to allocate application-local update workspace")


def validate_update_session(app_dir: Path, target: Path) -> Path:
    """Validate a cleanup target as one child session under ``<app>/update``."""

    root = update_root(app_dir, create=False).resolve(strict=False)
    candidate = Path(target).resolve(strict=False)
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("cleanup target is outside the application update directory") from exc
    if candidate == root or not relative.parts:
        raise ValueError("refusing to delete the update root itself")
    return candidate


def cleanup_update_session(app_dir: Path, target: Path) -> bool:
    """Best-effort cleanup of one validated session and the empty update root."""

    try:
        candidate = validate_update_session(app_dir, target)
    except ValueError:
        return False
    try:
        if candidate.is_symlink() or candidate.is_file():
            candidate.unlink(missing_ok=True)
        elif candidate.exists():
            shutil.rmtree(candidate)
    except OSError:
        return False
    try:
        update_root(app_dir, create=False).rmdir()
    except OSError:
        pass
    return not candidate.exists()


__all__ = [
    "UPDATE_DIR_NAME",
    "allocate_update_session",
    "cleanup_update_session",
    "update_root",
    "validate_update_session",
]
