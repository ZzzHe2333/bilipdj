from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

ARCHIVE_DIR_NAME = "bilipdj"
SYNC_MARKER_RELATIVE = Path("core") / "cd" / ".bilipdj-appdata-sync-v1"

# Keep this list explicit.  ``core`` also contains Python/documentation files in
# source checkouts, so mirroring the whole directory would turn program files
# into user data and could restore stale code after an update.
ARCHIVE_ITEMS = (
    Path("core/config.yaml"),
    Path("core/quanxian.yaml"),
    Path("core/kaiguan.yaml"),
    Path("core/blacklist.csv"),
    Path("core/cd"),
    # Source-mode settings live in core; frozen builds keep the same settings at
    # the portable root.  Both forms are mirrored for migration compatibility.
    Path("core/style.json"),
    Path("core/appearance.json"),
    Path("core/webdav_backup.json"),
    Path("core/gift_compatibility.json"),
    Path("core/language.json"),
    Path("style.json"),
    Path("appearance.json"),
    Path("webdav_backup.json"),
    Path("gift_compatibility.json"),
    Path("language.json"),
    # Installed plugin packages, state and per-plugin private data are all below
    # this directory.
    Path("plugins"),
)


def default_archive_root() -> Path | None:
    """Return the per-user roaming archive root on Windows."""

    if os.name != "nt":
        return None
    raw = str(os.getenv("APPDATA", "") or "").strip()
    if raw:
        roaming = Path(raw).expanduser()
    else:
        roaming = Path.home() / "AppData" / "Roaming"
    return roaming / ARCHIVE_DIR_NAME


def _meaningful_exists(path: Path) -> bool:
    if path.is_symlink():
        return False
    if path.is_file():
        return True
    if not path.is_dir():
        return False
    try:
        for child in path.iterdir():
            if child.name == SYNC_MARKER_RELATIVE.name:
                continue
            return True
    except OSError:
        return False
    return False


def _remove_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif path.exists():
        shutil.rmtree(path)


def _ignore_symlinks(directory: str, names: list[str]) -> set[str]:
    ignored: set[str] = set()
    base = Path(directory)
    for name in names:
        try:
            if (base / name).is_symlink():
                ignored.add(name)
        except OSError:
            ignored.add(name)
    return ignored


def _atomic_copy_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent))
    os.close(fd)
    temp = Path(temp_name)
    try:
        shutil.copy2(source, temp, follow_symlinks=False)
        os.replace(temp, destination)
    finally:
        temp.unlink(missing_ok=True)


def _atomic_copy_dir(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)))
    # copytree requires a non-existing destination.  The mkdtemp directory is
    # only a unique reservation, so remove it before copying.
    temp.rmdir()
    old = destination.with_name(f".{destination.name}.old-{os.getpid()}")
    _remove_path(old)
    try:
        shutil.copytree(
            source,
            temp,
            copy_function=shutil.copy2,
            symlinks=False,
            ignore=_ignore_symlinks,
        )
        if destination.exists() or destination.is_symlink():
            os.replace(destination, old)
        os.replace(temp, destination)
        _remove_path(old)
    except Exception:
        _remove_path(temp)
        if old.exists() and not destination.exists():
            os.replace(old, destination)
        raise
    finally:
        _remove_path(old)


def _copy_entry(source: Path, destination: Path) -> None:
    if source.is_symlink():
        return
    if source.is_file():
        if destination.is_dir() and not destination.is_symlink():
            shutil.rmtree(destination)
        _atomic_copy_file(source, destination)
        return
    if source.is_dir():
        if destination.is_file() or destination.is_symlink():
            _remove_path(destination)
        _atomic_copy_dir(source, destination)


def _log(logger: Any | None, level: str, message: str, *args: Any) -> None:
    if logger is None:
        return
    callback = getattr(logger, level, None)
    if callable(callback):
        try:
            callback(message, *args)
        except Exception:
            pass


def sync_local_data_archive(
    app_dir: Path,
    *,
    archive_root: Path | None = None,
    enabled: bool | None = None,
    logger: Any | None = None,
) -> dict[str, Any]:
    """Synchronize portable runtime data with ``%APPDATA%\\bilipdj``.

    Existing installations use the portable/core copy as the authority.  A
    marker stored inside the preserved queue-data directory distinguishes an
    existing installation from a fresh/reinstalled one: when an archive exists
    but the local marker is absent, archived user data is restored over bundled
    defaults before the application reads it.

    For an existing installation the rule is applied per item: local+archive or
    local-only -> archive; archive-only -> local.  This also lets a missing local
    config recover from the archive without changing the authority of files that
    still exist locally.
    """

    root = Path(app_dir).resolve()
    if enabled is None:
        enabled = os.name == "nt"
    if not enabled:
        return {"enabled": False, "mode": "disabled", "archive_root": "", "copied": 0}

    archive = Path(archive_root).expanduser().resolve() if archive_root is not None else default_archive_root()
    if archive is None:
        return {"enabled": False, "mode": "disabled", "archive_root": "", "copied": 0}
    if archive == root or root in archive.parents or archive in root.parents:
        raise ValueError("AppData archive must be outside the application directory")

    archive.mkdir(parents=True, exist_ok=True)
    marker = root / SYNC_MARKER_RELATIVE
    archive_has_data = any(_meaningful_exists(archive / relative) for relative in ARCHIVE_ITEMS)
    fresh_restore = bool(archive_has_data and not marker.is_file())
    copied = 0

    for relative in ARCHIVE_ITEMS:
        local = root / relative
        saved = archive / relative
        try:
            local_exists = local.exists() and not local.is_symlink()
            saved_exists = saved.exists() and not saved.is_symlink()

            if fresh_restore:
                # On a new/reinstalled copy an existing roaming archive is older
                # user state and therefore wins over bundled defaults.
                if saved_exists:
                    _copy_entry(saved, local)
                    copied += 1
                elif local_exists:
                    _copy_entry(local, saved)
                    copied += 1
                continue

            if local_exists:
                _copy_entry(local, saved)
                copied += 1
            elif saved_exists:
                _copy_entry(saved, local)
                copied += 1
        except OSError as exc:
            _log(logger, "warning", "Local AppData archive sync skipped %s: %s", relative, exc)

    marker.parent.mkdir(parents=True, exist_ok=True)
    try:
        marker.write_text("1\n", encoding="utf-8")
        # Keep the marker inside the archived queue directory as well so the
        # archive remains a faithful mirror; only the local marker is consulted
        # when deciding whether an installation is fresh.
        archived_marker = archive / SYNC_MARKER_RELATIVE
        _atomic_copy_file(marker, archived_marker)
    except OSError as exc:
        _log(logger, "warning", "Unable to write AppData archive marker: %s", exc)

    mode = "restore" if fresh_restore else "mirror"
    _log(logger, "info", "Local AppData archive sync completed (%s, %s items)", mode, copied)
    return {
        "enabled": True,
        "mode": mode,
        "archive_root": str(archive),
        "copied": copied,
    }


__all__ = [
    "ARCHIVE_DIR_NAME",
    "ARCHIVE_ITEMS",
    "SYNC_MARKER_RELATIVE",
    "default_archive_root",
    "sync_local_data_archive",
]
