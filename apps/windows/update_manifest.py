from __future__ import annotations

import hashlib
from pathlib import Path, PurePosixPath

# Runtime/user-owned data must never be part of an incremental update plan.
# Matching is case-insensitive because the supported desktop target is Windows.
PRESERVED_FILES = frozenset(
    {
        # Legacy root locations are preserved for migration compatibility.
        "config.yaml",
        "quanxian.yaml",
        "kaiguan.yaml",
        "appearance.json",
        "style.json",
        "update-result.json",
        # Current runtime locations.
        "core/config.yaml",
        "core/quanxian.yaml",
        "core/kaiguan.yaml",
        "core/appearance.json",
        "core/style.json",
    }
)
PRESERVED_PREFIXES = (
    "core/cd/",
    "log/",
    "logs/",
    "backup/",
    "plugins/",
    "key/",
    # Update downloads, extracted staging trees and detached updater copies live
    # beside the application under app_dir/update. They are runtime state, not
    # package-managed files, and must never be replaced by an incremental plan.
    "update/",
)


class ManifestPathError(ValueError):
    pass


def normalize_manifest_path(value: str | Path) -> str:
    text = str(value).replace("\\", "/").strip()
    if not text or "\x00" in text:
        raise ManifestPathError("empty or invalid manifest path")
    path = PurePosixPath(text)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ManifestPathError(f"unsafe manifest path: {value!r}")
    if path.parts and ":" in path.parts[0]:
        raise ManifestPathError(f"drive-qualified manifest path: {value!r}")
    return path.as_posix()


def is_preserved_path(value: str | Path) -> bool:
    try:
        normalized = normalize_manifest_path(value).casefold()
    except ManifestPathError:
        return True
    if normalized in PRESERVED_FILES:
        return True
    return any(normalized.startswith(prefix) for prefix in PRESERVED_PREFIXES)


def resolve_managed_path(root: Path, value: str | Path) -> Path:
    normalized = normalize_manifest_path(value)
    if is_preserved_path(normalized):
        raise ManifestPathError(f"user-owned path is not update-managed: {normalized}")
    root_resolved = Path(root).resolve()
    target = (root_resolved / Path(*PurePosixPath(normalized).parts)).resolve()
    try:
        target.relative_to(root_resolved)
    except ValueError as exc:
        raise ManifestPathError(f"path escapes application root: {normalized}") from exc
    return target


def calculate_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
