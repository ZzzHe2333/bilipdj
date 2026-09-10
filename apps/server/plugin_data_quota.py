"""Shared private-data limits for external plugin host APIs."""
from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Iterator

MAX_PLUGIN_DATA_FILE_BYTES = 4 * 1024 * 1024
MAX_PLUGIN_DATA_TOTAL_BYTES = 64 * 1024 * 1024
MAX_PLUGIN_DATA_FILES = 1024
_QUOTA_LOCK = threading.RLock()


class PluginDataQuotaError(ValueError):
    """Raised when a plugin private-data operation exceeds a safety limit."""


def _is_within(root: Path, target: Path) -> bool:
    return target == root or root in target.parents


def _regular_files(root: Path) -> Iterator[Path]:
    if not root.exists():
        return
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        current = Path(dirpath)
        dirnames[:] = [name for name in dirnames if not (current / name).is_symlink()]
        for name in filenames:
            path = current / name
            try:
                if path.is_symlink() or not path.is_file():
                    continue
            except OSError:
                continue
            yield path


def plugin_data_usage(root: Path) -> tuple[int, int]:
    base = Path(root).resolve()
    total = 0
    files = 0
    for path in _regular_files(base):
        try:
            size = path.stat().st_size
        except OSError:
            continue
        total += max(0, int(size))
        files += 1
    return total, files


def validate_plugin_data_write(
    root: Path,
    target: Path,
    size: int,
    *,
    max_file_bytes: int | None = None,
    max_total_bytes: int | None = None,
    max_files: int | None = None,
) -> tuple[int, int]:
    file_limit = MAX_PLUGIN_DATA_FILE_BYTES if max_file_bytes is None else int(max_file_bytes)
    total_limit = MAX_PLUGIN_DATA_TOTAL_BYTES if max_total_bytes is None else int(max_total_bytes)
    file_count_limit = MAX_PLUGIN_DATA_FILES if max_files is None else int(max_files)
    new_size = int(size)
    if new_size < 0:
        raise PluginDataQuotaError("plugin data size cannot be negative")
    if new_size > file_limit:
        raise PluginDataQuotaError(f"plugin data file exceeds {file_limit} bytes")

    base = Path(root).resolve()
    resolved = Path(target).resolve()
    if not _is_within(base, resolved) or resolved == base:
        raise PermissionError("path escapes plugin data directory")

    total, files = plugin_data_usage(base)
    old_size = 0
    existed = False
    try:
        if resolved.exists():
            if not resolved.is_file():
                raise PluginDataQuotaError("plugin data target is not a regular file")
            old_size = max(0, int(resolved.stat().st_size))
            existed = True
    except OSError as exc:
        raise PluginDataQuotaError(f"cannot inspect plugin data target: {exc}") from exc

    projected_total = total - old_size + new_size
    projected_files = files + (0 if existed else 1)
    # If an externally modified directory is already over quota, allow an
    # existing file to shrink so the plugin can move back toward compliance.
    if projected_total > total_limit and new_size > old_size:
        raise PluginDataQuotaError(
            f"plugin private data exceeds total quota: {projected_total} > {total_limit} bytes"
        )
    if projected_files > file_count_limit and not existed:
        raise PluginDataQuotaError(
            f"plugin private data exceeds file-count quota: {projected_files} > {file_count_limit}"
        )
    return projected_total, projected_files


def write_plugin_data(
    root: Path,
    target: Path,
    data: bytes,
    *,
    max_file_bytes: int | None = None,
    max_total_bytes: int | None = None,
    max_files: int | None = None,
) -> None:
    payload = bytes(data)
    with _QUOTA_LOCK:
        validate_plugin_data_write(
            root,
            target,
            len(payload),
            max_file_bytes=max_file_bytes,
            max_total_bytes=max_total_bytes,
            max_files=max_files,
        )
        resolved = Path(target).resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)
        resolved.write_bytes(payload)


def read_plugin_data(
    root: Path,
    target: Path,
    *,
    max_file_bytes: int | None = None,
) -> bytes:
    file_limit = MAX_PLUGIN_DATA_FILE_BYTES if max_file_bytes is None else int(max_file_bytes)
    base = Path(root).resolve()
    resolved = Path(target).resolve()
    if not _is_within(base, resolved) or resolved == base:
        raise PermissionError("path escapes plugin data directory")
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    size = max(0, int(resolved.stat().st_size))
    if size > file_limit:
        raise PluginDataQuotaError(f"plugin data file exceeds {file_limit} bytes")
    data = resolved.read_bytes()
    if len(data) > file_limit:
        raise PluginDataQuotaError(f"plugin data file exceeds {file_limit} bytes")
    return data


__all__ = [
    "MAX_PLUGIN_DATA_FILE_BYTES",
    "MAX_PLUGIN_DATA_TOTAL_BYTES",
    "MAX_PLUGIN_DATA_FILES",
    "PluginDataQuotaError",
    "plugin_data_usage",
    "validate_plugin_data_write",
    "write_plugin_data",
    "read_plugin_data",
]
