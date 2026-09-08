from __future__ import annotations

import io
import os
import struct
import threading
import time
import zipfile
from datetime import datetime
from typing import Any

_EXTENDED_TIMESTAMP_ID = 0x5455
_EXTENDED_TIMESTAMP_MTIME = 0x01
_PATCH_LOCK = threading.RLock()


def _extended_timestamp_extra(mtime: float) -> bytes:
    """Return the standard ZIP Extended Timestamp field containing mtime."""
    seconds = max(0, min(0xFFFFFFFF, int(mtime)))
    payload = struct.pack("<BI", _EXTENDED_TIMESTAMP_MTIME, seconds)
    return struct.pack("<HH", _EXTENDED_TIMESTAMP_ID, len(payload)) + payload


def _read_extended_timestamp(info: zipfile.ZipInfo) -> float | None:
    extra = bytes(info.extra or b"")
    offset = 0
    while offset + 4 <= len(extra):
        header_id, size = struct.unpack_from("<HH", extra, offset)
        offset += 4
        end = offset + size
        if end > len(extra):
            break
        payload = extra[offset:end]
        offset = end
        if header_id != _EXTENDED_TIMESTAMP_ID or len(payload) < 5:
            continue
        flags = payload[0]
        if not (flags & _EXTENDED_TIMESTAMP_MTIME):
            continue
        return float(struct.unpack_from("<I", payload, 1)[0])
    return None


def _zip_datetime_for_mtime(mtime: float) -> tuple[int, int, int, int, int, int]:
    local = time.localtime(mtime)
    year = max(1980, min(2107, int(local.tm_year)))
    second = max(0, min(59, int(local.tm_sec)))
    return (
        year,
        int(local.tm_mon),
        int(local.tm_mday),
        int(local.tm_hour),
        int(local.tm_min),
        second,
    )


def _entry_mtime(info: zipfile.ZipInfo) -> float:
    extended = _read_extended_timestamp(info)
    if extended is not None:
        return extended
    try:
        return float(datetime(*info.date_time).timestamp())
    except (OverflowError, OSError, ValueError):
        return 0.0


def _zip_entry_mtimes(data: bytes, allowed_names: tuple[str, ...]) -> dict[str, float]:
    mtimes: dict[str, float] = {}
    with zipfile.ZipFile(io.BytesIO(bytes(data)), "r") as archive:
        for info in archive.infolist():
            name = str(info.filename or "")
            if name in allowed_names and name not in mtimes:
                mtimes[name] = _entry_mtime(info)
    return mtimes


def install_settings_mtime_guard(backup_module: Any) -> bool:
    """Preserve source config mtimes inside ZIP entries and restore them later."""
    service_class = getattr(backup_module, "SettingsBackupService", None)
    if not isinstance(service_class, type):
        return False

    with _PATCH_LOCK:
        if bool(getattr(service_class, "_bilipdj_settings_mtime_guard", False)):
            return True

        settings_files = tuple(getattr(backup_module, "SETTINGS_FILES"))

        def build_settings_zip(self: Any) -> tuple[bytes, list[str]]:
            payload = io.BytesIO()
            included: list[str] = []
            with zipfile.ZipFile(payload, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for name in settings_files:
                    path = self.settings_paths()[name]
                    if not path.is_file():
                        continue
                    stat = path.stat()
                    info = zipfile.ZipInfo(name, date_time=_zip_datetime_for_mtime(stat.st_mtime))
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.extra = _extended_timestamp_extra(stat.st_mtime)
                    archive.writestr(info, path.read_bytes())
                    included.append(name)
            if not included:
                raise backup_module.SettingsBackupError("没有找到可备份的设置文件")
            return payload.getvalue(), included

        def restore_settings_zip(self: Any, data: bytes, *, httpd: Any | None = None) -> list[str]:
            restored = self.validate_settings_zip(data)
            backed_up_mtimes = _zip_entry_mtimes(data, settings_files)
            paths = self.settings_paths()
            before: dict[str, tuple[bytes | None, int | None, int | None]] = {}
            written: list[str] = []

            safety_path = self.config_path.parent / ".settings-restore-safety.zip"
            safety_created = False
            with self._lock:
                for name, path in paths.items():
                    if path.is_file():
                        stat = path.stat()
                        before[name] = (path.read_bytes(), stat.st_atime_ns, stat.st_mtime_ns)
                    else:
                        before[name] = (None, None, None)

                if any(snapshot[0] is not None for snapshot in before.values()):
                    safety_data, _ = self.build_settings_zip()
                    backup_module._atomic_write_bytes(safety_path, safety_data)
                    safety_created = True
                try:
                    for name, content in restored.items():
                        path = paths[name]
                        backup_module._atomic_write_bytes(path, content)
                        written.append(name)
                        mtime = backed_up_mtimes.get(name)
                        if mtime is not None and mtime > 0:
                            os.utime(path, (mtime, mtime))
                    if httpd is not None:
                        self._reload_runtime(httpd)
                except Exception:
                    for name in written:
                        old, old_atime_ns, old_mtime_ns = before.get(name, (None, None, None))
                        path = paths[name]
                        try:
                            if old is None:
                                path.unlink(missing_ok=True)
                            else:
                                backup_module._atomic_write_bytes(path, old)
                                if old_atime_ns is not None and old_mtime_ns is not None:
                                    os.utime(path, ns=(old_atime_ns, old_mtime_ns))
                        except OSError:
                            pass
                    if httpd is not None:
                        try:
                            self._reload_runtime(httpd)
                        except Exception:
                            pass
                    if safety_created:
                        try:
                            safety_path.unlink(missing_ok=True)
                        except OSError:
                            pass
                    raise
                else:
                    if safety_created:
                        try:
                            safety_path.unlink(missing_ok=True)
                        except OSError:
                            pass
            return list(restored)

        service_class.build_settings_zip = build_settings_zip
        service_class.restore_settings_zip = restore_settings_zip
        service_class._bilipdj_settings_mtime_guard = True
        return True


__all__ = ["install_settings_mtime_guard"]
