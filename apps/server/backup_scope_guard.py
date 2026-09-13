from __future__ import annotations

import io
import json
import os
import threading
import zipfile
from pathlib import Path
from typing import Any

_PATCH_LOCK = threading.RLock()
_SELECTION_KEYS = ("backup_config", "backup_archive", "backup_style")
_STYLE_FILES = frozenset({"style.json", "appearance.json"})


def _read_raw_config(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _selection(cfg: dict[str, Any]) -> dict[str, bool]:
    return {key: bool(cfg.get(key, True)) for key in _SELECTION_KEYS}


def _ensure_selection(cfg: dict[str, Any], backup_module: Any) -> dict[str, bool]:
    selected = _selection(cfg)
    if not any(selected.values()):
        raise backup_module.SettingsBackupError("请至少选择一项备份内容，当前无法备份")
    return selected


def install_backup_scope_guard(backup_module: Any, server_module: Any) -> bool:
    """Add configurable config/archive/style backup scopes."""
    service_class = getattr(backup_module, "SettingsBackupService", None)
    if not isinstance(service_class, type):
        return False

    with _PATCH_LOCK:
        if bool(getattr(service_class, "_bilipdj_backup_scope_guard", False)):
            return True

        defaults = getattr(backup_module, "DEFAULT_WEBDAV_CONFIG", None)
        if isinstance(defaults, dict):
            for key in _SELECTION_KEYS:
                defaults.setdefault(key, True)

        max_slots = max(1, int(getattr(server_module, "MAX_QUEUE_ARCHIVE_SLOTS", 10) or 10))
        archive_names = (
            "queue_archive_state.json",
            "blacklist.csv",
            *(f"queue_archive_slot_{slot}.csv" for slot in range(1, max_slots + 1)),
        )
        archive_name_set = frozenset(archive_names)
        current_files = tuple(getattr(backup_module, "SETTINGS_FILES", ()))
        backup_module.SETTINGS_FILES = tuple(dict.fromkeys((*current_files, *archive_names)))
        backup_module.BACKUP_ARCHIVE_FILES = archive_names

        original_paths = service_class.settings_paths
        original_load_config = service_class.load_config
        original_save_config = service_class.save_config
        original_backup_now = service_class.backup_now
        original_restore = service_class.restore_settings_zip

        def settings_paths_with_archives(self: Any) -> dict[str, Path]:
            paths = dict(original_paths(self))
            pd_dir = Path(getattr(self.server, "PD_DIR"))
            paths["queue_archive_state.json"] = Path(
                getattr(self.server, "QUEUE_STATE_PATH", pd_dir / "queue_archive_state.json")
            )
            paths["blacklist.csv"] = Path(
                getattr(self.server, "BLACKLIST_PATH", pd_dir / "blacklist.csv")
            )
            local_max = max(1, int(getattr(self.server, "MAX_QUEUE_ARCHIVE_SLOTS", max_slots) or max_slots))
            for slot in range(1, local_max + 1):
                paths[f"queue_archive_slot_{slot}.csv"] = pd_dir / f"queue_archive_slot_{slot}.csv"
            return paths

        def load_config(self: Any, *, include_password: bool = False) -> dict[str, Any]:
            cfg = dict(original_load_config(self, include_password=include_password))
            for key in _SELECTION_KEYS:
                cfg[key] = bool(cfg.get(key, True))
            return cfg

        def save_config(self: Any, payload: dict[str, Any]) -> dict[str, Any]:
            incoming = payload if isinstance(payload, dict) else {}
            original_save_config(self, incoming)
            updates = {key: bool(incoming.get(key)) for key in _SELECTION_KEYS if key in incoming}
            if updates:
                with self._lock:
                    raw = _read_raw_config(self.config_path)
                    raw.update(updates)
                    backup_module._atomic_write_bytes(
                        self.config_path,
                        json.dumps(raw, ensure_ascii=False, indent=2).encode("utf-8"),
                    )
                    try:
                        self.config_path.chmod(0o600)
                    except OSError:
                        pass
            return self.load_config(include_password=False)

        def _selected_names(self: Any) -> tuple[str, ...]:
            selected = _ensure_selection(self.load_config(include_password=True), backup_module)
            names: list[str] = []
            for name in tuple(getattr(backup_module, "SETTINGS_FILES", ())):
                if name in _STYLE_FILES:
                    enabled = selected["backup_style"]
                elif name in archive_name_set:
                    enabled = selected["backup_archive"]
                else:
                    enabled = selected["backup_config"]
                if enabled:
                    names.append(name)
            return tuple(names)

        def build_settings_zip(self: Any) -> tuple[bytes, list[str]]:
            from . import settings_mtime_guard

            payload = io.BytesIO()
            included: list[str] = []
            paths = self.settings_paths()
            with zipfile.ZipFile(payload, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for name in _selected_names(self):
                    path = paths.get(name)
                    if path is None or not path.is_file():
                        continue
                    stat = path.stat()
                    info = zipfile.ZipInfo(
                        name,
                        date_time=settings_mtime_guard._zip_datetime_for_mtime(stat.st_mtime),
                    )
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.extra = settings_mtime_guard._extended_timestamp_extra(stat.st_mtime)
                    archive.writestr(info, path.read_bytes())
                    included.append(name)
            if not included:
                raise backup_module.SettingsBackupError("所选备份内容中没有找到可备份文件")
            return payload.getvalue(), included

        def backup_now(self: Any) -> dict[str, Any]:
            _ensure_selection(self.load_config(include_password=True), backup_module)
            return original_backup_now(self)

        def restore_settings_zip(self: Any, data: bytes, *, httpd: Any | None = None) -> list[str]:
            from . import settings_mtime_guard

            # Keep the existing validator/restore chain authoritative for malformed
            # or incompatible ZIPs, then apply mtimes to newly supported archive files.
            restored = list(original_restore(self, data, httpd=httpd))
            archive_mtimes = settings_mtime_guard._zip_entry_mtimes(data, tuple(archive_names))
            paths = self.settings_paths()
            for name in restored:
                mtime = archive_mtimes.get(name)
                path = paths.get(name)
                if path is None or mtime is None or mtime <= 0:
                    continue
                try:
                    os.utime(path, (mtime, mtime))
                except OSError:
                    pass
            return restored

        service_class.settings_paths = settings_paths_with_archives
        service_class.load_config = load_config
        service_class.save_config = save_config
        service_class.build_settings_zip = build_settings_zip
        service_class.backup_now = backup_now
        service_class.restore_settings_zip = restore_settings_zip
        service_class._bilipdj_backup_scope_guard = True
        return True


__all__ = ["install_backup_scope_guard"]
