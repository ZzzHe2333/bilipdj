from __future__ import annotations

import re
import threading
from datetime import datetime
from typing import Any

_OPERATION_LOCK = threading.RLock()
_PATCH_LOCK = threading.RLock()
_EXTENDED_BACKUP_RE = re.compile(r"^BiliPDJ-settings-(\d{8}-\d{6})(?:-(\d{6}))?\.zip$")


def install_settings_backup_bugfix_guard(backup_module: Any) -> bool:
    """Harden settings backup concurrency and backup-name uniqueness."""
    service_class = getattr(backup_module, "SettingsBackupService", None)
    if not isinstance(service_class, type):
        return False

    with _PATCH_LOCK:
        if bool(getattr(service_class, "_bilipdj_backup_bugfix_guard", False)):
            return True

        backup_module.BACKUP_NAME_RE = _EXTENDED_BACKUP_RE

        original_init = service_class.__init__
        original_backup_now = service_class.backup_now
        original_prune_backups = service_class.prune_backups
        original_restore_remote = service_class.restore_remote

        def shared_init(self: Any, *args: Any, **kwargs: Any) -> None:
            original_init(self, *args, **kwargs)
            self._lock = _OPERATION_LOCK

        @staticmethod
        def unique_backup_name() -> str:
            return f"BiliPDJ-settings-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.zip"

        def locked_backup_now(self: Any, *args: Any, **kwargs: Any) -> Any:
            with _OPERATION_LOCK:
                return original_backup_now(self, *args, **kwargs)

        def locked_prune_backups(self: Any, *args: Any, **kwargs: Any) -> Any:
            with _OPERATION_LOCK:
                return original_prune_backups(self, *args, **kwargs)

        def locked_restore_remote(self: Any, *args: Any, **kwargs: Any) -> Any:
            with _OPERATION_LOCK:
                return original_restore_remote(self, *args, **kwargs)

        service_class.__init__ = shared_init
        service_class._backup_name = unique_backup_name
        service_class.backup_now = locked_backup_now
        service_class.prune_backups = locked_prune_backups
        service_class.restore_remote = locked_restore_remote
        service_class._bilipdj_backup_bugfix_guard = True
        return True


__all__ = ["install_settings_backup_bugfix_guard"]
