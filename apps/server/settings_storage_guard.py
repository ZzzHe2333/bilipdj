from __future__ import annotations

import atexit
import json
import os
import re
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

_BACKENDS = {"webdav", "local", "smb"}
_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:[\\/]")
_DEVICE_PREFIXES = ("\\\\.\\", "\\\\?\\", "//./", "//?/")
_PATCH_LOCK = threading.RLock()


def _normalize_backend(value: Any) -> str:
    raw = str(value or "webdav").strip().lower()
    if raw in {"nas", "smb/nas", "smb-nas"}:
        raw = "smb"
    if raw not in _BACKENDS:
        raise ValueError(f"不支持的备份方式：{raw}")
    return raw


def _path_text(value: Any) -> str:
    return os.path.expandvars(os.path.expanduser(str(value or "").strip()))


def _filesystem_path(value: Any, *, backend: str) -> Path:
    raw = _path_text(value)
    if not raw:
        label = "本地备份目录" if backend == "local" else "SMB/NAS 路径"
        raise ValueError(f"请先填写{label}")
    if "\x00" in raw or raw.startswith(_DEVICE_PREFIXES):
        raise ValueError("备份路径包含不允许的设备路径")

    windows_style = raw.startswith("\\\\") or raw.startswith("//") or bool(_WINDOWS_DRIVE_RE.match(raw))
    if os.name != "nt" and windows_style:
        raise ValueError("当前系统不能直接访问 Windows UNC/盘符路径；请先挂载 NAS 后填写绝对挂载路径")

    path = Path(raw)
    if not path.is_absolute():
        raise ValueError("备份路径必须是绝对路径")
    return path


def _storage_target(cfg: dict[str, Any]) -> Path:
    backend = _normalize_backend(cfg.get("backend", "webdav"))
    if backend == "local":
        base = _filesystem_path(cfg.get("local_dir", ""), backend="local")
        return base / "BiliPDJ_Backup"
    if backend == "smb":
        return _filesystem_path(cfg.get("smb_path", ""), backend="smb")
    raise ValueError("WebDAV 不使用文件系统备份目录")


def _target_configured(cfg: dict[str, Any]) -> bool:
    try:
        backend = _normalize_backend(cfg.get("backend", "webdav"))
    except ValueError:
        return False
    if backend == "webdav":
        return bool(str(cfg.get("url", "") or "").strip())
    key = "local_dir" if backend == "local" else "smb_path"
    return bool(str(cfg.get(key, "") or "").strip())


def _atomic_filesystem_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temp.open("wb") as handle:
            handle.write(data)
            handle.flush()
            try:
                os.fsync(handle.fileno())
            except OSError:
                pass
        temp.replace(path)
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass


def _filesystem_items(backup_module: Any, target: Path) -> list[dict[str, Any]]:
    try:
        entries = list(target.iterdir()) if target.exists() else []
    except OSError as exc:
        raise backup_module.SettingsBackupError(f"无法读取备份目录：{exc}") from exc

    items: list[dict[str, Any]] = []
    for path in entries:
        if not path.is_file() or not backup_module.BACKUP_NAME_RE.fullmatch(path.name):
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        items.append(
            {
                "name": path.name,
                "size": int(stat.st_size),
                "modified": datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(timespec="seconds"),
                "mtime": float(stat.st_mtime),
            }
        )
    items.sort(key=lambda item: str(item["name"]), reverse=True)
    return items


def _read_json_config(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _unwrap_original_run_server(wrapped: Any) -> Any:
    code = getattr(wrapped, "__code__", None)
    closure = getattr(wrapped, "__closure__", None) or ()
    if code is None:
        return wrapped
    for name, cell in zip(getattr(code, "co_freevars", ()), closure):
        if name != "original_run_server":
            continue
        try:
            candidate = cell.cell_contents
        except ValueError:
            continue
        if callable(candidate):
            return candidate
    return wrapped


def install_settings_storage_guard(backup_module: Any, server_module: Any | None = None) -> bool:
    """Add Local Folder and SMB/NAS filesystem targets to settings backup."""
    service_class = getattr(backup_module, "SettingsBackupService", None)
    if not isinstance(service_class, type):
        return False

    with _PATCH_LOCK:
        if not bool(getattr(service_class, "_bilipdj_storage_guard", False)):
            defaults = getattr(backup_module, "DEFAULT_WEBDAV_CONFIG", None)
            if isinstance(defaults, dict):
                defaults.setdefault("backend", "webdav")
                defaults.setdefault("local_dir", "")
                defaults.setdefault("smb_path", "")

            original_load_config = service_class.load_config
            original_save_config = service_class.save_config
            original_test_connection = service_class.test_connection
            original_list_backups = service_class.list_backups
            original_backup_now = service_class.backup_now
            original_prune_backups = service_class.prune_backups
            original_restore_remote = service_class.restore_remote

            def load_config(self: Any, *, include_password: bool = False) -> dict[str, Any]:
                cfg = dict(original_load_config(self, include_password=include_password))
                try:
                    cfg["backend"] = _normalize_backend(cfg.get("backend", "webdav"))
                except ValueError:
                    cfg["backend"] = "webdav"
                cfg["local_dir"] = str(cfg.get("local_dir", "") or "").strip()
                cfg["smb_path"] = str(cfg.get("smb_path", "") or "").strip()
                return cfg

            def save_config(self: Any, payload: dict[str, Any]) -> dict[str, Any]:
                incoming = payload if isinstance(payload, dict) else {}
                if "backend" in incoming:
                    try:
                        backend = _normalize_backend(incoming.get("backend"))
                    except ValueError as exc:
                        raise backup_module.SettingsBackupError(str(exc)) from exc
                else:
                    backend = None

                result = original_save_config(self, incoming)
                with self._lock:
                    raw = _read_json_config(self.config_path)
                    if backend is not None:
                        raw["backend"] = backend
                    if "local_dir" in incoming:
                        raw["local_dir"] = str(incoming.get("local_dir", "") or "").strip()
                    if "smb_path" in incoming:
                        raw["smb_path"] = str(incoming.get("smb_path", "") or "").strip()
                    backup_module._atomic_write_bytes(
                        self.config_path,
                        json.dumps(raw, ensure_ascii=False, indent=2).encode("utf-8"),
                    )
                    try:
                        self.config_path.chmod(0o600)
                    except OSError:
                        pass
                return self.load_config(include_password=False)

            def test_connection(self: Any) -> dict[str, Any]:
                cfg = self.load_config(include_password=True)
                backend = _normalize_backend(cfg.get("backend", "webdav"))
                if backend == "webdav":
                    return original_test_connection(self)
                try:
                    target = _storage_target(cfg)
                    target.mkdir(parents=True, exist_ok=True)
                    probe = target / f".bilipdj-write-test-{uuid.uuid4().hex}.tmp"
                    _atomic_filesystem_write(probe, b"ok")
                    probe.unlink(missing_ok=True)
                except (OSError, ValueError) as exc:
                    label = "本地备份目录" if backend == "local" else "SMB/NAS 目录"
                    raise backup_module.SettingsBackupError(f"{label}不可用：{exc}") from exc
                label = "本地备份目录" if backend == "local" else "SMB/NAS 目录"
                return {"status": "ok", "message": f"{label}可读写", "backend": backend, "path": str(target)}

            def list_backups(self: Any) -> list[dict[str, Any]]:
                cfg = self.load_config(include_password=True)
                backend = _normalize_backend(cfg.get("backend", "webdav"))
                if backend == "webdav":
                    return original_list_backups(self)
                try:
                    target = _storage_target(cfg)
                    target.mkdir(parents=True, exist_ok=True)
                except (OSError, ValueError) as exc:
                    raise backup_module.SettingsBackupError(f"备份目录不可用：{exc}") from exc
                return _filesystem_items(backup_module, target)

            def backup_now(self: Any) -> dict[str, Any]:
                cfg = self.load_config(include_password=True)
                backend = _normalize_backend(cfg.get("backend", "webdav"))
                if backend == "webdav":
                    return original_backup_now(self)
                try:
                    target = _storage_target(cfg)
                    target.mkdir(parents=True, exist_ok=True)
                    data, included = self.build_settings_zip()
                    name = self._backup_name()
                    destination = target / name
                    _atomic_filesystem_write(destination, data)
                except backup_module.SettingsBackupError:
                    raise
                except (OSError, ValueError) as exc:
                    raise backup_module.SettingsBackupError(f"写入备份目录失败：{exc}") from exc
                self.prune_backups(cfg=cfg)
                return {
                    "status": "ok",
                    "backend": backend,
                    "name": name,
                    "size": len(data),
                    "included": included,
                    "path": str(destination),
                }

            def prune_backups(self: Any, *, cfg: dict[str, Any] | None = None) -> list[str]:
                active_cfg = cfg or self.load_config(include_password=True)
                backend = _normalize_backend(active_cfg.get("backend", "webdav"))
                if backend == "webdav":
                    return original_prune_backups(self, cfg=active_cfg)
                try:
                    target = _storage_target(active_cfg)
                except ValueError as exc:
                    raise backup_module.SettingsBackupError(str(exc)) from exc
                keep_last = max(1, min(100, int(active_cfg.get("keep_last", 10))))
                items = _filesystem_items(backup_module, target)
                removed: list[str] = []
                for item in items[keep_last:]:
                    name = str(item.get("name", "") or "")
                    if not backup_module.BACKUP_NAME_RE.fullmatch(name):
                        continue
                    try:
                        (target / name).unlink(missing_ok=True)
                    except OSError as exc:
                        raise backup_module.SettingsBackupError(f"清理旧备份失败：{exc}") from exc
                    removed.append(name)
                return removed

            def restore_remote(self: Any, name: str, *, httpd: Any | None = None) -> dict[str, Any]:
                if not backup_module.BACKUP_NAME_RE.fullmatch(str(name or "")):
                    raise backup_module.SettingsBackupError("备份文件名无效")
                cfg = self.load_config(include_password=True)
                backend = _normalize_backend(cfg.get("backend", "webdav"))
                if backend == "webdav":
                    return original_restore_remote(self, name, httpd=httpd)
                try:
                    target = _storage_target(cfg)
                    source = target / name
                    stat = source.stat()
                    if not source.is_file():
                        raise FileNotFoundError(name)
                    if int(stat.st_size) > int(getattr(backup_module, "_MAX_BACKUP_BYTES", 32 * 1024 * 1024)):
                        raise backup_module.SettingsBackupError("备份文件过大")
                    data = source.read_bytes()
                except backup_module.SettingsBackupError:
                    raise
                except (OSError, ValueError) as exc:
                    raise backup_module.SettingsBackupError(f"读取备份失败：{exc}") from exc
                restored = self.restore_settings_zip(data, httpd=httpd)
                return {"status": "ok", "backend": backend, "name": name, "restored": restored}

            service_class.load_config = load_config
            service_class.save_config = save_config
            service_class.test_connection = test_connection
            service_class.list_backups = list_backups
            service_class.backup_now = backup_now
            service_class.prune_backups = prune_backups
            service_class.restore_remote = restore_remote
            service_class._bilipdj_storage_guard = True

        if server_module is None:
            return True
        if bool(getattr(server_module, "_settings_storage_guard_installed", False)):
            return True

        handler_class = server_module.ApiHandler
        original_get = handler_class.do_GET
        original_post = handler_class.do_POST

        def do_GET(self: Any) -> None:  # noqa: N802
            import urllib.parse

            path = urllib.parse.urlparse(self.path).path
            if path not in {"/api/backup/settings/config", "/api/backup/settings/list"}:
                return original_get(self)
            if not self._require_loopback():
                return
            service = backup_module.SettingsBackupService(server_module)
            try:
                if path == "/api/backup/settings/config":
                    self._write_json({"status": "ok", "config": service.load_config(include_password=False)})
                    return
                self._write_json({"status": "ok", "backups": service.list_backups()})
            except backup_module.SettingsBackupError as exc:
                self._write_json(backup_module._json_response_error(str(exc)), status=400)

        def do_POST(self: Any) -> None:  # noqa: N802
            import urllib.parse

            path = urllib.parse.urlparse(self.path).path
            if path not in {
                "/api/backup/settings/config",
                "/api/backup/settings/test",
                "/api/backup/settings/run",
                "/api/backup/settings/restore",
            }:
                return original_post(self)
            if not self._require_loopback():
                return
            service = backup_module.SettingsBackupService(server_module)
            try:
                payload = backup_module._read_json_body(self)
                if path == "/api/backup/settings/config":
                    config_payload = payload.get("config", payload)
                    self._write_json({"status": "ok", "config": service.save_config(config_payload)})
                    return
                if path == "/api/backup/settings/test":
                    self._write_json(service.test_connection())
                    return
                if path == "/api/backup/settings/run":
                    self._write_json(service.backup_now())
                    return
                name = str(payload.get("name", "") or "")
                self._write_json(service.restore_remote(name, httpd=self.server))
            except backup_module.SettingsBackupError as exc:
                self._write_json(backup_module._json_response_error(str(exc)), status=400)
            except Exception as exc:  # noqa: BLE001
                self._write_json(backup_module._json_response_error(f"设置备份操作失败：{exc}"), status=500)

        handler_class.do_GET = do_GET
        handler_class.do_POST = do_POST

        def safe_auto_backup(active_server: Any, reason: str) -> None:
            try:
                service = backup_module.SettingsBackupService(active_server)
                cfg = service.load_config(include_password=True)
                key = "auto_on_start" if reason == "start" else "auto_on_exit"
                if not bool(cfg.get(key, False)) or not _target_configured(cfg):
                    return
                if reason == "exit":
                    with getattr(backup_module, "_PATCH_LOCK", _PATCH_LOCK):
                        if bool(getattr(backup_module, "_EXIT_BACKUP_DONE", False)):
                            return
                        backup_module._EXIT_BACKUP_DONE = True
                service.backup_now()
            except Exception as exc:  # noqa: BLE001
                try:
                    print(f"[bilipdj-backup] {reason} backup skipped: {exc}")
                except Exception:
                    pass

        backup_module._safe_auto_backup = safe_auto_backup
        wrapped_run_server = server_module.run_server
        base_run_server = _unwrap_original_run_server(wrapped_run_server)

        def run_server_with_storage_backup(*args: Any, **kwargs: Any) -> Any:
            backup_module._EXIT_BACKUP_DONE = False
            cfg = backup_module.SettingsBackupService(server_module).load_config(include_password=True)
            if bool(cfg.get("auto_on_exit", False)) and _target_configured(cfg):
                atexit.register(safe_auto_backup, server_module, "exit")
            if bool(cfg.get("auto_on_start", False)) and _target_configured(cfg):
                threading.Thread(
                    target=safe_auto_backup,
                    args=(server_module, "start"),
                    name="bilipdj-settings-backup-start",
                    daemon=True,
                ).start()
            try:
                return base_run_server(*args, **kwargs)
            finally:
                safe_auto_backup(server_module, "exit")

        server_module.run_server = run_server_with_storage_backup
        server_module._settings_storage_guard_installed = True
        return True


__all__ = ["install_settings_storage_guard"]
