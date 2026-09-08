from __future__ import annotations

import atexit
import base64
import io
import json
import re
import threading
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any

SETTINGS_FILES = ("config.yaml", "quanxian.yaml", "kaiguan.yaml", "style.json")
BACKUP_NAME_RE = re.compile(r"^BiliPDJ-settings-(\d{8}-\d{6})\.zip$")
DEFAULT_WEBDAV_CONFIG: dict[str, Any] = {
    "url": "",
    "username": "",
    "password": "",
    "remote_dir": "BiliPDJ_Backup",
    "auto_on_start": False,
    "auto_on_exit": False,
    "keep_last": 10,
}
_MAX_CONFIG_BYTES = 128 * 1024
_MAX_BACKUP_BYTES = 32 * 1024 * 1024
_PATCH_LOCK = threading.RLock()
_EXIT_BACKUP_DONE = False


class SettingsBackupError(RuntimeError):
    pass


def _normalize_remote_dir(value: Any) -> str:
    raw = str(value or "BiliPDJ_Backup").strip().replace("\\", "/").strip("/")
    if not raw:
        return "BiliPDJ_Backup"
    parts = [part.strip() for part in raw.split("/") if part.strip()]
    if any(part in {".", ".."} for part in parts):
        raise SettingsBackupError("WebDAV 远程目录不能包含 . 或 ..")
    if any("/" in part or "\\" in part for part in parts):
        raise SettingsBackupError("WebDAV 远程目录格式无效")
    return "/".join(parts)


def _normalize_url(value: Any, *, required: bool = False) -> str:
    raw = str(value or "").strip().rstrip("/")
    if not raw:
        if required:
            raise SettingsBackupError("请先填写 WebDAV 地址")
        return ""
    parsed = urllib.parse.urlsplit(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise SettingsBackupError("WebDAV 地址必须是 http:// 或 https:// URL")
    if parsed.query or parsed.fragment:
        raise SettingsBackupError("WebDAV 地址不能包含查询参数或 #fragment")
    return raw


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp")
    temp.write_bytes(data)
    temp.replace(path)


def _json_response_error(message: str) -> dict[str, Any]:
    return {"status": "error", "message": str(message)}


class SettingsBackupService:
    def __init__(self, server_module: Any) -> None:
        self.server = server_module
        self._lock = threading.RLock()

    @property
    def config_path(self) -> Path:
        yaml_dir = Path(getattr(self.server, "_YAML_DIR"))
        return yaml_dir / "webdav_backup.json"

    def settings_paths(self) -> dict[str, Path]:
        return {
            "config.yaml": Path(getattr(self.server, "CONFIG_PATH")),
            "quanxian.yaml": Path(getattr(self.server, "QUANXIAN_PATH")),
            "kaiguan.yaml": Path(getattr(self.server, "KAIGUAN_PATH")),
            "style.json": Path(getattr(self.server, "STYLE_PATH")),
        }

    def load_config(self, *, include_password: bool = False) -> dict[str, Any]:
        cfg = dict(DEFAULT_WEBDAV_CONFIG)
        try:
            raw = json.loads(self.config_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                cfg.update(raw)
        except (OSError, json.JSONDecodeError):
            pass

        cfg["url"] = _normalize_url(cfg.get("url", ""), required=False)
        cfg["username"] = str(cfg.get("username", "") or "").strip()
        cfg["password"] = str(cfg.get("password", "") or "")
        cfg["remote_dir"] = _normalize_remote_dir(cfg.get("remote_dir", "BiliPDJ_Backup"))
        cfg["auto_on_start"] = bool(cfg.get("auto_on_start", False))
        cfg["auto_on_exit"] = bool(cfg.get("auto_on_exit", False))
        try:
            keep_last = int(cfg.get("keep_last", 10))
        except (TypeError, ValueError):
            keep_last = 10
        cfg["keep_last"] = max(1, min(100, keep_last))

        if include_password:
            return cfg
        public_cfg = {key: value for key, value in cfg.items() if key != "password"}
        public_cfg["password_set"] = bool(cfg.get("password"))
        return public_cfg

    def save_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            current = self.load_config(include_password=True)
            incoming = payload if isinstance(payload, dict) else {}
            merged = dict(current)

            if "url" in incoming:
                merged["url"] = _normalize_url(incoming.get("url"), required=False)
            if "username" in incoming:
                merged["username"] = str(incoming.get("username", "") or "").strip()
            if bool(incoming.get("clear_password", False)):
                merged["password"] = ""
            elif "password" in incoming and str(incoming.get("password", "")) != "":
                merged["password"] = str(incoming.get("password", ""))
            if "remote_dir" in incoming:
                merged["remote_dir"] = _normalize_remote_dir(incoming.get("remote_dir"))
            for key in ("auto_on_start", "auto_on_exit"):
                if key in incoming:
                    merged[key] = bool(incoming.get(key))
            if "keep_last" in incoming:
                try:
                    keep_last = int(incoming.get("keep_last", 10))
                except (TypeError, ValueError):
                    keep_last = 10
                merged["keep_last"] = max(1, min(100, keep_last))

            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            _atomic_write_bytes(
                self.config_path,
                json.dumps(merged, ensure_ascii=False, indent=2).encode("utf-8"),
            )
            try:
                self.config_path.chmod(0o600)
            except OSError:
                pass
            return self.load_config(include_password=False)

    def build_settings_zip(self) -> tuple[bytes, list[str]]:
        payload = io.BytesIO()
        included: list[str] = []
        with zipfile.ZipFile(payload, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name in SETTINGS_FILES:
                path = self.settings_paths()[name]
                if not path.is_file():
                    continue
                archive.writestr(name, path.read_bytes())
                included.append(name)
        if not included:
            raise SettingsBackupError("没有找到可备份的设置文件")
        return payload.getvalue(), included

    def validate_settings_zip(self, data: bytes) -> dict[str, bytes]:
        if not isinstance(data, (bytes, bytearray)) or not data:
            raise SettingsBackupError("备份文件为空")
        if len(data) > _MAX_BACKUP_BYTES:
            raise SettingsBackupError("备份文件过大")
        try:
            archive = zipfile.ZipFile(io.BytesIO(bytes(data)), "r")
        except zipfile.BadZipFile as exc:
            raise SettingsBackupError("备份 ZIP 无效") from exc

        restored: dict[str, bytes] = {}
        total_size = 0
        with archive:
            for info in archive.infolist():
                name = str(info.filename or "")
                normalized = name.replace("\\", "/")
                if (
                    not name
                    or normalized.startswith("/")
                    or "/" in normalized
                    or normalized in {".", ".."}
                    or name not in SETTINGS_FILES
                ):
                    raise SettingsBackupError(f"备份中包含不允许的文件：{name}")
                if name in restored:
                    raise SettingsBackupError(f"备份中存在重复文件：{name}")
                if info.is_dir():
                    raise SettingsBackupError(f"备份中包含目录：{name}")
                total_size += int(info.file_size)
                if total_size > _MAX_BACKUP_BYTES:
                    raise SettingsBackupError("备份解压后的设置文件过大")
                restored[name] = archive.read(info)

        if not restored:
            raise SettingsBackupError("备份 ZIP 中没有设置文件")
        return restored

    def restore_settings_zip(self, data: bytes, *, httpd: Any | None = None) -> list[str]:
        restored = self.validate_settings_zip(data)
        paths = self.settings_paths()
        before: dict[str, bytes | None] = {}
        written: list[str] = []

        safety_path = self.config_path.parent / ".settings-restore-safety.zip"
        with self._lock:
            for name, path in paths.items():
                before[name] = path.read_bytes() if path.is_file() else None
            safety_data, _ = self.build_settings_zip()
            _atomic_write_bytes(safety_path, safety_data)
            try:
                for name, content in restored.items():
                    _atomic_write_bytes(paths[name], content)
                    written.append(name)
                if httpd is not None:
                    self._reload_runtime(httpd)
            except Exception:
                for name in written:
                    old = before.get(name)
                    path = paths[name]
                    try:
                        if old is None:
                            path.unlink(missing_ok=True)
                        else:
                            _atomic_write_bytes(path, old)
                    except OSError:
                        pass
                if httpd is not None:
                    try:
                        self._reload_runtime(httpd)
                    except Exception:
                        pass
                try:
                    safety_path.unlink(missing_ok=True)
                except OSError:
                    pass
                raise
            else:
                try:
                    safety_path.unlink(missing_ok=True)
                except OSError:
                    pass
        return list(restored)

    def _reload_runtime(self, httpd: Any) -> None:
        runtime_config = self.server.load_config()
        httpd.runtime_config = runtime_config
        queue_manager = getattr(httpd, "queue_manager", None)
        if queue_manager is not None:
            queue_manager.load_config(
                runtime_config.get("myjs", {}),
                anchor_uid=self.server._get_anchor_uid_for_platform(runtime_config),
            )
            queue_manager.load_quanxian(self.server.load_quanxian())
            queue_manager.load_kaiguan(self.server.load_kaiguan())
        if hasattr(self.server, "_ensure_danmu_relay"):
            self.server._ensure_danmu_relay(httpd, reconnect=True)
        try:
            self.server.reconcile_live_css_with_archive(self.server._current_style_slot())
        except Exception:
            pass

    @staticmethod
    def _backup_name() -> str:
        return f"BiliPDJ-settings-{datetime.now().strftime('%Y%m%d-%H%M%S')}.zip"

    @staticmethod
    def _auth_headers(cfg: dict[str, Any]) -> dict[str, str]:
        username = str(cfg.get("username", "") or "")
        password = str(cfg.get("password", "") or "")
        if not username and not password:
            return {}
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    def _request(
        self,
        method: str,
        url: str,
        *,
        cfg: dict[str, Any],
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
        expected: tuple[int, ...] = (200, 201, 204, 207),
    ) -> tuple[int, bytes, dict[str, str]]:
        request_headers = {"User-Agent": "BiliPDJ-WebDAV/1.0", **self._auth_headers(cfg)}
        if headers:
            request_headers.update(headers)
        request = urllib.request.Request(url, data=data, headers=request_headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=12.0) as response:
                status = int(getattr(response, "status", 200))
                body = response.read()
                response_headers = {str(k): str(v) for k, v in response.headers.items()}
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            if status not in expected:
                detail = exc.read(512).decode("utf-8", errors="replace").strip()
                raise SettingsBackupError(
                    f"WebDAV {method} 失败：HTTP {status}" + (f" - {detail}" if detail else "")
                ) from exc
            body = exc.read()
            response_headers = {str(k): str(v) for k, v in exc.headers.items()}
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise SettingsBackupError(f"WebDAV 连接失败：{exc}") from exc

        if status not in expected:
            raise SettingsBackupError(f"WebDAV {method} 返回异常状态：HTTP {status}")
        return status, body, response_headers

    def _remote_collection_url(self, cfg: dict[str, Any]) -> str:
        base = _normalize_url(cfg.get("url"), required=True)
        remote_dir = _normalize_remote_dir(cfg.get("remote_dir"))
        encoded = "/".join(urllib.parse.quote(part, safe="") for part in remote_dir.split("/"))
        return f"{base}/{encoded}".rstrip("/")

    def _ensure_remote_collection(self, cfg: dict[str, Any]) -> str:
        base = _normalize_url(cfg.get("url"), required=True)
        remote_dir = _normalize_remote_dir(cfg.get("remote_dir"))
        current = base
        for part in remote_dir.split("/"):
            current = f"{current.rstrip('/')}/{urllib.parse.quote(part, safe='')}"
            self._request(
                "MKCOL",
                current,
                cfg=cfg,
                expected=(200, 201, 204, 301, 302, 405),
            )
        return current

    def test_connection(self) -> dict[str, Any]:
        cfg = self.load_config(include_password=True)
        target = self._ensure_remote_collection(cfg)
        self._request(
            "PROPFIND",
            target,
            cfg=cfg,
            data=b'<?xml version="1.0"?><propfind xmlns="DAV:"><prop><displayname/></prop></propfind>',
            headers={"Depth": "0", "Content-Type": "application/xml; charset=utf-8"},
            expected=(200, 207),
        )
        return {"status": "ok", "message": "WebDAV 连接成功", "remote_dir": cfg["remote_dir"]}

    def list_backups(self) -> list[dict[str, Any]]:
        cfg = self.load_config(include_password=True)
        target = self._ensure_remote_collection(cfg)
        _, body, _ = self._request(
            "PROPFIND",
            target,
            cfg=cfg,
            data=(
                b'<?xml version="1.0"?><propfind xmlns="DAV:"><prop>'
                b"<displayname/><getcontentlength/><getlastmodified/>"
                b"</prop></propfind>"
            ),
            headers={"Depth": "1", "Content-Type": "application/xml; charset=utf-8"},
            expected=(200, 207),
        )
        items: list[dict[str, Any]] = []
        try:
            root = ET.fromstring(body)
        except ET.ParseError as exc:
            raise SettingsBackupError("WebDAV 返回的备份列表 XML 无法解析") from exc

        ns = {"d": "DAV:"}
        for response in root.findall(".//d:response", ns):
            href = (response.findtext("d:href", default="", namespaces=ns) or "").strip()
            name = urllib.parse.unquote(urllib.parse.urlsplit(href).path.rstrip("/").split("/")[-1])
            if not BACKUP_NAME_RE.fullmatch(name):
                continue
            size_text = response.findtext(".//d:getcontentlength", default="0", namespaces=ns) or "0"
            modified = response.findtext(".//d:getlastmodified", default="", namespaces=ns) or ""
            try:
                size = int(size_text)
            except ValueError:
                size = 0
            items.append({"name": name, "size": size, "modified": modified})
        items.sort(key=lambda item: str(item["name"]), reverse=True)
        return items

    def backup_now(self) -> dict[str, Any]:
        cfg = self.load_config(include_password=True)
        target = self._ensure_remote_collection(cfg)
        data, included = self.build_settings_zip()
        name = self._backup_name()
        remote_url = f"{target.rstrip('/')}/{urllib.parse.quote(name, safe='')}"
        self._request(
            "PUT",
            remote_url,
            cfg=cfg,
            data=data,
            headers={"Content-Type": "application/zip"},
            expected=(200, 201, 204),
        )
        self.prune_backups(cfg=cfg)
        return {"status": "ok", "name": name, "size": len(data), "included": included}

    def prune_backups(self, *, cfg: dict[str, Any] | None = None) -> list[str]:
        active_cfg = cfg or self.load_config(include_password=True)
        keep_last = int(active_cfg.get("keep_last", 10))
        items = self.list_backups()
        removed: list[str] = []
        target = self._remote_collection_url(active_cfg)
        for item in items[keep_last:]:
            name = str(item["name"])
            self._request(
                "DELETE",
                f"{target.rstrip('/')}/{urllib.parse.quote(name, safe='')}",
                cfg=active_cfg,
                expected=(200, 204, 404),
            )
            removed.append(name)
        return removed

    def restore_remote(self, name: str, *, httpd: Any | None = None) -> dict[str, Any]:
        if not BACKUP_NAME_RE.fullmatch(str(name or "")):
            raise SettingsBackupError("备份文件名无效")
        cfg = self.load_config(include_password=True)
        target = self._remote_collection_url(cfg)
        _, data, _ = self._request(
            "GET",
            f"{target.rstrip('/')}/{urllib.parse.quote(name, safe='')}",
            cfg=cfg,
            expected=(200,),
        )
        included = self.restore_settings_zip(data, httpd=httpd)
        return {"status": "ok", "name": name, "restored": included}


def _read_json_body(handler: Any) -> dict[str, Any]:
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except (TypeError, ValueError):
        length = 0
    if length < 0 or length > _MAX_CONFIG_BYTES:
        raise SettingsBackupError("请求体过大")
    if length == 0:
        return {}
    raw = handler.rfile.read(length).decode("utf-8", errors="replace")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SettingsBackupError("请求体必须是有效 JSON") from exc
    if not isinstance(payload, dict):
        raise SettingsBackupError("请求体必须是 JSON 对象")
    return payload


def _safe_auto_backup(server_module: Any, reason: str) -> None:
    global _EXIT_BACKUP_DONE
    try:
        service = SettingsBackupService(server_module)
        cfg = service.load_config(include_password=True)
        key = "auto_on_start" if reason == "start" else "auto_on_exit"
        if not bool(cfg.get(key, False)) or not str(cfg.get("url", "")).strip():
            return
        if reason == "exit":
            with _PATCH_LOCK:
                if _EXIT_BACKUP_DONE:
                    return
                _EXIT_BACKUP_DONE = True
        service.backup_now()
    except Exception as exc:  # noqa: BLE001
        try:
            print(f"[bilipdj-backup] {reason} backup skipped: {exc}")
        except Exception:
            pass


def install_settings_backup(server_module: Any) -> bool:
    if bool(getattr(server_module, "_settings_backup_installed", False)):
        return True

    with _PATCH_LOCK:
        if bool(getattr(server_module, "_settings_backup_installed", False)):
            return True

        handler_class = server_module.ApiHandler
        original_get = handler_class.do_GET
        original_post = handler_class.do_POST

        def do_GET(self: Any) -> None:  # noqa: N802
            path = urllib.parse.urlparse(self.path).path
            if path not in {"/api/backup/webdav/config", "/api/backup/webdav/list"}:
                return original_get(self)
            if not self._require_loopback():
                return
            service = SettingsBackupService(server_module)
            try:
                if path == "/api/backup/webdav/config":
                    self._write_json({"status": "ok", "config": service.load_config(include_password=False)})
                    return
                self._write_json({"status": "ok", "backups": service.list_backups()})
            except SettingsBackupError as exc:
                self._write_json(_json_response_error(str(exc)), status=400)

        def do_POST(self: Any) -> None:  # noqa: N802
            path = urllib.parse.urlparse(self.path).path
            if path not in {
                "/api/backup/webdav/config",
                "/api/backup/webdav/test",
                "/api/backup/webdav/run",
                "/api/backup/webdav/restore",
            }:
                return original_post(self)
            if not self._require_loopback():
                return
            service = SettingsBackupService(server_module)
            try:
                payload = _read_json_body(self)
                if path == "/api/backup/webdav/config":
                    config_payload = payload.get("config", payload)
                    self._write_json({"status": "ok", "config": service.save_config(config_payload)})
                    return
                if path == "/api/backup/webdav/test":
                    self._write_json(service.test_connection())
                    return
                if path == "/api/backup/webdav/run":
                    self._write_json(service.backup_now())
                    return
                name = str(payload.get("name", "") or "")
                self._write_json(service.restore_remote(name, httpd=self.server))
            except SettingsBackupError as exc:
                self._write_json(_json_response_error(str(exc)), status=400)
            except Exception as exc:  # noqa: BLE001
                self._write_json(_json_response_error(f"设置备份操作失败：{exc}"), status=500)

        handler_class.do_GET = do_GET
        handler_class.do_POST = do_POST

        original_run_server = server_module.run_server

        def run_server_with_settings_backup(*args: Any, **kwargs: Any) -> Any:
            cfg = SettingsBackupService(server_module).load_config(include_password=True)
            if bool(cfg.get("auto_on_exit", False)) and str(cfg.get("url", "")).strip():
                atexit.register(_safe_auto_backup, server_module, "exit")
            if bool(cfg.get("auto_on_start", False)) and str(cfg.get("url", "")).strip():
                threading.Thread(
                    target=_safe_auto_backup,
                    args=(server_module, "start"),
                    name="bilipdj-settings-backup-start",
                    daemon=True,
                ).start()
            try:
                return original_run_server(*args, **kwargs)
            finally:
                _safe_auto_backup(server_module, "exit")

        server_module.run_server = run_server_with_settings_backup
        server_module.SettingsBackupService = SettingsBackupService
        server_module._settings_backup_installed = True
        return True


__all__ = [
    "BACKUP_NAME_RE",
    "DEFAULT_WEBDAV_CONFIG",
    "SETTINGS_FILES",
    "SettingsBackupError",
    "SettingsBackupService",
    "install_settings_backup",
]
