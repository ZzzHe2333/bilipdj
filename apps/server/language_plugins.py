from __future__ import annotations

import copy
import json
import os
import re
import threading
from http import HTTPStatus
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

LANGUAGE_PLUGIN_TYPE = "language"
LANGUAGE_RUNTIME = "resource"
DEFAULT_LANGUAGE = "zh-CN"
LANGUAGE_CONFIG_NAME = "language.json"
MAX_TRANSLATIONS = 8000
MAX_TRANSLATION_KEY = 500
MAX_TRANSLATION_VALUE = 4000
_LANGUAGE_RE = re.compile(r"^[a-z]{2,3}(?:-[A-Z][a-z]{3})?(?:-[A-Z]{2}|-[0-9]{3})?$")
_PATCH_LOCK = threading.RLock()


class LanguagePluginError(RuntimeError):
    pass


def _load_translation_mapping(path: Path) -> dict[str, str]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LanguagePluginError(f"语言包翻译文件无法读取：{exc}") from exc
    if isinstance(raw, dict) and isinstance(raw.get("translations"), dict):
        raw = raw["translations"]
    if not isinstance(raw, dict):
        raise LanguagePluginError("语言包翻译文件必须是 JSON 对象")
    if len(raw) > MAX_TRANSLATIONS:
        raise LanguagePluginError(f"语言包最多允许 {MAX_TRANSLATIONS} 条翻译")
    result: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise LanguagePluginError("翻译 key 和 value 都必须是字符串")
        if not key or len(key) > MAX_TRANSLATION_KEY or len(value) > MAX_TRANSLATION_VALUE:
            raise LanguagePluginError("语言包包含空 key 或过长文本")
        result[key] = value
    return result


def _validate_language_manifest(pm: Any, manifest: Any, current_version: str) -> dict[str, Any]:
    if not isinstance(manifest, dict):
        raise pm.PluginError("manifest.json must be a JSON object")
    required = (
        "schema", "id", "name", "version", "plugin_api", "type", "platform", "runtime",
        "entry", "language", "language_name", "translations", "min_bilipdj_version",
        "permissions", "capabilities", "files",
    )
    missing = [key for key in required if key not in manifest]
    if missing:
        raise pm.PluginError("manifest missing fields: " + ", ".join(missing))
    if int(manifest.get("schema", 0) or 0) != pm.MANIFEST_SCHEMA:
        raise pm.PluginError(f"unsupported manifest schema: {manifest.get('schema')}")

    plugin_id = str(manifest.get("id", "") or "")
    if plugin_id != plugin_id.strip() or plugin_id != plugin_id.lower():
        raise pm.PluginError("plugin id must already be lowercase canonical text")
    if not pm._ID_RE.fullmatch(plugin_id) or plugin_id.startswith("builtin."):
        raise pm.PluginError("invalid/reserved plugin id")
    reserved = set(getattr(pm, "_issue130_reserved_plugin_ids", ())) | {"data", "state", "trusted_keys", "plugins"}
    if plugin_id in reserved:
        raise pm.PluginError("invalid/reserved plugin id")

    if int(manifest.get("plugin_api", 0) or 0) != pm.PLUGIN_API_VERSION:
        raise pm.PluginError(
            f"incompatible plugin_api={manifest.get('plugin_api')}; BiliPDJ supports {pm.PLUGIN_API_VERSION}"
        )
    if str(manifest.get("type", "") or "") != LANGUAGE_PLUGIN_TYPE:
        raise pm.PluginError(f"unsupported plugin type: {manifest.get('type')}")
    if str(manifest.get("platform", "") or "") != "language":
        raise pm.PluginError("language plugin platform must be 'language'")
    if str(manifest.get("runtime", "") or "") != LANGUAGE_RUNTIME:
        raise pm.PluginError("language plugin runtime must be 'resource'")
    if not str(manifest.get("name", "") or "").strip():
        raise pm.PluginError("plugin name is required")

    pm._version_key(str(manifest.get("version", "") or ""))
    min_version = str(manifest.get("min_bilipdj_version", "") or "").strip()
    max_version = str(manifest.get("max_bilipdj_version", "") or "").strip()
    if pm._version_key(current_version) < pm._version_key(min_version):
        raise pm.PluginError(f"plugin requires BiliPDJ >= {min_version}; current {current_version}")
    if max_version and pm._version_key(current_version) > pm._version_key(max_version):
        raise pm.PluginError(f"plugin requires BiliPDJ <= {max_version}; current {current_version}")

    language = str(manifest.get("language", "") or "")
    if not _LANGUAGE_RE.fullmatch(language) or language == DEFAULT_LANGUAGE:
        raise pm.PluginError("language must be a canonical BCP-47 code other than zh-CN")
    if not str(manifest.get("language_name", "") or "").strip():
        raise pm.PluginError("language_name is required")

    translations = str(manifest.get("translations", "") or "")
    entry = str(manifest.get("entry", "") or "")
    if translations != translations.strip() or pm._safe_member_name(translations) != translations:
        raise pm.PluginError("translations path must use canonical forward-slash form")
    if not translations.lower().endswith(".json"):
        raise pm.PluginError("translations must point to a JSON file")
    if entry != translations:
        raise pm.PluginError("language plugin entry must equal translations path")

    files = pm._normalize_files_mapping(manifest)
    if translations not in files:
        raise pm.PluginError("translations file must be included in manifest.files")
    raw_files = manifest.get("files")
    if not isinstance(raw_files, dict) or any(str(k) != pm._safe_member_name(str(k)) for k in raw_files):
        raise pm.PluginError("manifest file paths must use canonical forward-slash form")
    if any(str(v) != str(v).strip().lower() for v in raw_files.values()):
        raise pm.PluginError("manifest SHA-256 values must use lowercase canonical form")

    permissions = manifest.get("permissions")
    if permissions != []:
        raise pm.PluginError("language resource plugins cannot request permissions")
    capabilities = manifest.get("capabilities")
    if not isinstance(capabilities, list) or any(not isinstance(item, str) or not item.strip() for item in capabilities):
        raise pm.PluginError("capabilities must be a non-empty string array")
    if "ui_translation" not in capabilities:
        raise pm.PluginError("language plugin must declare ui_translation capability")

    normalized = copy.deepcopy(manifest)
    normalized["files"] = files
    return normalized


class LanguageService:
    def __init__(self, server_module: Any, manager: Any) -> None:
        self.server = server_module
        self.manager = manager
        self._lock = threading.RLock()
        self._translation_cache: dict[tuple[str, int], dict[str, str]] = {}

    @property
    def config_path(self) -> Path:
        return Path(getattr(self.server, "_YAML_DIR")) / LANGUAGE_CONFIG_NAME

    def _read_selected(self) -> str:
        try:
            raw = json.loads(self.config_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return DEFAULT_LANGUAGE
        value = str(raw.get("language", DEFAULT_LANGUAGE) or DEFAULT_LANGUAGE) if isinstance(raw, dict) else DEFAULT_LANGUAGE
        return value if value == DEFAULT_LANGUAGE or _LANGUAGE_RE.fullmatch(value) else DEFAULT_LANGUAGE

    def _write_selected(self, language: str) -> None:
        path = self.config_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(f".{path.name}.tmp")
        temp.write_text(json.dumps({"schema": 1, "language": language}, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp, path)

    def _language_records(self) -> list[Any]:
        records = []
        with getattr(self.manager, "_lock"):
            values = list(getattr(self.manager, "_records", {}).values())
        for record in values:
            manifest = getattr(record, "manifest", {})
            if (
                isinstance(manifest, dict)
                and str(manifest.get("type", "")) == LANGUAGE_PLUGIN_TYPE
                and bool(getattr(record, "enabled", False))
                and bool(getattr(record, "verified", False))
                and not str(getattr(record, "error", "") or "")
            ):
                records.append(record)
        return records

    def list_languages(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = [{
            "code": DEFAULT_LANGUAGE,
            "name": "简体中文",
            "plugin_id": "builtin.zh-cn.language",
            "source": "builtin",
        }]
        for record in sorted(self._language_records(), key=lambda item: str(item.manifest.get("language", ""))):
            rows.append({
                "code": str(record.manifest.get("language", "")),
                "name": str(record.manifest.get("language_name", record.name) or record.name),
                "plugin_id": record.plugin_id,
                "source": "external",
            })
        return rows

    def _record_for_language(self, language: str) -> Any | None:
        for record in self._language_records():
            if str(record.manifest.get("language", "")) == language:
                return record
        return None

    def active_language(self) -> str:
        selected = self._read_selected()
        if selected == DEFAULT_LANGUAGE:
            return selected
        return selected if self._record_for_language(selected) is not None else DEFAULT_LANGUAGE

    def translations(self, language: str | None = None) -> dict[str, str]:
        selected = str(language or self.active_language())
        if selected == DEFAULT_LANGUAGE:
            return {}
        record = self._record_for_language(selected)
        if record is None:
            return {}
        relative = str(record.manifest.get("translations", "") or "")
        path = record.root.joinpath(*PurePosixPath(relative).parts)
        try:
            stamp = int(path.stat().st_mtime_ns)
        except OSError as exc:
            raise LanguagePluginError(f"语言包翻译文件不存在：{relative}") from exc
        cache_key = (record.plugin_id, stamp)
        with self._lock:
            cached = self._translation_cache.get(cache_key)
            if cached is not None:
                return dict(cached)
        mapping = _load_translation_mapping(path)
        with self._lock:
            self._translation_cache = {cache_key: dict(mapping)}
        return mapping

    def set_active(self, language: str) -> str:
        value = str(language or "").strip()
        allowed = {item["code"] for item in self.list_languages()}
        if value not in allowed:
            raise LanguagePluginError("该语言未安装、未启用或语言包校验失败")
        self._write_selected(value)
        return value

    def payload(self) -> dict[str, Any]:
        active = self.active_language()
        return {
            "status": "ok",
            "default": DEFAULT_LANGUAGE,
            "active": active,
            "languages": self.list_languages(),
            "translations": self.translations(active),
        }


def install_language_plugin_system(server_module: Any, pm: Any, backup_module: Any | None = None) -> bool:
    with _PATCH_LOCK:
        if bool(getattr(server_module, "_language_plugin_system_installed", False)):
            return True
        manager = getattr(server_module, "plugin_manager", None)
        if manager is None:
            raise RuntimeError("PluginManager must be installed before language plugins")

        original_validate = pm.validate_manifest
        original_public_info = pm.InstalledPluginRecord.public_info

        def validate_manifest(manifest: Any, current_version: str) -> dict[str, Any]:
            if isinstance(manifest, dict) and str(manifest.get("type", "") or "") == LANGUAGE_PLUGIN_TYPE:
                return _validate_language_manifest(pm, manifest, current_version)
            return original_validate(manifest, current_version)

        def public_info(self: Any) -> dict[str, Any]:
            payload = original_public_info(self)
            manifest = self.manifest if isinstance(self.manifest, dict) else {}
            if str(manifest.get("type", "")) == LANGUAGE_PLUGIN_TYPE:
                payload["runtime"] = LANGUAGE_RUNTIME
                payload["permission_enforcement"] = "resource-only"
                payload["category"] = "language"
                payload["language"] = str(manifest.get("language", ""))
                payload["language_name"] = str(manifest.get("language_name", ""))
                payload["platform"] = "language"
            return payload

        def discover(self: Any) -> list[dict[str, Any]]:
            with self._lock:
                self._unregister_external()
                state = self._state()
                records: dict[str, Any] = {}
                enabled_languages: dict[str, str] = {}
                for root in sorted(self.plugins_root.iterdir()):
                    if not root.is_dir() or root.name.startswith(".") or root.name == "data":
                        continue
                    manifest_path = root / "manifest.json"
                    meta_path = root / ".install.json"
                    if not manifest_path.is_file() or not meta_path.is_file():
                        continue
                    plugin_id = root.name
                    try:
                        manifest = pm.validate_manifest(json.loads(manifest_path.read_text(encoding="utf-8")), self.current_version)
                        if manifest["id"] != plugin_id:
                            raise pm.PluginError("installed directory does not match plugin id")
                        meta = json.loads(meta_path.read_text(encoding="utf-8"))
                        package_hash, signature_status = self._verify_installed(root, manifest, meta)
                        record = pm.InstalledPluginRecord(
                            plugin_id=plugin_id,
                            root=root,
                            manifest=manifest,
                            enabled=bool(state.get(plugin_id, False)),
                            package_sha256=package_hash,
                            signature_status=signature_status,
                            verified=True,
                        )
                        plugin_type = str(manifest.get("type", "") or "")
                        if record.enabled and plugin_type == LANGUAGE_PLUGIN_TYPE:
                            code = str(manifest.get("language", ""))
                            if code in enabled_languages:
                                raise pm.PluginError(
                                    f"language {code} already provided by {enabled_languages[code]}"
                                )
                            _load_translation_mapping(root.joinpath(*PurePosixPath(str(manifest["translations"])).parts))
                            enabled_languages[code] = plugin_id
                        elif record.enabled:
                            existing = self.registry.get(record.platform)
                            if existing is not None:
                                raise pm.PluginError(f"platform already registered by {existing.plugin_id}")
                            self.registry.register(self._load_external_plugin(record))
                    except Exception as exc:
                        try:
                            raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                        except Exception:
                            raw_manifest = {"id": plugin_id, "platform": "", "name": plugin_id, "version": ""}
                        record = pm.InstalledPluginRecord(
                            plugin_id=plugin_id,
                            root=root,
                            manifest=raw_manifest if isinstance(raw_manifest, dict) else {"id": plugin_id},
                            enabled=bool(state.get(plugin_id, False)),
                            package_sha256="",
                            signature_status="error",
                            verified=False,
                            error=str(exc),
                        )
                    records[plugin_id] = record
                self._records = records
                pm._plugin_core._sync_legacy_platform_metadata(self.server_module, self.issue79_module, self.registry)
                pm._plugin_core._install_registry_factory(self.server_module, self.registry)
                pm._plugin_core._install_issue79_plugin_bridge(self.server_module, self.issue79_module, self.registry)
                return self.list_public()

        pm.validate_manifest = validate_manifest
        pm.InstalledPluginRecord.public_info = public_info
        pm.PluginManager.discover = discover
        pm.LANGUAGE_PLUGIN_TYPE = LANGUAGE_PLUGIN_TYPE
        pm.SUPPORTED_PLUGIN_TYPES = frozenset({pm.PLUGIN_TYPE, LANGUAGE_PLUGIN_TYPE})
        manager.discover()

        service = LanguageService(server_module, manager)
        server_module.language_service = service
        server_module.LANGUAGE_SERVICE = service

        handler_class = server_module.ApiHandler
        original_get = handler_class.do_GET
        original_post = handler_class.do_POST

        def do_GET(self: Any) -> None:
            if urlparse(self.path).path == "/api/language":
                try:
                    self._write_json(service.payload())
                except Exception as exc:  # noqa: BLE001
                    self._write_json({"status": "error", "message": f"读取语言配置失败：{exc}"}, status=500)
                return
            return original_get(self)

        def do_POST(self: Any) -> None:
            if urlparse(self.path).path != "/api/language":
                return original_post(self)
            if not self._require_loopback():
                return
            try:
                payload = pm._json_body(self)
                active = service.set_active(str(payload.get("language", "") or ""))
                hub = getattr(self.server, "ws_hub", None)
                if hub is not None and hasattr(hub, "broadcast_json"):
                    hub.broadcast_json(None, {"type": "LANGUAGE_UPDATE", "language": active})
                self._write_json(service.payload())
            except (LanguagePluginError, pm.PluginError) as exc:
                self._write_json({"status": "error", "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            except Exception as exc:  # noqa: BLE001
                self._write_json({"status": "error", "message": f"保存语言配置失败：{exc}"}, status=500)

        handler_class.do_GET = do_GET
        handler_class.do_POST = do_POST

        if backup_module is not None:
            files = tuple(getattr(backup_module, "SETTINGS_FILES", ()))
            if LANGUAGE_CONFIG_NAME not in files:
                backup_module.SETTINGS_FILES = files + (LANGUAGE_CONFIG_NAME,)
            backup_service = getattr(backup_module, "SettingsBackupService", None)
            if isinstance(backup_service, type) and not bool(getattr(backup_service, "_bilipdj_language_paths", False)):
                original_paths = backup_service.settings_paths

                def settings_paths(self: Any) -> dict[str, Path]:
                    paths = dict(original_paths(self))
                    paths[LANGUAGE_CONFIG_NAME] = Path(getattr(self.server, "_YAML_DIR")) / LANGUAGE_CONFIG_NAME
                    return paths

                backup_service.settings_paths = settings_paths
                backup_service._bilipdj_language_paths = True

        server_module._language_plugin_system_installed = True
        return True


__all__ = [
    "DEFAULT_LANGUAGE",
    "LANGUAGE_PLUGIN_TYPE",
    "LanguagePluginError",
    "LanguageService",
    "install_language_plugin_system",
]
