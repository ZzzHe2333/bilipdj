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
BUNDLED_LANGUAGES: dict[str, dict[str, str]] = {
    "en-US": {
        "name": "English",
        "plugin_id": "builtin.en-us.language",
        "resource": "languages/en-US.json",
    },
}
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


def _language_config_path(server_module: Any) -> Path:
    return Path(getattr(server_module, "_YAML_DIR")) / LANGUAGE_CONFIG_NAME


def _read_selected_language(server_module: Any) -> str:
    path = _language_config_path(server_module)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return DEFAULT_LANGUAGE
    value = str(raw.get("language", DEFAULT_LANGUAGE) or DEFAULT_LANGUAGE) if isinstance(raw, dict) else DEFAULT_LANGUAGE
    return value if value == DEFAULT_LANGUAGE or _LANGUAGE_RE.fullmatch(value) else DEFAULT_LANGUAGE


def _bundled_language_path(server_module: Any, language: str) -> Path | None:
    spec = BUNDLED_LANGUAGES.get(str(language or ""))
    if not isinstance(spec, dict):
        return None
    resource = str(spec.get("resource", "") or "")
    if not resource:
        return None
    return Path(getattr(server_module, "UI_DIR")) / Path(resource)


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
    reserved_languages = {DEFAULT_LANGUAGE, *BUNDLED_LANGUAGES.keys()}
    if not _LANGUAGE_RE.fullmatch(language) or language in reserved_languages:
        raise pm.PluginError("language must be a canonical BCP-47 code not reserved by a bundled language")
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
        return _language_config_path(self.server)

    def _read_selected(self) -> str:
        return _read_selected_language(self.server)

    def _write_selected(self, language: str) -> None:
        path = self.config_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(f".{path.name}.tmp")
        temp.write_text(json.dumps({"schema": 1, "language": language}, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp, path)

    def _language_records(self, *, enabled_only: bool = False) -> list[Any]:
        records: list[Any] = []
        with getattr(self.manager, "_lock"):
            values = list(getattr(self.manager, "_records", {}).values())
        for record in values:
            manifest = getattr(record, "manifest", {})
            if not isinstance(manifest, dict) or str(manifest.get("type", "")) != LANGUAGE_PLUGIN_TYPE:
                continue
            if not bool(getattr(record, "verified", False)) or str(getattr(record, "error", "") or ""):
                continue
            if enabled_only and not bool(getattr(record, "enabled", False)):
                continue
            records.append(record)
        return records

    def _record_for_language(self, language: str, *, enabled_only: bool = False) -> Any | None:
        for record in self._language_records(enabled_only=enabled_only):
            if str(record.manifest.get("language", "")) == language:
                return record
        return None

    def _bundled_available(self, language: str) -> bool:
        path = _bundled_language_path(self.server, language)
        return bool(path is not None and path.is_file())

    def list_languages(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = [{
            "code": DEFAULT_LANGUAGE,
            "name": "简体中文",
            "plugin_id": "builtin.zh-cn.language",
            "source": "builtin",
        }]
        for code, spec in BUNDLED_LANGUAGES.items():
            if not self._bundled_available(code):
                continue
            rows.append({
                "code": code,
                "name": str(spec.get("name", code) or code),
                "plugin_id": str(spec.get("plugin_id", f"builtin.{code.lower()}.language")),
                "source": "builtin",
            })
        seen = {str(item["code"]) for item in rows}
        for record in sorted(self._language_records(), key=lambda item: str(item.manifest.get("language", ""))):
            code = str(record.manifest.get("language", ""))
            if not code or code in seen:
                continue
            seen.add(code)
            rows.append({
                "code": code,
                "name": str(record.manifest.get("language_name", record.name) or record.name),
                "plugin_id": record.plugin_id,
                "source": "external",
            })
        return rows

    def active_language(self) -> str:
        selected = self._read_selected()
        if selected == DEFAULT_LANGUAGE:
            return DEFAULT_LANGUAGE
        if selected in BUNDLED_LANGUAGES:
            return selected if self._bundled_available(selected) else DEFAULT_LANGUAGE
        return selected if self._record_for_language(selected, enabled_only=True) is not None else DEFAULT_LANGUAGE

    def translations(self, language: str | None = None) -> dict[str, str]:
        selected = str(language or self.active_language())
        if selected == DEFAULT_LANGUAGE:
            return {}
        plugin_id = ""
        path: Path | None = None
        if selected in BUNDLED_LANGUAGES:
            spec = BUNDLED_LANGUAGES[selected]
            plugin_id = str(spec.get("plugin_id", selected))
            path = _bundled_language_path(self.server, selected)
        else:
            record = self._record_for_language(selected, enabled_only=True)
            if record is not None:
                plugin_id = record.plugin_id
                relative = str(record.manifest.get("translations", "") or "")
                path = record.root.joinpath(*PurePosixPath(relative).parts)
        if path is None:
            return {}
        try:
            stamp = int(path.stat().st_mtime_ns)
        except OSError as exc:
            raise LanguagePluginError(f"语言包翻译文件不存在：{selected}") from exc
        cache_key = (plugin_id or selected, stamp)
        with self._lock:
            cached = self._translation_cache.get(cache_key)
            if cached is not None:
                return dict(cached)
        mapping = _load_translation_mapping(path)
        with self._lock:
            self._translation_cache = {cache_key: dict(mapping)}
        return mapping

    def _sync_external_language_state(self, language: str) -> None:
        with getattr(self.manager, "_lock"):
            state = self.manager._state()
            changed = False
            for record in list(getattr(self.manager, "_records", {}).values()):
                manifest = getattr(record, "manifest", {})
                if not isinstance(manifest, dict) or str(manifest.get("type", "")) != LANGUAGE_PLUGIN_TYPE:
                    continue
                desired = (
                    bool(getattr(record, "verified", False))
                    and not str(getattr(record, "error", "") or "")
                    and str(manifest.get("language", "")) == language
                )
                if bool(state.get(record.plugin_id, False)) != desired:
                    state[record.plugin_id] = desired
                    changed = True
            if changed:
                self.manager._save_state(state)
            self.manager.discover()

    def set_active(self, language: str) -> str:
        value = str(language or "").strip()
        allowed = {str(item["code"]) for item in self.list_languages()}
        if value not in allowed:
            raise LanguagePluginError("该语言未安装、未打包或语言包校验失败")
        previous = self.active_language()
        self._write_selected(value)
        try:
            self._sync_external_language_state(value)
            active = self.active_language()
            if active != value:
                raise LanguagePluginError("语言切换后未形成唯一活动语言")
            return active
        except Exception:
            self._write_selected(previous)
            try:
                self._sync_external_language_state(previous)
            except Exception:
                pass
            raise

    def reconcile(self) -> str:
        selected = self._read_selected()
        active = self.active_language()
        if selected != active:
            self._write_selected(active)
        self._sync_external_language_state(active)
        return self.active_language()

    def payload(self) -> dict[str, Any]:
        active = self.active_language()
        languages = []
        for item in self.list_languages():
            row = dict(item)
            row["enabled"] = str(row.get("code", "")) == active
            languages.append(row)
        return {
            "status": "ok",
            "default": DEFAULT_LANGUAGE,
            "active": active,
            "single_active": True,
            "languages": languages,
            "translations": self.translations(active),
        }

    def bundled_plugin_public_info(self) -> list[dict[str, Any]]:
        active = self.active_language()
        rows = [{
            "id": "builtin.zh-cn.language",
            "platform": "language",
            "name": "简体中文",
            "version": "builtin",
            "plugin_api": 1,
            "type": LANGUAGE_PLUGIN_TYPE,
            "source": "builtin",
            "enabled": active == DEFAULT_LANGUAGE,
            "available": True,
            "permissions": [],
            "capabilities": ["ui_translation"],
            "package_sha256": "",
            "signature_status": "builtin",
            "verified": True,
            "error": "",
            "runtime": LANGUAGE_RUNTIME,
            "permission_enforcement": "resource-only",
            "category": "language",
            "language": DEFAULT_LANGUAGE,
            "language_name": "简体中文",
        }]
        for code, spec in BUNDLED_LANGUAGES.items():
            available = self._bundled_available(code)
            rows.append({
                "id": str(spec.get("plugin_id", f"builtin.{code.lower()}.language")),
                "platform": "language",
                "name": str(spec.get("name", code) or code),
                "version": "builtin",
                "plugin_api": 1,
                "type": LANGUAGE_PLUGIN_TYPE,
                "source": "builtin",
                "enabled": bool(available and active == code),
                "available": available,
                "permissions": [],
                "capabilities": ["ui_translation"],
                "package_sha256": "",
                "signature_status": "builtin",
                "verified": available,
                "error": "" if available else "bundled language resource is missing",
                "runtime": LANGUAGE_RUNTIME,
                "permission_enforcement": "resource-only",
                "category": "language",
                "language": code,
                "language_name": str(spec.get("name", code) or code),
            })
        return rows


def install_language_plugin_system(server_module: Any, pm: Any, backup_module: Any | None = None) -> bool:
    with _PATCH_LOCK:
        if bool(getattr(server_module, "_language_plugin_system_installed", False)):
            return True
        manager = getattr(server_module, "plugin_manager", None)
        if manager is None:
            raise RuntimeError("PluginManager must be installed before language plugins")

        original_validate = pm.validate_manifest
        original_public_info = pm.InstalledPluginRecord.public_info
        original_set_enabled = pm.PluginManager.set_enabled
        original_list_public = pm.PluginManager.list_public

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
                selected_language = _read_selected_language(self.server_module)
                records: dict[str, Any] = {}
                state_changed = False
                seen_language_codes: dict[str, str] = {}
                for root in sorted(self.plugins_root.iterdir()):
                    if not root.is_dir() or root.name.startswith(".") or root.name == "data":
                        continue
                    manifest_path = root / "manifest.json"
                    meta_path = root / ".install.json"
                    if not manifest_path.is_file() or not meta_path.is_file():
                        continue
                    plugin_id = root.name
                    raw_manifest: dict[str, Any] = {}
                    try:
                        loaded_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                        raw_manifest = loaded_manifest if isinstance(loaded_manifest, dict) else {}
                        manifest = pm.validate_manifest(loaded_manifest, self.current_version)
                        if manifest["id"] != plugin_id:
                            raise pm.PluginError("installed directory does not match plugin id")
                        meta = json.loads(meta_path.read_text(encoding="utf-8"))
                        package_hash, signature_status = self._verify_installed(root, manifest, meta)
                        plugin_type = str(manifest.get("type", "") or "")
                        enabled = bool(state.get(plugin_id, False))
                        if plugin_type == LANGUAGE_PLUGIN_TYPE:
                            code = str(manifest.get("language", ""))
                            previous_provider = seen_language_codes.get(code)
                            if previous_provider is not None:
                                raise pm.PluginError(f"language {code} already provided by {previous_provider}")
                            seen_language_codes[code] = plugin_id
                            enabled = selected_language == code
                            if bool(state.get(plugin_id, False)) != enabled:
                                state[plugin_id] = enabled
                                state_changed = True
                            if enabled:
                                _load_translation_mapping(
                                    root.joinpath(*PurePosixPath(str(manifest["translations"])).parts)
                                )
                        record = pm.InstalledPluginRecord(
                            plugin_id=plugin_id,
                            root=root,
                            manifest=manifest,
                            enabled=enabled,
                            package_sha256=package_hash,
                            signature_status=signature_status,
                            verified=True,
                        )
                        if record.enabled and plugin_type != LANGUAGE_PLUGIN_TYPE:
                            existing = self.registry.get(record.platform)
                            if existing is not None:
                                raise pm.PluginError(f"platform already registered by {existing.plugin_id}")
                            self.registry.register(self._load_external_plugin(record))
                    except Exception as exc:
                        if str(raw_manifest.get("type", "") or "") == LANGUAGE_PLUGIN_TYPE and bool(state.get(plugin_id, False)):
                            state[plugin_id] = False
                            state_changed = True
                        if not raw_manifest:
                            try:
                                fallback = json.loads(manifest_path.read_text(encoding="utf-8"))
                                raw_manifest = fallback if isinstance(fallback, dict) else {}
                            except Exception:
                                raw_manifest = {}
                        if not raw_manifest:
                            raw_manifest = {"id": plugin_id, "platform": "", "name": plugin_id, "version": ""}
                        record = pm.InstalledPluginRecord(
                            plugin_id=plugin_id,
                            root=root,
                            manifest=raw_manifest,
                            enabled=False if str(raw_manifest.get("type", "")) == LANGUAGE_PLUGIN_TYPE else bool(state.get(plugin_id, False)),
                            package_sha256="",
                            signature_status="error",
                            verified=False,
                            error=str(exc),
                        )
                    records[plugin_id] = record
                if state_changed:
                    self._save_state(state)
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

        def list_public(self: Any) -> list[dict[str, Any]]:
            rows = list(original_list_public(self))
            existing_ids = {str(item.get("id", "")) for item in rows if isinstance(item, dict)}
            for item in service.bundled_plugin_public_info():
                if str(item.get("id", "")) not in existing_ids:
                    rows.append(item)
            return rows

        def set_enabled(self: Any, plugin_id: str, enabled: bool) -> dict[str, Any]:
            key = str(plugin_id or "").strip().lower()
            with self._lock:
                record = self._records.get(key)
                manifest = getattr(record, "manifest", {}) if record is not None else {}
                if isinstance(manifest, dict) and str(manifest.get("type", "")) == LANGUAGE_PLUGIN_TYPE:
                    code = str(manifest.get("language", "") or "")
                    if enabled:
                        service.set_active(code)
                    elif service.active_language() == code:
                        service.set_active(DEFAULT_LANGUAGE)
                    else:
                        state = self._state()
                        if bool(state.get(key, False)):
                            state[key] = False
                            self._save_state(state)
                            self.discover()
                    refreshed = self._records.get(key)
                    if refreshed is None:
                        raise pm.PluginError("plugin disappeared during reload")
                    return refreshed.public_info()
            return original_set_enabled(self, plugin_id, enabled)

        pm.PluginManager.list_public = list_public
        pm.PluginManager.set_enabled = set_enabled
        service.reconcile()

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
    "BUNDLED_LANGUAGES",
    "DEFAULT_LANGUAGE",
    "LANGUAGE_PLUGIN_TYPE",
    "LanguagePluginError",
    "LanguageService",
    "install_language_plugin_system",
]
