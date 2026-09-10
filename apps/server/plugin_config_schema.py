"""Plugin-owned config schema, persistence and runtime bridge (Issue #136)."""
from __future__ import annotations

import copy
import json
import math
import re
from http import HTTPStatus
from typing import Any
from urllib.parse import parse_qs, urlparse

from . import plugin_secret_guard

CONFIG_STORE_SCHEMA = 1
MAX_PLUGIN_CONFIG_BYTES = 64 * 1024
MAX_CONFIG_FIELDS = 64
MAX_ENUM_VALUES = 128
_FIELD_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_SUPPORTED_TYPES = frozenset({"string", "integer", "number", "boolean"})
_ROOT_KEYS = frozenset({"type", "title", "description", "properties", "required", "additionalProperties"})
_FIELD_KEYS = frozenset({
    "type", "title", "description", "default", "enum", "minimum", "maximum",
    "minLength", "maxLength", "secret",
})


def _fail(pm: Any, message: str) -> None:
    raise pm.PluginError(message)


def _check_text(pm: Any, value: Any, label: str, limit: int) -> None:
    if value is not None and (not isinstance(value, str) or len(value) > limit):
        _fail(pm, f"{label} must be a string of at most {limit} characters")


def _value_matches_type(value: Any, kind: str) -> bool:
    if kind == "string":
        return isinstance(value, str)
    if kind == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if kind == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))
    if kind == "boolean":
        return isinstance(value, bool)
    return False


def _validate_value(pm: Any, field: str, spec: dict[str, Any], value: Any) -> None:
    kind = str(spec.get("type", ""))
    if not _value_matches_type(value, kind):
        _fail(pm, f"plugin config field '{field}' must be {kind}")
    enum = spec.get("enum")
    if isinstance(enum, list) and value not in enum:
        _fail(pm, f"plugin config field '{field}' must be one of the declared enum values")
    if kind == "string":
        minimum = spec.get("minLength")
        maximum = spec.get("maxLength")
        if isinstance(minimum, int) and len(value) < minimum:
            _fail(pm, f"plugin config field '{field}' is shorter than minLength={minimum}")
        if isinstance(maximum, int) and len(value) > maximum:
            _fail(pm, f"plugin config field '{field}' is longer than maxLength={maximum}")
    if kind in {"integer", "number"}:
        minimum = spec.get("minimum")
        maximum = spec.get("maximum")
        if isinstance(minimum, (int, float)) and value < minimum:
            _fail(pm, f"plugin config field '{field}' is below minimum={minimum}")
        if isinstance(maximum, (int, float)) and value > maximum:
            _fail(pm, f"plugin config field '{field}' is above maximum={maximum}")


def validate_config_schema(pm: Any, schema: Any, permissions: Any = ()) -> None:
    if not isinstance(schema, dict):
        _fail(pm, "manifest.config_schema must be an object")
    unknown_root = sorted(set(schema) - _ROOT_KEYS)
    if unknown_root:
        _fail(pm, "unsupported config_schema keys: " + ", ".join(unknown_root))
    if schema.get("type") != "object":
        _fail(pm, "config_schema.type must be 'object'")
    _check_text(pm, schema.get("title"), "config_schema.title", 120)
    _check_text(pm, schema.get("description"), "config_schema.description", 1000)
    additional = schema.get("additionalProperties", False)
    if additional is not False:
        _fail(pm, "config_schema.additionalProperties must be false when present")
    properties = schema.get("properties", {})
    if not isinstance(properties, dict):
        _fail(pm, "config_schema.properties must be an object")
    if len(properties) > MAX_CONFIG_FIELDS:
        _fail(pm, f"config_schema cannot declare more than {MAX_CONFIG_FIELDS} fields")
    permission_set = {str(item) for item in permissions if isinstance(item, str)}
    for field, raw_spec in properties.items():
        if not isinstance(field, str) or not _FIELD_RE.fullmatch(field):
            _fail(pm, f"invalid config_schema field name: {field}")
        if not isinstance(raw_spec, dict):
            _fail(pm, f"config_schema.properties.{field} must be an object")
        unknown = sorted(set(raw_spec) - _FIELD_KEYS)
        if unknown:
            _fail(pm, f"unsupported config_schema keys for '{field}': " + ", ".join(unknown))
        kind = str(raw_spec.get("type", ""))
        if kind not in _SUPPORTED_TYPES:
            _fail(pm, f"config_schema field '{field}' has unsupported type: {kind}")
        _check_text(pm, raw_spec.get("title"), f"config_schema.properties.{field}.title", 120)
        _check_text(pm, raw_spec.get("description"), f"config_schema.properties.{field}.description", 1000)
        secret = raw_spec.get("secret", False)
        if not isinstance(secret, bool):
            _fail(pm, f"config_schema field '{field}'.secret must be boolean")
        if secret and kind != "string":
            _fail(pm, f"secret config field '{field}' must use type 'string'")
        if secret and "secrets" not in permission_set:
            _fail(pm, f"secret config field '{field}' requires the 'secrets' permission")
        if secret and "default" in raw_spec:
            _fail(pm, f"secret config field '{field}' cannot declare a default")
        enum = raw_spec.get("enum")
        if enum is not None:
            if not isinstance(enum, list) or not enum or len(enum) > MAX_ENUM_VALUES:
                _fail(pm, f"config_schema field '{field}'.enum must contain 1-{MAX_ENUM_VALUES} values")
            for item in enum:
                if not _value_matches_type(item, kind):
                    _fail(pm, f"config_schema field '{field}'.enum contains a value of the wrong type")
            if len({json.dumps(item, sort_keys=True, ensure_ascii=False) for item in enum}) != len(enum):
                _fail(pm, f"config_schema field '{field}'.enum values must be unique")
        minimum = raw_spec.get("minimum")
        maximum = raw_spec.get("maximum")
        if minimum is not None or maximum is not None:
            if kind not in {"integer", "number"}:
                _fail(pm, f"minimum/maximum are only valid for numeric field '{field}'")
            for label, value in (("minimum", minimum), ("maximum", maximum)):
                if value is not None and (not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(float(value))):
                    _fail(pm, f"config_schema field '{field}'.{label} must be a finite number")
            if minimum is not None and maximum is not None and minimum > maximum:
                _fail(pm, f"config_schema field '{field}' minimum cannot exceed maximum")
        min_length = raw_spec.get("minLength")
        max_length = raw_spec.get("maxLength")
        if min_length is not None or max_length is not None:
            if kind != "string":
                _fail(pm, f"minLength/maxLength are only valid for string field '{field}'")
            for label, value in (("minLength", min_length), ("maxLength", max_length)):
                if value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > 1_000_000):
                    _fail(pm, f"config_schema field '{field}'.{label} must be an integer from 0 to 1000000")
            if min_length is not None and max_length is not None and min_length > max_length:
                _fail(pm, f"config_schema field '{field}' minLength cannot exceed maxLength")
        if "default" in raw_spec:
            _validate_value(pm, field, raw_spec, raw_spec["default"])
    required = schema.get("required", [])
    if not isinstance(required, list) or any(not isinstance(item, str) for item in required):
        _fail(pm, "config_schema.required must be a string array")
    if len(required) != len(set(required)):
        _fail(pm, "config_schema.required values must be unique")
    unknown_required = sorted(set(required) - set(properties))
    if unknown_required:
        _fail(pm, "config_schema.required references unknown fields: " + ", ".join(unknown_required))


def _schema(record: Any) -> dict[str, Any] | None:
    manifest = getattr(record, "manifest", None)
    value = manifest.get("config_schema") if isinstance(manifest, dict) else None
    return value if isinstance(value, dict) else None


def _defaults(schema: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field, spec in dict(schema.get("properties", {}) or {}).items():
        if isinstance(spec, dict) and "default" in spec:
            result[str(field)] = copy.deepcopy(spec["default"])
    return result


def _validate_effective(pm: Any, schema: dict[str, Any], values: dict[str, Any], *, required: bool) -> None:
    properties = dict(schema.get("properties", {}) or {})
    unknown = sorted(set(values) - set(properties))
    if unknown:
        _fail(pm, "unknown plugin config fields: " + ", ".join(unknown))
    for field, value in values.items():
        _validate_value(pm, field, properties[field], value)
    if required:
        missing = [field for field in schema.get("required", []) if field not in values]
        if missing:
            _fail(pm, "required plugin config fields are missing: " + ", ".join(missing))


def install_plugin_config_schema(server_module: Any, pm: Any, security_guard: Any) -> Any:
    """Install config-schema behavior after PluginManager and browser security guards exist."""
    if bool(getattr(pm, "_issue136_config_schema_installed", False)):
        return getattr(server_module, "plugin_manager", None)
    manager = getattr(server_module, "plugin_manager", None)
    registry = getattr(server_module, "danmu_plugin_registry", None)
    if manager is None or registry is None:
        raise RuntimeError("PluginManager must be installed before plugin config schema support")

    original_validate = pm.validate_manifest
    original_context_get_config = pm.PluginContext.get_config
    original_context_get_secret = pm.PluginContext.get_secret
    original_uninstall = pm.PluginManager.uninstall
    original_set_enabled = pm.PluginManager.set_enabled
    original_public_info = pm.InstalledPluginRecord.public_info

    def validate_manifest(manifest: Any, current_version: str) -> dict[str, Any]:
        normalized = original_validate(manifest, current_version)
        if isinstance(manifest, dict) and "config_schema" in manifest:
            validate_config_schema(pm, manifest.get("config_schema"), normalized.get("permissions", []))
            normalized["config_schema"] = copy.deepcopy(manifest["config_schema"])
        return normalized

    def _record(self: Any, plugin_id: str) -> Any:
        key = str(plugin_id or "").strip().lower()
        record = self._records.get(key)
        if record is None:
            _fail(pm, "external plugin is not installed")
        return record

    def _store(self: Any) -> dict[str, dict[str, Any]]:
        path = self.plugins_root / ".plugin-config.json"
        payload = self._load_json_file(path, {"schema": CONFIG_STORE_SCHEMA, "plugins": {}})
        raw = payload.get("plugins", {}) if isinstance(payload, dict) else {}
        result: dict[str, dict[str, Any]] = {}
        if isinstance(raw, dict):
            for key, value in raw.items():
                if isinstance(value, dict):
                    result[str(key)] = copy.deepcopy(value)
        return result

    def _save_store(self: Any, plugins: dict[str, dict[str, Any]]) -> None:
        self._atomic_write_json(
            self.plugins_root / ".plugin-config.json",
            {"schema": CONFIG_STORE_SCHEMA, "plugins": plugins},
        )

    def _effective(self: Any, plugin_id: str, *, require_required: bool = False) -> dict[str, Any]:
        record = _record(self, plugin_id)
        schema = _schema(record)
        if schema is None:
            _fail(pm, "plugin does not declare config_schema")
        properties = dict(schema.get("properties", {}) or {})
        stored = _store(self).get(record.plugin_id, {})
        explicit = {key: copy.deepcopy(value) for key, value in stored.items() if key in properties}
        result = _defaults(schema)
        result.update(explicit)
        _validate_effective(pm, schema, result, required=require_required)
        return result

    def get_plugin_config(self: Any, plugin_id: str, *, public: bool = False) -> dict[str, Any]:
        with self._lock:
            record = _record(self, plugin_id)
            schema = _schema(record)
            if schema is None:
                return {"id": record.plugin_id, "configurable": False, "schema": None, "values": {}, "secret_fields": {}}
            values = _effective(self, record.plugin_id, require_required=False)
            stored = _store(self).get(record.plugin_id, {})
            if not public:
                return copy.deepcopy(values)
            properties = dict(schema.get("properties", {}) or {})
            public_values = {
                key: copy.deepcopy(value)
                for key, value in values.items()
                if not bool(properties.get(key, {}).get("secret", False))
            }
            secret_fields = {
                key: bool(key in stored and stored.get(key) not in (None, ""))
                for key, spec in properties.items()
                if isinstance(spec, dict) and bool(spec.get("secret", False))
            }
            return {
                "id": record.plugin_id,
                "configurable": True,
                "schema": copy.deepcopy(schema),
                "values": public_values,
                "secret_fields": secret_fields,
            }

    def save_plugin_config(self: Any, plugin_id: str, values: Any, unset: Any = None) -> dict[str, Any]:
        if not isinstance(values, dict):
            _fail(pm, "plugin config values must be an object")
        if unset is None:
            unset = []
        if not isinstance(unset, list) or any(not isinstance(item, str) for item in unset):
            _fail(pm, "plugin config unset must be a string array")
        with self._lock:
            record = _record(self, plugin_id)
            schema = _schema(record)
            if schema is None:
                _fail(pm, "plugin does not declare config_schema")
            properties = dict(schema.get("properties", {}) or {})
            unknown = sorted((set(values) | set(unset)) - set(properties))
            if unknown:
                _fail(pm, "unknown plugin config fields: " + ", ".join(unknown))
            store = _store(self)
            explicit = {
                key: copy.deepcopy(value)
                for key, value in store.get(record.plugin_id, {}).items()
                if key in properties
            }
            for field in unset:
                explicit.pop(field, None)
            for field, value in values.items():
                spec = properties[field]
                if bool(spec.get("secret", False)) and value == "" and field in explicit:
                    continue
                _validate_value(pm, field, spec, value)
                explicit[field] = copy.deepcopy(value)
            effective = _defaults(schema)
            effective.update(explicit)
            _validate_effective(pm, schema, effective, required=True)
            encoded = json.dumps(explicit, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            if len(encoded) > MAX_PLUGIN_CONFIG_BYTES:
                _fail(pm, f"plugin config exceeds {MAX_PLUGIN_CONFIG_BYTES} bytes")
            store[record.plugin_id] = explicit
            _save_store(self, store)
            return get_plugin_config(self, record.plugin_id, public=True)

    def context_get_config(self: Any) -> dict[str, Any]:
        if _schema(self._record) is None:
            return original_context_get_config(self)
        value = self._manager.get_plugin_config(self.plugin_id, public=False)
        if "secrets" not in getattr(self, "permissions", frozenset()):
            value = plugin_secret_guard.redact_secrets(value)
            schema = _schema(self._record) or {}
            for field, spec in dict(schema.get("properties", {}) or {}).items():
                if isinstance(spec, dict) and bool(spec.get("secret", False)):
                    value.pop(field, None)
        return value if isinstance(value, dict) else {}

    def context_get_secret(self: Any, key: str) -> str:
        if _schema(self._record) is None:
            return original_context_get_secret(self, key)
        self._require("secrets")
        value = self._manager.get_plugin_config(self.plugin_id, public=False)
        return str(value.get(str(key), "") or "") if isinstance(value, dict) else ""

    def set_enabled(self: Any, plugin_id: str, enabled: bool) -> dict[str, Any]:
        with self._lock:
            record = self._records.get(str(plugin_id or "").strip().lower())
            if enabled and record is not None and _schema(record) is not None:
                self.get_plugin_config(record.plugin_id, public=False)
                _effective(self, record.plugin_id, require_required=True)
            return original_set_enabled(self, plugin_id, enabled)

    def uninstall(self: Any, plugin_id: str) -> None:
        key = str(plugin_id or "").strip().lower()
        with self._lock:
            original_uninstall(self, key)
            store = _store(self)
            if key in store:
                store.pop(key, None)
                _save_store(self, store)

    def public_info(self: Any) -> dict[str, Any]:
        payload = original_public_info(self)
        payload["configurable"] = _schema(self) is not None
        return payload

    pm.validate_manifest = validate_manifest
    pm.PluginManager.get_plugin_config = get_plugin_config
    pm.PluginManager.save_plugin_config = save_plugin_config
    pm.PluginManager.set_enabled = set_enabled
    pm.PluginManager.uninstall = uninstall
    pm.PluginContext.get_config = context_get_config
    pm.PluginContext.get_secret = context_get_secret
    pm.InstalledPluginRecord.public_info = public_info

    try:
        from . import javascript_plugin_runtime as js_runtime
        original_js_secret_config = js_runtime.JavascriptRelay._secret_config

        def js_secret_config(self: Any) -> dict[str, Any]:
            if _schema(self.record) is None:
                return original_js_secret_config(self)
            if "secrets" not in set(self.record.permissions):
                return {}
            value = self.context.get_config()
            return dict(value) if isinstance(value, dict) else {}

        js_runtime.JavascriptRelay._secret_config = js_secret_config
    except Exception:
        pass

    manager.discover()

    handler_class = server_module.ApiHandler
    original_get = handler_class.do_GET
    original_post = handler_class.do_POST

    def do_GET(self: Any) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/api/plugins/config":
            return original_get(self)
        if not self._require_loopback():
            return
        plugin_id = str(parse_qs(parsed.query).get("id", [""])[0] or "").strip().lower()
        try:
            self._write_json({"status": "ok", **manager.get_plugin_config(plugin_id, public=True)})
        except pm.PluginError as exc:
            self._write_json({"status": "error", "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def do_POST(self: Any) -> None:  # noqa: N802
        if urlparse(self.path).path != "/api/plugins/config":
            return original_post(self)
        if not self._require_loopback():
            return
        if not security_guard.require_safe_plugin_management_request(self):
            return
        try:
            payload = pm._json_body(self)
            plugin_id = str(payload.get("id", "") or "").strip().lower()
            result = manager.save_plugin_config(
                plugin_id,
                payload.get("values", {}),
                payload.get("unset", []),
            )
            pm._reconcile_runtime(server_module, self.server, registry)
            self._write_json({"status": "ok", **result})
        except pm.PluginError as exc:
            self._write_json({"status": "error", "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # noqa: BLE001
            logger = getattr(self.server, "logger", None)
            if logger is not None:
                logger.exception("plugin config request failed")
            self._write_json({"status": "error", "message": f"plugin config failed: {exc}"}, status=500)

    do_GET._issue136_plugin_config = True  # type: ignore[attr-defined]
    do_POST._issue136_plugin_config = True  # type: ignore[attr-defined]
    handler_class.do_GET = do_GET
    handler_class.do_POST = do_POST
    pm._issue136_config_schema_installed = True
    server_module._plugin_config_schema_installed = True
    return manager


__all__ = [
    "CONFIG_STORE_SCHEMA", "MAX_PLUGIN_CONFIG_BYTES", "install_plugin_config_schema",
    "validate_config_schema",
]
