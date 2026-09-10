"""External .bilipdj-plugin package manager for BiliPDJ (Issue #130)."""
from __future__ import annotations

import base64
import binascii
import copy
import hashlib
import importlib.util
import json
import logging
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import types
import urllib.request
import zipfile
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

from . import danmu_plugins as _plugin_core
from .danmu_event import DanmuEvent
from .danmu_plugins import PLUGIN_API_VERSION, PLUGIN_TYPE, DanmuPlugin, DanmuPluginRegistry

MANIFEST_SCHEMA = 1
PACKAGE_SUFFIX = ".bilipdj-plugin"
MAX_PACKAGE_BYTES = 1024 * 1024
MAX_UNPACKED_BYTES = 8 * 1024 * 1024
MAX_MEMBER_BYTES = 2 * 1024 * 1024
MAX_ARCHIVE_FILES = 128
MAX_COMPRESSION_RATIO = 100
SUPPORTED_PERMISSIONS = frozenset(
    {"network", "filesystem_read", "filesystem_write", "subprocess", "secrets"}
)
SENSITIVE_KEYS = frozenset(
    {"cookie", "auth_token", "token", "password", "passwd", "secret", "api_key", "access_token"}
)
_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,79}$")
_PLATFORM_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,39}$")
_ENTRY_RE = re.compile(r"^[A-Za-z0-9_./-]+\.py:[A-Za-z_][A-Za-z0-9_]*$")
_VERSION_RE = re.compile(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:[-+.]([0-9A-Za-z.-]+))?$")
_PATCH_LOCK = threading.RLock()


class PluginError(RuntimeError):
    pass


@dataclass
class InstalledPluginRecord:
    plugin_id: str
    root: Path
    manifest: dict[str, Any]
    enabled: bool
    package_sha256: str
    signature_status: str
    verified: bool = False
    error: str = ""

    @property
    def platform(self) -> str:
        return str(self.manifest.get("platform", "") or "").strip().lower()

    @property
    def name(self) -> str:
        return str(self.manifest.get("name", self.plugin_id) or self.plugin_id)

    @property
    def version(self) -> str:
        return str(self.manifest.get("version", "") or "")

    @property
    def permissions(self) -> tuple[str, ...]:
        raw = self.manifest.get("permissions", [])
        return tuple(str(item) for item in raw) if isinstance(raw, list) else ()

    def public_info(self) -> dict[str, Any]:
        return {
            "id": self.plugin_id,
            "platform": self.platform,
            "name": self.name,
            "version": self.version,
            "plugin_api": int(self.manifest.get("plugin_api", 0) or 0),
            "type": str(self.manifest.get("type", "") or ""),
            "source": "external",
            "enabled": bool(self.enabled),
            "available": bool(self.enabled and self.verified and not self.error),
            "permissions": list(self.permissions),
            "capabilities": list(self.manifest.get("capabilities", []) or []),
            "package_sha256": self.package_sha256,
            "signature_status": self.signature_status,
            "verified": bool(self.verified),
            "error": self.error,
        }


def _read_version(server_module: Any) -> str:
    for path in (
        Path(getattr(server_module, "BUNDLE_DIR", ".")) / "VERSION",
        Path(getattr(server_module, "APP_DIR", ".")) / "VERSION",
        Path(getattr(server_module, "REPO_DIR", ".")) / "VERSION",
    ):
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if value:
            return value
    return "0.0.0"


def _version_key(value: str) -> tuple[int, int, int, int, str]:
    match = _VERSION_RE.fullmatch(str(value or "").strip())
    if not match:
        raise PluginError(f"invalid version: {value}")
    major, minor, patch = (int(match.group(i) or 0) for i in (1, 2, 3))
    suffix = str(match.group(4) or "")
    return major, minor, patch, 1 if not suffix else 0, suffix


def _canonical_manifest(manifest: dict[str, Any]) -> bytes:
    payload = copy.deepcopy(manifest)
    payload.pop("signature", None)
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _safe_member_name(name: str) -> str:
    raw = str(name or "").replace("\\", "/")
    if not raw or raw.startswith("/") or "\x00" in raw:
        raise PluginError("archive contains an invalid absolute/empty path")
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise PluginError(f"archive contains unsafe path: {raw}")
    if len(path.parts) > 12:
        raise PluginError(f"archive path is too deep: {raw}")
    if len(raw) > 240:
        raise PluginError("archive path is too long")
    return path.as_posix()


def _is_zip_symlink(info: zipfile.ZipInfo) -> bool:
    mode = (int(info.external_attr) >> 16) & 0xFFFF
    return stat.S_ISLNK(mode)


def _normalize_files_mapping(manifest: dict[str, Any]) -> dict[str, str]:
    raw = manifest.get("files")
    if not isinstance(raw, dict) or not raw:
        raise PluginError("manifest.files is required and must contain SHA-256 digests")
    result: dict[str, str] = {}
    for key, value in raw.items():
        name = _safe_member_name(str(key))
        digest = str(value or "").strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise PluginError(f"invalid SHA-256 for {name}")
        if name == "manifest.json" or name.startswith("."):
            raise PluginError(f"manifest.files cannot include reserved file: {name}")
        if name in result:
            raise PluginError(f"duplicate manifest file: {name}")
        result[name] = digest
    return result


def validate_manifest(manifest: Any, current_version: str) -> dict[str, Any]:
    if not isinstance(manifest, dict):
        raise PluginError("manifest.json must be a JSON object")
    required = (
        "schema", "id", "name", "version", "plugin_api", "type", "platform",
        "entry", "min_bilipdj_version", "permissions", "capabilities", "files",
    )
    missing = [key for key in required if key not in manifest]
    if missing:
        raise PluginError("manifest missing fields: " + ", ".join(missing))
    if int(manifest.get("schema", 0) or 0) != MANIFEST_SCHEMA:
        raise PluginError(f"unsupported manifest schema: {manifest.get('schema')}")
    plugin_id = str(manifest.get("id", "") or "").strip().lower()
    platform = str(manifest.get("platform", "") or "").strip().lower()
    if not _ID_RE.fullmatch(plugin_id) or plugin_id.startswith("builtin."):
        raise PluginError("invalid/reserved plugin id")
    if not _PLATFORM_RE.fullmatch(platform):
        raise PluginError("invalid platform id")
    if int(manifest.get("plugin_api", 0) or 0) != PLUGIN_API_VERSION:
        raise PluginError(
            f"incompatible plugin_api={manifest.get('plugin_api')}; BiliPDJ supports {PLUGIN_API_VERSION}"
        )
    if str(manifest.get("type", "") or "") != PLUGIN_TYPE:
        raise PluginError(f"unsupported plugin type: {manifest.get('type')}")
    if not str(manifest.get("name", "") or "").strip():
        raise PluginError("plugin name is required")
    _version_key(str(manifest.get("version", "") or ""))
    min_version = str(manifest.get("min_bilipdj_version", "") or "").strip()
    max_version = str(manifest.get("max_bilipdj_version", "") or "").strip()
    if _version_key(current_version) < _version_key(min_version):
        raise PluginError(f"plugin requires BiliPDJ >= {min_version}; current {current_version}")
    if max_version and _version_key(current_version) > _version_key(max_version):
        raise PluginError(f"plugin requires BiliPDJ <= {max_version}; current {current_version}")
    entry = str(manifest.get("entry", "") or "").strip()
    if not _ENTRY_RE.fullmatch(entry):
        raise PluginError("entry must look like path/to/plugin.py:Plugin")
    entry_path = _safe_member_name(entry.split(":", 1)[0])
    files = _normalize_files_mapping(manifest)
    if entry_path not in files:
        raise PluginError("entry module must be included in manifest.files")
    permissions = manifest.get("permissions")
    if not isinstance(permissions, list) or any(not isinstance(item, str) for item in permissions):
        raise PluginError("permissions must be a string array")
    permission_set = {item.strip() for item in permissions if item.strip()}
    unknown = sorted(permission_set - SUPPORTED_PERMISSIONS)
    if unknown:
        raise PluginError("unknown permissions: " + ", ".join(unknown))
    if len(permission_set) != len(permissions):
        raise PluginError("permissions must be unique and non-empty")
    capabilities = manifest.get("capabilities")
    if not isinstance(capabilities, list) or any(not isinstance(item, str) for item in capabilities):
        raise PluginError("capabilities must be a string array")
    normalized = copy.deepcopy(manifest)
    normalized["id"] = plugin_id
    normalized["platform"] = platform
    normalized["entry"] = entry
    normalized["permissions"] = sorted(permission_set)
    normalized["files"] = files
    return normalized


class PluginContext:
    """Capability object passed to external plugins; this is not an OS sandbox."""

    def __init__(self, manager: "PluginManager", active_server: Any, record: InstalledPluginRecord) -> None:
        self._manager = manager
        self._server = active_server
        self._record = record
        self.plugin_id = record.plugin_id
        self.platform = record.platform
        self.permissions = frozenset(record.permissions)
        base_logger = getattr(active_server, "logger", logging.getLogger("bilipdj.plugin"))
        self.logger = logging.LoggerAdapter(base_logger, {"plugin": self.plugin_id})

    def _require(self, permission: str) -> None:
        if permission not in self.permissions:
            raise PermissionError(f"plugin permission not granted: {permission}")

    def get_config(self) -> dict[str, Any]:
        runtime = getattr(self._server, "runtime_config", {})
        section = runtime.get(self.platform, {}) if isinstance(runtime, dict) else {}
        result = copy.deepcopy(section) if isinstance(section, dict) else {}
        if "secrets" not in self.permissions:
            for key in list(result):
                if str(key).lower() in SENSITIVE_KEYS:
                    result.pop(key, None)
        return result

    def get_secret(self, key: str) -> str:
        self._require("secrets")
        runtime = getattr(self._server, "runtime_config", {})
        section = runtime.get(self.platform, {}) if isinstance(runtime, dict) else {}
        return str(section.get(str(key), "") or "") if isinstance(section, dict) else ""

    def emit(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            raise TypeError("payload must be a dict")
        hub = getattr(self._server, "ws_hub", None)
        if hub is not None and hasattr(hub, "broadcast_json"):
            hub.broadcast_json(None, dict(payload))

    def process_danmu_event(self, event: DanmuEvent | dict[str, Any]) -> None:
        if isinstance(event, DanmuEvent):
            normalized = event
        elif isinstance(event, dict):
            normalized = DanmuEvent.from_mapping(event, default_platform=self.platform)
        else:
            raise TypeError("event must be a DanmuEvent or dict")
        if normalized.platform != self.platform:
            raise ValueError("plugin cannot emit a danmu event for another platform")
        manager = getattr(self._server, "queue_manager", None)
        if manager is None or not hasattr(manager, "process_danmu_event"):
            raise RuntimeError("queue manager is unavailable")
        manager.process_danmu_event(normalized)

    def process_danmu_json(self, payload: dict[str, Any]) -> None:
        """Deprecated Bilibili-shaped compatibility API; use process_danmu_event()."""
        if not isinstance(payload, dict):
            raise TypeError("payload must be a dict")
        manager = getattr(self._server, "queue_manager", None)
        if manager is None or not hasattr(manager, "process_danmu_json"):
            raise RuntimeError("queue manager is unavailable")
        manager.process_danmu_json(dict(payload))

    def http_request(self, url: str, *, timeout: float = 10.0, headers: dict[str, str] | None = None) -> bytes:
        self._require("network")
        parsed = urlparse(str(url or ""))
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("only http/https URLs are allowed")
        request = urllib.request.Request(url, headers=dict(headers or {}))
        with urllib.request.urlopen(request, timeout=max(1.0, min(30.0, float(timeout)))) as response:
            return response.read(4 * 1024 * 1024 + 1)[: 4 * 1024 * 1024]

    def _data_path(self, relative: str) -> Path:
        safe = _safe_member_name(relative)
        base = self._manager.data_root / self.plugin_id
        target = (base / Path(*PurePosixPath(safe).parts)).resolve()
        if base.resolve() not in target.parents and target != base.resolve():
            raise PermissionError("path escapes plugin data directory")
        return target

    def read_data(self, relative: str) -> bytes:
        self._require("filesystem_read")
        return self._data_path(relative).read_bytes()

    def write_data(self, relative: str, data: bytes) -> None:
        self._require("filesystem_write")
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data must be bytes")
        target = self._data_path(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(bytes(data))

    def run_process(self, argv: list[str], *, timeout: float = 15.0) -> subprocess.CompletedProcess[str]:
        self._require("subprocess")
        if not isinstance(argv, list) or not argv or any(not isinstance(item, str) or not item for item in argv):
            raise ValueError("argv must be a non-empty string array")
        return subprocess.run(
            argv, shell=False, check=False, capture_output=True, text=True,
            timeout=max(1.0, min(60.0, float(timeout))),
        )


class ManagedExternalRelay:
    def __init__(self, relay: Any, record: InstalledPluginRecord) -> None:
        self._relay = relay
        self._record = record
        self.platform = record.platform

    def start(self) -> Any:
        return self._relay.start()

    def stop(self) -> Any:
        callback = getattr(self._relay, "stop", None)
        return callback() if callable(callback) else None

    def join(self, timeout: float | None = None) -> Any:
        callback = getattr(self._relay, "join", None)
        return callback(timeout=timeout) if callable(callback) else None

    def request_reconnect(self) -> Any:
        callback = getattr(self._relay, "request_reconnect", None)
        return callback() if callable(callback) else None

    def get_runtime_status(self) -> dict[str, Any]:
        callback = getattr(self._relay, "get_runtime_status", None)
        value = callback() if callable(callback) else {}
        result = dict(value) if isinstance(value, dict) else {}
        result.setdefault("platform", self.platform)
        result.setdefault("plugin_id", self._record.plugin_id)
        return result


class PluginManager:
    def __init__(self, server_module: Any, registry: DanmuPluginRegistry, issue79_module: Any = None) -> None:
        self.server_module = server_module
        self.registry = registry
        self.issue79_module = issue79_module
        self.plugins_root = Path(getattr(server_module, "APP_DIR", ".")) / "plugins"
        self.data_root = self.plugins_root / "data"
        self.state_path = self.plugins_root / "state.json"
        self.trusted_keys_path = self.plugins_root / "trusted_keys.json"
        self.current_version = _read_version(server_module)
        self._records: dict[str, InstalledPluginRecord] = {}
        self._loaded_modules: dict[str, set[str]] = {}
        self._lock = threading.RLock()
        self.plugins_root.mkdir(parents=True, exist_ok=True)
        self.data_root.mkdir(parents=True, exist_ok=True)

    def _load_json_file(self, path: Path, default: Any) -> Any:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return copy.deepcopy(default)

    def _atomic_write_json(self, path: Path, value: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, path)

    def _state(self) -> dict[str, bool]:
        payload = self._load_json_file(self.state_path, {"schema": 1, "enabled": {}})
        raw = payload.get("enabled", {}) if isinstance(payload, dict) else {}
        return {str(k): bool(v) for k, v in raw.items()} if isinstance(raw, dict) else {}

    def _save_state(self, state: dict[str, bool]) -> None:
        self._atomic_write_json(self.state_path, {"schema": 1, "enabled": state})

    def list_trusted_keys(self) -> list[dict[str, str]]:
        payload = self._load_json_file(self.trusted_keys_path, {"schema": 1, "keys": {}})
        keys = payload.get("keys", {}) if isinstance(payload, dict) else {}
        if not isinstance(keys, dict):
            return []
        return [{"key_id": str(key_id), "algorithm": "ed25519"} for key_id in sorted(keys)]

    def _trusted_key_bytes(self, key_id: str) -> bytes | None:
        payload = self._load_json_file(self.trusted_keys_path, {"schema": 1, "keys": {}})
        keys = payload.get("keys", {}) if isinstance(payload, dict) else {}
        value = keys.get(key_id) if isinstance(keys, dict) else None
        if not isinstance(value, str):
            return None
        try:
            raw = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError):
            return None
        return raw if len(raw) == 32 else None

    def add_trusted_key(self, key_id: str, public_key_b64: str) -> None:
        key_id = str(key_id or "").strip()
        if not _ID_RE.fullmatch(key_id.lower()):
            raise PluginError("invalid trusted key id")
        try:
            raw = base64.b64decode(str(public_key_b64 or ""), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise PluginError("public key must be base64") from exc
        if len(raw) != 32:
            raise PluginError("Ed25519 public key must be exactly 32 bytes")
        payload = self._load_json_file(self.trusted_keys_path, {"schema": 1, "keys": {}})
        keys = dict(payload.get("keys", {}) or {}) if isinstance(payload, dict) else {}
        keys[key_id] = base64.b64encode(raw).decode("ascii")
        self._atomic_write_json(self.trusted_keys_path, {"schema": 1, "keys": keys})

    def remove_trusted_key(self, key_id: str) -> None:
        payload = self._load_json_file(self.trusted_keys_path, {"schema": 1, "keys": {}})
        keys = dict(payload.get("keys", {}) or {}) if isinstance(payload, dict) else {}
        keys.pop(str(key_id), None)
        self._atomic_write_json(self.trusted_keys_path, {"schema": 1, "keys": keys})

    def _verify_signature(self, manifest: dict[str, Any], *, allow_unsigned: bool) -> str:
        signature = manifest.get("signature")
        if signature in (None, {}, ""):
            if allow_unsigned:
                return "unsigned-approved"
            raise PluginError("plugin is unsigned; explicit allow_unsigned approval is required")
        if not isinstance(signature, dict):
            raise PluginError("manifest.signature must be an object")
        algorithm = str(signature.get("algorithm", "") or "").strip().lower()
        key_id = str(signature.get("key_id", "") or "").strip()
        encoded = str(signature.get("value", "") or "").strip()
        if algorithm != "ed25519" or not key_id or not encoded:
            raise PluginError("signature must use ed25519 with key_id and value")
        public_bytes = self._trusted_key_bytes(key_id)
        if public_bytes is None:
            raise PluginError(f"signature key is not trusted: {key_id}")
        try:
            signature_bytes = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise PluginError("signature value is not valid base64") from exc
        try:
            from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
            Ed25519PublicKey.from_public_bytes(public_bytes).verify(
                signature_bytes, _canonical_manifest(manifest)
            )
        except ImportError as exc:
            raise PluginError("cryptography package is required for signature verification") from exc
        except Exception as exc:  # noqa: BLE001
            raise PluginError("plugin signature verification failed") from exc
        return f"verified:{key_id}"

    def _read_package(self, data: bytes, *, allow_unsigned: bool) -> tuple[dict[str, Any], str, str, dict[str, bytes]]:
        if not isinstance(data, (bytes, bytearray)) or not data:
            raise PluginError("empty plugin package")
        package = bytes(data)
        if len(package) > MAX_PACKAGE_BYTES:
            raise PluginError(f"plugin package exceeds {MAX_PACKAGE_BYTES} bytes")
        package_hash = _sha256_bytes(package)
        files: dict[str, bytes] = {}
        try:
            from io import BytesIO
            archive = zipfile.ZipFile(BytesIO(package), "r")
        except (zipfile.BadZipFile, OSError) as exc:
            raise PluginError("invalid .bilipdj-plugin ZIP package") from exc
        with archive:
            infos = archive.infolist()
            if len(infos) > MAX_ARCHIVE_FILES:
                raise PluginError("plugin archive contains too many files")
            seen: set[str] = set()
            total = 0
            for info in infos:
                if info.is_dir():
                    continue
                name = _safe_member_name(info.filename)
                if name in seen:
                    raise PluginError(f"duplicate archive member: {name}")
                seen.add(name)
                if info.flag_bits & 0x1:
                    raise PluginError("encrypted ZIP members are not supported")
                if _is_zip_symlink(info):
                    raise PluginError(f"symbolic links are forbidden: {name}")
                if info.file_size > MAX_MEMBER_BYTES:
                    raise PluginError(f"archive member too large: {name}")
                total += int(info.file_size)
                if total > MAX_UNPACKED_BYTES:
                    raise PluginError("plugin archive expands beyond safety limit")
                if info.compress_size and info.file_size / max(1, info.compress_size) > MAX_COMPRESSION_RATIO:
                    raise PluginError(f"archive member compression ratio is unsafe: {name}")
                content = archive.read(info)
                if len(content) != info.file_size:
                    raise PluginError(f"archive member size mismatch: {name}")
                files[name] = content
        manifest_bytes = files.get("manifest.json")
        if manifest_bytes is None:
            raise PluginError("manifest.json is missing")
        try:
            raw_manifest = json.loads(manifest_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PluginError("manifest.json is invalid UTF-8 JSON") from exc
        manifest = validate_manifest(raw_manifest, self.current_version)
        expected_files = _normalize_files_mapping(manifest)
        actual_names = {name for name in files if name != "manifest.json"}
        if actual_names != set(expected_files):
            missing = sorted(set(expected_files) - actual_names)
            extra = sorted(actual_names - set(expected_files))
            detail = []
            if missing:
                detail.append("missing=" + ",".join(missing))
            if extra:
                detail.append("extra=" + ",".join(extra))
            raise PluginError("archive file list does not match manifest.files: " + " ".join(detail))
        for name, expected in expected_files.items():
            if _sha256_bytes(files[name]) != expected:
                raise PluginError(f"SHA-256 mismatch for {name}")
        signature_status = self._verify_signature(manifest, allow_unsigned=allow_unsigned)
        return manifest, package_hash, signature_status, files

    def install_bytes(self, data: bytes, *, filename: str = "plugin.bilipdj-plugin", allow_unsigned: bool = False) -> dict[str, Any]:
        if not str(filename or "").lower().endswith(PACKAGE_SUFFIX):
            raise PluginError(f"plugin filename must end with {PACKAGE_SUFFIX}")
        manifest, package_hash, signature_status, files = self._read_package(data, allow_unsigned=allow_unsigned)
        plugin_id = str(manifest["id"])
        platform = str(manifest["platform"])
        with self._lock:
            builtin = self.registry.get(platform)
            if builtin is not None and builtin.source == "builtin":
                raise PluginError(f"external plugin cannot replace built-in platform: {platform}")
            final_root = self.plugins_root / plugin_id
            stage = Path(tempfile.mkdtemp(prefix=".install-", dir=self.plugins_root))
            backup: Path | None = None
            try:
                for name, content in files.items():
                    target = stage.joinpath(*PurePosixPath(name).parts)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(content)
                (stage / ".package.bilipdj-plugin").write_bytes(bytes(data))
                meta = {
                    "schema": 1,
                    "package_sha256": package_hash,
                    "signature_status": signature_status,
                    "file_hashes": dict(manifest["files"]),
                }
                self._atomic_write_json(stage / ".install.json", meta)
                if final_root.exists():
                    backup = self.plugins_root / f".backup-{plugin_id}"
                    if backup.exists():
                        shutil.rmtree(backup, ignore_errors=True)
                    os.replace(final_root, backup)
                os.replace(stage, final_root)
                if backup is not None:
                    shutil.rmtree(backup, ignore_errors=True)
            except Exception:
                shutil.rmtree(stage, ignore_errors=True)
                if backup is not None and backup.exists() and not final_root.exists():
                    os.replace(backup, final_root)
                raise
            state = self._state()
            state[plugin_id] = False
            self._save_state(state)
            self.discover()
            return self._records[plugin_id].public_info()

    def _verify_installed(self, root: Path, manifest: dict[str, Any], meta: dict[str, Any]) -> tuple[str, str]:
        package_path = root / ".package.bilipdj-plugin"
        expected_package = str(meta.get("package_sha256", "") or "")
        if not package_path.is_file() or _sha256_file(package_path) != expected_package:
            raise PluginError("installed package snapshot SHA-256 mismatch")
        file_hashes = _normalize_files_mapping(manifest)
        for name, expected in file_hashes.items():
            target = root.joinpath(*PurePosixPath(name).parts)
            if not target.is_file() or _sha256_file(target) != expected:
                raise PluginError(f"installed file integrity check failed: {name}")
        signature = manifest.get("signature")
        if signature:
            signature_status = self._verify_signature(manifest, allow_unsigned=False)
        else:
            signature_status = str(meta.get("signature_status", "") or "")
            if signature_status != "unsigned-approved":
                raise PluginError("unsigned plugin lacks recorded explicit approval")
        return expected_package, signature_status

    def _cleanup_modules(self, plugin_id: str) -> None:
        names = self._loaded_modules.pop(plugin_id, set())
        for name in names:
            sys.modules.pop(name, None)

    def _load_external_plugin(self, record: InstalledPluginRecord) -> DanmuPlugin:
        entry_path, symbol = str(record.manifest["entry"]).split(":", 1)
        module_path = record.root.joinpath(*PurePosixPath(entry_path).parts)
        if not module_path.is_file():
            raise PluginError("entry module is missing")
        self._cleanup_modules(record.plugin_id)
        token = hashlib.sha256(f"{record.plugin_id}:{record.package_sha256}".encode()).hexdigest()[:16]
        package_name = f"_bilipdj_ext_{token}"
        package_module = types.ModuleType(package_name)
        package_module.__path__ = [str(record.root)]
        package_module.__package__ = package_name
        sys.modules[package_name] = package_module
        module_name = f"{package_name}.{Path(entry_path).stem}"
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            sys.modules.pop(package_name, None)
            raise PluginError("cannot create plugin import spec")
        module = importlib.util.module_from_spec(spec)
        module.__package__ = package_name
        sys.modules[module_name] = module
        before = set(sys.modules)
        try:
            spec.loader.exec_module(module)
            target = getattr(module, symbol)
            plugin_object = target() if isinstance(target, type) else target
            creator = getattr(plugin_object, "create_relay", None)
            if not callable(creator):
                raise PluginError("plugin entry must expose create_relay(context, config)")
        except Exception as exc:
            for name in set(sys.modules) - before | {package_name, module_name}:
                if name.startswith(package_name):
                    sys.modules.pop(name, None)
            if isinstance(exc, PluginError):
                raise
            raise PluginError(f"plugin import failed: {exc}") from exc
        loaded = {name for name in sys.modules if name == package_name or name.startswith(package_name + ".")}
        self._loaded_modules[record.plugin_id] = loaded

        def create_relay(active_server: Any) -> ManagedExternalRelay:
            context = PluginContext(self, active_server, record)
            config = context.get_config()
            relay = creator(context, config)
            if relay is None or not callable(getattr(relay, "start", None)):
                raise PluginError("create_relay must return an object with start()")
            return ManagedExternalRelay(relay, record)

        create_relay.__name__ = f"create_external_{record.platform}_relay"
        return DanmuPlugin(
            plugin_id=record.plugin_id,
            platform=record.platform,
            name=record.name,
            relay_factory=create_relay,
            version=record.version,
            plugin_api=PLUGIN_API_VERSION,
            plugin_type=PLUGIN_TYPE,
            source="external",
            config_section=str(record.manifest.get("config_section", record.platform) or record.platform),
            capabilities=tuple(str(item) for item in record.manifest.get("capabilities", []) or []),
            available=True,
        )

    def _unregister_external(self) -> None:
        lock = getattr(self.registry, "_lock")
        with lock:
            by_platform = getattr(self.registry, "_by_platform")
            by_id = getattr(self.registry, "_by_id")
            for platform, plugin in list(by_platform.items()):
                if plugin.source != "external":
                    continue
                by_platform.pop(platform, None)
                by_id.pop(plugin.plugin_id.lower(), None)

    def discover(self) -> list[dict[str, Any]]:
        with self._lock:
            self._unregister_external()
            state = self._state()
            records: dict[str, InstalledPluginRecord] = {}
            for root in sorted(self.plugins_root.iterdir()):
                if not root.is_dir() or root.name.startswith(".") or root.name == "data":
                    continue
                manifest_path = root / "manifest.json"
                meta_path = root / ".install.json"
                if not manifest_path.is_file() or not meta_path.is_file():
                    continue
                plugin_id = root.name
                try:
                    manifest = validate_manifest(json.loads(manifest_path.read_text(encoding="utf-8")), self.current_version)
                    if manifest["id"] != plugin_id:
                        raise PluginError("installed directory does not match plugin id")
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    package_hash, signature_status = self._verify_installed(root, manifest, meta)
                    record = InstalledPluginRecord(
                        plugin_id=plugin_id, root=root, manifest=manifest,
                        enabled=bool(state.get(plugin_id, False)), package_sha256=package_hash,
                        signature_status=signature_status, verified=True,
                    )
                    if record.enabled:
                        existing = self.registry.get(record.platform)
                        if existing is not None:
                            raise PluginError(f"platform already registered by {existing.plugin_id}")
                        self.registry.register(self._load_external_plugin(record))
                except Exception as exc:
                    try:
                        raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                    except Exception:
                        raw_manifest = {"id": plugin_id, "platform": "", "name": plugin_id, "version": ""}
                    record = InstalledPluginRecord(
                        plugin_id=plugin_id, root=root,
                        manifest=raw_manifest if isinstance(raw_manifest, dict) else {"id": plugin_id},
                        enabled=bool(state.get(plugin_id, False)), package_sha256="",
                        signature_status="error", verified=False, error=str(exc),
                    )
                records[plugin_id] = record
            self._records = records
            _plugin_core._sync_legacy_platform_metadata(self.server_module, self.issue79_module, self.registry)
            _plugin_core._install_registry_factory(self.server_module, self.registry)
            _plugin_core._install_issue79_plugin_bridge(self.server_module, self.issue79_module, self.registry)
            return self.list_public()

    def list_public(self) -> list[dict[str, Any]]:
        with self._lock:
            builtins = [
                {**plugin.public_info(), "enabled": True, "verified": True, "error": ""}
                for plugin in self.registry.list_plugins() if plugin.source == "builtin"
            ]
            external = [record.public_info() for record in self._records.values()]
            return builtins + external

    def verify(self, plugin_id: str) -> dict[str, Any]:
        plugin_id = str(plugin_id or "").strip().lower()
        root = self.plugins_root / plugin_id
        if not root.is_dir():
            raise PluginError("plugin is not installed")
        manifest = validate_manifest(json.loads((root / "manifest.json").read_text(encoding="utf-8")), self.current_version)
        meta = json.loads((root / ".install.json").read_text(encoding="utf-8"))
        package_hash, signature_status = self._verify_installed(root, manifest, meta)
        return {"status": "ok", "id": plugin_id, "verified": True, "package_sha256": package_hash, "signature_status": signature_status}

    def set_enabled(self, plugin_id: str, enabled: bool) -> dict[str, Any]:
        plugin_id = str(plugin_id or "").strip().lower()
        if plugin_id not in self._records:
            raise PluginError("external plugin is not installed")
        if enabled and not self._records[plugin_id].verified:
            raise PluginError("plugin integrity/compatibility verification failed")
        state = self._state()
        state[plugin_id] = bool(enabled)
        self._save_state(state)
        self.discover()
        record = self._records.get(plugin_id)
        if record is None:
            raise PluginError("plugin disappeared during reload")
        if enabled and record.error:
            state[plugin_id] = False
            self._save_state(state)
            self.discover()
            raise PluginError(record.error)
        return record.public_info()

    def uninstall(self, plugin_id: str) -> None:
        plugin_id = str(plugin_id or "").strip().lower()
        root = self.plugins_root / plugin_id
        if not root.is_dir() or plugin_id not in self._records:
            raise PluginError("external plugin is not installed")
        self._cleanup_modules(plugin_id)
        shutil.rmtree(root)
        state = self._state()
        state.pop(plugin_id, None)
        self._save_state(state)
        self.discover()


def _json_body(handler: Any) -> dict[str, Any]:
    try:
        length = int(handler.headers.get("Content-Length", "0") or 0)
    except (TypeError, ValueError):
        length = 0
    raw = handler.rfile.read(max(0, length)).decode("utf-8", errors="strict") if length else "{}"
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PluginError("request body must be valid JSON") from exc
    return payload if isinstance(payload, dict) else {}


def _reconcile_runtime(server_module: Any, active_server: Any, registry: DanmuPluginRegistry) -> None:
    config = server_module.load_config()
    active = config.get("active_platforms")
    if isinstance(active, (list, tuple)):
        allowed = set(registry.platform_ids())
        filtered = [str(item) for item in active if str(item) in allowed]
        if filtered != list(active):
            config["active_platforms"] = filtered
            if filtered:
                config["platform"] = filtered[0]
            server_module.save_config(config)
    active_server.runtime_config = server_module.load_config()
    callback = getattr(server_module, "_ensure_danmu_relay", None)
    if callable(callback):
        callback(active_server, reconnect=True)


def install_plugin_manager(server_module: Any, issue79_module: Any = None) -> PluginManager:
    registry = getattr(server_module, "danmu_plugin_registry", None)
    if not isinstance(registry, DanmuPluginRegistry):
        raise RuntimeError("DanmuPluginRegistry must be installed before PluginManager")
    with _PATCH_LOCK:
        manager = getattr(server_module, "plugin_manager", None)
        if not isinstance(manager, PluginManager):
            manager = PluginManager(server_module, registry, issue79_module)
            server_module.plugin_manager = manager
            server_module.PLUGIN_MANAGER = manager
        manager.discover()
        handler_class = server_module.ApiHandler
        original_get = handler_class.do_GET
        original_post = handler_class.do_POST
        if not bool(getattr(original_get, "_issue130_plugin_manager", False)):
            def do_GET(self: Any) -> None:
                path = urlparse(self.path).path
                if path == "/api/plugins/manage":
                    if not self._require_loopback():
                        return
                    self._write_json({
                        "status": "ok", "plugin_api": PLUGIN_API_VERSION,
                        "bilipdj_version": manager.current_version,
                        "supported_permissions": sorted(SUPPORTED_PERMISSIONS),
                        "plugins": manager.list_public(),
                    })
                    return
                if path == "/api/plugins/trusted-keys":
                    if not self._require_loopback():
                        return
                    self._write_json({"status": "ok", "keys": manager.list_trusted_keys()})
                    return
                return original_get(self)

            def do_POST(self: Any) -> None:
                path = urlparse(self.path).path
                managed = {
                    "/api/plugins/install", "/api/plugins/enable", "/api/plugins/disable",
                    "/api/plugins/uninstall", "/api/plugins/verify",
                    "/api/plugins/trusted-keys", "/api/plugins/trusted-keys/delete",
                }
                if path not in managed:
                    return original_post(self)
                if not self._require_loopback():
                    return
                try:
                    payload = _json_body(self)
                    if path == "/api/plugins/install":
                        filename = str(payload.get("filename", "") or "")
                        encoded = str(payload.get("data_base64", "") or "")
                        try:
                            data = base64.b64decode(encoded, validate=True)
                        except (binascii.Error, ValueError) as exc:
                            raise PluginError("data_base64 is invalid") from exc
                        info = manager.install_bytes(
                            data, filename=filename,
                            allow_unsigned=bool(payload.get("allow_unsigned", False)),
                        )
                        _reconcile_runtime(server_module, self.server, registry)
                        self._write_json({"status": "ok", "plugin": info})
                        return
                    plugin_id = str(payload.get("id", "") or "").strip().lower()
                    if path == "/api/plugins/enable":
                        info = manager.set_enabled(plugin_id, True)
                        _reconcile_runtime(server_module, self.server, registry)
                        self._write_json({"status": "ok", "plugin": info})
                        return
                    if path == "/api/plugins/disable":
                        info = manager.set_enabled(plugin_id, False)
                        _reconcile_runtime(server_module, self.server, registry)
                        self._write_json({"status": "ok", "plugin": info})
                        return
                    if path == "/api/plugins/uninstall":
                        manager.uninstall(plugin_id)
                        _reconcile_runtime(server_module, self.server, registry)
                        self._write_json({"status": "ok", "id": plugin_id})
                        return
                    if path == "/api/plugins/verify":
                        self._write_json(manager.verify(plugin_id))
                        return
                    if path == "/api/plugins/trusted-keys":
                        manager.add_trusted_key(
                            str(payload.get("key_id", "") or ""),
                            str(payload.get("public_key", "") or ""),
                        )
                        manager.discover()
                        self._write_json({"status": "ok", "keys": manager.list_trusted_keys()})
                        return
                    if path == "/api/plugins/trusted-keys/delete":
                        manager.remove_trusted_key(str(payload.get("key_id", "") or ""))
                        manager.discover()
                        self._write_json({"status": "ok", "keys": manager.list_trusted_keys()})
                        return
                except PluginError as exc:
                    self._write_json({"status": "error", "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                    return
                except Exception as exc:
                    logger = getattr(self.server, "logger", None)
                    if logger is not None:
                        logger.exception("plugin manager request failed")
                    self._write_json({"status": "error", "message": f"plugin operation failed: {exc}"}, status=500)
                    return

            do_GET._issue130_plugin_manager = True
            do_POST._issue130_plugin_manager = True
            handler_class.do_GET = do_GET
            handler_class.do_POST = do_POST
        server_module._plugin_manager_installed = True
        return manager


__all__ = [
    "MANIFEST_SCHEMA", "MAX_PACKAGE_BYTES", "SUPPORTED_PERMISSIONS",
    "PluginContext", "PluginError", "PluginManager", "install_plugin_manager", "validate_manifest",
]
