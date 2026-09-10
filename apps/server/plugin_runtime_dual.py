"""Dual Python/JavaScript runtime support and integrity hardening for Issue #130."""
from __future__ import annotations

import copy
import os
import shutil
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

from . import plugin_manager as pm

SUPPORTED_RUNTIMES = frozenset({"python", "javascript"})
_RESERVED_PLUGIN_IDS = frozenset({"data", "state", "trusted_keys", "plugins"})
_WINDOWS_RESERVED = frozenset(
    {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)), *(f"lpt{i}" for i in range(1, 10))}
)
_INTERNAL_INSTALLED_FILES = frozenset({"manifest.json", ".install.json", ".package.bilipdj-plugin"})


def _safe_member_name(name: str) -> str:
    raw = str(name or "").replace("\\", "/")
    path = PurePosixPath(raw)
    for part in path.parts:
        base = part.rstrip(" .").split(".", 1)[0].lower()
        if ":" in part or part.endswith((" ", ".")) or base in _WINDOWS_RESERVED:
            raise pm.PluginError(f"archive contains Windows-unsafe path: {raw}")
    return pm._issue130_original_safe_member_name(name)


def _validate_javascript_manifest(manifest: dict[str, Any], current_version: str) -> dict[str, Any]:
    required = (
        "schema", "id", "name", "version", "plugin_api", "type", "platform", "runtime",
        "entry", "min_bilipdj_version", "permissions", "capabilities", "files",
    )
    missing = [key for key in required if key not in manifest]
    if missing:
        raise pm.PluginError("manifest missing fields: " + ", ".join(missing))
    if int(manifest.get("schema", 0) or 0) != pm.MANIFEST_SCHEMA:
        raise pm.PluginError(f"unsupported manifest schema: {manifest.get('schema')}")
    plugin_id = str(manifest.get("id", "") or "").strip().lower()
    platform = str(manifest.get("platform", "") or "").strip().lower()
    if not pm._ID_RE.fullmatch(plugin_id) or plugin_id.startswith("builtin.") or plugin_id in _RESERVED_PLUGIN_IDS:
        raise pm.PluginError("invalid/reserved plugin id")
    if not pm._PLATFORM_RE.fullmatch(platform):
        raise pm.PluginError("invalid platform id")
    if int(manifest.get("plugin_api", 0) or 0) != pm.PLUGIN_API_VERSION:
        raise pm.PluginError(
            f"incompatible plugin_api={manifest.get('plugin_api')}; BiliPDJ supports {pm.PLUGIN_API_VERSION}"
        )
    if str(manifest.get("type", "") or "") != pm.PLUGIN_TYPE:
        raise pm.PluginError(f"unsupported plugin type: {manifest.get('type')}")
    if not str(manifest.get("name", "") or "").strip():
        raise pm.PluginError("plugin name is required")
    pm._version_key(str(manifest.get("version", "") or ""))
    min_version = str(manifest.get("min_bilipdj_version", "") or "").strip()
    max_version = str(manifest.get("max_bilipdj_version", "") or "").strip()
    if pm._version_key(current_version) < pm._version_key(min_version):
        raise pm.PluginError(f"plugin requires BiliPDJ >= {min_version}; current {current_version}")
    if max_version and pm._version_key(current_version) > pm._version_key(max_version):
        raise pm.PluginError(f"plugin requires BiliPDJ <= {max_version}; current {current_version}")
    entry = str(manifest.get("entry", "") or "").strip()
    if not entry.lower().endswith(".js") or ":" in entry:
        raise pm.PluginError("JavaScript entry must look like path/to/plugin.js")
    entry_path = pm._safe_member_name(entry)
    files = pm._normalize_files_mapping(manifest)
    if entry_path not in files:
        raise pm.PluginError("entry module must be included in manifest.files")
    permissions = manifest.get("permissions")
    if not isinstance(permissions, list) or any(not isinstance(item, str) for item in permissions):
        raise pm.PluginError("permissions must be a string array")
    permission_set = {item.strip() for item in permissions if item.strip()}
    unknown = sorted(permission_set - pm.SUPPORTED_PERMISSIONS)
    if unknown:
        raise pm.PluginError("unknown permissions: " + ", ".join(unknown))
    if len(permission_set) != len(permissions):
        raise pm.PluginError("permissions must be unique and non-empty")
    capabilities = manifest.get("capabilities")
    if not isinstance(capabilities, list) or any(not isinstance(item, str) for item in capabilities):
        raise pm.PluginError("capabilities must be a string array")
    normalized = copy.deepcopy(manifest)
    normalized["id"] = plugin_id
    normalized["platform"] = platform
    normalized["runtime"] = "javascript"
    normalized["entry"] = entry_path
    normalized["permissions"] = list(permissions)
    normalized["files"] = files
    return normalized


def _require_canonical_manifest(manifest: dict[str, Any], normalized: dict[str, Any]) -> None:
    raw_id = str(manifest.get("id", "") or "")
    raw_platform = str(manifest.get("platform", "") or "")
    raw_entry = str(manifest.get("entry", "") or "")
    if raw_id != raw_id.strip() or raw_id != raw_id.lower() or raw_id != normalized.get("id"):
        raise pm.PluginError("plugin id must already be lowercase canonical text")
    if raw_platform != raw_platform.strip() or raw_platform != raw_platform.lower() or raw_platform != normalized.get("platform"):
        raise pm.PluginError("platform id must already be lowercase canonical text")
    if raw_entry != raw_entry.strip():
        raise pm.PluginError("entry must not contain surrounding whitespace")

    runtime = str(normalized.get("runtime", "") or "")
    entry_path = raw_entry if runtime == "javascript" else raw_entry.split(":", 1)[0]
    if pm._safe_member_name(entry_path) != entry_path:
        raise pm.PluginError("entry path must use canonical forward-slash form")

    permissions = manifest.get("permissions", [])
    if not isinstance(permissions, list):
        raise pm.PluginError("permissions must be a string array")
    if any(not isinstance(item, str) or not item or item != item.strip() for item in permissions):
        raise pm.PluginError("permissions must use canonical non-empty names without whitespace")
    # Preserve the signed array order. Validation is not allowed to silently sort
    # signature-covered data before Ed25519 verification.
    normalized["permissions"] = list(permissions)

    raw_files = manifest.get("files", {})
    if not isinstance(raw_files, dict):
        raise pm.PluginError("manifest.files must be an object")
    for raw_name in raw_files:
        text = str(raw_name)
        if pm._safe_member_name(text) != text:
            raise pm.PluginError("manifest file paths must use canonical forward-slash form")


def _verify_installed_tree(root: Path, manifest: dict[str, Any]) -> None:
    allowed = set(str(name) for name in manifest.get("files", {})) | set(_INTERNAL_INSTALLED_FILES)
    for current, dirs, files in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        for dirname in list(dirs):
            path = current_path / dirname
            if path.is_symlink():
                raise pm.PluginError(f"installed plugin contains symbolic link: {path.relative_to(root).as_posix()}")
            if dirname == "__pycache__":
                shutil.rmtree(path, ignore_errors=True)
                dirs.remove(dirname)
        for filename in files:
            path = current_path / filename
            relative = path.relative_to(root).as_posix()
            if path.is_symlink():
                raise pm.PluginError(f"installed plugin contains symbolic link: {relative}")
            if relative not in allowed:
                raise pm.PluginError(f"installed plugin contains undeclared file: {relative}")


def install_dual_runtime_support() -> None:
    if bool(getattr(pm, "_issue130_dual_runtime_installed", False)):
        return

    original_validate = pm.validate_manifest
    original_loader = pm.PluginManager._load_external_plugin
    original_public_info = pm.InstalledPluginRecord.public_info
    original_verify_installed = pm.PluginManager._verify_installed
    original_safe_member_name = pm._safe_member_name
    pm._issue130_original_safe_member_name = original_safe_member_name
    pm._safe_member_name = _safe_member_name

    def validate_manifest(manifest: Any, current_version: str) -> dict[str, Any]:
        if not isinstance(manifest, dict):
            raise pm.PluginError("manifest.json must be a JSON object")
        runtime = str(manifest.get("runtime", "") or "").strip().lower()
        if runtime not in SUPPORTED_RUNTIMES:
            raise pm.PluginError("runtime must be 'python' or 'javascript'")
        plugin_id = str(manifest.get("id", "") or "").strip().lower()
        if plugin_id in _RESERVED_PLUGIN_IDS:
            raise pm.PluginError("invalid/reserved plugin id")
        if runtime == "javascript":
            normalized = _validate_javascript_manifest(manifest, current_version)
        else:
            normalized = original_validate(manifest, current_version)
            normalized["runtime"] = "python"
        _require_canonical_manifest(manifest, normalized)
        return normalized

    def create_context(self: Any, active_server: Any, record: Any) -> Any:
        return pm.PluginContext(self, active_server, record)

    def load_external_plugin(self: Any, record: Any) -> Any:
        runtime = str(record.manifest.get("runtime", "python") or "python").strip().lower()
        if runtime == "javascript":
            from .javascript_plugin_runtime import create_javascript_danmu_plugin
            return create_javascript_danmu_plugin(self, record)
        return original_loader(self, record)

    def verify_installed(self: Any, root: Any, manifest: dict[str, Any], meta: dict[str, Any]) -> tuple[str, str]:
        result = original_verify_installed(self, root, manifest, meta)
        package_path = root / ".package.bilipdj-plugin"
        try:
            with zipfile.ZipFile(package_path, "r") as archive:
                packaged_manifest = archive.read("manifest.json")
        except (OSError, KeyError, zipfile.BadZipFile) as exc:
            raise pm.PluginError("installed package snapshot is invalid") from exc
        try:
            installed_manifest = (root / "manifest.json").read_bytes()
        except OSError as exc:
            raise pm.PluginError("installed manifest is missing") from exc
        if installed_manifest != packaged_manifest:
            raise pm.PluginError("installed manifest integrity check failed")
        _verify_installed_tree(Path(root), manifest)
        return result

    def public_info(self: Any) -> dict[str, Any]:
        payload = original_public_info(self)
        runtime = str(self.manifest.get("runtime", "python") or "python")
        payload["runtime"] = runtime
        payload["permission_enforcement"] = "host-enforced" if runtime == "javascript" else "python-full-trust"
        return payload

    pm.validate_manifest = validate_manifest
    pm.PluginManager.create_context = create_context
    pm.PluginManager._load_external_plugin = load_external_plugin
    pm.PluginManager._verify_installed = verify_installed
    pm.InstalledPluginRecord.public_info = public_info
    pm.SUPPORTED_RUNTIMES = SUPPORTED_RUNTIMES
    pm._issue130_dual_runtime_installed = True


__all__ = ["SUPPORTED_RUNTIMES", "install_dual_runtime_support"]
