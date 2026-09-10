"""Browser request and trust-lifecycle hardening for plugin management (Issue #132)."""
from __future__ import annotations

import ipaddress
from http import HTTPStatus
from pathlib import Path
from threading import RLock
from typing import Any
from urllib.parse import urlparse

MAX_PLUGIN_MANAGEMENT_BODY_BYTES = 2 * 1024 * 1024
PLUGIN_MANAGEMENT_POST_PATHS = frozenset(
    {
        "/api/plugins/install",
        "/api/plugins/enable",
        "/api/plugins/disable",
        "/api/plugins/uninstall",
        "/api/plugins/verify",
        "/api/plugins/trusted-keys",
        "/api/plugins/trusted-keys/delete",
    }
)
TRUST_MUTATION_PATHS = frozenset(
    {"/api/plugins/trusted-keys", "/api/plugins/trusted-keys/delete"}
)
_PATCH_LOCK = RLock()


def _is_loopback_origin_host(hostname: str) -> bool:
    host = str(hostname or "").strip().lower().rstrip(".")
    if host == "localhost":
        return True
    try:
        return bool(ipaddress.ip_address(host).is_loopback)
    except ValueError:
        return False


def _request_content_length(handler: Any) -> int:
    raw = str(handler.headers.get("Content-Length", "") or "").strip()
    if not raw:
        return 0
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid Content-Length") from exc
    if value < 0:
        raise ValueError("invalid Content-Length")
    return value


def _reject(handler: Any, status: HTTPStatus, message: str) -> bool:
    handler._write_json({"status": "error", "message": message}, status=status)
    return False


def require_safe_plugin_management_request(handler: Any) -> bool:
    """Reject cross-site/simple browser requests before plugin-management code runs."""
    content_type = str(handler.headers.get("Content-Type", "") or "")
    media_type = content_type.split(";", 1)[0].strip().lower()
    if media_type != "application/json":
        return _reject(
            handler,
            HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
            "Plugin management POST requires Content-Type: application/json",
        )

    fetch_site = str(handler.headers.get("Sec-Fetch-Site", "") or "").strip().lower()
    if fetch_site == "cross-site":
        return _reject(handler, HTTPStatus.FORBIDDEN, "Cross-site plugin management request denied")

    origin = str(handler.headers.get("Origin", "") or "").strip()
    if origin:
        try:
            parsed = urlparse(origin)
            origin_host = str(parsed.hostname or "").strip().lower()
            origin_port = parsed.port if parsed.port is not None else (80 if parsed.scheme.lower() == "http" else 443)
        except (TypeError, ValueError):
            return _reject(handler, HTTPStatus.FORBIDDEN, "Invalid plugin management Origin")
        server_port = int(getattr(handler.server, "server_port", 0) or 0)
        if (
            parsed.scheme.lower() != "http"
            or not _is_loopback_origin_host(origin_host)
            or origin_port != server_port
        ):
            return _reject(handler, HTTPStatus.FORBIDDEN, "External Origin cannot manage BiliPDJ plugins")

    try:
        length = _request_content_length(handler)
    except ValueError:
        return _reject(handler, HTTPStatus.BAD_REQUEST, "Invalid Content-Length")
    if length <= 0:
        return _reject(handler, HTTPStatus.BAD_REQUEST, "Plugin management request body is required")
    if length > MAX_PLUGIN_MANAGEMENT_BODY_BYTES:
        return _reject(
            handler,
            HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            "Plugin management request body exceeds safety limit",
        )
    return True


def _read_file_bytes(path: Any) -> bytes:
    try:
        return Path(path).read_bytes()
    except OSError:
        return b""


def install_plugin_api_security_guard(server_module: Any, plugin_manager_module: Any) -> bool:
    """Install CSRF/body guards and immediately reconcile runtime after trust changes."""
    with _PATCH_LOCK:
        if bool(getattr(server_module, "_plugin_api_security_guard_installed", False)):
            return True

        manager = getattr(server_module, "plugin_manager", None)
        registry = getattr(server_module, "danmu_plugin_registry", None)
        if manager is None or registry is None:
            raise RuntimeError("PluginManager must be installed before plugin API security guard")

        original_json_body = plugin_manager_module._json_body
        if not bool(getattr(original_json_body, "_issue132_body_limit", False)):
            def guarded_json_body(handler: Any) -> dict[str, Any]:
                try:
                    length = _request_content_length(handler)
                except ValueError as exc:
                    raise plugin_manager_module.PluginError("invalid Content-Length") from exc
                if length <= 0:
                    raise plugin_manager_module.PluginError("plugin management request body is required")
                if length > MAX_PLUGIN_MANAGEMENT_BODY_BYTES:
                    raise plugin_manager_module.PluginError("plugin management request body exceeds safety limit")
                return original_json_body(handler)

            guarded_json_body._issue132_body_limit = True  # type: ignore[attr-defined]
            plugin_manager_module._json_body = guarded_json_body

        handler_class = server_module.ApiHandler
        original_post = handler_class.do_POST
        if bool(getattr(original_post, "_issue132_plugin_api_security", False)):
            server_module._plugin_api_security_guard_installed = True
            return True

        def do_POST(self: Any) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path not in PLUGIN_MANAGEMENT_POST_PATHS:
                return original_post(self)
            if not self._require_loopback():
                return
            if not require_safe_plugin_management_request(self):
                return

            trust_before = b""
            if path in TRUST_MUTATION_PATHS:
                trust_before = _read_file_bytes(getattr(manager, "trusted_keys_path", ""))

            original_post(self)

            if path in TRUST_MUTATION_PATHS:
                trust_after = _read_file_bytes(getattr(manager, "trusted_keys_path", ""))
                if trust_after != trust_before:
                    # The plugin-manager handler already re-discovers records. Re-run
                    # discovery defensively, then reconcile active_platforms/Relay so
                    # a revoked/replaced signing key takes effect immediately.
                    manager.discover()
                    plugin_manager_module._reconcile_runtime(server_module, self.server, registry)

        do_POST._issue132_plugin_api_security = True  # type: ignore[attr-defined]
        handler_class.do_POST = do_POST
        server_module._plugin_api_security_guard_installed = True
        return True


__all__ = [
    "MAX_PLUGIN_MANAGEMENT_BODY_BYTES",
    "PLUGIN_MANAGEMENT_POST_PATHS",
    "TRUST_MUTATION_PATHS",
    "install_plugin_api_security_guard",
    "require_safe_plugin_management_request",
]
