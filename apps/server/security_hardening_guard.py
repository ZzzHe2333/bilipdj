"""Security hardening for issue #126 and later runtime safety fixes.

Keeps the legacy backend compatible while adding bounded connection handling,
explicit LAN read-only access control, spreadsheet-safe CSV persistence and the
final request/state guards that must sit outside feature-specific wrappers.
"""
from __future__ import annotations

import functools
import hmac
import socket
import threading
import urllib.parse
from http import HTTPStatus
from typing import Any

from . import gift_compatibility as _gift_compatibility
from .server_runtime_guard import MAX_MANAGEMENT_BODY_BYTES, _same_host_origin

DEFAULT_MAX_CONNECTIONS = 128
DEFAULT_HTTP_SOCKET_TIMEOUT = 15.0
LAN_READ_PATHS = frozenset({"/api/runtime-status", "/api/queue/state"})
_CSV_FORMULA_PREFIXES = frozenset("=+-@")
_PATCH_LOCK = threading.RLock()


def _spreadsheet_safe(value: Any) -> Any:
    if not isinstance(value, str) or not value:
        return value
    probe = value.lstrip(" \t\r\n")
    if probe and probe[0] in _CSV_FORMULA_PREFIXES:
        return "'" + value
    return value


def _spreadsheet_restore(value: Any) -> Any:
    if not isinstance(value, str) or not value.startswith("'"):
        return value
    probe = value[1:].lstrip(" \t\r\n")
    if probe and probe[0] in _CSV_FORMULA_PREFIXES:
        return value[1:]
    return value


def _patch_csv_archives(server_module: Any) -> None:
    original_write = server_module.write_queue_archive_entries
    original_parse = server_module.parse_queue_archive_rows
    if getattr(original_write, "_issue126_hardened", False):
        return

    @functools.wraps(original_write)
    def safe_write(path: Any, entries: list[dict[str, Any]], meta: dict[str, Any] | None = None) -> None:
        safe_entries: list[dict[str, Any]] = []
        for entry in entries:
            if isinstance(entry, dict):
                copied = dict(entry)
                for key in ("id", "content", "last_operation_at"):
                    if key in copied:
                        copied[key] = _spreadsheet_safe(copied[key])
                safe_entries.append(copied)
            else:
                safe_entries.append(entry)
        safe_meta = dict(meta) if isinstance(meta, dict) else meta
        if isinstance(safe_meta, dict):
            for key in ("timestamp", "actor", "message"):
                if key in safe_meta:
                    safe_meta[key] = _spreadsheet_safe(safe_meta[key])
        return original_write(path, safe_entries, safe_meta)

    @functools.wraps(original_parse)
    def safe_parse(rows: list[list[str]]) -> tuple[dict[str, str], list[dict[str, str]]]:
        meta, entries = original_parse(rows)
        restored_meta = {str(key): str(_spreadsheet_restore(value)) for key, value in meta.items()}
        restored_entries: list[dict[str, str]] = []
        for entry in entries:
            copied = dict(entry)
            for key in ("id", "content", "last_operation_at"):
                if key in copied:
                    copied[key] = str(_spreadsheet_restore(copied[key]))
            restored_entries.append(copied)
        return restored_meta, restored_entries

    safe_write._issue126_hardened = True  # type: ignore[attr-defined]
    safe_parse._issue126_hardened = True  # type: ignore[attr-defined]
    server_module.write_queue_archive_entries = safe_write
    server_module.parse_queue_archive_rows = safe_parse


def _patch_server_config(server_module: Any) -> None:
    original_normalize = server_module.normalize_server_config
    if getattr(original_normalize, "_issue126_hardened", False):
        return

    @functools.wraps(original_normalize)
    def normalize_with_security(raw: Any) -> dict[str, Any]:
        source = raw if isinstance(raw, dict) else {}
        normalized = dict(original_normalize(raw))
        normalized["lan_readonly"] = bool(source.get("lan_readonly", False))
        normalized["lan_readonly_token"] = str(source.get("lan_readonly_token", "") or "").strip()
        try:
            max_connections = int(source.get("max_connections", DEFAULT_MAX_CONNECTIONS))
        except (TypeError, ValueError):
            max_connections = DEFAULT_MAX_CONNECTIONS
        try:
            socket_timeout = float(source.get("http_socket_timeout", DEFAULT_HTTP_SOCKET_TIMEOUT))
        except (TypeError, ValueError):
            socket_timeout = DEFAULT_HTTP_SOCKET_TIMEOUT
        normalized["max_connections"] = max(8, min(1024, max_connections))
        normalized["http_socket_timeout"] = max(2.0, min(120.0, socket_timeout))
        return normalized

    normalize_with_security._issue126_hardened = True  # type: ignore[attr-defined]
    server_module.normalize_server_config = normalize_with_security


def _server_security_config(active_server: Any) -> dict[str, Any]:
    runtime = getattr(active_server, "runtime_config", {})
    if not isinstance(runtime, dict):
        return {}
    config = runtime.get("server", {})
    return config if isinstance(config, dict) else {}


def _remote_read_allowed(handler: Any) -> bool:
    if handler._is_loopback_client():
        return True
    config = _server_security_config(handler.server)
    if not bool(config.get("lan_readonly", False)):
        return False
    expected = str(config.get("lan_readonly_token", "") or "").strip()
    if not expected:
        return True
    parsed = urllib.parse.urlparse(handler.path)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    supplied = str(handler.headers.get("X-BiliPDJ-Read-Token", "") or "").strip()
    if not supplied:
        supplied = str((query.get("token") or [""])[0] or "").strip()
    return bool(supplied) and hmac.compare_digest(supplied, expected)


def _patch_read_access(server_module: Any) -> None:
    handler_class = server_module.ApiHandler
    original_get = handler_class.do_GET
    if getattr(original_get, "_issue126_hardened", False):
        return

    @functools.wraps(original_get)
    def guarded_get(self: Any) -> None:
        path = urllib.parse.urlparse(self.path).path
        if path in LAN_READ_PATHS and not _remote_read_allowed(self):
            self._write_json(
                {"status": "error", "message": "LAN read-only API is disabled or unauthorized"},
                status=HTTPStatus.FORBIDDEN,
            )
            return
        return original_get(self)

    guarded_get._issue126_hardened = True  # type: ignore[attr-defined]
    handler_class.do_GET = guarded_get


def _patch_post_request_boundary(server_module: Any) -> None:
    """Re-apply the global POST Origin/body guard after feature route wrappers.

    Several feature installers intentionally replace ``ApiHandler.do_POST`` and
    may consume their own route before delegating to the original handler. This
    wrapper is installed after those features, so every POST reaches the same
    browser-origin and body-size boundary again.
    """

    handler_class = server_module.ApiHandler
    original_post = handler_class.do_POST
    if getattr(original_post, "_issue260_final_post_boundary", False):
        return

    @functools.wraps(original_post)
    def guarded_post(self: Any) -> None:
        if not _same_host_origin(self.headers):
            self._write_json(
                {"status": "error", "message": "Cross-origin management request rejected"},
                status=HTTPStatus.FORBIDDEN,
            )
            return
        raw_length = str(self.headers.get("Content-Length", "0") or "0").strip()
        try:
            content_length = int(raw_length)
        except ValueError:
            content_length = -1
        if content_length < 0:
            self._write_json(
                {"status": "error", "message": "Invalid Content-Length"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        if content_length > MAX_MANAGEMENT_BODY_BYTES:
            self._write_json(
                {"status": "error", "message": "Request body is too large"},
                status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            )
            return
        return original_post(self)

    guarded_post._issue260_final_post_boundary = True  # type: ignore[attr-defined]
    handler_class.do_POST = guarded_post


def _patch_gift_rule_uniqueness(server_module: Any) -> None:
    """Make gift lookup keys unique in the same way runtime resolution uses them."""

    service_class = getattr(server_module, "GiftCompatibilityService", None)
    if not isinstance(service_class, type):
        return
    original_save = service_class.save_rules
    if getattr(original_save, "_issue260_unique_gift_keys", False):
        return

    @functools.wraps(original_save)
    def save_rules(self: Any, rules: Any) -> list[dict[str, Any]]:
        if not isinstance(rules, list):
            return original_save(self, rules)
        seen_ids: set[tuple[str, str]] = set()
        seen_names: set[tuple[str, str]] = set()
        for raw in rules:
            row = _gift_compatibility._normalize_rule(raw)
            platform = str(row["platform"])
            gift_id = str(row["gift_id"])
            gift_name = str(row["gift_name"])
            if gift_id:
                key = (platform, gift_id)
                if key in seen_ids:
                    raise _gift_compatibility.GiftCompatibilityError(
                        f"同一平台的礼物 ID 不能重复：{platform}/{gift_id}"
                    )
                seen_ids.add(key)
            if gift_name:
                key = (platform, gift_name.casefold())
                if key in seen_names:
                    raise _gift_compatibility.GiftCompatibilityError(
                        f"同一平台的礼物名称不能重复：{platform}/{gift_name}"
                    )
                seen_names.add(key)
        return original_save(self, rules)

    save_rules._issue260_unique_gift_keys = True  # type: ignore[attr-defined]
    service_class.save_rules = save_rules


def _patch_language_uninstall(server_module: Any) -> None:
    """Reset the persisted selection before removing its active language pack."""

    manager = getattr(server_module, "plugin_manager", None)
    service = getattr(server_module, "LANGUAGE_SERVICE", None)
    if manager is None or service is None:
        return
    manager_class = type(manager)
    original_uninstall = getattr(manager_class, "uninstall", None)
    if not callable(original_uninstall) or getattr(original_uninstall, "_issue260_language_uninstall", False):
        return

    @functools.wraps(original_uninstall)
    def uninstall(self: Any, plugin_id: str) -> None:
        key = str(plugin_id or "").strip().lower()
        lock = getattr(self, "_lock", None)
        if lock is None:
            return original_uninstall(self, plugin_id)
        with lock:
            record = getattr(self, "_records", {}).get(key)
            manifest = getattr(record, "manifest", {}) if record is not None else {}
            code = str(manifest.get("language", "") or "") if isinstance(manifest, dict) else ""
            is_language = isinstance(manifest, dict) and str(manifest.get("type", "")) == "language"
            try:
                selected = str(service._read_selected())
            except Exception:
                selected = "zh-CN"
            reset_selection = bool(is_language and code and selected == code)
            if reset_selection:
                service._write_selected("zh-CN")
            try:
                result = original_uninstall(self, plugin_id)
            except Exception:
                if reset_selection:
                    try:
                        service._write_selected(selected)
                        service.reconcile()
                    except Exception:
                        pass
                raise
            if reset_selection:
                service.reconcile()
            return result

    uninstall._issue260_language_uninstall = True  # type: ignore[attr-defined]
    manager_class.uninstall = uninstall


def _patch_connection_limits(server_module: Any) -> None:
    server_class = server_module.BackendServer
    original_process_request = server_class.process_request
    original_process_request_thread = server_class.process_request_thread
    if getattr(original_process_request, "_issue126_hardened", False):
        return

    def _ensure_slots(active_server: Any) -> threading.BoundedSemaphore:
        slots = getattr(active_server, "_issue126_connection_slots", None)
        if slots is not None:
            return slots
        config = _server_security_config(active_server)
        try:
            limit = int(config.get("max_connections", DEFAULT_MAX_CONNECTIONS))
        except (TypeError, ValueError):
            limit = DEFAULT_MAX_CONNECTIONS
        limit = max(8, min(1024, limit))
        with _PATCH_LOCK:
            slots = getattr(active_server, "_issue126_connection_slots", None)
            if slots is None:
                slots = threading.BoundedSemaphore(limit)
                active_server._issue126_connection_slots = slots
        return slots

    @functools.wraps(original_process_request)
    def bounded_process_request(self: Any, request: socket.socket, client_address: Any) -> None:
        slots = _ensure_slots(self)
        if not slots.acquire(blocking=False):
            try:
                self.shutdown_request(request)
            finally:
                logger = getattr(self, "logger", None)
                if logger is not None:
                    logger.warning("拒绝连接：已达到安全并发连接上限")
            return
        config = _server_security_config(self)
        try:
            timeout = float(config.get("http_socket_timeout", DEFAULT_HTTP_SOCKET_TIMEOUT))
        except (TypeError, ValueError):
            timeout = DEFAULT_HTTP_SOCKET_TIMEOUT
        try:
            request.settimeout(max(2.0, min(120.0, timeout)))
            return original_process_request(self, request, client_address)
        except Exception:
            slots.release()
            raise

    @functools.wraps(original_process_request_thread)
    def bounded_process_request_thread(self: Any, request: socket.socket, client_address: Any) -> None:
        try:
            return original_process_request_thread(self, request, client_address)
        finally:
            slots = getattr(self, "_issue126_connection_slots", None)
            if slots is not None:
                try:
                    slots.release()
                except ValueError:
                    pass

    bounded_process_request._issue126_hardened = True  # type: ignore[attr-defined]
    bounded_process_request_thread._issue126_hardened = True  # type: ignore[attr-defined]
    server_class.process_request = bounded_process_request
    server_class.process_request_thread = bounded_process_request_thread


def install_security_hardening(server_module: Any) -> bool:
    """Install security/runtime hardening patches idempotently."""
    if server_module is None or not hasattr(server_module, "ApiHandler"):
        return False
    with _PATCH_LOCK:
        if bool(getattr(server_module, "_issue126_security_hardening_installed", False)):
            return True
        _patch_server_config(server_module)
        _patch_csv_archives(server_module)
        _patch_connection_limits(server_module)
        _patch_read_access(server_module)
        _patch_gift_rule_uniqueness(server_module)
        _patch_language_uninstall(server_module)
        _patch_post_request_boundary(server_module)
        server_module._issue126_security_hardening_installed = True
        return True


__all__ = [
    "DEFAULT_MAX_CONNECTIONS",
    "DEFAULT_HTTP_SOCKET_TIMEOUT",
    "LAN_READ_PATHS",
    "install_security_hardening",
]
