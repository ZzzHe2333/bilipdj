"""Runtime guards for the local HTTP/WebSocket server.

The server module is also executed directly in packaged builds, so these guards
are attached after its classes and style functions become available instead of
requiring package-only imports inside ``server.py``.
"""
from __future__ import annotations

import functools
import ipaddress
import json
import os
import socket
import struct
import sys
import threading
import time
from http import HTTPStatus
from typing import Any
from urllib.parse import urlparse

from .style_option_guard import patch_style_module

MAX_MANAGEMENT_BODY_BYTES = 2 * 1024 * 1024
_PATCH_LOCK = threading.RLock()
_SCHEDULED_MODULES: set[str] = set()


def _server_module_valid(module: Any) -> bool:
    if module is None:
        return False
    name = str(getattr(module, "__name__", "") or "")
    if name not in {"core.server", "server", "__main__"}:
        return False
    if name == "__main__":
        return os.path.basename(str(getattr(module, "__file__", ""))) == "server.py"
    return True


def _authority(value: str, *, scheme: str = "") -> tuple[str, int | None]:
    text = str(value or "").strip()
    if not text:
        return "", None
    parsed = urlparse(text if "://" in text else f"//{text}", scheme=scheme)
    host = str(parsed.hostname or "").rstrip(".").casefold()
    try:
        port = parsed.port
    except ValueError:
        return "", None
    if port is None and parsed.scheme == "http":
        port = 80
    elif port is None and parsed.scheme == "https":
        port = 443
    return host, port


def _host_is_loopback(host_header: str) -> bool:
    host, _ = _authority(host_header)
    if not host:
        return False
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _same_host_origin(headers: Any) -> bool:
    host_header = str(headers.get("Host", "") or "").strip()
    if not host_header:
        return True

    origin = str(headers.get("Origin", "") or "").strip()
    candidate = origin
    if not candidate:
        candidate = str(headers.get("Referer", "") or "").strip()
    if not candidate:
        return True
    if candidate.casefold() == "null":
        return False

    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        return False
    origin_authority = _authority(candidate)
    host_authority = _authority(host_header, scheme=parsed.scheme)
    return bool(origin_authority[0]) and origin_authority == host_authority


def _read_masked_client_frame(handler: Any, conn: socket.socket) -> tuple[int, bool, bytes] | None:
    module = sys.modules.get(str(getattr(handler.__class__, "__module__", "")))
    recv_exact = getattr(module, "_ws_recv_exact", None)
    max_frame = int(getattr(module, "MAX_WS_FRAME_BYTES", 1024 * 1024))
    if not callable(recv_exact):
        return None

    head = recv_exact(conn, 2)
    if head is None:
        return None
    first, second = head
    fin = bool(first & 0x80)
    if first & 0x70:  # RSV bits without an negotiated extension
        return None
    opcode = first & 0x0F
    if not (second & 0x80):  # browser/client frames must be masked
        return None

    payload_len = second & 0x7F
    if payload_len == 126:
        raw_length = recv_exact(conn, 2)
        if raw_length is None:
            return None
        payload_len = struct.unpack("!H", raw_length)[0]
    elif payload_len == 127:
        raw_length = recv_exact(conn, 8)
        if raw_length is None:
            return None
        payload_len = struct.unpack("!Q", raw_length)[0]

    is_control = opcode >= 0x8
    if payload_len > max_frame or (is_control and (not fin or payload_len > 125)):
        return None

    mask_key = recv_exact(conn, 4)
    if mask_key is None:
        return None
    payload = recv_exact(conn, payload_len)
    if payload is None:
        return None
    unmasked = bytes(value ^ mask_key[index % 4] for index, value in enumerate(payload))
    return opcode, fin, unmasked


def patch_api_handler(module: Any) -> bool:
    """Patch one fully imported server module, idempotently."""

    if not _server_module_valid(module):
        return False
    handler_class = getattr(module, "ApiHandler", None)
    if not isinstance(handler_class, type):
        return False

    with _PATCH_LOCK:
        if bool(getattr(handler_class, "_bilipdj_runtime_guard_installed", False)):
            return True

        original_post = getattr(handler_class, "do_POST", None)
        original_upgrade = getattr(handler_class, "_handle_websocket_upgrade", None)
        if not callable(original_post) or not callable(original_upgrade):
            return False

        local_get_paths = getattr(handler_class, "_LOCAL_ONLY_GET_PATHS", None)
        if isinstance(local_get_paths, set):
            # Style data contains no credentials and is required by LAN display clients.
            local_get_paths.discard("/api/style")

        def write_json_no_store(
            self: Any,
            payload: dict[str, Any],
            status: int = HTTPStatus.OK,
        ) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def require_loopback_and_local_host(self: Any) -> bool:
            if not self._is_loopback_client():
                self._write_json(
                    {"status": "error", "message": "Management API is local-only"},
                    status=HTTPStatus.FORBIDDEN,
                )
                return False
            host_header = str(self.headers.get("Host", "") or "").strip()
            if host_header and not _host_is_loopback(host_header):
                self._write_json(
                    {"status": "error", "message": "Invalid management Host header"},
                    status=HTTPStatus.FORBIDDEN,
                )
                return False
            return True

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
                self._write_json(
                    {"status": "error", "message": "Invalid Content-Length"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
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

        @functools.wraps(original_upgrade)
        def guarded_upgrade(self: Any) -> None:
            host_header = str(self.headers.get("Host", "") or "").strip()
            if self._is_loopback_client() and host_header and not _host_is_loopback(host_header):
                self._write_json(
                    {"status": "error", "message": "Invalid WebSocket Host header"},
                    status=HTTPStatus.FORBIDDEN,
                )
                return
            if not _same_host_origin(self.headers):
                self._write_json(
                    {"status": "error", "message": "Cross-origin WebSocket rejected"},
                    status=HTTPStatus.FORBIDDEN,
                )
                return
            return original_upgrade(self)

        def guarded_ws_recv_text(self: Any, conn: socket.socket) -> str | None:
            try:
                frame = _read_masked_client_frame(self, conn)
                if frame is None:
                    return None
                opcode, fin, payload = frame
                if opcode == 0x8:  # close
                    return None
                if opcode == 0x9:  # ping
                    self.server.ws_hub.send_frame(conn, payload, opcode=0xA)
                    return ""
                if opcode == 0xA:  # pong
                    return ""
                if opcode == 0x1:  # text
                    if not fin:
                        return None  # fragmented messages are not supported
                    if not self._is_loopback_client():
                        return ""  # LAN clients are subscribers, not publishers
                    return payload.decode("utf-8", errors="replace")
                if opcode == 0x2 and fin:  # binary frame, consumed and ignored
                    return ""
                return None
            except (ConnectionError, OSError, TimeoutError, struct.error):
                return None

        setattr(handler_class, "_write_json", write_json_no_store)
        setattr(handler_class, "_require_loopback", require_loopback_and_local_host)
        setattr(handler_class, "do_POST", guarded_post)
        setattr(handler_class, "_handle_websocket_upgrade", guarded_upgrade)
        setattr(handler_class, "_ws_recv_text", guarded_ws_recv_text)
        setattr(handler_class, "_bilipdj_runtime_guard_installed", True)
        return True


def patch_server_module(module: Any) -> bool:
    style_ready = patch_style_module(module)
    api_ready = patch_api_handler(module)
    return style_ready and api_ready


def schedule_server_runtime_guards(module_name: str, *, timeout: float = 10.0) -> bool:
    """Patch a server module as soon as its import finishes defining the APIs."""

    name = str(module_name or "").strip()
    module = sys.modules.get(name)
    if not _server_module_valid(module):
        return False

    with _PATCH_LOCK:
        if name in _SCHEDULED_MODULES:
            return False
        _SCHEDULED_MODULES.add(name)

    def worker() -> None:
        succeeded = False
        try:
            deadline = time.monotonic() + max(0.1, float(timeout))
            while time.monotonic() < deadline:
                current = sys.modules.get(name)
                if _server_module_valid(current) and patch_server_module(current):
                    succeeded = True
                    return
                time.sleep(0.005)
        finally:
            if not succeeded:
                with _PATCH_LOCK:
                    _SCHEDULED_MODULES.discard(name)

    threading.Thread(
        target=worker,
        name="bilipdj-server-runtime-guard",
        daemon=True,
    ).start()
    return True


__all__ = [
    "MAX_MANAGEMENT_BODY_BYTES",
    "patch_api_handler",
    "patch_server_module",
    "schedule_server_runtime_guards",
]
