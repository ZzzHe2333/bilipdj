from __future__ import annotations

import io
import struct
import sys
import types
import unittest
from http import HTTPStatus
from types import SimpleNamespace

from core.server_runtime_guard import MAX_MANAGEMENT_BODY_BYTES, patch_server_module


class _BufferedSocket:
    def __init__(self, data: bytes) -> None:
        self.buffer = bytearray(data)

    def recv(self, size: int) -> bytes:
        if not self.buffer:
            return b""
        chunk = bytes(self.buffer[:size])
        del self.buffer[:size]
        return chunk


def _recv_exact(conn: _BufferedSocket, size: int) -> bytes | None:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = conn.recv(size - len(chunks))
        if not chunk:
            return None
        chunks.extend(chunk)
    return bytes(chunks)


def _masked_frame(opcode: int, payload: bytes, *, fin: bool = True) -> bytes:
    first = (0x80 if fin else 0) | opcode
    mask = b"mask"
    length = len(payload)
    if length <= 125:
        header = bytes([first, 0x80 | length])
    elif length <= 65535:
        header = bytes([first, 0x80 | 126]) + struct.pack("!H", length)
    else:
        header = bytes([first, 0x80 | 127]) + struct.pack("!Q", length)
    encoded = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
    return header + mask + encoded


class _Hub:
    def __init__(self) -> None:
        self.frames: list[tuple[bytes, int]] = []

    def send_frame(self, _conn, payload: bytes, opcode: int) -> None:
        self.frames.append((payload, opcode))


class ServerRuntimeGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.previous_server = sys.modules.get("server")
        module = types.ModuleType("server")
        module.__file__ = "/tmp/server.py"
        module.MAX_WS_FRAME_BYTES = 1024 * 1024
        module._ws_recv_exact = _recv_exact
        module.DEFAULT_STYLE = {"bg1": "old"}
        module.DEFAULT_CONFIG = {"style": {"bg1": "old"}}
        module.style_state = {
            "bg1": "old",
            "auto_scroll": True,
            "show_sequence": True,
            "future_option": "keep",
        }

        def load_style():
            return dict(module.style_state)

        def save_style(data):
            module.style_state = dict(data)

        module.load_style = load_style
        module.save_style = save_style

        class ApiHandler:
            _LOCAL_ONLY_GET_PATHS = {"/api/style", "/api/config"}

            def __init__(self, *, host="127.0.0.1", origin="", client="127.0.0.1") -> None:
                self.client_address = (client, 10000)
                self.headers = {"Host": host, "Content-Length": "0"}
                if origin:
                    self.headers["Origin"] = origin
                self.wfile = io.BytesIO()
                self.status = None
                self.response_headers: dict[str, str] = {}
                self.post_called = False
                self.upgrade_called = False
                self.server = SimpleNamespace(ws_hub=_Hub())

            def _is_loopback_client(self) -> bool:
                return self.client_address[0] in {"127.0.0.1", "::1"}

            def _require_loopback(self) -> bool:
                return self._is_loopback_client()

            def _write_json(self, _payload, status=HTTPStatus.OK) -> None:
                self.status = status

            def send_response(self, status) -> None:
                self.status = status

            def send_header(self, key: str, value: str) -> None:
                self.response_headers[key] = value

            def end_headers(self) -> None:
                return None

            def do_POST(self) -> None:
                self.post_called = True

            def _handle_websocket_upgrade(self) -> None:
                self.upgrade_called = True

            def _ws_recv_text(self, _conn):
                return "old"

        ApiHandler.__module__ = "server"
        module.ApiHandler = ApiHandler
        sys.modules["server"] = module
        self.module = module
        self.assertTrue(patch_server_module(module))

    def tearDown(self) -> None:
        if self.previous_server is None:
            sys.modules.pop("server", None)
        else:
            sys.modules["server"] = self.previous_server

    def test_style_is_public_read_only_and_partial_saves_preserve_fields(self) -> None:
        self.assertNotIn("/api/style", self.module.ApiHandler._LOCAL_ONLY_GET_PATHS)
        self.module.save_style({"bg1": "new"})
        self.assertEqual(self.module.style_state["bg1"], "new")
        self.assertTrue(self.module.style_state["auto_scroll"])
        self.assertTrue(self.module.style_state["show_sequence"])
        self.assertEqual(self.module.style_state["future_option"], "keep")

    def test_management_api_rejects_dns_rebinding_host(self) -> None:
        handler = self.module.ApiHandler(host="attacker.example:9816")
        self.assertFalse(handler._require_loopback())
        self.assertEqual(handler.status, HTTPStatus.FORBIDDEN)

    def test_post_rejects_cross_origin_and_oversized_bodies(self) -> None:
        cross_origin = self.module.ApiHandler(
            host="127.0.0.1:9816",
            origin="https://attacker.example",
        )
        cross_origin.do_POST()
        self.assertFalse(cross_origin.post_called)
        self.assertEqual(cross_origin.status, HTTPStatus.FORBIDDEN)

        oversized = self.module.ApiHandler(
            host="127.0.0.1:9816",
            origin="http://127.0.0.1:9816",
        )
        oversized.headers["Content-Length"] = str(MAX_MANAGEMENT_BODY_BYTES + 1)
        oversized.do_POST()
        self.assertFalse(oversized.post_called)
        self.assertEqual(oversized.status, HTTPStatus.REQUEST_ENTITY_TOO_LARGE)

        valid = self.module.ApiHandler(
            host="127.0.0.1:9816",
            origin="http://127.0.0.1:9816",
        )
        valid.do_POST()
        self.assertTrue(valid.post_called)

    def test_websocket_consumes_unsupported_frames_without_losing_alignment(self) -> None:
        stream = _BufferedSocket(
            _masked_frame(0x2, b"binary") + _masked_frame(0x1, b"hello")
        )
        handler = self.module.ApiHandler()
        self.assertEqual(handler._ws_recv_text(stream), "")
        self.assertEqual(handler._ws_recv_text(stream), "hello")

    def test_lan_websocket_clients_are_read_only(self) -> None:
        stream = _BufferedSocket(_masked_frame(0x1, b"forged update"))
        handler = self.module.ApiHandler(
            host="192.168.1.20:9816",
            origin="http://192.168.1.20:9816",
            client="192.168.1.30",
        )
        self.assertEqual(handler._ws_recv_text(stream), "")

    def test_websocket_handshake_rejects_cross_origin_browser(self) -> None:
        handler = self.module.ApiHandler(
            host="127.0.0.1:9816",
            origin="https://attacker.example",
        )
        handler._handle_websocket_upgrade()
        self.assertFalse(handler.upgrade_called)
        self.assertEqual(handler.status, HTTPStatus.FORBIDDEN)


if __name__ == "__main__":
    unittest.main()
