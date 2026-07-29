from __future__ import annotations

import socket
import unittest
from types import SimpleNamespace

from core.bilibili_socket_guard import (
    PartialSocketReadError,
    install_bilibili_socket_guard,
    recv_exact_guarded,
)


class _ScriptedSocket:
    def __init__(self, events):
        self.events = iter(events)

    def recv(self, _size: int) -> bytes:
        event = next(self.events)
        if isinstance(event, BaseException):
            raise event
        return event


class BilibiliSocketGuardTests(unittest.TestCase):
    def test_complete_exact_read(self) -> None:
        conn = _ScriptedSocket([b"ab", b"cd"])
        self.assertEqual(recv_exact_guarded(conn, 4), b"abcd")
        self.assertEqual(recv_exact_guarded(conn, 0), b"")

    def test_idle_timeout_remains_normal_timeout(self) -> None:
        conn = _ScriptedSocket([socket.timeout("idle")])
        with self.assertRaises(TimeoutError) as ctx:
            recv_exact_guarded(conn, 4)
        self.assertNotIsInstance(ctx.exception, PartialSocketReadError)

    def test_partial_timeout_forces_reconnect_error(self) -> None:
        conn = _ScriptedSocket([b"ab", socket.timeout("partial")])
        with self.assertRaises(PartialSocketReadError) as ctx:
            recv_exact_guarded(conn, 4)
        self.assertIn("2 of 4", str(ctx.exception))

    def test_closed_socket_returns_none(self) -> None:
        conn = _ScriptedSocket([b""])
        self.assertIsNone(recv_exact_guarded(conn, 4))

    def test_install_is_idempotent(self) -> None:
        module = SimpleNamespace(_ws_recv_exact=lambda *_: b"old")
        self.assertTrue(install_bilibili_socket_guard(module))
        self.assertIs(module._ws_recv_exact, recv_exact_guarded)
        self.assertIs(module.PartialSocketReadError, PartialSocketReadError)
        self.assertFalse(install_bilibili_socket_guard(module))


if __name__ == "__main__":
    unittest.main()
