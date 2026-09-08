from __future__ import annotations

import json
import threading
import time
import unittest
from pathlib import Path

from apps.server.websocket_performance_guard import patch_websocket_hub

ROOT = Path(__file__).resolve().parents[1]


class _Logger:
    def info(self, *_args, **_kwargs) -> None:
        return None


class _SlowConnection:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.sent: list[str] = []


class _FakeHub:
    def __init__(self) -> None:
        self.logger = _Logger()
        self._clients: set[_SlowConnection] = set()
        self._client_locks: dict[_SlowConnection, threading.Lock] = {}
        self._lock = threading.Lock()

    def register(self, conn: _SlowConnection) -> None:
        with self._lock:
            self._clients.add(conn)
            self._client_locks.setdefault(conn, threading.Lock())

    def unregister(self, conn: _SlowConnection) -> None:
        with self._lock:
            self._clients.discard(conn)
            self._client_locks.pop(conn, None)

    def send_text(self, conn: _SlowConnection, text: str, opcode: int = 0x1) -> None:
        _ = opcode
        conn.started.set()
        conn.release.wait(0.5)
        conn.sent.append(text)

    def broadcast_text(self, sender: _SlowConnection | None, text: str) -> None:
        with self._lock:
            targets = list(self._clients)
        for conn in targets:
            if sender is not None and conn is sender:
                continue
            self.send_text(conn, text)


class RealtimePerformanceTests(unittest.TestCase):
    def test_slow_client_does_not_block_and_queue_updates_are_coalesced(self) -> None:
        self.assertTrue(patch_websocket_hub(_FakeHub))
        hub = _FakeHub()
        conn = _SlowConnection()
        hub.register(conn)
        first = json.dumps({"type": "QUEUE_UPDATE", "queue": ["A"]})
        second = json.dumps({"type": "QUEUE_UPDATE", "queue": ["B"]})
        latest = json.dumps({"type": "QUEUE_UPDATE", "queue": ["C"]})
        started_at = time.monotonic()
        hub.broadcast_text(None, first)
        self.assertLess(time.monotonic() - started_at, 0.1)
        self.assertTrue(conn.started.wait(0.5))
        hub.broadcast_text(None, second)
        hub.broadcast_text(None, latest)
        conn.release.set()
        deadline = time.monotonic() + 1.0
        while len(conn.sent) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertGreaterEqual(len(conn.sent), 2)
        self.assertEqual(conn.sent[0], first)
        self.assertEqual(conn.sent[-1], latest)
        self.assertNotIn(second, conn.sent)
        hub.unregister(conn)

    def test_obs_performance_layer_is_loaded(self) -> None:
        web = ROOT / "apps" / "web" / "static"
        index = (web / "index.html").read_text(encoding="utf-8")
        script = (web / "performance.js").read_text(encoding="utf-8")
        self.assertIn('<script src="performance.js"></script>', index)
        self.assertIn("incrementalQueueRender: true", script)
        self.assertIn("scrollFpsLimit: 30", script)
        self.assertIn("latest_queue_update", (ROOT / "apps" / "server" / "websocket_performance_guard.py").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
