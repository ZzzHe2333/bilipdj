"""Issue #319: Douyin WSS gift bridge regression tests (offline, no account needed)."""
from __future__ import annotations

import io
import json
import logging
import os
import tempfile
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from apps.server.danmu_event import DanmuEvent
from apps.server.douyin_gift_bridge import (
    API_PATH,
    DouyinGiftBridge,
    GiftComboTracker,
    install_douyin_gift_bridge,
)

ROOM = "7698556487468780322"
TOKEN = "offline-test-token-" + "A" * 32


def gift(msgid: str, count: int, *, ended: int | None = None,
         room: str = ROOM, created: int = 1792464867000,
         user: str = "123456789", gift_id: int = 3992) -> dict:
    return {
        "type": "WebcastGiftMessage",
        "msgId": msgid, "roomId": room, "createTime": created,
        "giftId": gift_id, "repeatCount": count, "repeatEnd": ended,
        "groupId": None,
        "user": {"id": user, "nickname": "sample-user"},
        "gift": {"id": gift_id, "name": "人气票", "diamondCount": 1},
    }


class Queue:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def process_gift_event(self, event: dict) -> dict:
        self.calls.append(event)
        return event


def test_combo_tracker() -> None:
    tracker = GiftComboTracker(idle_seconds=5.0)
    # The count supplied by Douyin is cumulative, not additive.
    assert tracker.accept(gift("m1", 1, created=1000), now=0) == []
    assert tracker.accept(gift("m2", 57, created=2000), now=1) == []
    finished = tracker.accept(gift("m3", 520, ended=1, created=3000), now=2)
    assert len(finished) == 1 and finished[0]["total_diamond"] == 520
    assert tracker.accept(gift("m3", 520, ended=1, created=3000), now=3) == []
    # A late lower cumulative packet must not re-settle the same combo.
    assert tracker.accept(gift("m-late", 510, created=2500), now=3) == []
    assert tracker.flush_due(now=10) == []
    # A later combo of the same gift/user is separate.
    next_combo = tracker.accept(gift("m4", 1, ended=1, created=5000), now=11)
    assert len(next_combo) == 1 and next_combo[0]["count"] == 1
    assert tracker.accept(gift("m5", 3, created=6000), now=12) == []
    timeout = tracker.flush_due(now=18)
    assert len(timeout) == 1 and timeout[0]["total_diamond"] == 3


def test_bridge_service() -> None:
    queue = Queue()
    logger = logging.getLogger("issue319")
    with tempfile.TemporaryDirectory() as td:
        module = SimpleNamespace(KEY_DIR=Path(td), _YAML_DIR=Path(td))
        bridge = DouyinGiftBridge(module, token=TOKEN)
        runtime = {"douyin": {"enabled": True, "live_info": {"room_id": ROOM}}}
        backend = SimpleNamespace(queue_manager=queue, logger=logger, runtime_config=runtime)
        with patch.dict(os.environ, {"BILIPDJ_DOUYIN_GIFT_QUEUE_CREDIT": "0"}):
            bridge.handle(gift("seq1", 1, created=1000), runtime, backend)
            bridge.handle(gift("seq2", 520, ended=1, created=3000), runtime, backend)
        assert len(queue.calls) == 1
        event = queue.calls[0]
        assert event["platform"] == "douyin"
        assert event["gift"]["count"] == 520
        assert event["gift"]["value"] == 1
        assert event["grant_queue_credit"] is False
        assert event["uid"] == DanmuEvent(platform="douyin", user_id="123456789",
                                         username="sample-user", content="排队").legacy_uid()
        assert event["uid"] != DanmuEvent(platform="bilibili", user_id="123456789",
                                         username="sample-user", content="排队").legacy_uid()
        assert bridge.authorize("Bearer " + TOKEN)
        assert not bridge.authorize("Bearer bad")
        assert bridge.status(runtime)["settled"] == 1
        try:
            bridge.handle(gift("bad-room", 1, room="different"), runtime, backend)
        except ValueError:
            pass
        else:
            raise AssertionError("room spoof must fail")


def test_http_auth_and_size() -> None:
    class Handler:
        def __init__(self, backend, body, *, token="bad", origin="", path=API_PATH,
                     content_type="application/json"):
            self.path = path
            self.server = backend
            self.client_address = ("127.0.0.1", 56422)
            self.headers = {
                "Authorization": "Bearer " + token,
                "Origin": origin,
                "Content-Type": content_type,
                "Content-Length": str(len(body)),
            }
            self.rfile = io.BytesIO(body)
            self.response = None

        def _require_loopback(self):
            return True

        def _write_json(self, payload, status=200):
            self.response = (int(status), payload)

        def do_POST(self):
            self.response = (200, {"previous": True})

        def do_GET(self):
            self.response = (200, {"previous": True})

    with tempfile.TemporaryDirectory() as td, patch.dict(
        os.environ, {"BILIPDJ_DOUYIN_GIFT_TOKEN": TOKEN}
    ):
        runtime = {"douyin": {"enabled": True, "live_info": {"room_id": ROOM}}}
        backend = SimpleNamespace(runtime_config=runtime, queue_manager=Queue(),
                                  logger=logging.getLogger("issue319"))
        module = SimpleNamespace(ApiHandler=Handler, KEY_DIR=Path(td),
                                 _YAML_DIR=Path(td), logger=backend.logger)
        assert install_douyin_gift_bridge(module)
        assert install_douyin_gift_bridge(module)
        body = json.dumps(gift("http1", 2, ended=1)).encode()
        req = Handler(backend, body, token="bad")
        req.do_POST()
        assert req.response[0] == 401
        req = Handler(backend, body, token=TOKEN,
                      origin="https://random-site.example")
        req.do_POST()
        assert req.response[0] == 403
        req = Handler(backend, body, token=TOKEN)
        req.do_POST()
        assert req.response[0] == 200
        req = Handler(backend, b"x" * (16 * 1024 + 1), token=TOKEN)
        req.do_POST()
        assert req.response[0] == 413
        req = Handler(backend, b"{}", path="/api/unrelated")
        req.do_POST()
        assert req.response[1] == {"previous": True}
        req = Handler(backend, b"", path="/api/douyin/gift-hook/status")
        req.do_GET()
        assert req.response[0] == 200
        assert "token" not in req.response[1]


def main() -> None:
    test_combo_tracker()
    test_bridge_service()
    test_http_auth_and_size()
    print("Issue #319 Douyin gift bridge offline regressions passed.")


if __name__ == "__main__":
    main()
