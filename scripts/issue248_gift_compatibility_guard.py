from __future__ import annotations

import sys
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    from apps.server.gift_compatibility import (
        GiftCompatibilityError,
        GiftCompatibilityService,
        install_gift_compatibility,
    )

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)

        class FakeServer:
            _YAML_DIR = root

        service = GiftCompatibilityService(FakeServer)

        # Existing Bilibili catalog remains the fallback when no custom rule exists.
        fallback = service.resolve(platform="bilibili", gift_name="比心", count=2)
        assert fallback["unit_value"] == 10
        assert fallback["value"] == 20
        assert fallback["source"] == "builtin:bilibili"

        # The same gift name may have different values on different platforms.
        saved = service.save_rules([
            {
                "platform": "bilibili",
                "gift_id": "1",
                "gift_name": "比心",
                "value": 88,
                "enabled": True,
            },
            {
                "platform": "douyin",
                "gift_id": "",
                "gift_name": "比心",
                "value": 5.5,
                "enabled": True,
            },
            {
                "platform": "douyin",
                "gift_id": "disabled",
                "gift_name": "停用礼物",
                "value": 999,
                "enabled": False,
            },
        ])
        assert len(saved) == 3

        by_id = service.resolve(platform="bilibili", gift_id="1", gift_name="比心", count=3)
        assert by_id["unit_value"] == 88
        assert by_id["value"] == 264
        assert by_id["source"] == "compatibility"

        douyin = service.resolve(platform="douyin", gift_name="比心", count=2)
        assert douyin["unit_value"] == 5.5
        assert douyin["value"] == 11
        assert douyin["source"] == "compatibility"

        disabled = service.resolve(
            platform="douyin",
            gift_id="disabled",
            gift_name="停用礼物",
            count=2,
            raw_unit_value=3,
        )
        assert disabled["unit_value"] == 3
        assert disabled["value"] == 6
        assert disabled["source"] == "platform"

        # ID matching still has priority when the incoming name points at a
        # different name-only rule; duplicate lookup keys themselves are now
        # rejected by the issue #260 guard.
        service.save_rules([
            {"platform": "douyin", "gift_id": "gift-7", "gift_name": "玫瑰礼盒", "value": 7, "enabled": True},
            {"platform": "douyin", "gift_id": "", "gift_name": "玫瑰", "value": 2, "enabled": True},
        ])
        priority = service.resolve(platform="douyin", gift_id="gift-7", gift_name="玫瑰", count=1)
        assert priority["unit_value"] == 7

        try:
            service.save_rules([
                {"platform": "douyin", "gift_id": "bad", "gift_name": "错误", "value": -1, "enabled": True}
            ])
        except GiftCompatibilityError as exc:
            assert "价值" in str(exc)
        else:
            raise AssertionError("negative gift values must be rejected")

        # Exercise the patched QueueManager contract without starting a server.
        broadcasts: list[dict[str, object]] = []
        logs: list[tuple[object, ...]] = []

        class FakeQueueManager:
            def __init__(self) -> None:
                self._lock = threading.RLock()
                self._gift_queue_enabled = True
                self._gift_queue_names = {"比心"}
                self._gift_queue_min_batteries = 0
                self._gift_queue_allow_multiple = False
                self._gift_queue_slots_per_gift = 1
                self._gift_queue_credits: dict[int, int] = {}
                self._gift_queue_used_uids: set[int] = set()
                self._gift_catalog: dict[object, dict[str, object]] = {}
                self._last_gift_event: dict[str, object] = {}
                self._ws_hub = SimpleNamespace(
                    broadcast_json=lambda _sender, payload: broadcasts.append(dict(payload))
                )
                self._logger = SimpleNamespace(info=lambda *args: logs.append(args))

            def get_gift_state(self) -> dict[str, object]:
                return {
                    "catalog": [],
                    "observed_catalog": list(self._gift_catalog.values()),
                    "last_event": dict(self._last_gift_event),
                }

        fake_module = SimpleNamespace(
            _YAML_DIR=root / "runtime",
            QueueManager=FakeQueueManager,
        )
        assert install_gift_compatibility(fake_module)
        manager = FakeQueueManager()
        first = manager.process_gift_event({
            "platform": "bilibili",
            "event": "gift",
            "uid": 123,
            "uname": "tester",
            "gift": {"id": 7, "name": "比心", "count": 2},
        })
        assert first["gift"]["value"] == 20
        assert first["gift"]["batteries"] == 20
        assert first["queue_credit_granted"] is True
        assert first["received_at"]
        assert first["guard_level"] == 0
        assert manager._gift_queue_credits == {123: 1}
        assert manager._gift_queue_used_uids == set()
        assert broadcasts[-1]["type"] == "LIVE_GIFT_EVENT"
        assert logs

        # allow_multiple=False keeps the legacy behavior: a second gift cannot
        # accumulate another unused credit before the first one is consumed.
        second = manager.process_gift_event({
            "platform": "bilibili",
            "event": "gift",
            "uid": 123,
            "uname": "tester",
            "gift": {"id": 7, "name": "比心", "count": 1},
        })
        assert "queue_credit_granted" not in second
        assert manager._gift_queue_credits == {123: 1}

    backend_source = (ROOT / "apps/server/gift_compatibility.py").read_text(encoding="utf-8")
    assert "process_gift_event" in backend_source
    assert "/api/gifts/state" in backend_source
    assert "compatibility_rules" in backend_source
    assert "plugin cannot emit a gift event for another platform" in backend_source
    assert "value_source" in backend_source

    ui_source = (ROOT / "apps/windows/gift_compatibility_ui.py").read_text(encoding="utf-8")
    assert "礼物兼容性" in ui_source
    assert "平台" in ui_source
    assert "礼物 ID" in ui_source
    assert "礼物名称" in ui_source
    assert "单个价值" in ui_source
    assert "保存全部" in ui_source
    assert '"/api/gifts/state"' in ui_source

    print("issue #248 gift compatibility guard: OK")


if __name__ == "__main__":
    main()
