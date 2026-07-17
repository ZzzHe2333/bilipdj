from __future__ import annotations

import logging
import unittest

from core.bilibili_protocol import extract_bilibili_room_id
from core.server import QueueManager
from core.mirrorchyan import MirrorChyanSettings, check_latest
from core.bilibili_gifts import GIFT_BATTERIES, batteries_for_gift


class Hub:
    def __init__(self) -> None:
        self.events = []

    def broadcast_json(self, _sender, payload) -> None:
        self.events.append(payload)


class GiftQueueTests(unittest.TestCase):
    def test_room_url_ignores_sensitive_query(self) -> None:
        self.assertEqual(extract_bilibili_room_id("https://live.bilibili.com/7777?ignored=1"), 7777)
        self.assertEqual(extract_bilibili_room_id("https://example.com/7777"), 0)

    def test_matching_gift_grants_only_one_credit(self) -> None:
        hub = Hub()
        queue = QueueManager(hub, object(), logging.getLogger("gift-test"))
        queue.load_config({"gift_queue_enabled": True, "gift_queue_names": ["辣条"]})
        payload = {"cmd": "SEND_GIFT", "data": {"uid": 123, "uname": "测试用户", "giftName": "辣条", "giftId": 1, "num": 1}}
        queue.process_live_event(payload)
        queue.process_live_event(payload)
        self.assertEqual(queue._gift_queue_credits, {123: 1})
        self.assertTrue(hub.events[0].get("queue_credit_granted"))
        self.assertFalse(hub.events[1].get("queue_credit_granted", False))

        queue._persist_myjs_state_unlocked = lambda: None
        changed, _ = queue._process(123, "测试用户", "插队", False, False, False, 0)
        self.assertTrue(changed)
        self.assertEqual(queue.get_queue(), ["测试用户"])
        queue.process_live_event(payload)
        self.assertEqual(queue._gift_queue_credits, {})

    def test_catalog_and_battery_threshold_multi_slot_order(self) -> None:
        self.assertEqual(len(GIFT_BATTERIES), 77)
        self.assertEqual(batteries_for_gift("梦幻游乐园"), 30000)
        hub = Hub()
        queue = QueueManager(hub, object(), logging.getLogger("battery-test"))
        queue.load_config({"gift_queue_enabled": True, "gift_queue_min_batteries": 500, "gift_queue_slots_per_gift": 2, "gift_queue_insert_rank": 1})
        queue.process_live_event({"cmd": "SEND_GIFT", "data": {"uid": 8, "uname": "赠送者", "giftName": "私人飞机", "giftId": 25, "num": 1}})
        queue._persist_myjs_state_unlocked = lambda: None
        changed, _ = queue._process(8, "赠送者", "插队 张三 李四", False, False, False, 0)
        self.assertTrue(changed)
        self.assertEqual(queue.get_queue(), ["张三", "李四"])

    def test_repeat_gifts_accumulate_slots(self) -> None:
        queue = QueueManager(Hub(), object(), logging.getLogger("repeat-test"))
        queue.load_config({"gift_queue_enabled": True, "gift_queue_names": ["足迹"], "gift_queue_allow_multiple": True, "gift_queue_slots_per_gift": 2})
        payload = {"cmd": "SEND_GIFT", "data": {"uid": 9, "uname": "重复用户", "giftName": "足迹", "giftId": 1, "num": 1}}
        queue.process_live_event(payload)
        queue.process_live_event(payload)
        self.assertEqual(queue._gift_queue_credits, {9: 4})

    def test_guard_buy_is_normalized_without_granting_credit(self) -> None:
        hub = Hub()
        queue = QueueManager(hub, object(), logging.getLogger("guard-test"))
        queue.load_config({"gift_queue_enabled": True, "gift_queue_names": ["舰长"]})
        queue.process_live_event({"cmd": "GUARD_BUY", "data": {"uid": 7, "username": "测试舰长", "gift_name": "舰长", "guard_level": 3, "num": 1}})
        self.assertEqual(hub.events[-1]["event"], "guard_buy")
        self.assertEqual(hub.events[-1]["guard_level"], 3)
        self.assertEqual(queue._gift_queue_credits, {})

    def test_mirrorchyan_is_off_and_does_not_request(self) -> None:
        result = check_latest(MirrorChyanSettings(), "v0.4.0")
        self.assertFalse(result["enabled"])
        self.assertFalse(result["checked"])


if __name__ == "__main__":
    unittest.main()
