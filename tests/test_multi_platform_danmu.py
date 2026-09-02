from __future__ import annotations

import threading
import unittest

from core.multi_platform_danmu import (
    enabled_stream_platforms,
    normalize_multi_platform_config,
    platform_runtime_uid,
    platform_user_key,
)
from core.platform_queue_identity import patch_queue_manager_platform_identity


class MultiPlatformConfigTests(unittest.TestCase):
    def test_disabled_by_default(self) -> None:
        cfg = normalize_multi_platform_config({})
        self.assertFalse(cfg["enabled"])
        self.assertTrue(cfg["platforms"]["bilibili"])
        self.assertTrue(cfg["platforms"]["douyin"])

    def test_detects_two_ready_streams(self) -> None:
        config = {
            "multi_platform_danmu": {
                "enabled": True,
                "platforms": {"bilibili": True, "douyin": True},
            },
            "bilibili": {"roomid": 12345},
            "douyin": {"enabled": True, "live_id": "998877"},
        }
        self.assertEqual(enabled_stream_platforms(config), ["bilibili", "douyin"])

    def test_douyin_requires_enabled_and_live_id(self) -> None:
        base = {
            "multi_platform_danmu": {"enabled": True},
            "bilibili": {"roomid": 12345},
        }
        self.assertEqual(
            enabled_stream_platforms({**base, "douyin": {"enabled": False, "live_id": "1"}}),
            ["bilibili"],
        )
        self.assertEqual(
            enabled_stream_platforms({**base, "douyin": {"enabled": True, "live_id": ""}}),
            ["bilibili"],
        )


class PlatformUserKeyTests(unittest.TestCase):
    def test_platform_is_part_of_internal_key(self) -> None:
        bili = platform_user_key("bilibili", uid=123, uname="同名用户")
        douyin = platform_user_key("douyin", uid=123, uname="同名用户")
        self.assertEqual(bili, "bilibili:uid:123")
        self.assertEqual(douyin, "douyin:uid:123")
        self.assertNotEqual(bili, douyin)

    def test_douyin_prefers_sec_uid(self) -> None:
        self.assertEqual(
            platform_user_key("douyin", uid=123, uname="用户", sec_uid="SEC-ABC"),
            "douyin:sec_uid:SEC-ABC",
        )

    def test_runtime_uid_namespaces_non_bilibili(self) -> None:
        bili_key = platform_user_key("bilibili", uid=123)
        douyin_key = platform_user_key("douyin", uid=123)
        self.assertEqual(platform_runtime_uid("bilibili", 123, bili_key), 123)
        self.assertNotEqual(platform_runtime_uid("douyin", 123, douyin_key), 123)
        self.assertEqual(
            platform_runtime_uid("douyin", 123, douyin_key),
            platform_runtime_uid("douyin", 123, douyin_key),
        )


class _DummyQueueManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._persons: list[str] = []
        self._entry_timestamps: list[str] = []

    def process_danmu_json(self, payload):
        info = payload["info"]
        uname = str(info[2][1])
        if self._find_index(uname) < 0:
            self._append_queue_item_unlocked(uname)

    def _find_index(self, uname: str) -> int:
        try:
            return self._persons.index(uname)
        except ValueError:
            return -1

    def _append_queue_item_unlocked(self, item, *args, **kwargs) -> bool:
        self._persons.append(str(item))
        self._entry_timestamps.append("")
        return True

    def _insert_queue_item_unlocked(self, pos, item, *args, **kwargs) -> bool:
        pos = max(0, min(int(pos), len(self._persons)))
        self._persons.insert(pos, str(item))
        self._entry_timestamps.insert(pos, "")
        return True

    def _remove_queue_item_unlocked(self, index, *args, **kwargs) -> bool:
        if not 0 <= int(index) < len(self._persons):
            return False
        self._persons.pop(int(index))
        self._entry_timestamps.pop(int(index))
        return True

    def _set_queue_from_entries_unlocked(self, entries) -> None:
        self._persons = [str(entry.get("id", "")) for entry in entries]
        self._entry_timestamps = ["" for _entry in entries]

    def _get_queue_entries_unlocked(self):
        return [{"id": item, "content": ""} for item in self._persons]

    def move_item(self, index: int, direction: str):
        if direction == "up" and 2 <= index <= len(self._persons):
            self._persons[index - 2], self._persons[index - 1] = self._persons[index - 1], self._persons[index - 2]
        elif direction == "down" and 1 <= index < len(self._persons):
            self._persons[index - 1], self._persons[index] = self._persons[index], self._persons[index - 1]
        return list(self._persons)

    def clear_queue(self):
        self._persons.clear()
        self._entry_timestamps.clear()
        return []


class PlatformQueueIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        patch_queue_manager_platform_identity(_DummyQueueManager)

    @staticmethod
    def _payload(platform: str, uid: int, name: str, sec_uid: str = ""):
        payload = {
            "cmd": "DANMU_MSG",
            "info": [[], "排队", [uid, name, 0], []],
            "_pdj_platform": platform,
        }
        if sec_uid:
            payload["_pdj_identity"] = {"sec_uid": sec_uid}
        return payload

    def test_same_display_name_can_exist_on_two_platforms(self) -> None:
        queue = _DummyQueueManager()
        queue.process_danmu_json(self._payload("bilibili", 1001, "同名用户"))
        queue.process_danmu_json(self._payload("douyin", 1001, "同名用户", "DY-1001"))
        self.assertEqual(queue._persons, ["同名用户", "同名用户"])
        self.assertEqual(
            queue._platform_queue_keys,
            ["bilibili:uid:1001", "douyin:sec_uid:DY-1001"],
        )

    def test_queue_entries_expose_internal_platform_key(self) -> None:
        queue = _DummyQueueManager()
        queue.process_danmu_json(self._payload("douyin", 77, "测试", "SEC77"))
        entries = queue._get_queue_entries_unlocked()
        self.assertEqual(entries[0]["platform_user_key"], "douyin:sec_uid:SEC77")
        self.assertEqual(entries[0]["source_platform"], "douyin")


if __name__ == "__main__":
    unittest.main()
