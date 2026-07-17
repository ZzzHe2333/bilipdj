from __future__ import annotations

import unittest

from core.bilibili_protocol import parse_bilibili_danmu_identity


class BilibiliIdentityTests(unittest.TestCase):
    def _payload(self, *, admin: int = 0, guard: int = 0, medal_level: int = 0) -> dict:
        medal = [medal_level, "测试牌" if medal_level else "", "测试主播", 7777, 0, "", 0, 0, 0, 0, guard]
        return {"cmd": "DANMU_MSG", "info": [[], "测试弹幕", [123456, "测试用户", admin], medal]}

    def test_room_admin_and_fan_medal(self) -> None:
        identity = parse_bilibili_danmu_identity(self._payload(admin=1, medal_level=12))
        self.assertTrue(identity["is_room_admin"])
        self.assertTrue(identity["has_fan_medal"])
        self.assertEqual(identity["fan_medal"]["level"], 12)
        self.assertIn("room_admin", identity["roles"])
        self.assertIn("fan_medal", identity["roles"])

    def test_all_guard_levels(self) -> None:
        expected = {1: "总督", 2: "提督", 3: "舰长"}
        for level, name in expected.items():
            with self.subTest(level=level):
                identity = parse_bilibili_danmu_identity(self._payload(guard=level, medal_level=21))
                self.assertTrue(identity["is_guard"])
                self.assertEqual(identity["guard_name"], name)

    def test_anchor(self) -> None:
        identity = parse_bilibili_danmu_identity(self._payload(), anchor_uid=123456)
        self.assertTrue(identity["is_anchor"])
        self.assertIn("anchor", identity["roles"])


if __name__ == "__main__":
    unittest.main()
