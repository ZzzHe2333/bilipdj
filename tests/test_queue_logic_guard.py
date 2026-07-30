from __future__ import annotations

import re
import unittest

from core.queue_logic_guard import patch_queue_manager


class QueueLogicGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        class QueueManager:
            @staticmethod
            def _strip_html(text: str) -> str:
                cleaned = re.sub(r"<[^>]*>", "", str(text))
                cleaned = re.sub(r"⏳待确认|等待确认", "", cleaned)
                return cleaned.strip()

            def __init__(self) -> None:
                self.items: list[str] = []

            def append(self, text: str) -> bool:
                cleaned = self._strip_html(text)
                if not cleaned:
                    return False
                self.items.append(cleaned)
                return True

        self.QueueManager = QueueManager
        self.assertTrue(patch_queue_manager(QueueManager))

    def test_super_queue_marker_is_preserved(self) -> None:
        manager = self.QueueManager()
        self.assertTrue(manager.append("<雪梦茉莉>"))
        self.assertEqual(manager.items, ["<雪梦茉莉>"])
        self.assertEqual(manager._strip_html("<用户> 游戏内容"), "<用户>游戏内容")

    def test_waiting_marker_is_removed_without_destroying_super_format(self) -> None:
        manager = self.QueueManager()
        self.assertEqual(manager._strip_html("<⏳待确认用户>"), "<用户>")
        self.assertEqual(manager._strip_html("<用户> 等待确认"), "<用户>")

    def test_nested_or_closed_html_still_uses_original_sanitizer(self) -> None:
        manager = self.QueueManager()
        self.assertEqual(manager._strip_html("<b>Alice</b>"), "Alice")
        self.assertEqual(manager._strip_html("<script>alert(1)</script>"), "alert(1)")

    def test_patch_is_idempotent(self) -> None:
        current = self.QueueManager._strip_html
        self.assertTrue(patch_queue_manager(self.QueueManager))
        self.assertIs(self.QueueManager._strip_html, current)


if __name__ == "__main__":
    unittest.main()
