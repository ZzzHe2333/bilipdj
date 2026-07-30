from __future__ import annotations

import re
import threading
import unittest

from core.queue_logic_guard import patch_queue_manager


class QueueSanitizerGuardTests(unittest.TestCase):
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


class QueueDuplicateInsertionGuardTests(unittest.TestCase):
    def _build_manager(self):
        class Logger:
            def __init__(self) -> None:
                self.messages: list[str] = []

            def warning(self, message: str, *args) -> None:
                self.messages.append(message % args if args else message)

        class QueueManager:
            @staticmethod
            def _strip_html(text: str) -> str:
                return str(text or "").strip()

            def __init__(self) -> None:
                self._lock = threading.Lock()
                self._persons = ["first"]
                self._entry_timestamps = ["old"]
                self._gift_queue_credits = {7: 1}
                self._gift_queue_insert_rank = 1
                self._logger = Logger()

            def _find_index(self, uname: str) -> int:
                for index, value in enumerate(self._persons):
                    if value.split(" ", 1)[0] == uname:
                        return index
                return -1

            def _remove_queue_item_unlocked(self, index: int) -> bool:
                self._persons.pop(index)
                self._entry_timestamps.pop(index)
                return True

            def _process(
                self,
                uid: int,
                uname: str,
                msg: str,
                _is_anchor: bool,
                _is_admin: bool,
                is_guard: bool,
                _guard_level: int,
            ):
                with self._lock:
                    credit = self._gift_queue_credits.get(uid, 0)
                    requested = [
                        value
                        for value in re.split(r"[\s,，、]+", msg[2:].strip())
                        if value
                    ] or [uname]
                    selected = requested[:credit]
                    position = min(len(self._persons), self._gift_queue_insert_rank - 1)
                    for offset, value in enumerate(selected):
                        self._persons.insert(position + offset, value)
                        self._entry_timestamps.insert(position + offset, "gift")
                    if is_guard:
                        self._persons.append(uname)
                        self._entry_timestamps.append("guard")
                    return bool(selected), None

        self.assertTrue(patch_queue_manager(QueueManager))
        return QueueManager()

    def test_same_user_is_inserted_only_once(self) -> None:
        manager = self._build_manager()
        modified, _ = manager._process(7, "Alice", "插队", False, False, True, 3)
        self.assertTrue(modified)
        self.assertEqual(manager._persons, ["Alice", "first"])
        self.assertEqual(manager._entry_timestamps, ["gift", "old"])
        self.assertEqual(len(manager._logger.messages), 1)

    def test_inserting_another_name_does_not_remove_guard_user(self) -> None:
        manager = self._build_manager()
        manager._process(7, "Alice", "插队 Bob", False, False, True, 3)
        self.assertEqual(manager._persons, ["Bob", "first", "Alice"])
        self.assertEqual(manager._entry_timestamps, ["gift", "old", "guard"])


if __name__ == "__main__":
    unittest.main()
