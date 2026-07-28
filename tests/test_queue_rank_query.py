from __future__ import annotations

import io
import logging
import unittest

from core.server import QueueManager


class _Hub:
    pass


class _Archive:
    pass


class QueueRankQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.stream = io.StringIO()
        self.logger = logging.getLogger(f"queue-rank-test-{id(self)}")
        self.logger.handlers.clear()
        self.logger.propagate = False
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler(self.stream))

        self.manager = QueueManager(_Hub(), _Archive(), self.logger)
        self.manager._persons = ["官|小明 任务甲", "小红 任务乙", "<小刚>任务丙"]
        self.manager._entry_timestamps = ["", "", ""]

    def test_interface_is_attached_to_queue_manager(self) -> None:
        self.assertTrue(getattr(QueueManager, "_queue_rank_query_installed", False))
        self.assertTrue(callable(getattr(self.manager, "query_queue_rank", None)))

    def test_supported_phrases_return_current_rank(self) -> None:
        first = self.manager.query_queue_rank(101, "小明", "我的排队")
        second = self.manager.query_queue_rank(102, "小红", "我的名次")

        self.assertEqual(first["rank"], 1)
        self.assertEqual(second["rank"], 2)
        self.assertEqual(second["total"], 3)
        self.assertTrue(second["queued"])
        self.assertIn("[智能查询]", self.stream.getvalue())
        self.assertIn("第 2 位", self.stream.getvalue())

    def test_missing_user_is_logged_without_mutating_queue(self) -> None:
        before = list(self.manager._persons)
        result = self.manager.query_queue_rank(103, "未排队用户", "我的名次")

        self.assertFalse(result["queued"])
        self.assertIsNone(result["rank"])
        self.assertEqual(result["total"], 3)
        self.assertEqual(self.manager._persons, before)
        self.assertIn("不在排队列表", self.stream.getvalue())

    def test_unrelated_message_is_not_handled(self) -> None:
        self.assertIsNone(self.manager.query_queue_rank(104, "小红", "排队"))

    def test_process_consumes_query_without_queue_change(self) -> None:
        before = list(self.manager._persons)
        modified, note = self.manager._process(
            102,
            "小红",
            "我的排队",
            False,
            False,
            False,
            0,
        )
        self.assertFalse(modified)
        self.assertIsNone(note)
        self.assertEqual(self.manager._persons, before)


if __name__ == "__main__":
    unittest.main()
