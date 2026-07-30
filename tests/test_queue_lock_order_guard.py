from __future__ import annotations

import sys
import threading
import types
import unittest
from contextlib import contextmanager

from core.queue_logic_guard import patch_queue_manager


class QueueLockOrderGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.events: list[str] = []
        module = types.ModuleType("queue_lock_order_module")

        @contextmanager
        def config_io_transaction():
            self.events.append("config-enter")
            try:
                yield
            finally:
                self.events.append("config-exit")

        module.config_io_transaction = config_io_transaction

        class QueueManager:
            @staticmethod
            def _strip_html(text: str) -> str:
                return str(text or "").strip()

            def __init__(instance) -> None:
                instance._lock = threading.Lock()
                instance._gift_queue_credits = {}
                instance._persons = []
                instance._entry_timestamps = []

            def _is_command_like(instance, msg: str) -> bool:
                return msg == "排队"

            def _process(instance, *_args):
                with instance._lock:
                    self.events.append("queue-enter")
                return False, None

            def add_blacklist_item(instance, _name: str):
                with instance._lock:
                    self.events.append("queue-enter")
                    with config_io_transaction():
                        self.events.append("persist")
                return []

        QueueManager.__module__ = module.__name__
        module.QueueManager = QueueManager
        sys.modules[module.__name__] = module
        self.module = module
        self.QueueManager = QueueManager
        self.assertTrue(patch_queue_manager(QueueManager))

    def tearDown(self) -> None:
        sys.modules.pop(self.module.__name__, None)

    def test_persistent_command_takes_config_lock_first(self) -> None:
        manager = self.QueueManager()
        manager._process(1, "Alice", "排队", False, False, False, 0)
        self.assertEqual(
            self.events,
            ["config-enter", "queue-enter", "config-exit"],
        )

    def test_non_command_does_not_take_config_lock(self) -> None:
        manager = self.QueueManager()
        manager._process(1, "Alice", "普通聊天", False, False, False, 0)
        self.assertEqual(self.events, ["queue-enter"])

    def test_blacklist_write_uses_config_before_queue(self) -> None:
        manager = self.QueueManager()
        manager.add_blacklist_item("Alice")
        self.assertEqual(
            self.events,
            [
                "config-enter",
                "queue-enter",
                "config-enter",
                "persist",
                "config-exit",
                "config-exit",
            ],
        )


if __name__ == "__main__":
    unittest.main()
