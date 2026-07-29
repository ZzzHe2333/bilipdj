from __future__ import annotations

import builtins
import io
import logging
import threading
import unittest

from core.queue_rank_query import attach_queue_rank_query, install_queue_rank_query_hook


class QueueRankSafetyTests(unittest.TestCase):
    def test_standalone_install_does_not_patch_global_builder(self) -> None:
        before = builtins.__build_class__
        self.assertFalse(install_queue_rank_query_hook())
        self.assertIs(builtins.__build_class__, before)

    def test_unrelated_main_queue_manager_is_not_patched(self) -> None:
        import __main__

        class UnrelatedQueueManager:
            def _find_index(self, name):
                return -1

            def _process(self, *args):
                return False, None

        previous_class = getattr(__main__, "QueueManager", None)
        previous_file = getattr(__main__, "__file__", None)
        try:
            __main__.QueueManager = UnrelatedQueueManager
            __main__.__file__ = "/tmp/other_app.py"
            before = builtins.__build_class__
            self.assertFalse(install_queue_rank_query_hook())
            self.assertFalse(hasattr(UnrelatedQueueManager, "query_queue_rank"))
            self.assertIs(builtins.__build_class__, before)
        finally:
            if previous_class is None:
                delattr(__main__, "QueueManager")
            else:
                __main__.QueueManager = previous_class
            if previous_file is None:
                try:
                    delattr(__main__, "__file__")
                except AttributeError:
                    pass
            else:
                __main__.__file__ = previous_file

    def test_direct_attachment_is_read_only(self) -> None:
        class Manager:
            def __init__(self) -> None:
                self._lock = threading.Lock()
                self._persons = ["甲", "乙"]
                self._logger = logging.getLogger("rank-safe")
                self._logger.handlers.clear()
                self._stream = io.StringIO()
                self._logger.addHandler(logging.StreamHandler(self._stream))
                self._logger.setLevel(logging.INFO)
                self._logger.propagate = False

            def _find_index(self, name: str) -> int:
                try:
                    return self._persons.index(name)
                except ValueError:
                    return -1

            def _process(self, *args):
                return True, "original"

        self.assertTrue(attach_queue_rank_query(Manager))
        manager = Manager()
        before = list(manager._persons)
        self.assertEqual(manager.query_queue_rank(2, "乙", "我的名次")["rank"], 2)
        self.assertEqual(manager._process(2, "乙", "我的排队", False, False, False, 0), (False, None))
        self.assertEqual(manager._persons, before)

    def test_server_import_context_hook_restores_after_class_creation(self) -> None:
        original = builtins.__build_class__
        namespace = {
            "__name__": "core.server",
            "__file__": "/tmp/core/server.py",
            "install_queue_rank_query_hook": install_queue_rank_query_hook,
            "threading": threading,
        }
        exec(
            """
installed = install_queue_rank_query_hook()
class QueueManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._persons = []
        self._logger = None
    def _find_index(self, name):
        return -1
    def _process(self, *args):
        return False, None
""",
            namespace,
        )
        self.assertTrue(namespace["installed"])
        self.assertTrue(namespace["QueueManager"]._queue_rank_query_installed)
        self.assertIs(builtins.__build_class__, original)


if __name__ == "__main__":
    unittest.main()
