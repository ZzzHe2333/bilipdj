from __future__ import annotations

import ast
import unittest
from pathlib import Path

SOURCE_PATH = Path(__file__).resolve().parents[1] / "core" / "control_panel.py"

class BackendStartupAnimationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = SOURCE_PATH.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def test_progress_and_button_animation_exist(self) -> None:
        self.assertIn('mode="indeterminate"', self.source)
        self.assertIn('self._startup_progress.start(12)', self.source)
        self.assertIn('text=f"启动中{dots}"', self.source)

    def test_start_is_deferred_until_ui_can_paint(self) -> None:
        self.assertIn('self.root.after(80, self._start_server_after_paint)', self.source)
        self.assertIn('use_backend_api=False, switch_queue_slot=False', self.source)

    def test_backend_readiness_is_checked_in_worker_thread(self) -> None:
        self.assertIn('target=self._wait_for_backend_ready', self.source)
        self.assertIn('self._startup_result_queue.put', self.source)
        self.assertIn('self.root.after(100, self._poll_backend_start_result)', self.source)

    def test_startup_save_avoids_dead_backend_http_calls(self) -> None:
        self.assertIn('prefer_backend=use_backend_api', self.source)
        self.assertIn('if not prefer_backend:', self.source)
        self.assertIn('if switch_queue_slot:', self.source)

    def test_source_is_valid_python(self) -> None:
        self.assertIsInstance(self.tree, ast.Module)

if __name__ == "__main__":
    unittest.main()
