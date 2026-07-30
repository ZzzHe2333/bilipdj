from __future__ import annotations

import sys
import threading
import time
import types
import unittest

from core.overlay_refresh_guard import patch_overlay_module


class _FakeRoot:
    def __init__(self) -> None:
        self.owner = threading.get_ident()
        self.callbacks: dict[str, object] = {}
        self.cancelled: list[str] = []
        self.next_id = 0

    def after(self, _delay: int, callback):
        if threading.get_ident() != self.owner:
            raise AssertionError("Tk after called from worker thread")
        self.next_id += 1
        job = f"job-{self.next_id}"
        self.callbacks[job] = callback
        return job

    def after_cancel(self, job: str) -> None:
        if threading.get_ident() != self.owner:
            raise AssertionError("Tk after_cancel called from worker thread")
        self.cancelled.append(job)
        self.callbacks.pop(job, None)

    def run_job(self, job: str) -> None:
        callback = self.callbacks.pop(job)
        callback()


class OverlayRefreshGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        module = types.ModuleType("fake_overlay_host")
        module.OVERLAY_REFRESH_MS = 100

        class OverlayHostApp:
            def __init__(self) -> None:
                self.root = _FakeRoot()
                self.items: list[str] = []
                self.style = {"auto_scroll": False}
                self._refresh_running = False
                self.reset_count = 0
                self.redraw_count = 0
                self.closed = False

            def _request_json(self, path: str):
                if path == "/api/queue/state":
                    return {
                        "entries": [
                            {"id": "Alice", "content": "官服"},
                        ]
                    }
                return {"auto_scroll": True, "show_sequence": True}

            def _refresh_async(self) -> None:
                raise AssertionError("old refresh loop should be replaced")

            def _refresh_worker(self) -> None:
                raise AssertionError("old worker should be replaced")

            def _reset_scroll(self) -> None:
                self.reset_count += 1

            def _redraw(self) -> None:
                self.redraw_count += 1

            def _close(self) -> None:
                self.closed = True

        module.OverlayHostApp = OverlayHostApp
        sys.modules[module.__name__] = module
        self.module = module
        self.App = OverlayHostApp
        self.assertTrue(patch_overlay_module(module))

    def tearDown(self) -> None:
        sys.modules.pop(self.module.__name__, None)

    def test_worker_returns_through_queue_and_main_thread_updates_ui(self) -> None:
        app = self.App()
        app._refresh_async()
        deadline = time.monotonic() + 1.0
        while app._refresh_result_queue.empty() and time.monotonic() < deadline:
            time.sleep(0.005)
        self.assertFalse(app._refresh_result_queue.empty())

        poll_job = app._refresh_poll_job
        self.assertIsNotNone(poll_job)
        app.root.run_job(poll_job)
        self.assertFalse(app._refresh_running)
        self.assertEqual(app.items, ["Alice 官服"])
        self.assertTrue(app.style["auto_scroll"])
        self.assertTrue(app.style["show_sequence"])
        self.assertEqual(app.reset_count, 1)
        self.assertEqual(app.redraw_count, 1)
        self.assertIsNotNone(app._refresh_timer_job)

    def test_close_cancels_poll_and_refresh_timer(self) -> None:
        app = self.App()
        app._refresh_async()
        poll_job = app._refresh_poll_job
        app._refresh_timer_job = app.root.after(100, lambda: None)
        timer_job = app._refresh_timer_job
        app._close()
        self.assertTrue(app.closed)
        self.assertTrue(app._refresh_closed)
        self.assertFalse(app._refresh_running)
        self.assertIn(poll_job, app.root.cancelled)
        self.assertIn(timer_job, app.root.cancelled)

    def test_patch_is_idempotent(self) -> None:
        method = self.App._refresh_async
        self.assertTrue(patch_overlay_module(self.module))
        self.assertIs(self.App._refresh_async, method)


if __name__ == "__main__":
    unittest.main()
