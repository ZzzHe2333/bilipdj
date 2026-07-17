from __future__ import annotations

import unittest

from core.control_panel import ControlPanelApp, sanitize_log_message


class ControlPanelTests(unittest.TestCase):
    def test_log_type_classification(self) -> None:
        self.assertEqual(ControlPanelApp._classify_log("[弹幕] 用户：你好"), "DANMU")
        self.assertEqual(ControlPanelApp._classify_log("ONLINE_RANK_UPDATE"), "EVENT")
        self.assertEqual(ControlPanelApp._classify_log("[WARNING] reconnect"), "WARNING")
        self.assertEqual(ControlPanelApp._classify_log("connection failed", warn=True), "ERROR")

    def test_sensitive_log_values_are_hidden(self) -> None:
        sanitized = sanitize_log_message("SESSDATA=secret-value cookie=another-secret")
        self.assertNotIn("secret-value", sanitized)
        self.assertNotIn("another-secret", sanitized)


if __name__ == "__main__":
    unittest.main()
