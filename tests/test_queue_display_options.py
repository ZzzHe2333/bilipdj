from __future__ import annotations

import json
import unittest
from pathlib import Path

from core.overlay_host import DEFAULT_STYLE, _display_queue_text


REPO_DIR = Path(__file__).resolve().parents[1]


class QueueDisplayOptionsTests(unittest.TestCase):
    def test_display_options_default_to_disabled(self) -> None:
        style = json.loads((REPO_DIR / "core" / "style.json").read_text(encoding="utf-8"))
        self.assertIs(style["auto_scroll"], False)
        self.assertIs(style["show_sequence"], False)
        self.assertIs(DEFAULT_STYLE["auto_scroll"], False)
        self.assertIs(DEFAULT_STYLE["show_sequence"], False)

    def test_overlay_sequence_format_is_optional(self) -> None:
        self.assertEqual(_display_queue_text(3, "玩家甲", False), "玩家甲")
        self.assertEqual(_display_queue_text(3, "玩家甲", True), "03  玩家甲")

    def test_browser_queue_contains_scroll_and_sequence_controls(self) -> None:
        script = (REPO_DIR / "core" / "ui" / "myjs.js").read_text(encoding="utf-8")
        page = (REPO_DIR / "core" / "ui" / "config.html").read_text(encoding="utf-8")

        self.assertIn("PDJ_AutoScrollStep", script)
        self.assertIn("pdjDisplayOptions.show_sequence", script)
        self.assertIn('id="auto-scroll-switch"', page)
        self.assertIn('id="sequence-switch"', page)
        self.assertIn('role="switch"', page)
        self.assertIn("#22c55e", page)


if __name__ == "__main__":
    unittest.main()
