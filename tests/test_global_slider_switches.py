from __future__ import annotations

import re
import unittest
from pathlib import Path
from tkinter import ttk

import core


REPO_DIR = Path(__file__).resolve().parents[1]


class GlobalSliderSwitchTests(unittest.TestCase):
    def test_ttk_checkbutton_is_replaced_globally(self) -> None:
        self.assertIs(ttk.Checkbutton, core.SliderCheckbutton)
        self.assertTrue(issubclass(core.SliderCheckbutton, ttk._bilipdj_original_checkbutton))

    def test_control_panel_boolean_controls_use_the_shared_widget(self) -> None:
        source = (REPO_DIR / "core" / "control_panel.py").read_text(encoding="utf-8")
        self.assertGreaterEqual(source.count("ttk.Checkbutton("), 10)
        self.assertIsNone(re.search(r"(?<!t)tk\.Checkbutton\(", source))

    def test_switch_visual_has_explicit_on_and_off_states(self) -> None:
        source = (REPO_DIR / "core" / "slider_switches.py").read_text(encoding="utf-8")
        self.assertIn('state_text = "ON" if selected else "OFF"', source)
        self.assertIn('track_fill = "#22c55e"', source)
        self.assertIn('track_fill = "#c9cdd3"', source)
        self.assertIn('"selected disabled"', source)


if __name__ == "__main__":
    unittest.main()
