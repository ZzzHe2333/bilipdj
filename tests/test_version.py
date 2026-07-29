from __future__ import annotations

import re
import unittest
from pathlib import Path

from core.version import APP_VERSION, load_app_version

ROOT = Path(__file__).resolve().parents[1]


class VersionSourceTests(unittest.TestCase):
    def test_version_file_is_the_runtime_version(self) -> None:
        expected = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
        self.assertEqual(APP_VERSION, expected)
        self.assertEqual(load_app_version(), expected)

    def test_version_uses_numeric_release_format(self) -> None:
        self.assertRegex(APP_VERSION, re.compile(r"^\d+(?:\.\d+){2,}$"))

    def test_desktop_title_is_corrected_from_version_module(self) -> None:
        source = (ROOT / "core" / "update_ui.py").read_text(encoding="utf-8")
        self.assertIn("from .version import APP_VERSION", source)
        self.assertIn(
            'app.root.title(f"{app_name} 控制台 v{current_version}")',
            source,
        )


if __name__ == "__main__":
    unittest.main()
