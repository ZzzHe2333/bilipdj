from __future__ import annotations

import importlib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class MonorepoPhysicalMigrationTests(unittest.TestCase):
    def test_backend_official_module_is_apps_server(self) -> None:
        backend = importlib.import_module("apps.server.server")
        legacy = importlib.import_module("core.server")
        self.assertIs(backend, legacy)
        self.assertIn("apps/server/server.py", str(Path(backend.__file__).as_posix()))

    def test_windows_control_panel_implementation_is_in_apps(self) -> None:
        panel = importlib.import_module("apps.windows.control_panel")
        self.assertIn("apps/windows/control_panel.py", str(Path(panel.__file__).as_posix()))
        legacy_source = (ROOT / "core" / "control_panel.py").read_text(encoding="utf-8")
        self.assertIn("apps.windows", legacy_source)
        self.assertLess(len(legacy_source), 2000)

    def test_web_has_single_source_directory(self) -> None:
        self.assertTrue((ROOT / "apps" / "web" / "static" / "index.html").is_file())
        self.assertFalse((ROOT / "core" / "ui").exists())


if __name__ == "__main__":
    unittest.main()
