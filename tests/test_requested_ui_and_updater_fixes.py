from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from core.control_panel_guard import patch_control_panel_class
from core.updater import UpdaterError
from core.updater_v2 import replace_path_with_retry


ROOT = Path(__file__).resolve().parents[1]


class RequestedUiLayoutTests(unittest.TestCase):
    def test_login_page_no_longer_contains_display_switches(self) -> None:
        source = (ROOT / "core" / "ui" / "config.html").read_text(encoding="utf-8")
        self.assertNotIn('id="auto-scroll-switch"', source)
        self.assertNotIn('id="sequence-switch"', source)
        self.assertNotIn("loadDisplayStyle", source)
        self.assertIn("已移至桌面程序的“透明窗口”页面", source)

    def test_queue_page_contains_only_queue_container(self) -> None:
        source = (ROOT / "core" / "ui" / "index.html").read_text(encoding="utf-8")
        body = source.split("<body>", 1)[1].split("</body>", 1)[0]
        self.assertNotIn("LIVE QUEUE", body)
        self.assertNotIn("弹幕排队", body)
        self.assertNotIn("实时连接", body)
        self.assertNotIn("connectionBadge", body)
        self.assertNotIn("emptyState", body)
        self.assertIn('id="danmu"', body)

    def test_overlay_controls_are_defined_in_desktop_guard(self) -> None:
        source = (ROOT / "core" / "control_panel_guard.py").read_text(encoding="utf-8")
        self.assertIn('text="滚动设置"', source)
        self.assertIn('text="自动滚动"', source)
        self.assertIn('text="序号"', source)
        self.assertIn('text="显示序号"', source)


class ThemePersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module_name = "test_control_panel_theme_module"
        module = types.ModuleType(self.module_name)
        module.__file__ = "/tmp/control_panel.py"

        class Backend:
            def __init__(self) -> None:
                self.config = {"ui": {}}

            def load_config(self):
                return {"ui": dict(self.config.get("ui", {}))}

            def save_config(self, payload):
                self.config = payload

        backend = Backend()
        module.backend = backend
        module.load_backend_server_module = lambda: backend
        module.load_simple_yaml = lambda _path: {}
        module.CONFIG_PATH = Path("config.yaml")
        sys.modules[self.module_name] = module
        self.module = module

        class ControlPanelApp:
            def _build_ui(self):
                return None

            def _build_overlay_tab(self, _frame):
                return None

            def _toggle_theme(self):
                self._dark_mode = not self._dark_mode

            def load_from_file(self):
                return None

            def gather_config(self):
                return {"ui": {"language": "中文"}}

        ControlPanelApp.__module__ = self.module_name
        self.ControlPanelApp = ControlPanelApp
        self.assertTrue(patch_control_panel_class(ControlPanelApp))

    def tearDown(self) -> None:
        sys.modules.pop(self.module_name, None)

    def test_theme_is_included_in_normal_config_save(self) -> None:
        panel = self.ControlPanelApp()
        panel._dark_mode = False
        payload = panel.gather_config()
        self.assertEqual(payload["ui"]["theme"], "light")
        panel._dark_mode = True
        self.assertEqual(panel.gather_config()["ui"]["theme"], "dark")

    def test_toggle_persists_theme_immediately(self) -> None:
        panel = self.ControlPanelApp()
        panel._dark_mode = False
        panel._toggle_theme()
        self.assertEqual(self.module.backend.config["ui"]["theme"], "dark")
        panel._toggle_theme()
        self.assertEqual(self.module.backend.config["ui"]["theme"], "light")


class UpdaterDirectoryRetryTests(unittest.TestCase):
    def test_directory_move_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "current"
            destination = root / "backup"
            source.mkdir()
            (source / "main.exe").write_bytes(b"test")
            replace_path_with_retry(source, destination, attempts=2, delay=0.01)
            self.assertFalse(source.exists())
            self.assertTrue((destination / "main.exe").is_file())

    def test_persistent_access_denied_has_clear_error(self) -> None:
        with mock.patch.object(Path, "replace", side_effect=PermissionError(13, "denied")):
            with self.assertRaisesRegex(UpdaterError, "后端、透明窗口和杀毒软件"):
                replace_path_with_retry(
                    Path("current"),
                    Path("backup"),
                    attempts=2,
                    delay=0.01,
                )

    def test_updater_spec_uses_resilient_entry(self) -> None:
        source = (ROOT / "updater.spec").read_text(encoding="utf-8")
        self.assertIn('"core/updater_v2.py"', source)


if __name__ == "__main__":
    unittest.main()
