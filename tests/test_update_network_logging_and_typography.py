from __future__ import annotations

import datetime as dt
import os
import tempfile
import time
import types
import unittest
from pathlib import Path

from core import log_manager, update_network
from core.style_option_guard import STYLE_OPTION_DEFAULTS, patch_style_module
from core.updater_gui import PIECE_COLUMNS, progress_for_message, select_update_log_root


ROOT = Path(__file__).resolve().parents[1]


class UpdateNetworkTests(unittest.TestCase):
    def test_defaults_are_disabled_and_modes_are_mutually_exclusive(self) -> None:
        defaults = update_network.normalize_update_network({})
        self.assertFalse(defaults["bypass_system_proxy"])
        self.assertFalse(defaults["use_third_party_proxy"])
        self.assertFalse(defaults["use_mirrorchyan"])

        explicit = update_network.normalize_update_network(
            {
                "bypass_system_proxy": True,
                "use_third_party_proxy": True,
                "proxy_host": "127.0.0.1",
                "proxy_port": "7890",
            }
        )
        self.assertFalse(explicit["bypass_system_proxy"])
        self.assertTrue(explicit["use_third_party_proxy"])
        self.assertEqual(explicit["proxy_port"], "7890")

    def test_settings_round_trip_in_preserved_core_cd_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            saved = update_network.save_update_network(
                {
                    "use_third_party_proxy": True,
                    "proxy_host": "127.0.0.1",
                    "proxy_port": "7890",
                },
                root,
            )
            self.assertEqual(saved, update_network.load_update_network(root))
            self.assertEqual(
                update_network.settings_path(root),
                root / "core" / "cd" / "update_settings.json",
            )

    def test_bypass_opener_does_not_use_system_proxy_mapping(self) -> None:
        opener = update_network.build_opener({"bypass_system_proxy": True})
        handlers = [handler for handler in opener.handlers if handler.__class__.__name__ == "ProxyHandler"]
        self.assertEqual(len(handlers), 1)
        self.assertEqual(getattr(handlers[0], "proxies", {}), {})


class CategorizedLogTests(unittest.TestCase):
    def test_daily_names_use_kind_date_and_room(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            when = dt.datetime(2026, 7, 30, 12, 54, 32)
            common = log_manager.daily_log_path("common", "125432", Path(temp_dir), when=when)
            error = log_manager.daily_log_path("error", "", Path(temp_dir), when=when)
            update = log_manager.daily_log_path("update", "3049445", Path(temp_dir), when=when)
            self.assertEqual(common.name, "common_20260730_125432.log")
            self.assertEqual(error.name, "error_20260730_unknow.log")
            self.assertEqual(update.name, "update_20260730_3049445.log")

    def test_room_token_uses_active_platform(self) -> None:
        self.assertEqual(
            log_manager.room_token_from_config({"platform": "bilibili", "bilibili": {"roomid": 123}}),
            "123",
        )
        self.assertEqual(
            log_manager.room_token_from_config({"platform": "douyin", "douyin": {"live_id": "abc-456"}}),
            "abc-456",
        )
        self.assertEqual(log_manager.room_token_from_config({}), "unknow")

    def test_cleanup_removes_only_expired_managed_logs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            directory = root / "log"
            directory.mkdir()
            old = directory / "common_20260101_1.log"
            fresh = directory / "error_20260730_1.log"
            unrelated = directory / "notes.log"
            for path in (old, fresh, unrelated):
                path.write_text("x", encoding="utf-8")
            old_time = time.time() - 40 * 86400
            os.utime(old, (old_time, old_time))
            deleted = log_manager.cleanup_logs(root, 31)
            self.assertEqual(deleted, 1)
            self.assertFalse(old.exists())
            self.assertTrue(fresh.exists())
            self.assertTrue(unrelated.exists())


class TypographyTests(unittest.TestCase):
    def test_style_guard_preserves_options_and_generates_css(self) -> None:
        module = types.SimpleNamespace()
        module.DEFAULT_STYLE = {"queue_font_size": 50}
        module.DEFAULT_CONFIG = {"style": {}}
        module.state = {"queue_font_size": 50}
        module.load_style = lambda: dict(module.state)

        def save_style(payload):
            module.state = dict(payload)

        def build_index_css(_payload=None):
            return ":root {\n    --queue-font-size: 50px;\n}\n"

        module.save_style = save_style
        module.build_index_css = build_index_css
        self.assertTrue(patch_style_module(module))
        module.save_style({"queue_letter_spacing": 4})
        self.assertEqual(module.state["queue_letter_spacing"], 4)
        for key in STYLE_OPTION_DEFAULTS:
            self.assertIn(key, module.state)
        css = module.build_index_css(module.state)
        self.assertIn("--queue-letter-spacing: 4px", css)
        self.assertIn("--queue-line-height:", css)
        self.assertIn(".queue-content", css)

    def test_bundled_css_exposes_advanced_variables(self) -> None:
        source = (ROOT / "core" / "ui" / "moren.css").read_text(encoding="utf-8")
        for variable in (
            "--queue-font-family",
            "--queue-letter-spacing",
            "--queue-word-spacing",
            "--queue-line-height",
            "--queue-item-gap",
            "--queue-text-align",
            "--queue-text-opacity",
        ):
            self.assertIn(variable, source)


class UpdaterAnimationTests(unittest.TestCase):
    def test_tetris_sequence_is_fixed_and_fills_two_rows(self) -> None:
        self.assertEqual(PIECE_COLUMNS, (4, 0, 8, 2, 6))
        occupied = set()
        for column in PIECE_COLUMNS:
            occupied.update((column + dx, 8 + dy) for dx, dy in ((0, 0), (1, 0), (0, 1), (1, 1)))
        self.assertEqual(occupied, {(column, row) for column in range(10) for row in (8, 9)})

    def test_update_progress_follows_fixed_stages(self) -> None:
        self.assertEqual(progress_for_message("解压更新包", 3), 22)
        self.assertEqual(progress_for_message("备份当前版本", 22), 46)
        self.assertEqual(progress_for_message("启动成功", 90), 100)

    def test_log_root_follows_directory_rename(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            app = root / "app"
            backup = root / ".app.update-backup"
            app.mkdir()
            self.assertEqual(select_update_log_root(app), app)
            app.rename(backup)
            self.assertEqual(select_update_log_root(app), backup)


class SourceWiringTests(unittest.TestCase):
    def test_update_page_contains_requested_controls(self) -> None:
        source = (ROOT / "core" / "update_page.py").read_text(encoding="utf-8")
        self.assertIn('text="绕过系统代理"', source)
        self.assertIn('text="使用第三方代理"', source)
        self.assertIn('text="检测连接"', source)
        self.assertIn('text="使用 Mirror酱更新"', source)
        self.assertIn("暂未接入", source)

    def test_about_page_no_longer_builds_update_controls(self) -> None:
        source = (ROOT / "core" / "update_page.py").read_text(encoding="utf-8")
        about = source.split("def build_about_tab", 1)[1]
        self.assertNotIn("检查更新", about)
        self.assertNotIn("下载并安装", about)

    def test_style_save_is_background_and_overlay_restart_is_suppressed(self) -> None:
        source = (ROOT / "core" / "control_panel_features.py").read_text(encoding="utf-8")
        self.assertIn('name="bilipdj-style-save"', source)
        self.assertIn("_bilipdj_suppress_overlay_restart", source)
        self.assertIn("_bilipdj_force_sync_style_save", source)


if __name__ == "__main__":
    unittest.main()
