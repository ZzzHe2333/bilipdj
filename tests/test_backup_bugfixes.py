from __future__ import annotations

import importlib.util
import re
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
BACKUP_PATH = ROOT / "apps" / "server" / "settings_backup.py"
BUGFIX_PATH = ROOT / "apps" / "server" / "settings_backup_bugfix_guard.py"
TK_UI_PATH = ROOT / "apps" / "windows" / "webdav_backup_ui.py"
WEB_UI_PATH = ROOT / "apps" / "web" / "static" / "config.html"


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BackupBugfixGuardTests(unittest.TestCase):
    def test_backup_names_are_collision_resistant_and_legacy_names_still_match(self) -> None:
        backup = _load(BACKUP_PATH, "bilipdj_backup_bugfix_backup")
        guard = _load(BUGFIX_PATH, "bilipdj_backup_bugfix_guard")
        self.assertTrue(guard.install_settings_backup_bugfix_guard(backup))

        name1 = backup.SettingsBackupService._backup_name()
        time.sleep(0.001)
        name2 = backup.SettingsBackupService._backup_name()
        self.assertNotEqual(name1, name2)
        self.assertRegex(name1, r"^BiliPDJ-settings-\d{8}-\d{6}-\d{6}\.zip$")
        self.assertTrue(backup.BACKUP_NAME_RE.fullmatch(name1))
        self.assertTrue(backup.BACKUP_NAME_RE.fullmatch("BiliPDJ-settings-20260908-124100.zip"))
        self.assertIsNone(backup.BACKUP_NAME_RE.fullmatch("../BiliPDJ-settings-20260908-124100.zip"))

    def test_different_service_instances_share_one_operation_lock(self) -> None:
        guard = _load(BUGFIX_PATH, "bilipdj_backup_bugfix_guard_fake")
        active = 0
        max_active = 0
        counter_lock = threading.Lock()

        class FakeService:
            def __init__(self, _server=None) -> None:
                self._lock = threading.RLock()

            @staticmethod
            def _backup_name() -> str:
                return "old.zip"

            def backup_now(self):
                nonlocal active, max_active
                with counter_lock:
                    active += 1
                    max_active = max(max_active, active)
                time.sleep(0.03)
                with counter_lock:
                    active -= 1
                return "ok"

            def prune_backups(self, *args, **kwargs):
                return []

            def restore_remote(self, *args, **kwargs):
                return {"status": "ok"}

        fake_module = SimpleNamespace(
            SettingsBackupService=FakeService,
            BACKUP_NAME_RE=re.compile(r"^old$"),
        )
        self.assertTrue(guard.install_settings_backup_bugfix_guard(fake_module))
        one = FakeService(None)
        two = FakeService(None)
        self.assertIs(one._lock, two._lock)

        threads = [threading.Thread(target=service.backup_now) for service in (one, two)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)
        self.assertEqual(max_active, 1)


class BackupUiBugfixTests(unittest.TestCase):
    def test_tk_backup_is_a_scrollable_settings_subpage_not_root_row_50(self) -> None:
        source = TK_UI_PATH.read_text(encoding="utf-8")
        self.assertIn('add_page(notebook, "数据备份")', source)
        self.assertIn("_add_scrollable_settings_page", source)
        self.assertNotIn("box.grid(row=50", source)
        self.assertIn("_backup_list_signature", source)
        self.assertIn("备份目标或路径已变化，请先刷新备份列表再恢复", source)

    def test_tk_refresh_and_restore_sync_current_target(self) -> None:
        source = TK_UI_PATH.read_text(encoding="utf-8")
        refresh_start = source.index("def _refresh_backups")
        restore_start = source.index("def _restore_selected")
        self.assertIn("_save_config_sync(panel)", source[refresh_start:restore_start])
        self.assertIn("_save_config_sync(panel)", source[restore_start:])
        self.assertIn("%Y-%m-%d %H:%M:%S", source)
        self.assertIn("已保存密码（留空表示保持不变）", source)

    def test_web_refresh_and_restore_sync_and_verify_target_signature(self) -> None:
        source = WEB_UI_PATH.read_text(encoding="utf-8")
        self.assertIn("async function refresh(syncConfig=true){if(syncConfig)await saveConfig(true)", source)
        self.assertIn("listSignature!==targetSignature()", source)
        self.assertIn("await saveConfig(true);const out=await api(\"/restore\"", source)
        self.assertIn("function formatModified(value)", source)
        self.assertIn("minmax(0,1fr)", source)

    def test_bugfix_guard_is_installed_and_packaged_in_both_portables(self) -> None:
        server_init = (ROOT / "apps" / "server" / "__init__.py").read_text(encoding="utf-8")
        windows_spec = (ROOT / "apps" / "windows" / "bilipdj_onedir.spec").read_text(encoding="utf-8")
        web_spec = (ROOT / "apps" / "web" / "web_portable.spec").read_text(encoding="utf-8")
        self.assertIn("install_settings_backup_bugfix_guard(_settings_backup)", server_init)
        self.assertGreater(
            server_init.index("install_settings_backup_bugfix_guard(_settings_backup)"),
            server_init.index("install_settings_storage_guard(_settings_backup, server)"),
        )
        self.assertIn("apps.server.settings_backup_bugfix_guard", windows_spec)
        self.assertIn("apps.server.settings_backup_bugfix_guard", web_spec)


if __name__ == "__main__":
    unittest.main()
