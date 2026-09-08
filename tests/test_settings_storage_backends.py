from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
BACKUP_PATH = ROOT / "apps" / "server" / "settings_backup.py"
STORAGE_PATH = ROOT / "apps" / "server" / "settings_storage_guard.py"

BACKUP_SPEC = importlib.util.spec_from_file_location("bilipdj_settings_backup_storage_test", BACKUP_PATH)
assert BACKUP_SPEC and BACKUP_SPEC.loader
backup = importlib.util.module_from_spec(BACKUP_SPEC)
BACKUP_SPEC.loader.exec_module(backup)

STORAGE_SPEC = importlib.util.spec_from_file_location("bilipdj_settings_storage_guard_test", STORAGE_PATH)
assert STORAGE_SPEC and STORAGE_SPEC.loader
storage = importlib.util.module_from_spec(STORAGE_SPEC)
STORAGE_SPEC.loader.exec_module(storage)
storage.install_settings_storage_guard(backup)


class SettingsStorageBackendTests(unittest.TestCase):
    def _server(self, root: Path) -> SimpleNamespace:
        return SimpleNamespace(
            _YAML_DIR=root,
            CONFIG_PATH=root / "config.yaml",
            QUANXIAN_PATH=root / "quanxian.yaml",
            KAIGUAN_PATH=root / "kaiguan.yaml",
            STYLE_PATH=root / "style.json",
        )

    def _write_settings(self, root: Path, prefix: str = "old") -> None:
        for name in backup.SETTINGS_FILES:
            (root / name).write_text(f"{prefix}-{name}\n", encoding="utf-8")

    def test_local_folder_backup_uses_dedicated_subdirectory_and_restores(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "runtime"
            destination_parent = Path(tmp) / "user-backups"
            root.mkdir()
            self._write_settings(root, "before")
            service = backup.SettingsBackupService(self._server(root))
            service.save_config({"backend": "local", "local_dir": str(destination_parent), "keep_last": 10})

            with mock.patch.object(service, "_backup_name", return_value="BiliPDJ-settings-20260908-110000.zip"):
                result = service.backup_now()

            target = destination_parent / "BiliPDJ_Backup"
            self.assertEqual(result["backend"], "local")
            self.assertTrue((target / result["name"]).is_file())
            self.assertEqual([item["name"] for item in service.list_backups()], [result["name"]])

            (root / "config.yaml").write_text("changed\n", encoding="utf-8")
            restored = service.restore_remote(result["name"])
            self.assertIn("config.yaml", restored["restored"])
            self.assertEqual((root / "config.yaml").read_text(encoding="utf-8"), "before-config.yaml\n")

    def test_retention_only_removes_managed_backup_zip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "runtime"
            parent = Path(tmp) / "backup-root"
            root.mkdir()
            self._write_settings(root)
            service = backup.SettingsBackupService(self._server(root))
            service.save_config({"backend": "local", "local_dir": str(parent), "keep_last": 2})
            target = parent / "BiliPDJ_Backup"
            target.mkdir(parents=True)
            (target / "family-photo.zip").write_bytes(b"do not delete")
            (target / "notes.txt").write_text("user data", encoding="utf-8")

            names = [
                "BiliPDJ-settings-20260908-110000.zip",
                "BiliPDJ-settings-20260908-110100.zip",
                "BiliPDJ-settings-20260908-110200.zip",
            ]
            for name in names:
                (target / name).write_bytes(b"zip-placeholder")

            removed = service.prune_backups()
            self.assertEqual(removed, [names[0]])
            self.assertTrue((target / "family-photo.zip").exists())
            self.assertTrue((target / "notes.txt").exists())
            self.assertFalse((target / names[0]).exists())
            self.assertTrue((target / names[1]).exists())
            self.assertTrue((target / names[2]).exists())

    def test_smb_nas_uses_os_mounted_path_without_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "runtime"
            nas_mount = Path(tmp) / "nas-mount" / "BiliPDJ"
            root.mkdir()
            self._write_settings(root)
            service = backup.SettingsBackupService(self._server(root))
            public = service.save_config({"backend": "smb", "smb_path": str(nas_mount), "keep_last": 5})
            self.assertEqual(public["backend"], "smb")
            self.assertEqual(public["smb_path"], str(nas_mount))

            test_result = service.test_connection()
            self.assertEqual(test_result["backend"], "smb")
            self.assertTrue(nas_mount.is_dir())

            with mock.patch.object(service, "_backup_name", return_value="BiliPDJ-settings-20260908-112000.zip"):
                result = service.backup_now()
            self.assertTrue((nas_mount / result["name"]).is_file())

            raw = json.loads((root / "webdav_backup.json").read_text(encoding="utf-8"))
            self.assertNotIn("smb_username", raw)
            self.assertNotIn("smb_password", raw)
            self.assertNotIn("nas_password", raw)

    def test_filesystem_target_requires_absolute_path(self) -> None:
        with self.assertRaises(ValueError):
            storage._filesystem_path("relative/backup", backend="local")
        with self.assertRaises(ValueError):
            storage._filesystem_path("", backend="smb")

    def test_webdav_remains_default_for_old_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "webdav_backup.json").write_text(
                json.dumps({"url": "https://dav.example.test/root", "remote_dir": "BiliPDJ_Backup"}),
                encoding="utf-8",
            )
            cfg = backup.SettingsBackupService(self._server(root)).load_config(include_password=True)
            self.assertEqual(cfg["backend"], "webdav")
            self.assertEqual(cfg["url"], "https://dav.example.test/root")


class SettingsStorageWiringTests(unittest.TestCase):
    def test_server_installs_storage_guard_after_mtime_guard(self) -> None:
        source = (ROOT / "apps" / "server" / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("install_settings_storage_guard", source)
        self.assertLess(source.index("install_settings_mtime_guard"), source.index("install_settings_storage_guard"))

    def test_unified_api_and_frontends_offer_all_three_backends(self) -> None:
        server_source = STORAGE_PATH.read_text(encoding="utf-8")
        windows_source = (ROOT / "apps" / "windows" / "webdav_backup_ui.py").read_text(encoding="utf-8")
        web_source = (ROOT / "apps" / "web" / "static" / "config.html").read_text(encoding="utf-8")
        for route in (
            "/api/backup/settings/config",
            "/api/backup/settings/list",
            "/api/backup/settings/test",
            "/api/backup/settings/run",
            "/api/backup/settings/restore",
        ):
            self.assertIn(route, server_source)
        for text in ("WebDAV", "本地文件夹", "SMB / NAS"):
            self.assertIn(text, windows_source)
            self.assertIn(text, web_source)
        self.assertIn("程序不保存 NAS 密码", windows_source)
        self.assertIn("BiliPDJ 不保存 NAS 用户名或密码", web_source)

    def test_portable_specs_bundle_storage_guard(self) -> None:
        windows_spec = (ROOT / "apps" / "windows" / "bilipdj_onedir.spec").read_text(encoding="utf-8")
        web_spec = (ROOT / "apps" / "web" / "web_portable.spec").read_text(encoding="utf-8")
        self.assertIn("apps.server.settings_storage_guard", windows_spec)
        self.assertIn("apps.server.settings_storage_guard", web_spec)


if __name__ == "__main__":
    unittest.main()
