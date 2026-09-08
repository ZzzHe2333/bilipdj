from __future__ import annotations

import importlib.util
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "apps" / "server" / "settings_backup.py"
SPEC = importlib.util.spec_from_file_location("bilipdj_settings_backup_under_test", MODULE_PATH)
assert SPEC and SPEC.loader
backup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(backup)


class SettingsBackupServiceTests(unittest.TestCase):
    def _server(self, root: Path) -> SimpleNamespace:
        return SimpleNamespace(
            _YAML_DIR=root,
            CONFIG_PATH=root / "config.yaml",
            QUANXIAN_PATH=root / "quanxian.yaml",
            KAIGUAN_PATH=root / "kaiguan.yaml",
            STYLE_PATH=root / "style.json",
        )

    def test_settings_zip_contains_only_four_allowed_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            server = self._server(root)
            for name in backup.SETTINGS_FILES:
                (root / name).write_text(f"{name}: yes\n", encoding="utf-8")
            (root / "core" / "cd").mkdir(parents=True)
            (root / "core" / "cd" / "blacklist.csv").write_text("secret queue data", encoding="utf-8")
            (root / "log").mkdir()
            (root / "log" / "server.log").write_text("log", encoding="utf-8")

            data, included = backup.SettingsBackupService(server).build_settings_zip()
            self.assertEqual(set(included), set(backup.SETTINGS_FILES))
            with zipfile.ZipFile(io.BytesIO(data), "r") as archive:
                self.assertEqual(set(archive.namelist()), set(backup.SETTINGS_FILES))
                self.assertNotIn("blacklist.csv", archive.namelist())
                self.assertNotIn("server.log", archive.namelist())

    def test_webdav_password_is_stored_separately_and_redacted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = backup.SettingsBackupService(self._server(root))
            public = service.save_config(
                {
                    "url": "https://dav.example.test/root",
                    "username": "alice",
                    "password": "very-secret",
                    "remote_dir": "BiliPDJ_Backup/settings",
                    "auto_on_start": True,
                    "auto_on_exit": True,
                    "keep_last": 12,
                }
            )
            self.assertNotIn("password", public)
            self.assertTrue(public["password_set"])
            stored = json.loads((root / "webdav_backup.json").read_text(encoding="utf-8"))
            self.assertEqual(stored["password"], "very-secret")
            self.assertNotIn("webdav_backup.json", backup.SETTINGS_FILES)

    def test_restore_rejects_unexpected_files_and_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = backup.SettingsBackupService(self._server(Path(tmp)))
            for bad_name in ("../config.yaml", "core/cd/blacklist.csv", "log/server.log", "webdav_backup.json"):
                payload = io.BytesIO()
                with zipfile.ZipFile(payload, "w") as archive:
                    archive.writestr(bad_name, b"x")
                with self.subTest(bad_name=bad_name):
                    with self.assertRaises(backup.SettingsBackupError):
                        service.validate_settings_zip(payload.getvalue())

    def test_restore_failure_rolls_back_partial_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            server = self._server(root)
            originals = {
                "config.yaml": b"old-config",
                "quanxian.yaml": b"old-quanxian",
                "kaiguan.yaml": b"old-kaiguan",
                "style.json": b"old-style",
            }
            for name, data in originals.items():
                (root / name).write_bytes(data)

            payload = io.BytesIO()
            with zipfile.ZipFile(payload, "w") as archive:
                for name in backup.SETTINGS_FILES:
                    archive.writestr(name, f"new-{name}".encode())

            real_atomic = backup._atomic_write_bytes
            failed = {"value": False}

            def flaky_atomic(path: Path, data: bytes) -> None:
                if path.name == "kaiguan.yaml" and not failed["value"]:
                    failed["value"] = True
                    raise OSError("simulated write failure")
                real_atomic(path, data)

            service = backup.SettingsBackupService(server)
            with mock.patch.object(backup, "_atomic_write_bytes", side_effect=flaky_atomic):
                with self.assertRaises(OSError):
                    service.restore_settings_zip(payload.getvalue())

            for name, data in originals.items():
                self.assertEqual((root / name).read_bytes(), data)
            self.assertFalse((root / ".settings-restore-safety.zip").exists())

    def test_remote_backup_name_is_strict(self) -> None:
        self.assertTrue(backup.BACKUP_NAME_RE.fullmatch("BiliPDJ-settings-20260908-170000.zip"))
        for bad in ("../x.zip", "settings.zip", "BiliPDJ-settings-20260908.zip"):
            self.assertIsNone(backup.BACKUP_NAME_RE.fullmatch(bad))


class SettingsBackupWiringTests(unittest.TestCase):
    def test_server_api_and_auto_backup_hooks_are_installed(self) -> None:
        source = MODULE_PATH.read_text(encoding="utf-8")
        for route in (
            "/api/backup/webdav/config",
            "/api/backup/webdav/list",
            "/api/backup/webdav/test",
            "/api/backup/webdav/run",
            "/api/backup/webdav/restore",
        ):
            self.assertIn(route, source)
        self.assertIn('"auto_on_start"', source)
        self.assertIn('"auto_on_exit"', source)
        init_source = (ROOT / "apps" / "server" / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("install_settings_backup(server)", init_source)

    def test_windows_and_web_expose_backup_controls(self) -> None:
        bootstrap = (ROOT / "apps" / "windows" / "control_panel_bootstrap.py").read_text(encoding="utf-8")
        windows_ui = (ROOT / "apps" / "windows" / "webdav_backup_ui.py").read_text(encoding="utf-8")
        web_ui = (ROOT / "apps" / "web" / "static" / "config.html").read_text(encoding="utf-8")
        self.assertIn("patch_control_panel_webdav_backup", bootstrap)
        self.assertIn("立即备份设置", windows_ui)
        self.assertIn("恢复选中设置", windows_ui)
        self.assertIn("WebDAV 设置备份", web_ui)
        self.assertIn("/api/backup/webdav/run", web_ui)
        self.assertIn("/api/backup/webdav/restore", web_ui)

    def test_portable_specs_include_backup_modules(self) -> None:
        windows_spec = (ROOT / "apps" / "windows" / "bilipdj_onedir.spec").read_text(encoding="utf-8")
        web_spec = (ROOT / "apps" / "web" / "web_portable.spec").read_text(encoding="utf-8")
        self.assertIn("apps.server.settings_backup", windows_spec)
        self.assertIn("apps.windows.webdav_backup_ui", windows_spec)
        self.assertIn("apps.server.settings_backup", web_spec)


if __name__ == "__main__":
    unittest.main()
