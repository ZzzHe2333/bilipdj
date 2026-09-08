from __future__ import annotations

import importlib.util
import io
import os
import tempfile
import unittest
import zipfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
BACKUP_PATH = ROOT / "apps" / "server" / "settings_backup.py"
GUARD_PATH = ROOT / "apps" / "server" / "settings_mtime_guard.py"

backup_spec = importlib.util.spec_from_file_location("bilipdj_settings_backup_mtime_test", BACKUP_PATH)
assert backup_spec and backup_spec.loader
backup = importlib.util.module_from_spec(backup_spec)
backup_spec.loader.exec_module(backup)

guard_spec = importlib.util.spec_from_file_location("bilipdj_settings_mtime_guard_test", GUARD_PATH)
assert guard_spec and guard_spec.loader
guard = importlib.util.module_from_spec(guard_spec)
guard_spec.loader.exec_module(guard)

guard.install_settings_mtime_guard(backup)


class SettingsBackupMtimeTests(unittest.TestCase):
    def _server(self, root: Path) -> SimpleNamespace:
        return SimpleNamespace(
            _YAML_DIR=root,
            CONFIG_PATH=root / "config.yaml",
            QUANXIAN_PATH=root / "quanxian.yaml",
            KAIGUAN_PATH=root / "kaiguan.yaml",
            STYLE_PATH=root / "style.json",
        )

    def test_backup_zip_uses_source_file_mtime_and_extended_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = backup.SettingsBackupService(self._server(root))
            expected = 1_725_123_456
            for index, name in enumerate(backup.SETTINGS_FILES):
                path = root / name
                path.write_text(name, encoding="utf-8")
                os.utime(path, (expected + index, expected + index))

            data, included = service.build_settings_zip()
            self.assertEqual(set(included), set(backup.SETTINGS_FILES))

            with zipfile.ZipFile(io.BytesIO(data), "r") as archive:
                for index, name in enumerate(backup.SETTINGS_FILES):
                    info = archive.getinfo(name)
                    self.assertEqual(int(guard._read_extended_timestamp(info) or 0), expected + index)
                    self.assertEqual(
                        info.date_time[:5],
                        datetime.fromtimestamp(expected + index).timetuple()[:5],
                    )

    def test_restore_writes_back_recorded_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = backup.SettingsBackupService(self._server(root))
            expected = 1_725_223_456
            source = root / "config.yaml"
            source.write_text("before", encoding="utf-8")
            os.utime(source, (expected, expected))
            data, _ = service.build_settings_zip()

            source.write_text("changed", encoding="utf-8")
            os.utime(source, (expected + 5000, expected + 5000))
            service.restore_settings_zip(data)

            self.assertEqual(source.read_text(encoding="utf-8"), "before")
            self.assertLessEqual(abs(source.stat().st_mtime - expected), 1.0)

    def test_old_backup_without_extended_timestamp_still_restores(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = backup.SettingsBackupService(self._server(root))
            payload = io.BytesIO()
            date_time = (2025, 8, 20, 11, 22, 32)
            with zipfile.ZipFile(payload, "w") as archive:
                info = zipfile.ZipInfo("config.yaml", date_time=date_time)
                archive.writestr(info, b"legacy")

            service.restore_settings_zip(payload.getvalue())
            restored = root / "config.yaml"
            self.assertEqual(restored.read_bytes(), b"legacy")
            expected = datetime(*date_time).timestamp()
            self.assertLessEqual(abs(restored.stat().st_mtime - expected), 2.0)

    def test_restore_failure_rolls_back_original_content_and_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = backup.SettingsBackupService(self._server(root))
            original_mtime = 1_700_000_000
            for name in ("config.yaml", "quanxian.yaml"):
                path = root / name
                path.write_text("old-" + name, encoding="utf-8")
                os.utime(path, (original_mtime, original_mtime))

            payload = io.BytesIO()
            with zipfile.ZipFile(payload, "w") as archive:
                for name in ("config.yaml", "quanxian.yaml"):
                    info = zipfile.ZipInfo(name, date_time=(2026, 9, 8, 12, 0, 0))
                    info.extra = guard._extended_timestamp_extra(1_800_000_000)
                    archive.writestr(info, ("new-" + name).encode())

            real_atomic = backup._atomic_write_bytes
            failed = {"value": False}

            def flaky_atomic(path: Path, data: bytes) -> None:
                if path.name == "quanxian.yaml" and not failed["value"]:
                    failed["value"] = True
                    raise OSError("simulated write failure")
                real_atomic(path, data)

            with mock.patch.object(backup, "_atomic_write_bytes", side_effect=flaky_atomic):
                with self.assertRaises(OSError):
                    service.restore_settings_zip(payload.getvalue())

            for name in ("config.yaml", "quanxian.yaml"):
                path = root / name
                self.assertEqual(path.read_text(encoding="utf-8"), "old-" + name)
                self.assertLessEqual(abs(path.stat().st_mtime - original_mtime), 1.0)

    def test_server_init_installs_mtime_guard(self) -> None:
        source = (ROOT / "apps" / "server" / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("settings_mtime_guard", source)
        self.assertIn("install_settings_mtime_guard(_settings_backup)", source)


if __name__ == "__main__":
    unittest.main()
