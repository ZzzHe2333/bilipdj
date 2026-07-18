from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


update_client = load_module("update_client_under_test", ROOT / "core" / "update_client.py")
updater = load_module("updater_under_test", ROOT / "core" / "updater.py")


class FakeRunningProcess:
    def poll(self):
        return None


class FakeFailedProcess:
    def poll(self):
        return 17


class UpdateClientTests(unittest.TestCase):
    def test_semantic_version_comparison(self) -> None:
        self.assertTrue(update_client.is_newer_version("v1.0.10", "1.0.9"))
        self.assertFalse(update_client.is_newer_version("1.0.7", "v1.0.7"))
        self.assertFalse(update_client.is_newer_version("1.0.6", "1.0.7"))

    def test_checksum_parsing_and_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload = root / "package.zip"
            payload.write_bytes(b"test package")
            digest = update_client.calculate_sha256(payload)
            checksum = root / "package.zip.sha256"
            checksum.write_text(f"{digest}  package.zip\n", encoding="utf-8")
            self.assertEqual(update_client.parse_checksum_file(checksum, payload.name), digest)
            self.assertEqual(update_client.verify_sha256(payload, digest), digest)


class UpdaterTests(unittest.TestCase):
    def _create_package(self, path: Path) -> None:
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("main.exe", b"new-main")
            archive.writestr("updater.exe", b"new-updater")
            archive.writestr("_internal/new-runtime.dll", b"runtime")
            archive.writestr("config.yaml", b"default-new-config")

    def test_safe_extract_rejects_parent_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            package = root / "bad.zip"
            with zipfile.ZipFile(package, "w") as archive:
                archive.writestr("../escape.txt", b"bad")
            with self.assertRaises(updater.UpdaterError):
                updater.safe_extract(package, root / "out")

    def test_update_preserves_runtime_data_and_keeps_backup(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            app_dir = root / "bilipdj"
            app_dir.mkdir()
            (app_dir / "main.exe").write_bytes(b"old-main")
            (app_dir / "config.yaml").write_text("user-config", encoding="utf-8")
            (app_dir / "core" / "cd").mkdir(parents=True)
            (app_dir / "core" / "cd" / "queue.csv").write_text("queue", encoding="utf-8")
            package = root / "update.zip"
            self._create_package(package)

            with (
                mock.patch.object(updater, "wait_for_process_exit", return_value=True),
                mock.patch.object(updater, "launch_main", return_value=FakeRunningProcess()),
                mock.patch.object(updater, "STARTUP_GRACE_SECONDS", 0.01),
            ):
                updater.perform_update(
                    pid=123,
                    app_dir=app_dir,
                    zip_path=package,
                    main_exe_name="main.exe",
                    target_version="1.0.8",
                )

            self.assertEqual((app_dir / "main.exe").read_bytes(), b"new-main")
            self.assertEqual((app_dir / "config.yaml").read_text(encoding="utf-8"), "user-config")
            self.assertTrue((app_dir / "core" / "cd" / "queue.csv").is_file())
            self.assertTrue((root / ".bilipdj.update-backup" / "main.exe").is_file())
            self.assertEqual(
                __import__("json").loads((app_dir / "update-result.json").read_text(encoding="utf-8"))["version"],
                "1.0.8",
            )

    def test_failed_new_version_rolls_back_old_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            app_dir = root / "bilipdj"
            app_dir.mkdir()
            (app_dir / "main.exe").write_bytes(b"old-main")
            package = root / "update.zip"
            self._create_package(package)

            launch_results = iter([FakeFailedProcess(), FakeRunningProcess()])
            with (
                mock.patch.object(updater, "wait_for_process_exit", return_value=True),
                mock.patch.object(updater, "launch_main", side_effect=lambda *_: next(launch_results)),
                mock.patch.object(updater, "STARTUP_GRACE_SECONDS", 0.01),
            ):
                with self.assertRaises(updater.UpdaterError):
                    updater.perform_update(
                        pid=123,
                        app_dir=app_dir,
                        zip_path=package,
                        main_exe_name="main.exe",
                        target_version="1.0.8",
                    )

            self.assertEqual((app_dir / "main.exe").read_bytes(), b"old-main")


if __name__ == "__main__":
    unittest.main()
