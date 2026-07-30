from __future__ import annotations

import importlib.util
import io
import json
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


class _Response:
    def __init__(self, data: bytes, content_length: str = "") -> None:
        self._stream = io.BytesIO(data)
        self.headers = {"Content-Length": content_length}

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


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

    def test_download_rejects_truncated_content_length(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            destination = Path(temp_dir) / "asset.zip"
            with mock.patch.object(
                update_client,
                "_request",
                return_value=_Response(b"abc", "9"),
            ):
                with self.assertRaises(update_client.UpdateError):
                    update_client.download_file(
                        "https://example.invalid/asset.zip",
                        destination,
                    )
            self.assertFalse(destination.exists())
            self.assertFalse(destination.with_suffix(".zip.part").exists())

    def test_invalid_asset_size_is_wrapped(self) -> None:
        payload = {
            "tag_name": "v1.2.3",
            "assets": [
                {
                    "name": "bilibili-danmuji-windows-x64-v1.2.3.zip",
                    "browser_download_url": "https://example.invalid/a",
                    "size": "not-a-number",
                }
            ],
        }
        response = _Response(json.dumps(payload).encode("utf-8"))
        with mock.patch.object(update_client, "_request", return_value=response):
            with self.assertRaises(update_client.UpdateError):
                update_client.fetch_latest_release()

    def test_owned_work_dir_is_removed_after_download_failure(self) -> None:
        release = update_client.ReleaseInfo(
            version="1.2.3",
            tag_name="v1.2.3",
            name="test",
            body="",
            page_url="",
            zip_asset=update_client.ReleaseAsset(
                "package.zip",
                "https://example.invalid/package.zip",
                10,
            ),
            checksum_asset=update_client.ReleaseAsset(
                "package.zip.sha256",
                "https://example.invalid/package.zip.sha256",
                10,
            ),
        )
        created: list[Path] = []

        def fake_mkdtemp(*, prefix: str):
            path = Path(tempfile.gettempdir()) / f"{prefix}cleanup-test"
            if path.exists():
                import shutil

                shutil.rmtree(path)
            path.mkdir()
            created.append(path)
            return str(path)

        with (
            mock.patch.object(update_client.tempfile, "mkdtemp", side_effect=fake_mkdtemp),
            mock.patch.object(
                update_client,
                "download_file",
                side_effect=update_client.UpdateError("failed"),
            ),
        ):
            with self.assertRaises(update_client.UpdateError):
                update_client.prepare_release_download(release)

        self.assertEqual(len(created), 1)
        self.assertFalse(created[0].exists())


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

    def test_safe_extract_rejects_case_collisions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            package = root / "duplicate.zip"
            with zipfile.ZipFile(package, "w") as archive:
                archive.writestr("main.exe", b"a")
                archive.writestr("MAIN.EXE", b"b")
            with self.assertRaises(updater.UpdaterError):
                updater.safe_extract(package, root / "out")

    def test_unsafe_main_executable_names_are_rejected(self) -> None:
        for value in (
            "",
            ".",
            "..",
            "../outside.exe",
            "sub/main.exe",
            r"sub\main.exe",
            r"C:\outside.exe",
        ):
            with self.subTest(value=value):
                with self.assertRaises(updater.UpdaterError):
                    updater.validate_executable_name(value)
        self.assertEqual(updater.validate_executable_name("main.exe"), "main.exe")

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
            result = json.loads((app_dir / "update-result.json").read_text(encoding="utf-8"))
            self.assertEqual(result["version"], "1.0.8")
            self.assertEqual(result["status"], "installed")
            self.assertEqual(result["cleanup_dir"], str(root))

    def test_failed_new_version_rolls_back_and_restarts_old_directory(self) -> None:
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
                mock.patch.object(
                    updater,
                    "launch_main",
                    side_effect=lambda *_: next(launch_results),
                ) as launch_mock,
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

            self.assertEqual(launch_mock.call_count, 2)
            self.assertEqual((app_dir / "main.exe").read_bytes(), b"old-main")
            result = json.loads((app_dir / "update-result.json").read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "rolled_back")
            self.assertEqual(result["cleanup_dir"], str(root))

    def test_preflight_failure_restarts_untouched_old_version(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            app_dir = root / "bilipdj"
            app_dir.mkdir()
            (app_dir / "main.exe").write_bytes(b"old-main")
            package = root / "invalid-update.zip"
            with zipfile.ZipFile(package, "w") as archive:
                archive.writestr("readme.txt", b"missing executables")

            with (
                mock.patch.object(updater, "wait_for_process_exit", return_value=True),
                mock.patch.object(
                    updater,
                    "launch_main",
                    return_value=FakeRunningProcess(),
                ) as launch_mock,
            ):
                with self.assertRaises(updater.UpdaterError):
                    updater.perform_update(
                        pid=123,
                        app_dir=app_dir,
                        zip_path=package,
                        main_exe_name="main.exe",
                        target_version="1.0.8",
                    )

            self.assertEqual(launch_mock.call_count, 1)
            self.assertEqual((app_dir / "main.exe").read_bytes(), b"old-main")
            result = json.loads((app_dir / "update-result.json").read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "preflight_failed")
            self.assertIn("缺少主程序", result["error"])


if __name__ == "__main__":
    unittest.main()
