from __future__ import annotations

import tempfile
from pathlib import Path


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def check_appdata_sync() -> None:
    from apps.server.local_data_archive import SYNC_MARKER_RELATIVE, sync_local_data_archive

    # Existing portable data with no roaming archive: portable -> AppData.
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        app = root / "app"
        archive = root / "roaming" / "bilipdj"
        _write(app / "core/config.yaml", "local-config\n")
        _write(app / "core/cd/queue_archive_slot_1.csv", "local-queue\n")
        result = sync_local_data_archive(app, archive_root=archive, enabled=True)
        assert result["mode"] == "mirror"
        assert (archive / "core/config.yaml").read_text(encoding="utf-8") == "local-config\n"
        assert (archive / "core/cd/queue_archive_slot_1.csv").read_text(encoding="utf-8") == "local-queue\n"
        assert (app / SYNC_MARKER_RELATIVE).is_file()

        # Both exist on a known installation: local/core is authoritative and
        # the archive directory is replaced exactly, including deletion state.
        _write(app / "core/config.yaml", "local-new\n")
        _write(archive / "core/config.yaml", "archive-old\n")
        _write(archive / "core/cd/stale.csv", "stale\n")
        sync_local_data_archive(app, archive_root=archive, enabled=True)
        assert (archive / "core/config.yaml").read_text(encoding="utf-8") == "local-new\n"
        assert not (archive / "core/cd/stale.csv").exists()

    # Fresh/reinstalled app: archive must beat bundled/default core data.
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        app = root / "fresh-app"
        archive = root / "roaming" / "bilipdj"
        _write(app / "core/config.yaml", "new-default\n")
        _write(app / "core/cd/queue_archive_slot_1.csv", "new-empty-ish\n")
        _write(archive / "core/config.yaml", "old-user-config\n")
        _write(archive / "core/cd/queue_archive_slot_1.csv", "old-user-queue\n")
        result = sync_local_data_archive(app, archive_root=archive, enabled=True)
        assert result["mode"] == "restore"
        assert (app / "core/config.yaml").read_text(encoding="utf-8") == "old-user-config\n"
        assert (app / "core/cd/queue_archive_slot_1.csv").read_text(encoding="utf-8") == "old-user-queue\n"
        assert (app / SYNC_MARKER_RELATIVE).is_file()

    # Per-item recovery: a missing local file is restored even on a known install.
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        app = root / "app"
        archive = root / "roaming" / "bilipdj"
        _write(app / SYNC_MARKER_RELATIVE, "1\n")
        _write(archive / "core/config.yaml", "saved-only\n")
        sync_local_data_archive(app, archive_root=archive, enabled=True)
        assert (app / "core/config.yaml").read_text(encoding="utf-8") == "saved-only\n"


def check_backup_cleanup() -> None:
    from apps.windows.legacy_backup_ui import collect_backup_packages, remove_backup_package

    with tempfile.TemporaryDirectory() as temp:
        app = Path(temp) / "app"
        snapshot = app / "backup" / "update-20260913-120000-to-v3.0.15-test"
        _write(snapshot / "VERSION", "3.0.14-test\n")
        (snapshot / "main.exe").write_bytes(b"a" * 13)
        (snapshot / "updater.exe").write_bytes(b"b" * 17)
        (snapshot / "nested/data.bin").parent.mkdir(parents=True, exist_ok=True)
        (snapshot / "nested/data.bin").write_bytes(b"c" * 23)

        rows = collect_backup_packages(app)
        assert len(rows) == 1
        assert rows[0].version == "3.0.14-test"
        assert rows[0].path == snapshot.resolve()
        assert rows[0].size_bytes >= 53

        outside = app / "not-backup"
        outside.mkdir(parents=True)
        try:
            remove_backup_package(app, outside)
        except ValueError:
            pass
        else:
            raise AssertionError("cleanup accepted a path outside backup/")

        remove_backup_package(app, snapshot)
        assert not snapshot.exists()


def check_double_confirmation() -> None:
    from apps.windows.queue_clear_dialog import DoubleConfirmGate, WARNING_TEXT, WARNING_TITLE

    assert WARNING_TITLE == "警告"
    assert WARNING_TEXT == "是否需要清空本存档的排队信息？如需清空，请连续点击确定2下（5s内）。"
    gate = DoubleConfirmGate(5.0)
    assert gate.click(100.0) is False
    assert gate.click(104.999) is True
    assert gate.click(200.0) is False
    assert gate.click(205.001) is False
    assert gate.click(209.0) is True


def check_runtime_wiring() -> None:
    root = Path(__file__).resolve().parents[2]
    desktop = (root / "apps/windows/desktop_runtime.py").read_text(encoding="utf-8")
    runtime_layout = (root / "apps/server/runtime_layout.py").read_text(encoding="utf-8")
    assert "install_backup_cleanup_ui()" in desktop
    assert "install_queue_clear_dialog(panel_class)" in desktop
    assert "sync_local_data_archive(app_root" in runtime_layout
    assert "if data_dir_overridden():" in runtime_layout


def main() -> None:
    check_appdata_sync()
    check_backup_cleanup()
    check_double_confirmation()
    check_runtime_wiring()
    print("issue268 local archive/backup cleanup/queue confirmation guard: OK")


if __name__ == "__main__":
    main()
