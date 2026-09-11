from __future__ import annotations

import json
import tempfile
import zipfile
from pathlib import Path

from apps.windows import updater


class _FakeProcess:
    def __init__(self, return_code: int | None) -> None:
        self.return_code = return_code

    def poll(self) -> int | None:
        return self.return_code


def _write_old_app(app_dir: Path) -> None:
    app_dir.mkdir(parents=True)
    (app_dir / "main.exe").write_bytes(b"old-main")
    (app_dir / "updater.exe").write_bytes(b"old-updater")
    (app_dir / "old-only.txt").write_text("old", encoding="utf-8")
    (app_dir / "config.yaml").write_text("room: 1\n", encoding="utf-8")
    (app_dir / "log").mkdir()
    (app_dir / "log" / "session.log").write_text("keep-log", encoding="utf-8")
    (app_dir / "backup" / "history").mkdir(parents=True)
    (app_dir / "backup" / "history" / "keep.txt").write_text(
        "historical-backup", encoding="utf-8"
    )


def _write_update_zip(zip_path: Path) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("main.exe", b"new-main")
        archive.writestr("updater.exe", b"new-updater")
        archive.writestr("new-only.txt", "new")


def _snapshot_dirs(app_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in (app_dir / "backup").iterdir()
        if path.is_dir() and path.name.startswith("update-")
    )


def _assert_common_backup_contract(app_dir: Path, snapshot: Path) -> None:
    assert (app_dir / "backup").parent == app_dir
    assert (app_dir / "log").parent == app_dir
    assert snapshot.parent == app_dir / "backup"
    assert not (snapshot / "backup").exists(), "snapshot must not recurse into backup/"
    assert (snapshot / "old-only.txt").read_text(encoding="utf-8") == "old"
    assert (snapshot / "log" / "session.log").read_text(encoding="utf-8") == "keep-log"
    assert (app_dir / "backup" / "history" / "keep.txt").read_text(
        encoding="utf-8"
    ) == "historical-backup"


def _test_successful_update(root: Path) -> None:
    app_dir = root / "success" / "BiliPDJ"
    zip_path = root / "success" / "download" / "update.zip"
    _write_old_app(app_dir)
    _write_update_zip(zip_path)

    original_launch = updater.launch_main
    original_grace = updater.STARTUP_GRACE_SECONDS
    try:
        updater.launch_main = lambda *_args, **_kwargs: _FakeProcess(None)  # type: ignore[assignment]
        updater.STARTUP_GRACE_SECONDS = 0.0
        updater.perform_update(
            pid=0,
            app_dir=app_dir,
            zip_path=zip_path,
            main_exe_name="main.exe",
            target_version="3.0.1",
        )
    finally:
        updater.launch_main = original_launch
        updater.STARTUP_GRACE_SECONDS = original_grace

    snapshots = _snapshot_dirs(app_dir)
    assert len(snapshots) == 1, snapshots
    snapshot = snapshots[0]
    _assert_common_backup_contract(app_dir, snapshot)

    assert (app_dir / "new-only.txt").read_text(encoding="utf-8") == "new"
    assert not (app_dir / "old-only.txt").exists()
    assert (app_dir / "config.yaml").read_text(encoding="utf-8") == "room: 1\n"
    assert (app_dir / "log" / "session.log").read_text(encoding="utf-8") == "keep-log"
    assert not (app_dir.parent / ".BiliPDJ.update-backup").exists()

    result = json.loads((app_dir / "update-result.json").read_text(encoding="utf-8"))
    assert result["status"] == "installed"
    assert Path(result["backup_dir"]) == snapshot

    second = updater.create_update_snapshot(app_dir, "../3.0.2 unsafe")
    assert second.parent == app_dir / "backup"
    assert second.name.startswith("update-")
    assert ".." not in second.name
    assert not (second / "backup").exists()
    assert len(_snapshot_dirs(app_dir)) == 2


def _test_failed_startup_rolls_back(root: Path) -> None:
    app_dir = root / "failure" / "BiliPDJ"
    zip_path = root / "failure" / "download" / "update.zip"
    _write_old_app(app_dir)
    _write_update_zip(zip_path)

    original_launch = updater.launch_main
    original_grace = updater.STARTUP_GRACE_SECONDS
    try:
        updater.launch_main = lambda *_args, **_kwargs: _FakeProcess(7)  # type: ignore[assignment]
        updater.STARTUP_GRACE_SECONDS = 1.0
        try:
            updater.perform_update(
                pid=0,
                app_dir=app_dir,
                zip_path=zip_path,
                main_exe_name="main.exe",
                target_version="3.0.1",
            )
        except updater.UpdaterError as exc:
            assert "退出码 7" in str(exc)
        else:
            raise AssertionError("startup failure must raise UpdaterError")
    finally:
        updater.launch_main = original_launch
        updater.STARTUP_GRACE_SECONDS = original_grace

    snapshots = _snapshot_dirs(app_dir)
    assert len(snapshots) == 1, snapshots
    snapshot = snapshots[0]
    _assert_common_backup_contract(app_dir, snapshot)

    assert (app_dir / "old-only.txt").read_text(encoding="utf-8") == "old"
    assert not (app_dir / "new-only.txt").exists()
    assert not (app_dir.parent / ".BiliPDJ.update-backup").exists()

    result = json.loads((app_dir / "update-result.json").read_text(encoding="utf-8"))
    assert result["status"] == "rolled_back"
    assert Path(result["backup_dir"]) == snapshot


def main() -> None:
    assert Path("log") in updater.PRESERVE_PATHS
    assert Path("backup") in updater.PRESERVE_PATHS
    with tempfile.TemporaryDirectory(prefix="bilipdj-issue150-") as temp_dir:
        root = Path(temp_dir)
        _test_successful_update(root)
        _test_failed_startup_rolls_back(root)
    print("issue150 updater backup guard: ok")


if __name__ == "__main__":
    main()
