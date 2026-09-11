from __future__ import annotations

import json
import sys
import tempfile
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from apps.windows import incremental_apply, incremental_update, updater  # noqa: E402
from apps.windows.update_manifest import is_preserved_path  # noqa: E402
from scripts import build_incremental_update  # noqa: E402


class _FakeProcess:
    def __init__(self, return_code: int | None) -> None:
        self.return_code = return_code

    def poll(self) -> int | None:
        return self.return_code


def _zip_tree(root: Path, destination: Path) -> None:
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in root.rglob("*"):
            if path.is_file():
                archive.write(path, path.relative_to(root).as_posix())


def _test_manifest_builder(root: Path) -> None:
    previous = root / "previous"
    current = root / "current"
    previous.mkdir(parents=True)
    current.mkdir(parents=True)
    for folder in (previous, current):
        (folder / "_internal").mkdir()
        (folder / "core" / "cd").mkdir(parents=True)
        (folder / "plugins" / "data").mkdir(parents=True)

    (previous / "main.exe").write_bytes(b"old-main")
    (previous / "_internal" / "same.dll").write_bytes(b"same")
    (previous / "removed.bin").write_bytes(b"remove-me")
    (previous / "VERSION").write_text("3.0.1\n", encoding="utf-8")
    (previous / "config.yaml").write_text("secret: old", encoding="utf-8")
    (previous / "core" / "cd" / "queue.csv").write_text("old", encoding="utf-8")
    (previous / "plugins" / "data" / "private.json").write_text("old", encoding="utf-8")

    (current / "main.exe").write_bytes(b"new-main")
    (current / "_internal" / "same.dll").write_bytes(b"same")
    (current / "new.bin").write_bytes(b"new-file")
    (current / "VERSION").write_text("3.0.2\n", encoding="utf-8")
    (current / "config.yaml").write_text("secret: new-default", encoding="utf-8")
    (current / "core" / "cd" / "queue.csv").write_text("new-default", encoding="utf-8")
    (current / "plugins" / "data" / "private.json").write_text("new-default", encoding="utf-8")

    previous_zip = root / "previous.zip"
    manifest_path = root / "files.json"
    pack_path = root / "incremental.pack"
    _zip_tree(previous, previous_zip)
    build_incremental_update.build(
        package_dir=current,
        version="3.0.2",
        package_sha256="a" * 64,
        output_manifest=manifest_path,
        output_pack=pack_path,
        previous_zip=previous_zip,
        base_version="3.0.1",
    )

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["schema"] == 2
    paths = {entry["path"] for entry in payload["files"]}
    assert paths == {"_internal/same.dll", "main.exe", "new.bin", "VERSION"}, paths
    assert payload["incremental"]["base_version"] == "3.0.1"
    assert set(payload["incremental"]["changed_from_base"]) == {"main.exe", "new.bin", "VERSION"}
    assert payload["incremental"]["removed_from_base"] == ["removed.bin"]

    pack = pack_path.read_bytes()
    for entry in payload["files"]:
        start = int(entry["offset"])
        size = int(entry["packed_size"])
        packed = pack[start : start + size]
        raw = incremental_update._unpack_file(entry, packed)  # noqa: SLF001
        assert raw == (current / Path(entry["path"])).read_bytes()

    assert is_preserved_path("config.yaml")
    assert is_preserved_path("core/cd/queue.csv")
    assert is_preserved_path("backup/update-old/main.exe")
    assert is_preserved_path("plugins/data/private.json")


def _plan(path: Path, *, replace: list[dict[str, object]], remove: list[str]) -> None:
    path.write_text(
        json.dumps(
            {
                "schema": 1,
                "kind": "bilipdj-incremental-plan",
                "version": "3.0.2",
                "base_version": "3.0.1",
                "target_package_sha256": "b" * 64,
                "replace": replace,
                "remove": remove,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _sha(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def _write_patch(zip_path: Path, values: dict[str, bytes]) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as archive:
        for name, data in values.items():
            archive.writestr(name, data)


def _run_apply_case(root: Path, *, startup_code: int | None) -> None:
    app_dir = root / ("success-app" if startup_code is None else "rollback-app")
    app_dir.mkdir()
    (app_dir / "main.exe").write_bytes(b"old-main")
    (app_dir / "removed.bin").write_bytes(b"old-removed")
    (app_dir / "VERSION").write_text("3.0.1\n", encoding="utf-8")
    (app_dir / "config.yaml").write_text("keep-me", encoding="utf-8")
    (app_dir / "core" / "cd").mkdir(parents=True)
    (app_dir / "core" / "cd" / "queue.csv").write_text("queue", encoding="utf-8")
    (app_dir / "plugins" / "data").mkdir(parents=True)
    (app_dir / "plugins" / "data" / "private.json").write_text("private", encoding="utf-8")

    work = root / ("success-work" if startup_code is None else "rollback-work")
    work.mkdir()
    patch_zip = work / "incremental-patch.zip"
    plan_path = work / "incremental-plan.json"
    new_main = b"new-main"
    new_file = b"new-file"
    new_version = b"3.0.2\n"
    _write_patch(patch_zip, {"main.exe": new_main, "new.bin": new_file, "VERSION": new_version})
    _plan(
        plan_path,
        replace=[
            {"path": "main.exe", "size": len(new_main), "sha256": _sha(new_main)},
            {"path": "new.bin", "size": len(new_file), "sha256": _sha(new_file)},
            {"path": "VERSION", "size": len(new_version), "sha256": _sha(new_version)},
        ],
        remove=["removed.bin"],
    )

    old_wait = updater.wait_for_process_exit
    old_launch = updater.launch_main
    old_grace = updater.STARTUP_GRACE_SECONDS
    updater.wait_for_process_exit = lambda *_args, **_kwargs: True  # type: ignore[assignment]
    updater.launch_main = lambda *_args, **_kwargs: _FakeProcess(startup_code)  # type: ignore[assignment]
    updater.STARTUP_GRACE_SECONDS = 0.02
    try:
        if startup_code is None:
            incremental_apply.perform_incremental_update_core(
                pid=0,
                app_dir=app_dir,
                zip_path=patch_zip,
                plan_path=plan_path,
                main_exe_name="main.exe",
                target_version="3.0.2",
            )
            assert (app_dir / "main.exe").read_bytes() == new_main
            assert (app_dir / "new.bin").read_bytes() == new_file
            assert (app_dir / "VERSION").read_bytes() == new_version
            assert not (app_dir / "removed.bin").exists()
        else:
            try:
                incremental_apply.perform_incremental_update_core(
                    pid=0,
                    app_dir=app_dir,
                    zip_path=patch_zip,
                    plan_path=plan_path,
                    main_exe_name="main.exe",
                    target_version="3.0.2",
                )
            except updater.UpdaterError:
                pass
            else:
                raise AssertionError("startup failure must raise UpdaterError")
            assert (app_dir / "main.exe").read_bytes() == b"old-main"
            assert (app_dir / "VERSION").read_text(encoding="utf-8") == "3.0.1\n"
            assert not (app_dir / "new.bin").exists()
            assert (app_dir / "removed.bin").read_bytes() == b"old-removed"

        assert (app_dir / "config.yaml").read_text(encoding="utf-8") == "keep-me"
        assert (app_dir / "core" / "cd" / "queue.csv").read_text(encoding="utf-8") == "queue"
        assert (app_dir / "plugins" / "data" / "private.json").read_text(encoding="utf-8") == "private"
        backup_root = app_dir / updater.BACKUP_DIR_NAME
        assert backup_root.is_dir() and any(backup_root.iterdir()), "persistent backup was not created"
    finally:
        updater.wait_for_process_exit = old_wait
        updater.launch_main = old_launch
        updater.STARTUP_GRACE_SECONDS = old_grace


def _test_preserved_plan_rejected(root: Path) -> None:
    plan_path = root / "unsafe-plan.json"
    _plan(
        plan_path,
        replace=[{"path": "config.yaml", "size": 1, "sha256": "0" * 64}],
        remove=[],
    )
    try:
        incremental_apply._load_plan(plan_path, "3.0.2")  # noqa: SLF001
    except updater.UpdaterError:
        return
    raise AssertionError("incremental plan must reject preserved config paths")


def _test_base_version_guard(root: Path) -> None:
    app = root / "base-version"
    app.mkdir()
    (app / "VERSION").write_text("3.0.1\n", encoding="utf-8")
    assert incremental_update._require_exact_base_version(app, "3.0.1") == "3.0.1"  # noqa: SLF001
    try:
        incremental_update._require_exact_base_version(app, "3.0.0")  # noqa: SLF001
    except incremental_update.IncrementalUnavailable:
        pass
    else:
        raise AssertionError("cross-version delta must fall back to full update")


def _test_source_contracts() -> None:
    update_page = (REPO_ROOT / "apps/windows/update_page.py").read_text(encoding="utf-8")
    workflow = (REPO_ROOT / ".github/workflows/package-windows-x64.yml").read_text(encoding="utf-8")
    client = (REPO_ROOT / "apps/windows/incremental_update.py").read_text(encoding="utf-8")
    assert 'text="全量更新"' in update_page
    assert 'text="增量更新"' in update_page
    assert "Windows-Tk-files.json" in workflow
    assert "Windows-Tk-Incremental-x64.pack" in workflow
    assert 'transport = "http-range"' in workflow
    assert "incremental-plan.json" in client
    assert '"Range": f"bytes={offset}-{end}"' in client


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="bilipdj-issue155-") as temp:
        root = Path(temp)
        _test_manifest_builder(root / "manifest")
        _run_apply_case(root, startup_code=None)
        _run_apply_case(root, startup_code=1)
        _test_preserved_plan_rejected(root)
        _test_base_version_guard(root)
    _test_source_contracts()
    print("issue #155 incremental update guard: OK")


if __name__ == "__main__":
    main()
