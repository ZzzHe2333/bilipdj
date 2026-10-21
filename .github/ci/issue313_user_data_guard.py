"""Issue 313 path, migration and client style isolation regression."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from apps.server.user_data_paths import (preferred_root, preferred_state_root, preferred_archive_dir,
    preferred_backup_dir, migrate, migrate_legacy_local_state, seed_client_styles,
    promote_legacy_archive_settings, migrate_explicit_root_files, DataConflictError)


def main():
    with tempfile.TemporaryDirectory() as td:
        home = Path(td) / "home"
        app = Path(td) / "project"
        home.mkdir()
        app.mkdir()
        assert preferred_root(app, platform="win32", environ={"APPDATA": str(home / "AppData" / "Roaming")}) == (home / "AppData" / "Roaming" / "bilipdj").resolve()
        assert preferred_root(app, platform="darwin", environ={}, home=home) == (home / "Library" / "Application Support" / "bilipdj").resolve()
        assert preferred_root(app, platform="linux", environ={}, home=home) == (home / ".local" / "share" / "bilipdj").resolve()
        assert preferred_root(app, platform="linux", environ={"XDG_DATA_HOME": str(home / "custom")}, home=home) == (home / "custom" / "bilipdj").resolve()
        assert preferred_root(app, platform="linux", environ={"XDG_DATA_HOME": "relative"}, home=home) == (home / ".local" / "share" / "bilipdj").resolve()
        explicit = home / "docker-volume"
        assert preferred_root(app, platform="darwin", environ={"BILIPDJ_DATA_DIR": str(explicit)}, home=home) == explicit.resolve()
        windows_env = {
            "APPDATA": str(home / "AppData" / "Roaming"),
            "LOCALAPPDATA": str(home / "AppData" / "Local"),
        }
        roaming = preferred_root(app, platform="win32", environ=windows_env, home=home)
        local = preferred_state_root(app, platform="win32", environ=windows_env, home=home)
        assert roaming == home / "AppData" / "Roaming" / "bilipdj"
        assert local == home / "AppData" / "Local" / "bilipdj"
        assert preferred_archive_dir(app, platform="win32", environ=windows_env, home=home) == local / "archives"
        assert preferred_backup_dir(app, platform="win32", environ=windows_env, home=home) == local / "backups"
        assert preferred_archive_dir(app, platform="darwin", environ={}, home=home) == home / "Library" / "Application Support" / "bilipdj" / "archives"
        assert preferred_backup_dir(app, platform="linux", environ={}, home=home) == home / ".local" / "share" / "bilipdj" / "backups"
        assert preferred_archive_dir(app, platform="linux", environ={"BILIPDJ_DATA_DIR": str(explicit)}, home=home) == explicit / "core" / "cd"
        assert preferred_backup_dir(app, platform="linux", environ={"BILIPDJ_DATA_DIR": str(explicit)}, home=home) == explicit / "backup"

        # Also honor legacy portable settings at the app root.
        (app / "config.yaml").write_text("portable root", encoding="utf-8")
        source = app / "core" / "config.yaml"
        source.parent.mkdir(parents=True)
        source.write_text("old", encoding="utf-8")
        target = home / "data"
        result = migrate(app, target)
        assert result["copied"] and (target / "core/config.yaml").read_text(encoding="utf-8") == "old"
        assert source.read_text(encoding="utf-8") == "old", "Original must be retained"
        assert (app / "config.yaml").read_text() == "portable root"
        (target / "core/config.yaml").write_text("user edits", encoding="utf-8")
        migrate(app, target, chooser=lambda conflicts: (_ for _ in ()).throw(
            AssertionError("Unchanged legacy source must not prompt after destination edits")
        ))
        assert (target / "core/config.yaml").read_text() == "user edits"
        source.write_text("project version", encoding="utf-8")
        prompts = []
        def select_user(c):
            prompts.append(len(c))
            return "user"
        migrate(app, target, chooser=select_user)
        assert prompts == [1] and (target / "core/config.yaml").read_text() == "user edits"
        migrate(app, target, chooser=lambda c: (_ for _ in ()).throw(AssertionError("Repeated prompt")))
        source.write_text("new project", encoding="utf-8")
        migrate(app, target, chooser=lambda c: "project")
        assert (target / "core/config.yaml").read_text() == "new project"
        assert len(list((target / "migration-backup/core").glob("config.yaml.before-import-*"))) == 1
        assert list((target / "migration-backup/core").glob("config.yaml.before-import-*"))[0].read_text() == "user edits"
        assert source.read_text() == "new project"
        # A previous version already mirrored user theme into Roaming/core.
        # It must win over the project-bundled default when root lacks it.
        (target / "core/appearance.json").write_text('{"mode":"light"}', encoding="utf-8")
        promote_legacy_archive_settings(target)
        other = target / "appearance.json"
        assert json.loads(other.read_text())["mode"] == "light"
        seed_client_styles(target)
        assert (target / "appearance-web.json").read_text() == other.read_text()
        assert (target / "appearance-win.json").read_text() == other.read_text()
        (target / "appearance-win.json").write_text('{"mode":"dark"}')
        assert "light" in (target / "appearance-web.json").read_text()
        invalid = app / "core" / "quanxian.yaml"
        invalid.write_text("new", encoding="utf-8")
        (target / "core/quanxian.yaml").write_text("old", encoding="utf-8")
        try:
            migrate(app, target, chooser=lambda c: "cancel")
        except DataConflictError:
            pass
        else:
            raise AssertionError("Cancel must stop without overwriting files")
        assert (target / "core/quanxian.yaml").read_text() == "old"
        assert invalid.read_text() == "new"

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        portable = root / "portable"
        (portable / "core").mkdir(parents=True)
        (portable / "core/config.yaml").write_text("bundled default", encoding="utf-8")
        (portable / "config.yaml").write_text("real portable user data", encoding="utf-8")
        migrate(portable, root / "new-user-data", portable_layout=True)
        assert (root / "new-user-data/core/config.yaml").read_text() == "real portable user data"
        assert (portable / "core/config.yaml").read_text() == "bundled default"


    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        roaming = root / "roaming" / "bilipdj"
        local = root / "local" / "bilipdj"
        project = root / "old-program"
        old_queue = roaming / "core" / "cd" / "queue_archive_slot_1.csv"
        old_queue.parent.mkdir(parents=True)
        old_queue.write_text("from-roaming,queue\\n", encoding="utf-8")
        old_backup = roaming / "backup" / "snapshot.zip"
        old_backup.parent.mkdir(parents=True, exist_ok=True)
        old_backup.write_text("old-backup", encoding="utf-8")
        moved = migrate_legacy_local_state(roaming, local)
        assert moved["copied"] == 2, moved
        assert (local / "archives" / "queue_archive_slot_1.csv").read_text() == "from-roaming,queue\\n"
        assert (local / "backups" / "snapshot.zip").read_text() == "old-backup"
        assert old_queue.is_file() and old_backup.is_file()
        (local / "archives" / "queue_archive_slot_1.csv").write_text("live-queue", encoding="utf-8")
        migrate_legacy_local_state(roaming, local, chooser=lambda c: (_ for _ in ()).throw(
            AssertionError("Already migrated source must not conflict with locally edited queue")
        ))
        assert (local / "archives" / "queue_archive_slot_1.csv").read_text() == "live-queue"
        app_queue = project / "core" / "cd" / "queue_archive_slot_1.csv"
        app_queue.parent.mkdir(parents=True)
        app_queue.write_text("another-platform-queue", encoding="utf-8")
        requested = []
        def choose_local(items):
            requested.extend(items)
            return "user"
        migrate(project, roaming, chooser=choose_local, state_root=local)
        assert len(requested) == 1
        assert (local / "archives" / "queue_archive_slot_1.csv").read_text() == "live-queue"
        app_queue.write_text("new-archive-source", encoding="utf-8")
        migrate(project, roaming, chooser=lambda c: "project", state_root=local)
        assert (local / "archives" / "queue_archive_slot_1.csv").read_text() == "new-archive-source"
        assert list((local / "migration-backup" / "archives").glob("queue_archive_slot_1.csv.before-import-*"))
        assert app_queue.read_text() == "new-archive-source"
        # Other platforms use a single shared archive directory, not
        # platform-specific state roots.
        assert "bilibili" not in str(local / "archives")
        assert "douyin" not in str(local / "archives")

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "core" / "cd").mkdir(parents=True)
        (root / "core" / "cd" / "queue_archive_slot_2.csv").write_text("mac-old")
        report = migrate_legacy_local_state(root, root)
        assert report["copied"] == 1
        assert (root / "archives" / "queue_archive_slot_2.csv").read_text() == "mac-old"

    # Integration: Linux user root carries config, archive root carries data.
    from unittest.mock import patch
    from apps.server.runtime_layout import ensure_runtime_layout
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        old_app = root / "repo"
        old_archive = old_app / "core" / "cd" / "queue_archive_slot_3.csv"
        old_archive.parent.mkdir(parents=True)
        old_archive.write_text("bilibili,douyin,shared", encoding="utf-8")
        xdg = root / "xdg"
        with patch.dict("os.environ", {"XDG_DATA_HOME": str(xdg), "BILIPDJ_DATA_DIR": ""}):
            config_core, _ = ensure_runtime_layout(old_app)
            assert config_core == xdg / "bilipdj" / "core"
            assert (xdg / "bilipdj" / "archives" / "queue_archive_slot_3.csv").read_text() == "bilibili,douyin,shared"
            assert (xdg / "bilipdj" / "backups").is_dir()
            assert (xdg / "bilipdj" / "cache").is_dir()
            assert old_archive.is_file()
        mounted = root / "mounted"
        with patch.dict("os.environ", {"BILIPDJ_DATA_DIR": str(mounted)}):
            core, _ = ensure_runtime_layout(old_app)
            assert core == mounted / "core"
            assert (mounted / "core" / "cd").is_dir()
            assert (mounted / "backup").is_dir()
            assert not (mounted / "archives").exists()

    # Runtime queue writes produce a bounded Local backup of the previous
    # unified (multi-platform) queue, without backing up unrelated CSV files.
    from apps.server import server as live_backend
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        old_pd_dir = live_backend.PD_DIR
        old_backup_dir = getattr(live_backend, "BACKUP_DIR", None)
        try:
            live_backend.PD_DIR = root / "archives"
            live_backend.BACKUP_DIR = root / "backups"
            slot = live_backend.PD_DIR / "queue_archive_slot_1.csv"
            first = [{"id": "A", "content": "first", "platform": "bilibili"}]
            second = [{"id": "B", "content": "second", "platform": "douyin"}]
            live_backend.write_queue_archive_entries(slot, first)
            live_backend.write_queue_archive_entries(slot, second)
            backups = list((root / "backups/queue/queue_archive_slot_1").glob("*.csv"))
            assert len(backups) == 1, backups
            assert "bilibili" in backups[0].read_text(encoding="utf-8-sig")
            assert "douyin" in slot.read_text(encoding="utf-8-sig")
            live_backend.write_queue_archive_entries(slot, first)
            assert len(list((root / "backups/queue/queue_archive_slot_1").glob("*.csv"))) == 1
        finally:
            live_backend.PD_DIR = old_pd_dir
            if old_backup_dir is None:
                delattr(live_backend, "BACKUP_DIR")
            else:
                live_backend.BACKUP_DIR = old_backup_dir


    # Regression: an explicit Docker/custom volume must never pick by mtime
    # or remove the portable root copy of config/permission/update metadata.
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        app = root / "portable"
        data = root / "docker-data"
        app.mkdir()
        (app / "config.yaml").write_text("project", encoding="utf-8")
        (app / "quanxian.yaml").write_text("admin: user", encoding="utf-8")
        (app / "update-result.json").write_text("result", encoding="utf-8")
        report = migrate_explicit_root_files(app, data)
        assert report["copied"] == 3, report
        assert (data / "core/config.yaml").read_text() == "project"
        assert (data / "core/quanxian.yaml").read_text() == "admin: user"
        assert (data / "key/update-result.json").read_text() == "result"
        assert (app / "config.yaml").read_text() == "project"
        (app / "config.yaml").write_text("new project", encoding="utf-8")
        prompts = []
        migrate_explicit_root_files(app, data, chooser=lambda conflicts: (prompts.extend(conflicts), "user")[1])
        assert len(prompts) == 1
        assert (data / "core/config.yaml").read_text() == "project"
        migrate_explicit_root_files(app, data, chooser=lambda conflicts: (_ for _ in ()).throw(
            AssertionError("Unchanged source must not prompt twice")
        ))
        (app / "config.yaml").write_text("newer project", encoding="utf-8")
        migrate_explicit_root_files(app, data, chooser=lambda conflicts: "project")
        assert (data / "core/config.yaml").read_text() == "newer project"
        assert (app / "config.yaml").read_text() == "newer project"
        assert list((data / "migration-backup/core").glob("config.yaml.before-import-*"))
        # Even if BILIPDJ_DATA_DIR is the portable root, source must survive.
        same_root = root / "same-root"
        same_root.mkdir()
        (same_root / "config.yaml").write_text("portable-root")
        migrate_explicit_root_files(same_root, same_root)
        assert (same_root / "config.yaml").is_file()
        assert (same_root / "core/config.yaml").read_text() == "portable-root"

    # Regression: keep full update rollback bundles discoverable under the
    # original app/backup directory; move only genuine application backups.
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        app = root / "program"
        user = root / "roaming"
        local = root / "local"
        (app / "backup/update-20261009-to-v3.0.23").mkdir(parents=True)
        (app / "backup/update-20261009-to-v3.0.23/VERSION").write_text("3.0.22")
        (app / "backup/update-20261009-to-v3.0.23/main.exe").write_bytes(b"exe")
        (app / "backup/BiliPDJ-settings.zip").write_bytes(b"zip")
        report = migrate(app, user, state_root=local)
        assert report["copied"] == 1, report
        assert (local / "backups/BiliPDJ-settings.zip").read_bytes() == b"zip"
        assert not (local / "backups/update-20261009-to-v3.0.23").exists()
        assert (app / "backup/update-20261009-to-v3.0.23/main.exe").is_file()

    # Explicit override must be wired into the full runtime layout, too.
    from unittest.mock import patch
    from apps.server.runtime_layout import ensure_runtime_layout as runtime_ensure
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        app = root / "program"
        app.mkdir()
        (app / "config.yaml").write_text("old", encoding="utf-8")
        target = root / "volume"
        with patch.dict("os.environ", {"BILIPDJ_DATA_DIR": str(target)}):
            core, _ = runtime_ensure(app)
            assert (core / "config.yaml").read_text() == "old"
            assert (app / "config.yaml").read_text() == "old"

    backend = (ROOT / "apps/server/server.py").read_text(encoding="utf-8")
    win = (ROOT / "apps/windows/control_panel.py").read_text(encoding="utf-8")
    layout = (ROOT / "apps/server/runtime_layout.py").read_text(encoding="utf-8")
    assert 'style-web.json' in layout
    assert 'appearance-web.json' in layout
    assert 'client="win"' in win
    assert '"style-win.json"' in backend
    from apps.server import server as live_server
    with tempfile.TemporaryDirectory() as td:
        current = live_server.BLACKLIST_PATH
        try:
            live_server.BLACKLIST_PATH = Path(td) / "user-config" / "blacklist.csv"
            live_server.write_blacklist_entries(
                live_server.BLACKLIST_PATH, [{"id": "runtime-admin", "content": ""}]
            )
            assert "runtime-admin" in [
                entry["id"] for entry in live_server.read_blacklist_entries()
            ], "read_blacklist_entries() must follow runtime BLACKLIST_PATH"
        finally:
            live_server.BLACKLIST_PATH = current
    print("issue #313 OS paths, conflict migration, Win/Web style separation: PASS")


if __name__ == "__main__":
    main()
