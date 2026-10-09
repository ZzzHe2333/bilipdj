"""Issue 313 path, migration and client style isolation regression."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from apps.server.user_data_paths import preferred_root, migrate, seed_client_styles, promote_legacy_archive_settings, DataConflictError


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

    backend = (ROOT / "apps/server/server.py").read_text(encoding="utf-8")
    win = (ROOT / "apps/windows/control_panel.py").read_text(encoding="utf-8")
    layout = (ROOT / "apps/server/runtime_layout.py").read_text(encoding="utf-8")
    assert 'style-web.json' in layout
    assert 'appearance-web.json' in layout
    assert 'client="win"' in win
    assert '"style-win.json"' in backend
    print("issue #313 OS paths, conflict migration, Win/Web style separation: PASS")


if __name__ == "__main__":
    main()
