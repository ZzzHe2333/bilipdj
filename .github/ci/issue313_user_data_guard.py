"""Issue 313 path, migration and client style isolation regression."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from apps.server.user_data_paths import preferred_root, migrate, seed_client_styles, DataConflictError


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
        source = app / "core" / "config.yaml"
        source.parent.mkdir(parents=True)
        source.write_text("old", encoding="utf-8")
        target = home / "data"
        result = migrate(app, target)
        assert result["copied"] and (target / "core/config.yaml").read_text(encoding="utf-8") == "old"
        assert source.read_text(encoding="utf-8") == "old", "Original must be retained"
        source.write_text("project version", encoding="utf-8")
        prompts = []
        def select_user(c):
            prompts.append(len(c))
            return "user"
        migrate(app, target, chooser=select_user)
        assert prompts == [1] and (target / "core/config.yaml").read_text() == "old"
        migrate(app, target, chooser=lambda c: (_ for _ in ()).throw(AssertionError("Repeated prompt")))
        source.write_text("new project", encoding="utf-8")
        migrate(app, target, chooser=lambda c: "project")
        assert (target / "core/config.yaml").read_text() == "new project"
        assert (target / "migration-backup/core/config.yaml").read_text() == "old"
        assert source.read_text() == "new project"
        other = target / "appearance.json"
        other.write_text(json.dumps({"mode": "light"}), encoding="utf-8")
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
