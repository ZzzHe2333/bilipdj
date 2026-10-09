"""Issue #311: OS data directories, safe migration and independent client styles."""
from __future__ import annotations

import importlib.util
import json
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load(path: str):
    spec = importlib.util.spec_from_file_location(path.rsplit("/", 1)[-1].split(".")[0] + "_311", ROOT / path)
    assert spec is not None and spec.loader is not None
    obj = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(obj)
    return obj


def main() -> None:
    data = load("apps/server/user_data.py")
    layout = load("apps/server/runtime_layout.py")
    with tempfile.TemporaryDirectory(prefix="bilipdj311-") as raw:
        home = Path(raw) / "user"
        app = Path(raw) / "old"
        roaming = home / "AppData/Roaming/bilipdj"
        local = home / "AppData/Local/bilipdj/log"
        assert data.user_data_root(platform="win32", home=home) == roaming
        assert data.local_log_root(platform="win32", home=home) == local
        assert data.user_data_root(platform="darwin", home=home) == home / "Library/Application Support/bilipdj"
        assert data.local_log_root(platform="darwin", home=home) == home / "Library/Logs/bilipdj"
        assert data.user_data_root(platform="linux", environment={}, home=home) == home / ".local/share/bilipdj"
        assert data.user_data_root(platform="linux", environment={"XDG_DATA_HOME": str(home / "xdg")}, home=home) == home / "xdg/bilipdj"
        assert data.user_data_root(platform="linux", environment={"XDG_DATA_HOME": "relative"}, home=home) == home / ".local/share/bilipdj"
        assert data.local_log_root(platform="linux", environment={"XDG_STATE_HOME": str(home / "state")}, home=home) == home / "state/bilipdj/log"

        (app / "core/cd").mkdir(parents=True)
        (app / "core/config.yaml").write_text("server:\n  port: 9816\n", encoding="utf-8")
        (app / "core/quanxian.yaml").write_text("admin:\n  - alice\n", encoding="utf-8")
        (app / "core/cd/queue_archive_slot_1.csv").write_text("Alice,douyin\n", encoding="utf-8")
        (app / "core/style.json").write_text('{"text_color":"#000000"}', encoding="utf-8")
        (app / "core/appearance.json").write_text('{"mode":"dark"}', encoding="utf-8")
        (app / "plugins").mkdir(parents=True)
        (app / "plugins/saved.json").write_text('{"state":1}', encoding="utf-8")
        (app / "apps").mkdir()
        (app / "apps/source.py").write_text("do-not-copy", encoding="utf-8")
        expected = app / "core/config.yaml"
        before = expected.read_bytes()
        new = home / "xdg/bilipdj"
        with patch.dict(os.environ, {"BILIPDJ_DATA_DIR": ""}):
            plan = data.storage_plan(app, destination=new)
            assert plan["migrate"] and plan["active"] == new
            count = data.migrate_if_needed(plan)
            assert count >= 5
            assert expected.read_bytes() == before, "migration must preserve the original"
            assert (new / "core/config.yaml").read_bytes() == before
            assert (new / "core/cd/queue_archive_slot_1.csv").is_file()
            assert (new / "plugins/saved.json").is_file()
            assert not (new / "apps/source.py").exists(), "never copy program code"
            second = data.storage_plan(app, destination=new)
            assert second["choice"] == "user" and second["active"] == new
            assert not second["migrate"]
            assert data.migrate_if_needed(second) == 0
            assert (new / data.DATA_CHOICE_FILE).is_file()

        conflicted = home / "separate/bilipdj"
        (conflicted / "core").mkdir(parents=True)
        (conflicted / "core/config.yaml").write_text("NEW-CANONICAL", encoding="utf-8")
        with patch.dict(os.environ, {"BILIPDJ_DATA_DIR": "", "BILIPDJ_DATA_CHOICE": ""}):
            clash = data.storage_plan(app, destination=conflicted)
            assert clash["conflict"] and clash["active"] == app and not clash["migrate"]
            assert not data.migrate_if_needed(clash)
            assert (conflicted / "core/config.yaml").read_text() == "NEW-CANONICAL"
            data.choose_storage(app, "user", destination=conflicted)
            selected = data.storage_plan(app, destination=conflicted)
            assert selected["active"] == conflicted and not selected["conflict"]
            assert (conflicted / "core/config.yaml").read_text() == "NEW-CANONICAL"

            data.choose_storage(app, "legacy", destination=conflicted)
            selected = data.storage_plan(app, destination=conflicted)
            assert selected["active"] == app
            assert expected.read_bytes() == before

        with patch.dict(os.environ, {"BILIPDJ_DATA_DIR": str(home / "docker")}, clear=False):
            explicit = data.storage_plan(app, destination=conflicted)
            assert explicit["mode"] == "explicit" and not explicit["migrate"]
            assert explicit["active"] == (home / "docker").resolve()

        with patch.dict(os.environ, {"BILIPDJ_DATA_DIR": "", "BILIPDJ_DATA_CHOICE": ""}):
            assert layout.storage_status(app)["mode"] == "managed"

        assert "style-win.json" in (ROOT / "apps/server/__init__.py").read_text(encoding="utf-8")
        assert "style-web.json" in (ROOT / "apps/server/__init__.py").read_text(encoding="utf-8")
        assert "appearance-win.json" in (ROOT / "apps/server/runtime_layout.py").read_text(encoding="utf-8")
        assert "appearance-web.json" in (ROOT / "apps/server/runtime_layout.py").read_text(encoding="utf-8")
        assert "save_win_style(data)" in (ROOT / "apps/windows/control_panel.py").read_text(encoding="utf-8")
        assert "client=win" in (ROOT / "apps/windows/unified_theme.py").read_text(encoding="utf-8")

        class Module:
            DEFAULT_STYLE = {"text_color": "#fff"}
            _YAML_DIR = new
            STYLE_WIN_PATH = new / "style-win.json"
            @staticmethod
            def _atomic_write_text(path, text, *, encoding):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding=encoding)
        mod = Module()
        client = load("apps/server/client_style.py")
        client.install_win_style(mod)
        mod.save_win_style({"text_color": "#abcabc"})
        assert mod.load_win_style()["text_color"] == "#abcabc"
        assert not (new / "style-web.json").exists(), "Win save must not create or overwrite Web style"

    print("issue #311 OS paths, copy-only migration, conflicts and split Win/Web style: PASS")


if __name__ == "__main__":
    main()
