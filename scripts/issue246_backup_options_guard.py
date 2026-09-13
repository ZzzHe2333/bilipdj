from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> None:
    from apps.server import settings_backup

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        pd_dir = root / "cd"

        class FakeServer:
            MAX_QUEUE_ARCHIVE_SLOTS = 3
            _YAML_DIR = root
            CONFIG_PATH = root / "config.yaml"
            QUANXIAN_PATH = root / "quanxian.yaml"
            KAIGUAN_PATH = root / "kaiguan.yaml"
            STYLE_PATH = root / "style.json"
            APPEARANCE_PATH = root / "appearance.json"
            PD_DIR = pd_dir
            QUEUE_STATE_PATH = pd_dir / "queue_archive_state.json"
            BLACKLIST_PATH = pd_dir / "blacklist.csv"

            @staticmethod
            def platform_config_path(slot: int) -> Path:
                return root / f"platform-slot-{slot}.yaml"

        fixtures = {
            FakeServer.CONFIG_PATH: "myjs: {}\n",
            FakeServer.QUANXIAN_PATH: "admins: []\n",
            FakeServer.KAIGUAN_PATH: "enabled: true\n",
            FakeServer.STYLE_PATH: "{}\n",
            FakeServer.APPEARANCE_PATH: "{}\n",
            FakeServer.platform_config_path(1): "platform: bilibili\n",
            FakeServer.QUEUE_STATE_PATH: '{"active_slot": 1}\n',
            FakeServer.BLACKLIST_PATH: "name\n",
            pd_dir / "queue_archive_slot_1.csv": "name\nalpha\n",
        }
        for path, text in fixtures.items():
            _write(path, text)

        service = settings_backup.SettingsBackupService(FakeServer)

        cfg = service.load_config(include_password=False)
        assert cfg["backup_config"] is True
        assert cfg["backup_archive"] is True
        assert cfg["backup_style"] is True

        service.save_config({"backup_config": True, "backup_archive": False, "backup_style": False})
        _data, included = service.build_settings_zip()
        assert "config.yaml" in included
        assert "quanxian.yaml" in included
        assert "kaiguan.yaml" in included
        assert "platform-slot-1.yaml" in included
        assert "style.json" not in included
        assert "appearance.json" not in included
        assert "queue_archive_state.json" not in included

        service.save_config({"backup_config": False, "backup_archive": True, "backup_style": False})
        _data, included = service.build_settings_zip()
        assert "queue_archive_state.json" in included
        assert "blacklist.csv" in included
        assert "queue_archive_slot_1.csv" in included
        assert "config.yaml" not in included
        assert "style.json" not in included

        service.save_config({"backup_config": False, "backup_archive": False, "backup_style": True})
        style_data, included = service.build_settings_zip()
        assert set(included) == {"style.json", "appearance.json"}

        service.save_config({"backup_config": False, "backup_archive": False, "backup_style": False})
        try:
            service.build_settings_zip()
        except settings_backup.SettingsBackupError as exc:
            assert "至少选择一项" in str(exc)
        else:
            raise AssertionError("backup should fail when every scope is disabled")

        # Restore is a separate operation: disabling future backup scopes must
        # not prevent a valid existing ZIP from being restored.
        FakeServer.STYLE_PATH.write_text('{"changed": true}\n', encoding="utf-8")
        restored = service.restore_settings_zip(style_data)
        assert set(restored) == {"style.json", "appearance.json"}
        assert FakeServer.STYLE_PATH.read_text(encoding="utf-8") == "{}\n"

    ui_source = (ROOT / "apps/windows/backup_options_guard.py").read_text(encoding="utf-8")
    assert "备份配置" in ui_source
    assert "备份存档" in ui_source
    assert "备份样式" in ui_source
    assert 'configure(text="立即备份")' in ui_source
    assert "未选择任何备份内容，无法备份" in ui_source
    assert "note_var_name" in ui_source
    assert 'text.startswith("只备份 config.yaml")' in ui_source

    print("issue246 backup options guard: OK")


if __name__ == "__main__":
    main()
