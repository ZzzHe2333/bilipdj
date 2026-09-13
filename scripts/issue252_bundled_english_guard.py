from __future__ import annotations

import sys
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _record(plugin_id: str, code: str, name: str, root: Path):
    return SimpleNamespace(
        plugin_id=plugin_id,
        root=root,
        manifest={
            "type": "language",
            "language": code,
            "language_name": name,
            "translations": "translations.json",
        },
        enabled=False,
        verified=True,
        error="",
        name=name,
    )


class FakeManager:
    def __init__(self, records):
        self._lock = threading.RLock()
        self._records = {record.plugin_id: record for record in records}
        self._state_data = {record.plugin_id: False for record in records}

    def _state(self):
        return dict(self._state_data)

    def _save_state(self, state):
        self._state_data = dict(state)

    def discover(self):
        for record in self._records.values():
            record.enabled = bool(self._state_data.get(record.plugin_id, False))
        return []


def main() -> None:
    from apps.server import language_plugins

    english_path = ROOT / "apps/web/static/languages/en-US.json"
    assert english_path.is_file(), "bundled English resource must exist"
    english = language_plugins._load_translation_mapping(english_path)
    assert english.get("设置") == "Settings"
    assert english.get("插件管理") == "Plugins"
    assert english.get("数据备份") == "Data Backup"
    assert len(english) >= 100, "bundled English should cover the main current UI text"

    for spec_path in (
        ROOT / "apps/windows/bilipdj_onedir.spec",
        ROOT / "apps/windows/bilipdj_onedir_mac.spec",
        ROOT / "apps/web/web_portable.spec",
    ):
        source = spec_path.read_text(encoding="utf-8")
        assert 'apps" / "web" / "static' in source, f"{spec_path.name} must bundle Web static resources"

    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        ja_root = root / "ja"
        ko_root = root / "ko"
        ja_root.mkdir()
        ko_root.mkdir()
        (ja_root / "translations.json").write_text('{"设置":"設定"}', encoding="utf-8")
        (ko_root / "translations.json").write_text('{"设置":"설정"}', encoding="utf-8")
        ja = _record("example.ja-jp.language", "ja-JP", "日本語", ja_root)
        ko = _record("example.ko-kr.language", "ko-KR", "한국어", ko_root)
        manager = FakeManager([ja, ko])
        server = SimpleNamespace(_YAML_DIR=root, UI_DIR=ROOT / "apps/web/static")
        service = language_plugins.LanguageService(server, manager)

        assert service.active_language() == "zh-CN"
        assert service._translation_cache == {}, "English must be packaged but not loaded by default"
        payload = service.payload()
        codes = [item["code"] for item in payload["languages"]]
        assert codes[:2] == ["zh-CN", "en-US"]
        assert sum(1 for item in payload["languages"] if item.get("enabled")) == 1
        assert payload["languages"][0]["enabled"] is True

        assert service.set_active("en-US") == "en-US"
        assert not any(manager._state_data.values()), "bundled English must disable external language plugins"
        assert service.translations()["设置"] == "Settings"
        payload = service.payload()
        assert sum(1 for item in payload["languages"] if item.get("enabled")) == 1
        assert next(item for item in payload["languages"] if item["code"] == "en-US")["enabled"] is True

        assert service.set_active("ja-JP") == "ja-JP"
        assert manager._state_data == {
            "example.ja-jp.language": True,
            "example.ko-kr.language": False,
        }
        assert service.translations()["设置"] == "設定"

        assert service.set_active("ko-KR") == "ko-KR"
        assert manager._state_data == {
            "example.ja-jp.language": False,
            "example.ko-kr.language": True,
        }
        assert service.translations()["设置"] == "설정"

        assert service.set_active("zh-CN") == "zh-CN"
        assert not any(manager._state_data.values())
        assert service.translations() == {}
        payload = service.payload()
        assert sum(1 for item in payload["languages"] if item.get("enabled")) == 1

    source = (ROOT / "apps/server/language_plugins.py").read_text(encoding="utf-8")
    assert '"en-US"' in source
    assert '"resource": "languages/en-US.json"' in source
    assert '"single_active": True' in source
    assert "original_set_enabled" in source
    assert "service.reconcile()" in source

    print("issue #252 bundled English / single-active language guard: OK")


if __name__ == "__main__":
    main()
