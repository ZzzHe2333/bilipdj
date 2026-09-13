from __future__ import annotations

import hashlib
import json
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    from apps.server import language_plugins
    from apps.server import plugin_manager as pm

    translations_bytes = json.dumps(
        {"运行日志": "Runtime Logs", "设置": "Settings", "刷新": "Refresh"},
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")
    digest = hashlib.sha256(translations_bytes).hexdigest()
    manifest = {
        "schema": 1,
        "id": "example.en-us.language",
        "name": "English UI language pack",
        "version": "1.0.0",
        "plugin_api": 1,
        "type": "language",
        "platform": "language",
        "runtime": "resource",
        "entry": "translations.json",
        "language": "en-US",
        "language_name": "English",
        "translations": "translations.json",
        "min_bilipdj_version": "0.0.0",
        "permissions": [],
        "capabilities": ["ui_translation"],
        "files": {"translations.json": digest},
    }

    normalized = pm.validate_manifest(manifest, "99.0.0")
    assert normalized["type"] == "language"
    assert normalized["runtime"] == "resource"
    assert normalized["language"] == "en-US"

    invalid = dict(manifest)
    invalid["permissions"] = ["network"]
    try:
        pm.validate_manifest(invalid, "99.0.0")
    except pm.PluginError:
        pass
    else:
        raise AssertionError("language resource plugins must not request executable permissions")

    invalid_code = dict(manifest)
    invalid_code["language"] = "en-us"
    try:
        pm.validate_manifest(invalid_code, "99.0.0")
    except pm.PluginError:
        pass
    else:
        raise AssertionError("non-canonical language code must be rejected")

    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        plugin_root = root / "plugin"
        plugin_root.mkdir()
        (plugin_root / "translations.json").write_bytes(translations_bytes)
        record = SimpleNamespace(
            plugin_id="example.en-us.language",
            root=plugin_root,
            manifest=manifest,
            enabled=True,
            verified=True,
            error="",
            name="English UI language pack",
        )
        manager = SimpleNamespace(_lock=threading.RLock(), _records={record.plugin_id: record})
        server = SimpleNamespace(_YAML_DIR=root)
        service = language_plugins.LanguageService(server, manager)
        languages = service.list_languages()
        assert [item["code"] for item in languages] == ["zh-CN", "en-US"]
        assert service.active_language() == "zh-CN"
        assert service.set_active("en-US") == "en-US"
        assert service.active_language() == "en-US"
        mapping = service.translations()
        assert mapping["运行日志"] == "Runtime Logs"
        assert mapping["设置"] == "Settings"
        payload = service.payload()
        assert payload["active"] == "en-US"
        assert payload["translations"]["刷新"] == "Refresh"

    backend_source = (ROOT / "apps/server/language_plugins.py").read_text(encoding="utf-8")
    assert 'LANGUAGE_PLUGIN_TYPE = "language"' in backend_source
    assert 'LANGUAGE_RUNTIME = "resource"' in backend_source
    assert "/api/language" in backend_source
    assert "resource-only" in backend_source
    assert "ui_translation" in backend_source

    windows_source = (ROOT / "apps/windows/language_ui.py").read_text(encoding="utf-8")
    assert "界面语言" in windows_source
    assert "apply_translations" in windows_source
    assert "<<ComboboxSelected>>" in windows_source

    web_source = (ROOT / "apps/web/static/i18n.js").read_text(encoding="utf-8")
    assert "MutationObserver" in web_source
    assert "bilipdj-language-select" in web_source
    assert "BiliPDJI18n" in web_source

    desktop_runtime = (ROOT / "apps/windows/desktop_runtime.py").read_text(encoding="utf-8")
    assert "install_language_ui" in desktop_runtime
    server_init = (ROOT / "apps/server/__init__.py").read_text(encoding="utf-8")
    assert "install_language_plugin_system" in server_init

    print("issue #250 language plugin guard: OK")


if __name__ == "__main__":
    main()
