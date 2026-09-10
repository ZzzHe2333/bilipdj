from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.server import plugin_manager as pm  # noqa: E402
from apps.server.danmu_plugins import DanmuPluginRegistry  # noqa: E402


def fake_module(root: Path) -> Any:
    return SimpleNamespace(
        APP_DIR=root,
        BUNDLE_DIR=ROOT,
        REPO_DIR=ROOT,
        DEFAULT_PLATFORM="bilibili",
        RESERVED_RUNTIME_PLATFORMS=(),
        ALL_RUNTIME_PLATFORMS=(),
        PLATFORM_DISPLAY_NAMES={},
        _get_runtime_platform=lambda cfg: str((cfg or {}).get("platform", "bilibili")),
    )


def manifest(plugin_id: str = "example.config.test", *, with_secret_permission: bool = True) -> dict[str, Any]:
    permissions = ["secrets"] if with_secret_permission else []
    return {
        "schema": 1,
        "id": plugin_id,
        "name": "Config test",
        "version": "1.0.0",
        "plugin_api": 1,
        "type": "danmu_source",
        "platform": plugin_id.split(".")[-2] + "_" + plugin_id.split(".")[-1],
        "runtime": "python",
        "entry": "plugin.py:Plugin",
        "min_bilipdj_version": "0.0.0",
        "permissions": permissions,
        "capabilities": ["danmu"],
        "files": {"plugin.py": "0" * 64},
        "config_schema": {
            "type": "object",
            "title": "Test config",
            "additionalProperties": False,
            "required": ["room_id"],
            "properties": {
                "room_id": {"type": "string", "title": "Room", "minLength": 1},
                "retry": {"type": "integer", "default": 3, "minimum": 0, "maximum": 10},
                "mode": {"type": "string", "enum": ["live", "test"], "default": "live"},
                "enabled": {"type": "boolean", "default": True},
                "access_token": {"type": "string", "secret": True, "maxLength": 256},
            },
        },
    }


def record_from(raw: dict[str, Any], root: Path) -> pm.InstalledPluginRecord:
    normalized = pm.validate_manifest(raw, "2.0.4")
    return pm.InstalledPluginRecord(
        plugin_id=normalized["id"],
        root=root / "plugins" / normalized["id"],
        manifest=normalized,
        enabled=False,
        package_sha256="a" * 64,
        signature_status="unsigned-approved",
        verified=True,
    )


def test_manifest_schema_and_secret_permission() -> None:
    normalized = pm.validate_manifest(manifest(), "2.0.4")
    assert normalized["config_schema"]["properties"]["retry"]["default"] == 3
    bad = manifest(with_secret_permission=False)
    try:
        pm.validate_manifest(bad, "2.0.4")
    except pm.PluginError as exc:
        assert "requires the 'secrets' permission" in str(exc)
    else:
        raise AssertionError("secret field without secrets permission was accepted")


def test_persistence_defaults_secret_redaction_and_runtime_isolation() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manager = pm.PluginManager(fake_module(root), DanmuPluginRegistry())
        a = record_from(manifest("example.alpha.config"), root)
        b = record_from(manifest("example.beta.config"), root)
        manager._records = {a.plugin_id: a, b.plugin_id: b}

        public = manager.save_plugin_config(a.plugin_id, {
            "room_id": "12345",
            "retry": 5,
            "access_token": "alpha-secret",
        })
        assert public["values"]["room_id"] == "12345"
        assert public["values"]["retry"] == 5
        assert public["values"]["mode"] == "live"
        assert public["values"]["enabled"] is True
        assert "access_token" not in public["values"]
        assert public["secret_fields"]["access_token"] is True

        preserved = manager.save_plugin_config(a.plugin_id, {"room_id": "12345", "access_token": ""})
        assert preserved["secret_fields"]["access_token"] is True
        assert manager.get_plugin_config(a.plugin_id)["access_token"] == "alpha-secret"

        manager.save_plugin_config(b.plugin_id, {"room_id": "999", "access_token": "beta-secret"})
        active_server = SimpleNamespace(runtime_config={a.platform: {"legacy": "wrong"}})
        context_a = pm.PluginContext(manager, active_server, a)
        context_b = pm.PluginContext(manager, active_server, b)
        assert context_a.get_config()["room_id"] == "12345"
        assert context_a.get_config()["access_token"] == "alpha-secret"
        assert context_b.get_config()["room_id"] == "999"
        assert context_b.get_config()["access_token"] == "beta-secret"
        assert context_a.get_secret("access_token") == "alpha-secret"

        manager2 = pm.PluginManager(fake_module(root), DanmuPluginRegistry())
        manager2._records = {a.plugin_id: a, b.plugin_id: b}
        restored = manager2.get_plugin_config(a.plugin_id)
        assert restored["room_id"] == "12345"
        assert restored["retry"] == 5
        store = json.loads((root / "plugins" / ".plugin-config.json").read_text(encoding="utf-8"))
        assert store["plugins"][a.plugin_id]["access_token"] == "alpha-secret"


def test_validation_and_required_fields() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manager = pm.PluginManager(fake_module(root), DanmuPluginRegistry())
        record = record_from(manifest(), root)
        manager._records = {record.plugin_id: record}
        for values, expected in [
            ({"retry": 3}, "required"),
            ({"room_id": "1", "retry": 99}, "maximum"),
            ({"room_id": "1", "unknown": True}, "unknown"),
        ]:
            try:
                manager.save_plugin_config(record.plugin_id, values)
            except pm.PluginError as exc:
                assert expected in str(exc).lower(), str(exc)
            else:
                raise AssertionError(f"invalid config accepted: {values}")


def test_uninstall_removes_config_but_upgrade_preserves_it() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manager = pm.PluginManager(fake_module(root), DanmuPluginRegistry())
        record = record_from(manifest(), root)
        record.root.mkdir(parents=True)
        manager._records = {record.plugin_id: record}
        manager.save_plugin_config(record.plugin_id, {"room_id": "upgrade-me", "access_token": "keep"})
        record.manifest["version"] = "1.1.0"
        assert manager.get_plugin_config(record.plugin_id)["room_id"] == "upgrade-me"
        manager.uninstall(record.plugin_id)
        store_path = root / "plugins" / ".plugin-config.json"
        payload = json.loads(store_path.read_text(encoding="utf-8")) if store_path.exists() else {"plugins": {}}
        assert record.plugin_id not in payload.get("plugins", {})


def test_web_ui_and_api_are_present() -> None:
    control = (ROOT / "apps/web/static/control.html").read_text(encoding="utf-8")
    schema_source = (ROOT / "apps/server/plugin_config_schema.py").read_text(encoding="utf-8")
    web_source = (ROOT / "apps/server/plugin_config_web.py").read_text(encoding="utf-8")
    assert 'data-settings="plugin-config"' in control
    assert 'src="/plugin-config"' in control
    assert '"/api/plugins/config"' in schema_source
    assert "secret_fields" in schema_source
    assert "secrets.compare_digest" in web_source
    assert "Content-Security-Policy" in web_source
    assert "敏感字段不会明文回显" in web_source


def main() -> None:
    tests = [
        test_manifest_schema_and_secret_permission,
        test_persistence_defaults_secret_redaction_and_runtime_isolation,
        test_validation_and_required_fields,
        test_uninstall_removes_config_but_upgrade_preserves_it,
        test_web_ui_and_api_are_present,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print("issue #136 plugin config schema guard: OK")


if __name__ == "__main__":
    main()
