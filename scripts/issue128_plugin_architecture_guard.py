from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.server import issue79_guard  # noqa: E402
from apps.server import server  # noqa: E402
from apps.server.danmu_plugins import (  # noqa: E402
    PLUGIN_API_VERSION,
    DanmuPlugin,
    DanmuPluginRegistry,
)

EXPECTED = {
    "bilibili": "Bilibili 获取弹幕插件",
    "douyin": "抖音获取弹幕插件",
    "huya": "虎牙获取弹幕插件",
    "youtube": "YouTube 获取弹幕插件",
    "twitch": "Twitch 获取弹幕插件",
}


def test_builtin_plugins_registered() -> None:
    registry = server.danmu_plugin_registry
    assert isinstance(registry, DanmuPluginRegistry)
    assert tuple(registry.platform_ids()) == tuple(EXPECTED.keys())
    plugins = {item.platform: item for item in registry.list_plugins()}
    assert set(plugins) == set(EXPECTED)
    for platform, expected_name in EXPECTED.items():
        plugin = plugins[platform]
        assert plugin.name == expected_name
        assert plugin.plugin_api == PLUGIN_API_VERSION
        assert plugin.plugin_type == "danmu_source"
        assert plugin.source == "builtin"
        assert callable(plugin.relay_factory)
        assert plugin.relay_factory.__name__.startswith("create_")


def test_registry_is_authoritative_factory() -> None:
    factory = server._create_danmu_relay
    assert getattr(factory, "_bilipdj_plugin_registry_factory", False)
    # The final factory must not be one of the old per-platform wrappers.
    assert not getattr(factory, "_bilipdj_huya_wrapped", False)
    assert not getattr(factory, "_bilipdj_redtv_wrapped", False)
    assert not getattr(factory, "_bilipdj_purple_mouse_wrapped", False)
    assert tuple(server.SUPPORTED_RUNTIME_PLATFORMS) == tuple(EXPECTED.keys())


def test_issue79_uses_plugin_registry() -> None:
    normalized = issue79_guard._normalize_active_platforms(
        server,
        {"active_platforms": ["bilibili", "huya", "youtube", "twitch", "unknown"]},
    )
    assert normalized == ("bilibili", "huya", "youtube", "twitch")

    fake_server = SimpleNamespace(
        runtime_config={"active_platforms": ["douyin", "huya"]},
        danmu_relay=SimpleNamespace(
            get_runtime_status=lambda: {
                "platform": "multi",
                "connected": True,
                "platforms": {
                    "douyin": {"connected": True},
                    "huya": {"connected": False},
                },
            }
        ),
    )
    payload = issue79_guard._platform_payload(server, fake_server)
    assert payload["plugin_api"] == PLUGIN_API_VERSION
    assert payload["supported"] == list(EXPECTED.keys())
    assert payload["active"] == ["douyin", "huya"]
    assert [item["name"] for item in payload["plugins"]] == list(EXPECTED.values())


def test_future_plugin_can_register_without_core_wrapper() -> None:
    registry = DanmuPluginRegistry()

    class FakeRelay:
        def __init__(self, active_server):
            self.server = active_server

    registry.register(
        DanmuPlugin(
            plugin_id="external.kuaishou.danmu",
            platform="kuaishou",
            name="快手获取弹幕插件",
            version="1.0.0",
            source="external",
            relay_factory=FakeRelay,
        )
    )
    fake_server = SimpleNamespace()
    relay = registry.create_relay("kuaishou", fake_server)
    assert isinstance(relay, FakeRelay)
    assert relay.server is fake_server
    info = registry.list_public()[0]
    assert info["platform"] == "kuaishou"
    assert info["name"] == "快手获取弹幕插件"
    assert info["plugin_api"] == PLUGIN_API_VERSION


def test_plugin_api_and_web_bridge_present() -> None:
    assert getattr(server.ApiHandler.do_GET, "_bilipdj_plugin_api", False)
    text = (ROOT / "apps/web/static/control_plugins.js").read_text(encoding="utf-8")
    assert "获取弹幕插件" in text
    assert "/api/platforms/active" in text
    assert "data-danmu-plugin-platform" in text
    assert "payload.plugins" in text


def main() -> None:
    tests = [
        test_builtin_plugins_registered,
        test_registry_is_authoritative_factory,
        test_issue79_uses_plugin_registry,
        test_future_plugin_can_register_without_core_wrapper,
        test_plugin_api_and_web_bridge_present,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print("issue #128 plugin architecture guard: OK")


if __name__ == "__main__":
    main()
