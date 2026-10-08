"""Issue #284: more than two relays can be active without GUI alias/state loss."""
from __future__ import annotations

import importlib
import json
import logging
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from issue283_multi_platform_guard import FakeHandler, FakeRelay, isolated_server_data


def backend_five_relay_test():
    backend = importlib.import_module("apps.server.server")
    guard = importlib.import_module("apps.server.issue79_guard")
    wanted = ["bilibili", "douyin", "huya", "youtube", "twitch"]
    registry = backend.danmu_plugin_registry
    for name in wanted:
        plugin = registry.get(name)
        assert plugin is not None and plugin.available, name
    assert all(name in guard.SUPPORTED_ACTIVE_PLATFORMS for name in wanted)

    with tempfile.TemporaryDirectory(prefix="bilipdj-284-") as folder:
        with isolated_server_data(backend, Path(folder)):
            backend.save_config(backend.DEFAULT_CONFIG)
            backend.save_platform_config_slot(
                1,
                backend._merge_config(
                    backend.load_platform_config_slot(1),
                    {
                        "platform": "huya",
                        "bilibili": {"roomid": 30094659, "uid": 18461303},
                        "douyin": {"live_id": "simulated-douyin", "enabled": True},
                        "huya": {"room_url": "https://www.huya.com/example", "enabled": True},
                        "youtube": {"room_url": "https://www.youtube.com/watch?v=AAAABBBBCCC", "enabled": True},
                        "twitch": {"room_url": "https://www.twitch.tv/example", "enabled": True},
                    },
                ),
            )
            backend.save_config(backend._merge_config(backend.load_config(), {"platform": "huya"}))
            fake_module = SimpleNamespace(
                ApiHandler=FakeHandler,
                load_config=backend.load_config,
                save_config=backend.save_config,
                config_io_transaction=backend.config_io_transaction,
                danmu_plugin_registry=registry,
                _create_danmu_relay=lambda proxy: FakeRelay(proxy),
            )
            assert guard.install_issue79_guard(fake_module)
            fake_server = SimpleNamespace(
                runtime_config=backend.load_config(),
                logger=logging.getLogger("issue284"),
                danmu_relay=None,
            )
            request = FakeHandler(fake_server, [*wanted, "missing-platform", "bilibili"])
            request.do_POST()
            assert request.response["active"] == wanted, request.response
            assert fake_server.runtime_config["platform"] == "huya"
            assert list(fake_server.danmu_relay._relays) == wanted
            for platform, relay in fake_server.danmu_relay._relays.items():
                assert relay.started
                assert relay.proxy.runtime_config["platform"] == platform
            reloaded = backend.load_config()
            assert reloaded["active_platforms"] == wanted, reloaded
            backend.save_config(backend._merge_config(reloaded, {"platform": "douyin"}))
            assert backend.load_config()["active_platforms"] == wanted
            assert backend.load_config()["platform"] == "douyin"
            request = FakeHandler(fake_server, [])
            request.do_POST()
            assert backend.load_config()["active_platforms"] == []
            assert fake_server.danmu_relay.active_platforms == ()


def windows_gui_plugin_list_test():
    from apps.windows import platform_features as features

    sample = {
        "supported": ["bilibili", "douyin", "huya", "youtube", "twitch", "future-plugin"],
        "plugins": [
            {"platform": "bilibili", "name": "Bilibili 获取弹幕插件", "available": True},
            {"platform": "douyin", "name": "抖音 获取弹幕插件", "available": True},
            {"platform": "huya", "name": "虎牙 获取弹幕插件", "available": True},
            {"platform": "youtube", "name": "红色小电视 获取弹幕插件", "available": True},
            {"platform": "twitch", "name": "紫色老鼠 获取弹幕插件", "available": True},
            {"platform": "future-plugin", "name": "未来直播 获取弹幕插件", "available": True},
            {"platform": "unavailable", "name": "未实现", "available": False},
        ],
    }
    specs = features._registered_active_plugin_specs(sample)
    assert [name for name, _ in specs] == sample["supported"], specs

    class Variable:
        def __init__(self, value=False):
            self.value = value
        def get(self):
            return self.value
        def set(self, value):
            self.value = value

    class Checkbutton:
        def __init__(self, parent, *, text, variable):
            parent.options.append((text, variable))
        def grid(self, **kwargs):
            pass

    variables = {name: Variable() for name in ["bilibili", "douyin", "huya", "youtube", "twitch"]}
    choices = SimpleNamespace(options=[])
    panel = SimpleNamespace()
    module = SimpleNamespace(tk=SimpleNamespace(BooleanVar=Variable), ttk=SimpleNamespace(Checkbutton=Checkbutton))
    features._sync_active_plugin_controls(panel, module, choices, variables, sample)
    assert set(panel._platform_supported) == set(sample["supported"])
    assert "future-plugin" in variables
    assert len(choices.options) == 1
    assert "未来直播" in choices.options[0][0]

    # A YouTube/Twitch checkbox is not created before its guarded configuration
    # is complete; existing configured widgets remain connected by key.
    missing = {"bilibili": Variable(), "douyin": Variable()}
    choices = SimpleNamespace(options=[])
    features._sync_active_plugin_controls(panel, module, choices, missing, sample)
    assert "huya" in missing and "future-plugin" in missing
    assert "youtube" not in missing and "twitch" not in missing

    # Platform-specific GUI status adapters must use structured active IDs, not
    # localized status text as the source of truth.
    from apps.windows.huya_control_guard import _sync_huya_var_from_status
    huya = Variable()
    panel._platform_vars = {"huya": huya}
    panel._platform_status_var = Variable("已激活：红色小电视, 紫色老鼠")
    panel._platform_active_ids = frozenset(sample["supported"])
    _sync_huya_var_from_status(panel)
    assert huya.get()
    for filename in ("redtv_control_guard.py", "purple_mouse_control_guard.py"):
        source = (ROOT / "apps" / "windows" / filename).read_text(encoding="utf-8")
        assert 'getattr(panel, "_platform_active_ids", None)' in source, filename
    unavailable = features._registered_active_plugin_specs({
        "supported": ["huya", "unavailable"],
        "plugins": [
            {"platform": "huya", "name": "虎牙", "available": True},
            {"platform": "unavailable", "name": "Not ready", "available": False},
        ],
    })
    assert unavailable == [("huya", "虎牙")]


if __name__ == "__main__":
    backend_five_relay_test()
    windows_gui_plugin_list_test()
    print("issue #284 dynamic five-platform danmu activation: OK")
