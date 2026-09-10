from __future__ import annotations

import hashlib
import io
import json
import logging
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.server import plugin_manager as pm  # noqa: E402
from apps.server import plugin_runtime_dual, server  # noqa: E402
from apps.server.danmu_event import DanmuEvent  # noqa: E402
from apps.server.danmu_plugins import DanmuPluginRegistry  # noqa: E402


class Hub:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def broadcast_json(self, _sender, payload) -> None:
        self.events.append(dict(payload))


class Archive:
    def write_snapshot(self, *_args, **_kwargs):
        return None


def check_model_and_queue() -> None:
    first = DanmuEvent(platform="kuaishou", user_id="ks:user:abc", username="Alice", content="hello")
    second = DanmuEvent(platform="kuaishou", user_id="ks:user:abc", username="Alice", content="hello")
    assert first.legacy_uid() == second.legacy_uid() > 0
    assert first.identity_dict()["user_id"] == "ks:user:abc"

    spaced = DanmuEvent(platform="kuaishou", user_id="space-id", username="Alice", content="  hello  ")
    assert spaced.content == "  hello  ", "DanmuEvent must preserve message whitespace"
    preserved_identity = DanmuEvent(
        platform="bilibili",
        user_id="123",
        username="MedalUser",
        content="medal",
        fan_medal={"level": 0, "name": ""},
        metadata={"identity": {"has_fan_medal": False}},
    ).identity_dict()
    assert preserved_identity["has_fan_medal"] is False

    hub = Hub()
    manager = server.QueueManager(hub, Archive(), logging.getLogger("issue142"))
    calls = []
    manager._process = lambda uid, uname, msg, is_anchor, is_admin, is_guard, guard_level: (calls.append((uid, uname, msg, is_anchor, is_admin, is_guard, guard_level)) or (False, None))
    manager.process_danmu_event(first)
    assert calls[-1][0] == first.legacy_uid()
    assert calls[-1][1:3] == ("Alice", "hello")
    public = manager.get_last_danmu_event()
    assert public["platform"] == "kuaishou"
    assert public["identity"]["user_id"] == "ks:user:abc"

    manager.process_danmu_json({"cmd": "DANMU_MSG", "info": [[], " legacy ", [12345, "LegacyUser", 0], []]})
    legacy = manager.get_last_danmu_event()
    assert legacy["platform"] == "bilibili"
    assert legacy["message"] == " legacy "
    assert legacy["identity"]["uid"] == 12345


def check_builtin_sources() -> None:
    for name in ("douyin_protocol.py", "huya_protocol.py", "twitch_protocol.py", "youtube_protocol.py"):
        source = (ROOT / "apps" / "server" / name).read_text(encoding="utf-8")
        assert "_to_bilibili_like_danmu_payload" not in source, name
        assert "process_danmu_event" in source, name
        assert "DanmuEvent(" in source, name
    douyin = (ROOT / "apps/server/douyin_protocol.py").read_text(encoding="utf-8")
    assert "is_room_admin=event.user_role >= 3" in douyin


def check_python_plugin_context() -> None:
    captured = []
    queue = SimpleNamespace(
        process_danmu_event=lambda event: captured.append(event),
        process_danmu_json=lambda payload: captured.append(dict(payload)),
    )
    active = SimpleNamespace(runtime_config={}, logger=logging.getLogger("issue142-plugin"), queue_manager=queue)
    record = pm.InstalledPluginRecord(
        plugin_id="example.event",
        root=ROOT,
        manifest={"platform": "kuaishou", "permissions": []},
        enabled=True,
        package_sha256="0" * 64,
        signature_status="unsigned",
    )
    ctx = pm.PluginContext(SimpleNamespace(data_root=ROOT), active, record)
    ctx.process_danmu_event({"user_id": "native:id", "username": "Alice", "content": "event"})
    assert isinstance(captured[-1], DanmuEvent)
    assert captured[-1].platform == "kuaishou"
    assert captured[-1].user_id == "native:id"
    try:
        ctx.process_danmu_event({"platform": "douyin", "user_id": "x", "username": "Alice", "content": "bad"})
    except ValueError:
        pass
    else:
        raise AssertionError("plugin platform spoof should be rejected")


JS_SOURCE = b'''function createRelay(config, host) {
  let state = {connected: false};
  return {
    start() {
      state.connected = true;
      host.processDanmuEvent({user_id: 'native:js:id', username: 'JSUser', content: 'event-js'});
      host.processDanmu({cmd: 'DANMU_MSG', info: [[], 'legacy-js', [2468, 'LegacyJS', 0], []]});
      host.setStatus({connected: true});
    },
    tick() {},
    stop() { state.connected = false; host.setStatus({connected: false}); },
    getRuntimeStatus() { return state; }
  };
}
'''


def js_package() -> bytes:
    manifest = {
        "schema": 1,
        "id": "example.event.javascript",
        "name": "Event JS Probe",
        "version": "1.0.0",
        "plugin_api": 1,
        "type": "danmu_source",
        "platform": "js_event_probe",
        "runtime": "javascript",
        "entry": "plugin.js",
        "min_bilipdj_version": "2.0.0",
        "permissions": [],
        "capabilities": ["danmu"],
        "files": {"plugin.js": hashlib.sha256(JS_SOURCE).hexdigest()},
    }
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(manifest).encode())
        archive.writestr("plugin.js", JS_SOURCE)
    return out.getvalue()


def check_javascript_plugin_api() -> None:
    plugin_runtime_dual.install_dual_runtime_support()
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        module = SimpleNamespace(
            APP_DIR=root,
            BUNDLE_DIR=ROOT,
            REPO_DIR=ROOT,
            DEFAULT_PLATFORM="bilibili",
            RESERVED_RUNTIME_PLATFORMS=(),
            ALL_RUNTIME_PLATFORMS=(),
            PLATFORM_DISPLAY_NAMES={},
            _get_runtime_platform=lambda cfg: str((cfg or {}).get("platform", "bilibili")),
        )
        registry = DanmuPluginRegistry()
        manager = pm.PluginManager(module, registry)
        manager.install_bytes(js_package(), filename="event-probe.bilipdj-plugin", allow_unsigned=True)
        manager.set_enabled("example.event.javascript", True)
        canonical = []
        legacy = []
        active = SimpleNamespace(
            runtime_config={"platform": "js_event_probe", "js_event_probe": {}},
            logger=logging.getLogger("issue142-js"),
            ws_hub=SimpleNamespace(broadcast_json=lambda _sender, _payload: None),
            queue_manager=SimpleNamespace(
                process_danmu_event=lambda event: canonical.append(event),
                process_danmu_json=lambda payload: legacy.append(dict(payload)),
            ),
        )
        relay = registry.create_relay("js_event_probe", active)
        relay.start()
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline and (not canonical or not legacy):
            status = relay.get_runtime_status()
            if status.get("state") == "error":
                raise AssertionError(status)
            time.sleep(0.02)
        assert canonical and isinstance(canonical[0], DanmuEvent), canonical
        assert canonical[0].platform == "js_event_probe"
        assert canonical[0].user_id == "native:js:id"
        assert legacy and legacy[0].get("cmd") == "DANMU_MSG", legacy
        relay.stop()
        relay.join(timeout=2)


def main() -> None:
    check_model_and_queue()
    check_builtin_sources()
    check_python_plugin_context()
    check_javascript_plugin_api()
    print("issue #142 DanmuEvent regression guard: OK")


if __name__ == "__main__":
    main()
