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
from apps.server import plugin_runtime_dual  # noqa: E402
from apps.server.danmu_plugins import DanmuPluginRegistry  # noqa: E402

plugin_runtime_dual.install_dual_runtime_support()

SOURCE = b'''\
function createRelay(config, host) {
  let state = {connected: false, roomid: config.roomid || '', reconnects: 0};
  return {
    start() {
      state.connected = true;
      host.setStatus({connected: true, roomid: state.roomid});
      host.emit({type: 'PLUGIN_TEST', platform: 'kuaishou_js'});
      host.processDanmu({cmd: 'DANMU_MSG', info: [[], 'js-test']});
    },
    tick() {},
    stop() { state.connected = false; host.setStatus({connected: false}); },
    requestReconnect() { state.reconnects += 1; },
    getRuntimeStatus() { return state; }
  };
}
'''


def package() -> bytes:
    manifest = {
        "schema": 1,
        "id": "example.javascript.probe",
        "name": "JS Probe",
        "version": "1.0.0",
        "plugin_api": 1,
        "type": "danmu_source",
        "platform": "js_probe",
        "runtime": "javascript",
        "entry": "plugin.js",
        "min_bilipdj_version": "2.0.0",
        "permissions": [],
        "capabilities": ["danmu"],
        "files": {"plugin.js": hashlib.sha256(SOURCE).hexdigest()},
    }
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(manifest).encode())
        archive.writestr("plugin.js", SOURCE)
    return out.getvalue()


def main() -> None:
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
        manager.install_bytes(package(), filename="probe.bilipdj-plugin", allow_unsigned=True)
        manager.set_enabled("example.javascript.probe", True)
        events = []
        danmu = []
        active = SimpleNamespace(
            runtime_config={"platform": "js_probe", "js_probe": {"roomid": "room-test"}},
            logger=logging.getLogger("js-probe"),
            ws_hub=SimpleNamespace(broadcast_json=lambda _sender, payload: events.append(dict(payload))),
            queue_manager=SimpleNamespace(process_danmu_json=lambda payload: danmu.append(dict(payload))),
        )
        relay = registry.create_relay("js_probe", active)
        relay.start()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and relay.get_runtime_status().get("state") not in {"running", "error"}:
            time.sleep(0.02)
        status = relay.get_runtime_status()
        print("JS_PROBE_STATUS", json.dumps(status, ensure_ascii=False, sort_keys=True))
        print("JS_PROBE_EVENTS", json.dumps(events, ensure_ascii=False))
        print("JS_PROBE_DANMU", json.dumps(danmu, ensure_ascii=False))
        assert status.get("state") == "running", status
        assert status.get("connected") is True, status
        assert status.get("roomid") == "room-test", status
        assert any(item.get("type") == "PLUGIN_TEST" for item in events), events
        assert danmu and danmu[0].get("cmd") == "DANMU_MSG", danmu
        relay.stop()
        relay.join(timeout=2)
        stopped = relay.get_runtime_status()
        assert stopped.get("state") == "stopped", stopped
        assert stopped.get("connected") is False, stopped
        print("issue #130 JavaScript runtime probe: OK")


if __name__ == "__main__":
    main()
