from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import sys
import tempfile
import threading
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace

from apps.server import plugin_manager as pm
from apps.server import plugin_runtime_dual
from apps.server.danmu_event import DanmuEvent
from apps.server.danmu_plugins import DanmuPluginRegistry

JS_SOURCE = b"""function createRelay(config, host) {
  let state = {connected: false};
  return {
    start() {
      state.connected = true;
      host.processDanmuEvent({user_id: 'frozen:user', username: 'FrozenProbe', content: 'self-test'});
      host.setStatus({connected: true, probe: 'ok'});
    },
    tick() {},
    stop() { state.connected = false; host.setStatus({connected: false}); },
    getRuntimeStatus() { return state; }
  };
}
"""


def _plugin_package() -> bytes:
    manifest = {
        "schema": 1,
        "id": "selftest.frozen.javascript",
        "name": "Frozen JavaScript Runtime Self-test",
        "version": "1.0.0",
        "plugin_api": 1,
        "type": "danmu_source",
        "platform": "frozen_probe",
        "runtime": "javascript",
        "entry": "plugin.js",
        "min_bilipdj_version": "2.0.0",
        "permissions": [],
        "capabilities": ["danmu"],
        "files": {"plugin.js": hashlib.sha256(JS_SOURCE).hexdigest()},
    }
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(manifest).encode("utf-8"))
        archive.writestr("plugin.js", JS_SOURCE)
    return output.getvalue()


def _arm_frozen_probe_watchdog(timeout: float = 20.0) -> None:
    """Prevent a frozen self-test worker from hanging CI indefinitely."""

    if not getattr(sys, "frozen", False) or "--plugin-runtime-self-test" not in sys.argv[1:]:
        return

    def watchdog() -> None:
        time.sleep(max(5.0, float(timeout)))
        os._exit(124)

    threading.Thread(target=watchdog, name="bilipdj-frozen-probe-watchdog", daemon=True).start()


def run_frozen_plugin_probe() -> None:
    _arm_frozen_probe_watchdog()
    plugin_runtime_dual.install_dual_runtime_support()
    bundle_root = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parents[2])).resolve()
    with tempfile.TemporaryDirectory(prefix="bilipdj-frozen-js-probe-") as temp:
        root = Path(temp)
        module = SimpleNamespace(
            APP_DIR=root,
            BUNDLE_DIR=bundle_root,
            REPO_DIR=bundle_root,
            DEFAULT_PLATFORM="bilibili",
            RESERVED_RUNTIME_PLATFORMS=(),
            ALL_RUNTIME_PLATFORMS=(),
            PLATFORM_DISPLAY_NAMES={},
            _get_runtime_platform=lambda cfg: str((cfg or {}).get("platform", "bilibili")),
        )
        registry = DanmuPluginRegistry()
        manager = pm.PluginManager(module, registry)
        manager.install_bytes(_plugin_package(), filename="frozen-probe.bilipdj-plugin", allow_unsigned=True)
        manager.set_enabled("selftest.frozen.javascript", True)
        events: list[DanmuEvent] = []
        active = SimpleNamespace(
            runtime_config={"platform": "frozen_probe", "frozen_probe": {}},
            logger=logging.getLogger("bilipdj.frozen_probe"),
            ws_hub=SimpleNamespace(broadcast_json=lambda _sender, _payload: None),
            queue_manager=SimpleNamespace(
                process_danmu_event=lambda event: events.append(event),
                process_danmu_json=lambda _payload: None,
            ),
        )
        relay = registry.create_relay("frozen_probe", active)
        relay.start()
        try:
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and not events:
                status = relay.get_runtime_status()
                if status.get("state") == "error":
                    raise RuntimeError(f"frozen JavaScript runtime error: {status}")
                time.sleep(0.03)
            if not events:
                raise RuntimeError("frozen JavaScript plugin did not emit a DanmuEvent")
            event = events[0]
            if not isinstance(event, DanmuEvent):
                raise RuntimeError(f"unexpected event type: {type(event)!r}")
            if event.platform != "frozen_probe" or event.user_id != "frozen:user" or event.content != "self-test":
                raise RuntimeError(f"unexpected frozen plugin event: {event.to_dict()}")
        finally:
            relay.stop()
            relay.join(timeout=2)


def main() -> int:
    try:
        run_frozen_plugin_probe()
    except Exception as exc:
        print(f"BILIPDJ_FROZEN_JS_PLUGIN_SELF_TEST_FAILED: {exc}", file=sys.stderr)
        return 1
    print("BILIPDJ_FROZEN_JS_PLUGIN_SELF_TEST_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
