"""Issue #283: activation is a persistent global multi-select, not the editor's platform."""
from __future__ import annotations

import importlib
import io
import json
import logging
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@contextmanager
def isolated_server_data(backend, root: Path):
    overrides = {
        "CONFIG_PATH": root / "core" / "config.yaml",
        "PD_DIR": root / "core" / "cd",
        "LOG_DIR": root / "log",
        "QUANXIAN_PATH": root / "core" / "quanxian.yaml",
        "KAIGUAN_PATH": root / "core" / "kaiguan.yaml",
        "BLACKLIST_PATH": root / "core" / "blacklist.csv",
        "QUEUE_STATE_PATH": root / "core" / "cd" / "queue_archive_state.json",
        "STYLE_PATH": root / "style.json",
        "UI_DIR": root / "ui",
        "LIVE_STYLE_CSS_PATH": root / "ui" / "moren.css",
        "_CONFIG_LOCK_PATH": root / "core" / ".config.lock",
    }
    previous = {key: getattr(backend, key) for key in overrides}
    try:
        for key, value in overrides.items():
            setattr(backend, key, value)
        yield
    finally:
        for key, value in previous.items():
            setattr(backend, key, value)


class FakeRelay:
    def __init__(self, proxy):
        self.proxy = proxy
        self.started = False

    def start(self):
        self.started = True

    def stop(self):
        self.started = False

    def join(self, timeout=None):
        pass

    def request_reconnect(self):
        self.proxy.refresh_runtime_config()

    def get_runtime_status(self):
        return {"platform": self.proxy.platform, "connected": self.started}


class FakeHandler:
    def __init__(self, server, active):
        self.server = server
        self.path = "/api/platforms/active"
        body = json.dumps({"active": active}).encode("utf-8")
        self.headers = {"Content-Length": str(len(body))}
        self.rfile = io.BytesIO(body)
        self.response = None

    def _require_loopback(self):
        return True

    def _write_json(self, payload, **kwargs):
        self.response = payload

    def do_GET(self):
        raise AssertionError("unexpected GET")

    def do_POST(self):
        raise AssertionError("POST route was not installed")


def main():
    backend = importlib.import_module("apps.server.server")
    issue79 = importlib.import_module("apps.server.issue79_guard")
    with tempfile.TemporaryDirectory(prefix="bilipdj-283-") as temp:
        with isolated_server_data(backend, Path(temp)):
            # Legacy config has no active_platforms and remains single-platform.
            initial = backend._merge_config(backend.DEFAULT_CONFIG, {"platform": "douyin"})
            backend.save_config(initial)
            legacy = backend.load_config()
            assert "active_platforms" not in backend.load_simple_yaml(backend.CONFIG_PATH)
            assert issue79._normalize_active_platforms(backend, legacy) == ("douyin",)

            backend.save_platform_config_slot(
                1,
                backend._merge_config(
                    backend.load_platform_config_slot(1),
                    {"platform": "douyin", "bilibili": {"roomid": 30094659, "uid": 18461303},
                     "douyin": {"live_id": "mock-douyin-room", "enabled": True}},
                ),
            )
            backend.save_config(backend._merge_config(backend.load_config(), {"platform": "douyin"}))

            # Call the actual issue79 POST route, using an isolated fake server.
            api_module = SimpleNamespace(
                ApiHandler=FakeHandler,
                load_config=backend.load_config,
                save_config=backend.save_config,
                config_io_transaction=backend.config_io_transaction,
                _create_danmu_relay=lambda proxy: FakeRelay(proxy),
            )
            assert issue79.install_issue79_guard(api_module)
            fake_server = SimpleNamespace(
                runtime_config=backend.load_config(),
                logger=logging.getLogger("issue283"),
                danmu_relay=None,
            )
            handler = FakeHandler(fake_server, ["bilibili", "douyin"])
            handler.do_POST()
            assert handler.response["active"] == ["bilibili", "douyin"], handler.response
            assert list(fake_server.danmu_relay._relays) == ["bilibili", "douyin"]
            assert all(relay.started for relay in fake_server.danmu_relay._relays.values())
            for name, relay in fake_server.danmu_relay._relays.items():
                assert relay.proxy.runtime_config["platform"] == name
            assert fake_server.runtime_config["platform"] == "douyin"

            # GUI saves and platform editor switching must not turn off either relay.
            changed = backend._merge_config(backend.load_config(), {"platform": "bilibili"})
            backend.save_platform_config_slot(1, backend._build_platform_config_payload(changed))
            backend.save_config(changed)
            restored = backend.load_config()
            assert restored["active_platforms"] == ["bilibili", "douyin"], restored
            assert restored["platform"] == "bilibili"
            assert restored["bilibili"]["roomid"] == 30094659
            assert restored["douyin"]["live_id"] == "mock-douyin-room"
            assert issue79._normalize_active_platforms(backend, restored) == ("bilibili", "douyin")

            # Activating none must survive serializing and loading (not fall back).
            handler = FakeHandler(fake_server, [])
            handler.do_POST()
            assert handler.response["active"] == [], handler.response
            assert backend.load_config()["active_platforms"] == []
            assert fake_server.danmu_relay.active_platforms == ()
            assert issue79._normalize_active_platforms(backend, backend.load_config()) == ()
            # No implicit change to editor selection.
            assert backend.load_config()["platform"] == "bilibili"

    print("issue #283 multi-platform activation/config persistence: OK")


if __name__ == "__main__":
    main()
