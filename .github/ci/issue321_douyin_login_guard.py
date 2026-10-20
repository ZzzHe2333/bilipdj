"""Issue #321 regression guard: Douyin QR login and private parameter retrieval.

All fixtures are synthetic. No network, actual browser or account credentials.
"""
from __future__ import annotations

import io
import json
import os
import stat
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from apps.server import douyin_login


def test_cookie_filtering() -> None:
    fixtures = [
        {"domain": ".douyin.com", "name": "sessionid", "value": "SYNTHETIC_SESSION"},
        {"domain": ".douyin.com", "name": "ttwid", "value": "SYNTHETIC_TTWID"},
        {"domain": ".douyin.com", "name": "msToken", "value": "SYNTHETIC_TOKEN"},
        {"domain": ".douyin.com", "name": "unrelated_tracking", "value": "SHOULD_NOT_COPY"},
        {"domain": "attacker.example", "name": "sessionid", "value": "EVIL"},
        {"domain": ".douyin.com", "name": "sessionid_ss", "value": "bad\r\nInjected: x"},
    ]
    encoded = douyin_login._redacted_cookie_header(fixtures)
    assert "SYNTHETIC_SESSION" in encoded
    assert "SYNTHETIC_TTWID" in encoded
    assert "SHOULD_NOT_COPY" not in encoded
    assert "EVIL" not in encoded
    assert "Injected" not in encoded


def test_local_private_credentials_and_params() -> None:
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        cfg = {
            "platform": "bilibili",
            "douyin": {"enabled": False, "live_id": "", "cookie": "",
                       "live_info": {}},
            "platform_config_archive": {"active_slot": 1},
        }
        saves = []
        def write_slot(slot, payload):
            saves.append(("slot", slot, payload))
        def write_config(payload):
            saved = json.loads(json.dumps(payload))
            cfg.clear()
            cfg.update(saved)
            saves.append(("config", saved))
        module = SimpleNamespace(
            KEY_DIR=base, _YAML_DIR=base,
            load_config=lambda: cfg,
            save_config=write_config,
            save_platform_config_slot=write_slot,
            _build_platform_config_payload=lambda c: c,
            _ensure_danmu_relay=lambda *_args, **_kwargs: saves.append(("reconnect",)),
        )
        login = douyin_login.DouyinLoginManager(module)
        login._write = None
        douyin_login._write_private_json(login.secret_path, {
            "schema": 1,
            "cookies": [{"name": "sessionid", "domain": ".douyin.com",
                         "value": "SYNTHETIC_SESSION"}],
        })
        assert login.status()["logged_in"] is True
        assert "cookies" not in login.status()
        assert "session" not in json.dumps(login.status()).lower()
        if os.name != "nt":
            assert stat.S_IMODE(login.secret_path.stat().st_mode) == 0o600

        info = SimpleNamespace(
            live_id="12345678901", room_id="987654321",
            user_id="PRIVATE_VIEWER_UID", user_unique_id="PRIVATE_VIEWER_ID",
            anchor_id="PRIVATE_ANCHOR_ID", room_status="2",
            room_title="Synthetic Live", ttwid="PRIVATE_TTWID",
        )
        backend = SimpleNamespace(runtime_config=cfg,
                                  logger=SimpleNamespace(exception=lambda *_: None))
        with patch("apps.server.douyin_protocol.fetch_douyin_live_info", return_value=info), \
             patch("apps.server.douyin_protocol.map_room_status", return_value="live"):
            res = login.fetch_params("12345678901", backend=backend, apply=True)
        assert res["room"]["room_id"] == "987654321"
        assert res["applied"] is True
        serialized = json.dumps(res)
        for secret in ("SYNTHETIC_SESSION", "PRIVATE_VIEWER_UID",
                       "PRIVATE_VIEWER_ID", "PRIVATE_TTWID", "PRIVATE_ANCHOR_ID"):
            assert secret not in serialized, secret
        assert backend.runtime_config["douyin"]["live_info"]["user_id"] == "PRIVATE_VIEWER_UID"
        assert backend.runtime_config["douyin"]["cookie"] == ""
        assert saves[-1] == ("reconnect",)
        assert saves[-2][0] == "config"


def test_http_guards() -> None:
    class Relay:
        def _load_runtime_cfg(self):
            return {"cookie": ""}

    class Handler:
        def __init__(self, url: str, data=b"{}", origin="http://127.0.0.1:9816"):
            self.path = url
            self.headers = {
                "Origin": origin, "Host": "127.0.0.1:9816",
                "Sec-Fetch-Site": "same-origin",
                "Content-Type": "application/json",
                "Content-Length": str(len(data)),
            }
            self.rfile = io.BytesIO(data)
            self.client_address = ("127.0.0.1", 10000)
            self.server = SimpleNamespace(runtime_config={}, logger=SimpleNamespace(exception=lambda *_: None))
            self.result = None

        def _require_loopback(self): return True

        def _write_json(self, payload, status=200):
            self.result = (int(status), payload)

        def do_POST(self):
            self.result = (200, {"prior_post": True})

        def do_GET(self):
            self.result = (200, {"prior_get": True})

    with tempfile.TemporaryDirectory() as td:
        module = SimpleNamespace(
            ApiHandler=Handler, KEY_DIR=Path(td), _YAML_DIR=Path(td),
            douyin_protocol=SimpleNamespace(DouyinDanmuRelay=Relay),
        )
        assert douyin_login.install_douyin_login(module)
        assert douyin_login.install_douyin_login(module)
        h = Handler(douyin_login.PARAMS_PATH, origin="https://evil.example")
        h.do_POST()
        assert h.result[0] == 403
        h = Handler(douyin_login.PARAMS_PATH, data=b'{"live_id":"123"}')
        h.do_POST()
        assert h.result[0] == 400   # no stored login
        h = Handler("/api/unrelated")
        h.do_POST()
        assert h.result[1] == {"prior_post": True}
        h = Handler(douyin_login.STATUS_PATH)
        h.do_GET()
        assert h.result[0] == 200
        assert "cookie" not in json.dumps(h.result[1]).lower()


def main() -> None:
    test_cookie_filtering()
    test_local_private_credentials_and_params()
    test_http_guards()
    print("Issue #321 Douyin login and parameter security checks passed.")


if __name__ == "__main__":
    main()
