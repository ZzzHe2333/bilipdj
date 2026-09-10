from __future__ import annotations

import io
import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.server import plugin_api_security_guard as guard  # noqa: E402
from apps.server import plugin_manager as pm  # noqa: E402


class FakeHandler:
    def __init__(self, headers: dict[str, str], *, port: int = 9816) -> None:
        self.headers = headers
        self.server = SimpleNamespace(server_port=port)
        self.responses: list[tuple[int, dict[str, Any]]] = []

    def _write_json(self, payload: dict[str, Any], status: int = 200) -> None:
        self.responses.append((int(status), dict(payload)))


def _safe(headers: dict[str, str], *, port: int = 9816) -> tuple[bool, FakeHandler]:
    handler = FakeHandler(headers, port=port)
    return guard.require_safe_plugin_management_request(handler), handler


def test_cross_site_origin_is_rejected() -> None:
    ok, handler = _safe(
        {
            "Content-Type": "application/json",
            "Content-Length": "2",
            "Origin": "https://evil.example",
            "Sec-Fetch-Site": "cross-site",
        }
    )
    assert ok is False
    assert handler.responses[-1][0] == 403


def test_simple_text_plain_csrf_is_rejected() -> None:
    ok, handler = _safe(
        {
            "Content-Type": "text/plain",
            "Content-Length": "2",
            "Origin": "http://evil.example",
        }
    )
    assert ok is False
    assert handler.responses[-1][0] == 415


def test_dns_rebinding_style_origin_is_rejected() -> None:
    ok, handler = _safe(
        {
            "Content-Type": "application/json; charset=utf-8",
            "Content-Length": "2",
            "Origin": "http://attacker.invalid:9816",
        }
    )
    assert ok is False
    assert handler.responses[-1][0] == 403


def test_same_origin_loopback_and_local_script_are_allowed() -> None:
    for origin in (
        "http://127.0.0.1:9816",
        "http://localhost:9816",
        "http://[::1]:9816",
    ):
        ok, handler = _safe(
            {
                "Content-Type": "application/json; charset=utf-8",
                "Content-Length": "2",
                "Origin": origin,
                "Sec-Fetch-Site": "same-origin",
            }
        )
        assert ok is True, (origin, handler.responses)
        assert not handler.responses

    ok, handler = _safe({"Content-Type": "application/json", "Content-Length": "2"})
    assert ok is True
    assert not handler.responses


def test_wrong_port_empty_and_oversized_bodies_are_rejected() -> None:
    ok, handler = _safe(
        {
            "Content-Type": "application/json",
            "Content-Length": "2",
            "Origin": "http://127.0.0.1:9999",
        }
    )
    assert ok is False and handler.responses[-1][0] == 403

    ok, handler = _safe({"Content-Type": "application/json", "Content-Length": "0"})
    assert ok is False and handler.responses[-1][0] == 400

    ok, handler = _safe(
        {
            "Content-Type": "application/json",
            "Content-Length": str(guard.MAX_PLUGIN_MANAGEMENT_BODY_BYTES + 1),
        }
    )
    assert ok is False and handler.responses[-1][0] == 413


def test_json_body_guard_caps_direct_reads() -> None:
    original = pm._json_body
    fake_pm = SimpleNamespace(
        _json_body=original,
        PluginError=pm.PluginError,
        _reconcile_runtime=lambda *_args, **_kwargs: None,
    )

    class Handler:
        def do_POST(self) -> None:  # noqa: N802
            return None

    manager = SimpleNamespace(
        trusted_keys_path=Path(tempfile.gettempdir()) / "bilipdj-issue132-unused.json",
        discover=lambda: None,
    )
    server_module = SimpleNamespace(
        ApiHandler=Handler,
        plugin_manager=manager,
        danmu_plugin_registry=object(),
    )
    guard.install_plugin_api_security_guard(server_module, fake_pm)

    oversized = SimpleNamespace(
        headers={"Content-Length": str(guard.MAX_PLUGIN_MANAGEMENT_BODY_BYTES + 1)},
        rfile=io.BytesIO(b"{}"),
    )
    try:
        fake_pm._json_body(oversized)
    except pm.PluginError as exc:
        assert "safety limit" in str(exc)
    else:
        raise AssertionError("oversized body should be rejected before read")


def test_trust_mutation_reconciles_runtime_only_after_state_change() -> None:
    with tempfile.TemporaryDirectory() as temp:
        trust_path = Path(temp) / "trusted_keys.json"
        trust_path.write_text('{"schema":1,"keys":{"old":"abc"}}\n', encoding="utf-8")
        calls: list[str] = []

        class Manager:
            trusted_keys_path = trust_path

            def discover(self) -> None:
                calls.append("discover")

        manager = Manager()

        def reconcile(_server_module: Any, active_server: Any, _registry: Any) -> None:
            calls.append("reconcile")
            active_server.reconciled = True

        fake_pm = SimpleNamespace(
            _json_body=lambda _handler: {},
            PluginError=pm.PluginError,
            _reconcile_runtime=reconcile,
        )

        class Handler:
            def __init__(self, *, mutate: bool) -> None:
                self.path = "/api/plugins/trusted-keys/delete"
                self.headers = {
                    "Content-Type": "application/json",
                    "Content-Length": "18",
                    "Origin": "http://127.0.0.1:9816",
                    "Sec-Fetch-Site": "same-origin",
                }
                self.server = SimpleNamespace(server_port=9816, reconciled=False)
                self.mutate = mutate
                self.original_called = False
                self.responses: list[tuple[int, dict[str, Any]]] = []

            def _require_loopback(self) -> bool:
                return True

            def _write_json(self, payload: dict[str, Any], status: int = 200) -> None:
                self.responses.append((int(status), dict(payload)))

            def do_POST(self) -> None:  # noqa: N802
                self.original_called = True
                if self.mutate:
                    trust_path.write_text('{"schema":1,"keys":{}}\n', encoding="utf-8")

        server_module = SimpleNamespace(
            ApiHandler=Handler,
            plugin_manager=manager,
            danmu_plugin_registry=object(),
        )
        guard.install_plugin_api_security_guard(server_module, fake_pm)

        changed = Handler(mutate=True)
        changed.do_POST()
        assert changed.original_called is True
        assert changed.server.reconciled is True
        assert calls == ["discover", "reconcile"]

        calls.clear()
        unchanged = Handler(mutate=False)
        unchanged.do_POST()
        assert unchanged.original_called is True
        assert unchanged.server.reconciled is False
        assert calls == []


def test_reconcile_removes_revoked_platform_and_restarts_relay() -> None:
    state = {
        "config": {
            "active_platforms": ["revoked_platform"],
            "platform": "revoked_platform",
        }
    }
    relay_calls: list[bool] = []

    def load_config() -> dict[str, Any]:
        return json.loads(json.dumps(state["config"]))

    def save_config(value: dict[str, Any]) -> None:
        state["config"] = json.loads(json.dumps(value))

    module = SimpleNamespace(
        load_config=load_config,
        save_config=save_config,
        _ensure_danmu_relay=lambda _server, reconnect=False: relay_calls.append(bool(reconnect)),
    )
    active = SimpleNamespace(runtime_config=load_config())
    registry = SimpleNamespace(platform_ids=lambda: ("bilibili",))

    pm._reconcile_runtime(module, active, registry)
    assert state["config"]["active_platforms"] == []
    assert active.runtime_config["active_platforms"] == []
    assert relay_calls == [True]


def test_external_origin_never_reaches_plugin_handler() -> None:
    calls: list[str] = []
    with tempfile.TemporaryDirectory() as temp:
        manager = SimpleNamespace(
            trusted_keys_path=Path(temp) / "trusted.json",
            discover=lambda: calls.append("discover"),
        )
        fake_pm = SimpleNamespace(
            _json_body=lambda _handler: {},
            PluginError=pm.PluginError,
            _reconcile_runtime=lambda *_args: calls.append("reconcile"),
        )

        class Handler:
            def __init__(self) -> None:
                self.path = "/api/plugins/install"
                self.headers = {
                    "Content-Type": "application/json",
                    "Content-Length": "2",
                    "Origin": "https://evil.example",
                }
                self.server = SimpleNamespace(server_port=9816)
                self.responses: list[tuple[int, dict[str, Any]]] = []

            def _require_loopback(self) -> bool:
                return True

            def _write_json(self, payload: dict[str, Any], status: int = 200) -> None:
                self.responses.append((int(status), dict(payload)))

            def do_POST(self) -> None:  # noqa: N802
                calls.append("original")

        server_module = SimpleNamespace(
            ApiHandler=Handler,
            plugin_manager=manager,
            danmu_plugin_registry=object(),
        )
        guard.install_plugin_api_security_guard(server_module, fake_pm)
        handler = Handler()
        handler.do_POST()
        assert calls == []
        assert handler.responses[-1][0] == 403


def main() -> None:
    tests = [
        test_cross_site_origin_is_rejected,
        test_simple_text_plain_csrf_is_rejected,
        test_dns_rebinding_style_origin_is_rejected,
        test_same_origin_loopback_and_local_script_are_allowed,
        test_wrong_port_empty_and_oversized_bodies_are_rejected,
        test_json_body_guard_caps_direct_reads,
        test_trust_mutation_reconciles_runtime_only_after_state_change,
        test_reconcile_removes_revoked_platform_and_restarts_relay,
        test_external_origin_never_reaches_plugin_handler,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print("issue #132 plugin API security guard: OK")


if __name__ == "__main__":
    main()
