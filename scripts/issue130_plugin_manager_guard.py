from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import logging
import stat
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey  # noqa: E402

from apps.server import plugin_manager as pm  # noqa: E402
from apps.server import plugin_runtime_dual  # noqa: E402
from apps.server import server  # noqa: E402
from apps.server.danmu_plugins import DanmuPluginRegistry  # noqa: E402

plugin_runtime_dual.install_dual_runtime_support()

PY_SOURCE = b'''\
class Relay:
    def __init__(self, context, config):
        self.context = context
        self.config = config
        self.connected = False
        self.reconnects = 0

    def start(self):
        self.connected = True

    def stop(self):
        self.connected = False

    def join(self, timeout=None):
        return None

    def request_reconnect(self):
        self.reconnects += 1

    def get_runtime_status(self):
        return {"connected": self.connected, "runtime": "python", "roomid": self.config.get("roomid", "")}

class Plugin:
    def create_relay(self, context, config):
        return Relay(context, config)
'''

JS_SOURCE = b'''\
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
    stop() {
      state.connected = false;
      host.setStatus({connected: false});
    },
    requestReconnect() { state.reconnects += 1; },
    getRuntimeStatus() { return state; }
  };
}
'''

JS_SPIN_SOURCE = b'''\
function createRelay(config, host) {
  return {
    start() { while (true) {} },
    stop() {},
    getRuntimeStatus() { return {connected: false}; }
  };
}
'''


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def base_manifest(
    *,
    plugin_id: str,
    platform: str,
    runtime: str,
    entry: str,
    source: bytes,
    permissions: list[str] | None = None,
    min_version: str = "2.0.0",
    version: str = "1.0.0",
) -> dict[str, Any]:
    entry_path = entry.split(":", 1)[0]
    return {
        "schema": 1,
        "id": plugin_id,
        "name": f"{platform} 获取弹幕插件",
        "version": version,
        "plugin_api": 1,
        "type": "danmu_source",
        "platform": platform,
        "runtime": runtime,
        "entry": entry,
        "min_bilipdj_version": min_version,
        "permissions": list(permissions or []),
        "capabilities": ["danmu"],
        "files": {entry_path: sha256(source)},
    }


def package_bytes(
    manifest: dict[str, Any],
    source: bytes,
    *,
    private_key: Ed25519PrivateKey | None = None,
    key_id: str = "publisher.example",
    extra_members: list[tuple[str | zipfile.ZipInfo, bytes]] | None = None,
) -> bytes:
    payload = copy.deepcopy(manifest)
    if private_key is not None:
        signature = private_key.sign(pm._canonical_manifest(payload))
        payload["signature"] = {
            "algorithm": "ed25519",
            "key_id": key_id,
            "value": base64.b64encode(signature).decode("ascii"),
        }
    entry_path = str(payload["entry"]).split(":", 1)[0]
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))
        archive.writestr(entry_path, source)
        for name, content in extra_members or []:
            archive.writestr(name, content)
    return out.getvalue()


def expect_plugin_error(callback: Callable[[], Any], contains: str = "") -> None:
    try:
        callback()
    except pm.PluginError as exc:
        if contains:
            assert contains.lower() in str(exc).lower(), (contains, str(exc))
        return
    raise AssertionError("PluginError was expected")


def fake_module(tmp: Path) -> Any:
    return SimpleNamespace(
        APP_DIR=tmp,
        BUNDLE_DIR=ROOT,
        REPO_DIR=ROOT,
        DEFAULT_PLATFORM="bilibili",
        RESERVED_RUNTIME_PLATFORMS=(),
        ALL_RUNTIME_PLATFORMS=(),
        PLATFORM_DISPLAY_NAMES={},
        _get_runtime_platform=lambda cfg: str((cfg or {}).get("platform", "bilibili")),
    )


def active_server(platform: str) -> tuple[Any, list[dict[str, Any]], list[dict[str, Any]]]:
    events: list[dict[str, Any]] = []
    danmu: list[dict[str, Any]] = []
    active = SimpleNamespace(
        runtime_config={platform: {"roomid": "room-test", "cookie": "secret-cookie"}, "platform": platform},
        logger=logging.getLogger("issue130-test"),
        ws_hub=SimpleNamespace(broadcast_json=lambda _sender, payload: events.append(dict(payload))),
        queue_manager=SimpleNamespace(process_danmu_json=lambda payload: danmu.append(dict(payload))),
    )
    return active, events, danmu


def test_global_manager_and_builtin_plugins_preserved() -> None:
    assert getattr(server, "_plugin_manager_installed", False)
    assert getattr(pm, "_issue130_dual_runtime_installed", False)
    builtins = {item.platform for item in server.danmu_plugin_registry.list_plugins() if item.source == "builtin"}
    assert {"bilibili", "douyin", "huya", "youtube", "twitch"}.issubset(builtins)


def test_manifest_runtime_and_compatibility_validation() -> None:
    py = base_manifest(
        plugin_id="example.py.danmu", platform="kuaishou_py", runtime="python",
        entry="plugin.py:Plugin", source=PY_SOURCE,
    )
    js = base_manifest(
        plugin_id="example.js.danmu", platform="kuaishou_js", runtime="javascript",
        entry="plugin.js", source=JS_SOURCE,
    )
    assert pm.validate_manifest(py, "2.0.4")["runtime"] == "python"
    assert pm.validate_manifest(js, "2.0.4")["runtime"] == "javascript"

    bad_runtime = dict(js, runtime="lua")
    expect_plugin_error(lambda: pm.validate_manifest(bad_runtime, "2.0.4"), "runtime")
    bad_version = dict(js, min_bilipdj_version="99.0.0")
    expect_plugin_error(lambda: pm.validate_manifest(bad_version, "2.0.4"), "requires")
    bad_permission = dict(js, permissions=["root_everything"])
    expect_plugin_error(lambda: pm.validate_manifest(bad_permission, "2.0.4"), "unknown permissions")
    bad_entry = dict(js, entry="plugin.py:Plugin")
    expect_plugin_error(lambda: pm.validate_manifest(bad_entry, "2.0.4"), "javascript entry")
    reserved = dict(js, id="data")
    expect_plugin_error(lambda: pm.validate_manifest(reserved, "2.0.4"), "reserved")


def test_unsigned_install_requires_explicit_approval_and_integrity() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        registry = DanmuPluginRegistry()
        manager = pm.PluginManager(fake_module(root), registry)
        manifest = base_manifest(
            plugin_id="example.unsigned.danmu", platform="unsigned_test", runtime="python",
            entry="plugin.py:Plugin", source=PY_SOURCE,
        )
        package = package_bytes(manifest, PY_SOURCE)
        expect_plugin_error(
            lambda: manager.install_bytes(package, filename="test.bilipdj-plugin"),
            "unsigned",
        )
        info = manager.install_bytes(
            package, filename="test.bilipdj-plugin", allow_unsigned=True,
        )
        assert info["enabled"] is False
        assert info["signature_status"] == "unsigned-approved"
        assert info["runtime"] == "python"
        assert manager.verify("example.unsigned.danmu")["verified"] is True

        installed = manager.plugins_root / "example.unsigned.danmu" / "plugin.py"
        installed.write_text("# tampered\n", encoding="utf-8")
        expect_plugin_error(lambda: manager.verify("example.unsigned.danmu"), "integrity")


def test_manifest_tamper_is_detected_even_for_unsigned_plugin() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manager = pm.PluginManager(fake_module(root), DanmuPluginRegistry())
        manifest = base_manifest(
            plugin_id="example.manifest.danmu", platform="manifest_test", runtime="python",
            entry="plugin.py:Plugin", source=PY_SOURCE,
        )
        package = package_bytes(manifest, PY_SOURCE)
        manager.install_bytes(package, filename="manifest.bilipdj-plugin", allow_unsigned=True)
        manifest_path = manager.plugins_root / "example.manifest.danmu" / "manifest.json"
        edited = json.loads(manifest_path.read_text(encoding="utf-8"))
        edited["permissions"] = ["subprocess"]
        manifest_path.write_text(json.dumps(edited), encoding="utf-8")
        expect_plugin_error(lambda: manager.verify("example.manifest.danmu"), "manifest integrity")


def test_archive_path_symlink_and_bomb_guards() -> None:
    with tempfile.TemporaryDirectory() as temp:
        manager = pm.PluginManager(fake_module(Path(temp)), DanmuPluginRegistry())
        manifest = base_manifest(
            plugin_id="example.archive.danmu", platform="archive_test", runtime="python",
            entry="plugin.py:Plugin", source=PY_SOURCE,
        )
        unsafe = package_bytes(manifest, PY_SOURCE, extra_members=[("../escape.py", b"x")])
        expect_plugin_error(
            lambda: manager.install_bytes(unsafe, filename="unsafe.bilipdj-plugin", allow_unsigned=True),
            "unsafe path",
        )

        drive = package_bytes(manifest, PY_SOURCE, extra_members=[("C:/escape.py", b"x")])
        expect_plugin_error(
            lambda: manager.install_bytes(drive, filename="drive.bilipdj-plugin", allow_unsigned=True),
            "windows-unsafe",
        )

        link = zipfile.ZipInfo("link.py")
        link.create_system = 3
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        symlink_pkg = package_bytes(manifest, PY_SOURCE, extra_members=[(link, b"plugin.py")])
        expect_plugin_error(
            lambda: manager.install_bytes(symlink_pkg, filename="link.bilipdj-plugin", allow_unsigned=True),
            "symbolic links",
        )

        bomb = b"A" * 300_000
        bomb_manifest = copy.deepcopy(manifest)
        bomb_manifest["files"]["bomb.txt"] = sha256(bomb)
        bomb_pkg = package_bytes(bomb_manifest, PY_SOURCE, extra_members=[("bomb.txt", bomb)])
        expect_plugin_error(
            lambda: manager.install_bytes(bomb_pkg, filename="bomb.bilipdj-plugin", allow_unsigned=True),
            "compression ratio",
        )


def test_ed25519_signature_and_invalid_signature_rejection() -> None:
    with tempfile.TemporaryDirectory() as temp:
        manager = pm.PluginManager(fake_module(Path(temp)), DanmuPluginRegistry())
        private = Ed25519PrivateKey.generate()
        public = private.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        manager.add_trusted_key("publisher.example", base64.b64encode(public).decode("ascii"))
        assert manager.list_trusted_keys() == [{"key_id": "publisher.example", "algorithm": "ed25519"}]

        manifest = base_manifest(
            plugin_id="example.signed.danmu", platform="signed_test", runtime="python",
            entry="plugin.py:Plugin", source=PY_SOURCE,
        )
        signed = package_bytes(manifest, PY_SOURCE, private_key=private)
        info = manager.install_bytes(signed, filename="signed.bilipdj-plugin")
        assert info["signature_status"] == "verified:publisher.example"

        bad_manifest = copy.deepcopy(manifest)
        bad_manifest["id"] = "example.badsig.danmu"
        bad_manifest["platform"] = "badsig_test"
        bad = package_bytes(bad_manifest, PY_SOURCE, private_key=private)
        with zipfile.ZipFile(io.BytesIO(bad), "r") as archive:
            payload = json.loads(archive.read("manifest.json"))
        payload["signature"]["value"] = base64.b64encode(b"\x00" * 64).decode("ascii")
        out = io.BytesIO()
        with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("manifest.json", json.dumps(payload).encode())
            archive.writestr("plugin.py", PY_SOURCE)
        expect_plugin_error(
            lambda: manager.install_bytes(out.getvalue(), filename="bad.bilipdj-plugin"),
            "signature verification failed",
        )


def test_python_plugin_enable_disable_and_persistence() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        module = fake_module(root)
        registry = DanmuPluginRegistry()
        manager = pm.PluginManager(module, registry)
        manifest = base_manifest(
            plugin_id="example.python.danmu", platform="kuaishou_py", runtime="python",
            entry="plugin.py:Plugin", source=PY_SOURCE,
        )
        manager.install_bytes(
            package_bytes(manifest, PY_SOURCE), filename="python.bilipdj-plugin", allow_unsigned=True,
        )
        enabled = manager.set_enabled("example.python.danmu", True)
        assert enabled["enabled"] is True
        plugin = registry.get("kuaishou_py")
        assert plugin is not None and plugin.source == "external"
        active, _, _ = active_server("kuaishou_py")
        relay = registry.create_relay("kuaishou_py", active)
        relay.start()
        assert relay.get_runtime_status()["connected"] is True
        relay.request_reconnect()
        relay.stop()
        assert relay.get_runtime_status()["connected"] is False

        registry2 = DanmuPluginRegistry()
        manager2 = pm.PluginManager(module, registry2)
        manager2.discover()
        assert registry2.get("kuaishou_py") is not None
        manager2.set_enabled("example.python.danmu", False)
        assert registry2.get("kuaishou_py") is None
        manager2.uninstall("example.python.danmu")
        assert not (manager2.plugins_root / "example.python.danmu").exists()


def wait_for(predicate: Callable[[], bool], timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def test_javascript_plugin_runs_without_node_and_uses_host_bridge() -> None:
    with tempfile.TemporaryDirectory() as temp:
        registry = DanmuPluginRegistry()
        manager = pm.PluginManager(fake_module(Path(temp)), registry)
        manifest = base_manifest(
            plugin_id="example.javascript.danmu", platform="kuaishou_js", runtime="javascript",
            entry="plugin.js", source=JS_SOURCE,
        )
        info = manager.install_bytes(
            package_bytes(manifest, JS_SOURCE), filename="javascript.bilipdj-plugin", allow_unsigned=True,
        )
        assert info["runtime"] == "javascript"
        manager.set_enabled("example.javascript.danmu", True)
        plugin = registry.get("kuaishou_js")
        assert plugin is not None and plugin.source == "external"
        active, events, danmu = active_server("kuaishou_js")
        relay = registry.create_relay("kuaishou_js", active)
        relay.start()
        assert wait_for(lambda: relay.get_runtime_status().get("connected") is True)
        status = relay.get_runtime_status()
        assert status["runtime"] == "javascript"
        assert status["roomid"] == "room-test"
        assert any(item.get("type") == "PLUGIN_TEST" for item in events)
        assert danmu and danmu[0].get("cmd") == "DANMU_MSG"
        relay.request_reconnect()
        assert wait_for(lambda: int(relay.get_runtime_status().get("reconnects", 0)) >= 1)
        relay.stop()
        relay.join(timeout=2)
        assert relay.get_runtime_status().get("state") == "stopped"


def test_javascript_cpu_limit_stops_infinite_loop() -> None:
    with tempfile.TemporaryDirectory() as temp:
        registry = DanmuPluginRegistry()
        manager = pm.PluginManager(fake_module(Path(temp)), registry)
        manifest = base_manifest(
            plugin_id="example.javascript.spin", platform="spin_js", runtime="javascript",
            entry="plugin.js", source=JS_SPIN_SOURCE,
        )
        manager.install_bytes(
            package_bytes(manifest, JS_SPIN_SOURCE), filename="spin.bilipdj-plugin", allow_unsigned=True,
        )
        manager.set_enabled("example.javascript.spin", True)
        active, _, _ = active_server("spin_js")
        relay = registry.create_relay("spin_js", active)
        relay.start()
        assert wait_for(lambda: relay.get_runtime_status().get("state") == "error", timeout=2.5)
        relay.join(timeout=1)
        assert relay.get_runtime_status().get("connected") is False


def test_web_manager_and_local_only_api_present() -> None:
    web = (ROOT / "apps/web/static/control_plugins.js").read_text(encoding="utf-8")
    assert ".bilipdj-plugin" in web
    assert "/api/plugins/install" in web
    assert "/api/plugins/trusted-keys" in web
    assert "允许安装未签名" in web
    source = (ROOT / "apps/server/plugin_manager.py").read_text(encoding="utf-8")
    assert 'path == "/api/plugins/manage"' in source
    assert 'if not self._require_loopback()' in source


def main() -> None:
    tests = [
        test_global_manager_and_builtin_plugins_preserved,
        test_manifest_runtime_and_compatibility_validation,
        test_unsigned_install_requires_explicit_approval_and_integrity,
        test_manifest_tamper_is_detected_even_for_unsigned_plugin,
        test_archive_path_symlink_and_bomb_guards,
        test_ed25519_signature_and_invalid_signature_rejection,
        test_python_plugin_enable_disable_and_persistence,
        test_javascript_plugin_runs_without_node_and_uses_host_bridge,
        test_javascript_cpu_limit_stops_infinite_loop,
        test_web_manager_and_local_only_api_present,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print("issue #130 plugin manager guard: OK")


if __name__ == "__main__":
    main()
