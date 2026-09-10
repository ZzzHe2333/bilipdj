from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import os
import sys
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey  # noqa: E402

from apps.server import plugin_manager as pm  # noqa: E402
from apps.server import plugin_runtime_dual  # noqa: E402
from apps.server.danmu_plugins import DanmuPluginRegistry  # noqa: E402

plugin_runtime_dual.install_dual_runtime_support()

PY_SOURCE = b'''\
class Relay:
    def start(self):
        pass
class Plugin:
    def create_relay(self, context, config):
        return Relay()
'''

JS_SOURCE = b'''\
function createRelay(config, host) {
  return {start() {}, stop() {}, getRuntimeStatus() { return {connected:false}; }};
}
'''


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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


def manifest(*, runtime: str, plugin_id: str, platform: str, permissions: list[str]) -> dict[str, Any]:
    source = PY_SOURCE if runtime == "python" else JS_SOURCE
    entry = "plugin.py:Plugin" if runtime == "python" else "plugin.js"
    path = entry.split(":", 1)[0]
    return {
        "schema": 1,
        "id": plugin_id,
        "name": f"{platform} 获取弹幕插件",
        "version": "1.0.0",
        "plugin_api": 1,
        "type": "danmu_source",
        "platform": platform,
        "runtime": runtime,
        "entry": entry,
        "min_bilipdj_version": "2.0.0",
        "permissions": permissions,
        "capabilities": ["danmu"],
        "files": {path: sha256(source)},
    }


def package_bytes(payload: dict[str, Any], *, private_key: Ed25519PrivateKey | None = None) -> bytes:
    data = copy.deepcopy(payload)
    if private_key is not None:
        signature = private_key.sign(pm._canonical_manifest(data))
        data["signature"] = {
            "algorithm": "ed25519",
            "key_id": "publisher.hardening",
            "value": base64.b64encode(signature).decode("ascii"),
        }
    source = PY_SOURCE if data["runtime"] == "python" else JS_SOURCE
    entry = str(data["entry"]).split(":", 1)[0]
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"))
        archive.writestr(entry, source)
    return out.getvalue()


def expect_error(callback: Any, text: str) -> None:
    try:
        callback()
    except pm.PluginError as exc:
        assert text.lower() in str(exc).lower(), (text, str(exc))
        return
    raise AssertionError("PluginError expected")


def test_signature_preserves_permission_array_order() -> None:
    with tempfile.TemporaryDirectory() as temp:
        manager = pm.PluginManager(fake_module(Path(temp)), DanmuPluginRegistry())
        private = Ed25519PrivateKey.generate()
        public = private.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        manager.add_trusted_key("publisher.hardening", base64.b64encode(public).decode("ascii"))
        payload = manifest(
            runtime="python",
            plugin_id="example.order.danmu",
            platform="order_test",
            permissions=["secrets", "network"],
        )
        info = manager.install_bytes(package_bytes(payload, private_key=private), filename="order.bilipdj-plugin")
        assert info["signature_status"] == "verified:publisher.hardening"
        assert info["permissions"] == ["secrets", "network"]
        assert info["permission_enforcement"] == "python-full-trust"


def test_noncanonical_signature_fields_are_rejected() -> None:
    payload = manifest(
        runtime="javascript",
        plugin_id="example.canonical.danmu",
        platform="canonical_test",
        permissions=["network"],
    )
    bad_permission = copy.deepcopy(payload)
    bad_permission["permissions"] = [" network "]
    expect_error(lambda: pm.validate_manifest(bad_permission, "2.0.4"), "canonical")

    bad_id = copy.deepcopy(payload)
    bad_id["id"] = "Example.Canonical.Danmu"
    expect_error(lambda: pm.validate_manifest(bad_id, "2.0.4"), "lowercase canonical")

    bad_path = copy.deepcopy(payload)
    bad_path["entry"] = "dir\\plugin.js"
    bad_path["files"] = {"dir\\plugin.js": sha256(JS_SOURCE)}
    expect_error(lambda: pm.validate_manifest(bad_path, "2.0.4"), "canonical forward-slash")


def test_installed_tree_rejects_undeclared_files_and_symlinks() -> None:
    with tempfile.TemporaryDirectory() as temp:
        manager = pm.PluginManager(fake_module(Path(temp)), DanmuPluginRegistry())
        payload = manifest(
            runtime="javascript",
            plugin_id="example.tree.danmu",
            platform="tree_test",
            permissions=[],
        )
        manager.install_bytes(package_bytes(payload), filename="tree.bilipdj-plugin", allow_unsigned=True)
        root = manager.plugins_root / "example.tree.danmu"

        extra = root / "injected.js"
        extra.write_text("// injected", encoding="utf-8")
        expect_error(lambda: manager.verify("example.tree.danmu"), "undeclared file")
        extra.unlink()
        assert manager.verify("example.tree.danmu")["verified"] is True

        link = root / "link.js"
        try:
            os.symlink(root / "plugin.js", link)
        except (OSError, NotImplementedError):
            return
        expect_error(lambda: manager.verify("example.tree.danmu"), "symbolic link")


def test_javascript_reports_host_enforced_permissions() -> None:
    with tempfile.TemporaryDirectory() as temp:
        manager = pm.PluginManager(fake_module(Path(temp)), DanmuPluginRegistry())
        payload = manifest(
            runtime="javascript",
            plugin_id="example.permission.danmu",
            platform="permission_test",
            permissions=["network"],
        )
        info = manager.install_bytes(package_bytes(payload), filename="permission.bilipdj-plugin", allow_unsigned=True)
        assert info["runtime"] == "javascript"
        assert info["permission_enforcement"] == "host-enforced"


def main() -> None:
    tests = [
        test_signature_preserves_permission_array_order,
        test_noncanonical_signature_fields_are_rejected,
        test_installed_tree_rejects_undeclared_files_and_symlinks,
        test_javascript_reports_host_enforced_permissions,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print("issue #130 plugin hardening guard: OK")


if __name__ == "__main__":
    main()
