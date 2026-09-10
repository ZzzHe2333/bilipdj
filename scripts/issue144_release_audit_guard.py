from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.server import issue79_guard
from apps.server import javascript_plugin_runtime as js_runtime
from apps.server.plugin_data_quota import (
    PluginDataQuotaError,
    read_plugin_data,
    validate_plugin_data_write,
    write_plugin_data,
)
from apps.windows import update_client
from apps.windows.frozen_plugin_probe import run_frozen_plugin_probe

HEX = "a" * 64


class _Response:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


def check_quota_helper() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        first = root / "a.bin"
        second = root / "b.bin"
        write_plugin_data(root, first, b"1234", max_file_bytes=8, max_total_bytes=6, max_files=2)
        write_plugin_data(root, first, b"12", max_file_bytes=8, max_total_bytes=6, max_files=2)
        write_plugin_data(root, second, b"3456", max_file_bytes=8, max_total_bytes=6, max_files=2)
        assert read_plugin_data(root, second, max_file_bytes=8) == b"3456"
        try:
            validate_plugin_data_write(root, root / "c.bin", 1, max_file_bytes=8, max_total_bytes=6, max_files=2)
        except PluginDataQuotaError:
            pass
        else:
            raise AssertionError("file-count quota was not enforced")
        try:
            validate_plugin_data_write(root, second, 5, max_file_bytes=8, max_total_bytes=6, max_files=2)
        except PluginDataQuotaError:
            pass
        else:
            raise AssertionError("total quota was not enforced")
        try:
            validate_plugin_data_write(root, second, 9, max_file_bytes=8, max_total_bytes=99, max_files=2)
        except PluginDataQuotaError:
            pass
        else:
            raise AssertionError("per-file quota was not enforced")


def check_js_path_limits() -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        js_runtime._safe_data_path(str(root), "a/b/c.bin")
        deep = "/".join(["x"] * 13) + "/file.bin"
        try:
            js_runtime._safe_data_path(str(root), deep)
        except PermissionError:
            pass
        else:
            raise AssertionError("JavaScript plugin data path depth limit missing")
        try:
            js_runtime._safe_data_path(str(root), "x" * 241)
        except PermissionError:
            pass
        else:
            raise AssertionError("JavaScript plugin data path length limit missing")


def _release_payload() -> dict:
    return {
        "tag_name": "v3.0.0",
        "name": "BiliPDJ v3.0.0",
        "html_url": "https://example.invalid/release",
        "published_at": "2026-09-10T00:00:00Z",
        "assets": [
            {
                "name": "BiliPDJ-v3.0.0-Windows-Tk-Portable-x64.zip",
                "browser_download_url": "https://example.invalid/windows.zip",
                "size": 123,
                "digest": f"sha256:{HEX}",
            },
            {
                "name": "BiliPDJ-v3.0.0-Web-Portable-x64.zip",
                "browser_download_url": "https://example.invalid/web.zip",
                "size": 456,
                "digest": f"sha256:{HEX}",
            },
        ],
    }


def check_update_fallback_order() -> None:
    calls: list[str] = []
    original = update_client._request

    def fake_request(url: str, *, timeout: float = 15.0):
        calls.append(url)
        if url == update_client.LATEST_MANIFEST_URL:
            raise update_client.UpdateError("manifest unavailable")
        if url == update_client.LATEST_RELEASE_API:
            return _Response(_release_payload())
        return _Response({"version": "2.0.3", "packages": {}})

    update_client._request = fake_request
    try:
        release = update_client.fetch_latest_release()
    finally:
        update_client._request = original
    assert release.version == "3.0.0"
    assert calls == [update_client.LATEST_MANIFEST_URL, update_client.LATEST_RELEASE_API], calls

    web_calls: list[str] = []
    original_reader = issue79_guard._read_json_url

    def fake_reader(url: str, *, timeout: float = 12.0):
        web_calls.append(url)
        if url == issue79_guard.MANIFEST_URL:
            raise RuntimeError("manifest unavailable")
        if url == issue79_guard.LATEST_RELEASE_API:
            return _release_payload()
        return {"version": "2.0.3", "packages": {}}

    issue79_guard._read_json_url = fake_reader
    try:
        manifest = issue79_guard._load_update_manifest()
    finally:
        issue79_guard._read_json_url = original_reader
    assert manifest["version"] == "3.0.0"
    assert web_calls == [issue79_guard.MANIFEST_URL, issue79_guard.LATEST_RELEASE_API], web_calls


def check_static_integration() -> None:
    plugin_manager = (ROOT / "apps/server/plugin_manager.py").read_text(encoding="utf-8")
    javascript = (ROOT / "apps/server/javascript_plugin_runtime.py").read_text(encoding="utf-8")
    docs = (ROOT / "docs/PLUGIN_API_V1.md").read_text(encoding="utf-8")
    main = (ROOT / "apps/windows/main.py").read_text(encoding="utf-8")
    web = (ROOT / "apps/web/portable_launcher.py").read_text(encoding="utf-8")
    win_spec = (ROOT / "apps/windows/bilipdj_onedir.spec").read_text(encoding="utf-8")
    web_spec = (ROOT / "apps/web/web_portable.spec").read_text(encoding="utf-8")
    assert "write_plugin_data" in plugin_manager and "read_plugin_data" in plugin_manager
    assert "write_plugin_data" in javascript and "read_plugin_data" in javascript
    assert "host.processDanmuEvent" in docs
    assert "外部插件可以把解析后的弹幕转换成 Bilibili 兼容弹幕 JSON" not in docs
    assert "64 MiB" in docs and "1024" in docs
    assert "--plugin-runtime-self-test" in main and "--plugin-runtime-self-test" in web
    assert "apps.windows.frozen_plugin_probe" in win_spec and "apps.windows.frozen_plugin_probe" in web_spec


def main() -> None:
    check_quota_helper()
    check_js_path_limits()
    check_update_fallback_order()
    check_static_integration()
    run_frozen_plugin_probe()
    print("issue #144 release audit regression guard: OK")


if __name__ == "__main__":
    main()
