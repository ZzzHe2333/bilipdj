from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one match, got {count}: {old[:120]!r}")
    write(path, text.replace(old, new, 1))


PLUGIN_DATA_QUOTA = r'''"""Shared private-data limits for external plugin host APIs."""
from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Iterator

MAX_PLUGIN_DATA_FILE_BYTES = 4 * 1024 * 1024
MAX_PLUGIN_DATA_TOTAL_BYTES = 64 * 1024 * 1024
MAX_PLUGIN_DATA_FILES = 1024
_QUOTA_LOCK = threading.RLock()


class PluginDataQuotaError(ValueError):
    """Raised when a plugin private-data operation exceeds a safety limit."""


def _is_within(root: Path, target: Path) -> bool:
    return target == root or root in target.parents


def _regular_files(root: Path) -> Iterator[Path]:
    if not root.exists():
        return
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        current = Path(dirpath)
        dirnames[:] = [name for name in dirnames if not (current / name).is_symlink()]
        for name in filenames:
            path = current / name
            try:
                if path.is_symlink() or not path.is_file():
                    continue
            except OSError:
                continue
            yield path


def plugin_data_usage(root: Path) -> tuple[int, int]:
    base = Path(root).resolve()
    total = 0
    files = 0
    for path in _regular_files(base):
        try:
            size = path.stat().st_size
        except OSError:
            continue
        total += max(0, int(size))
        files += 1
    return total, files


def validate_plugin_data_write(
    root: Path,
    target: Path,
    size: int,
    *,
    max_file_bytes: int | None = None,
    max_total_bytes: int | None = None,
    max_files: int | None = None,
) -> tuple[int, int]:
    file_limit = MAX_PLUGIN_DATA_FILE_BYTES if max_file_bytes is None else int(max_file_bytes)
    total_limit = MAX_PLUGIN_DATA_TOTAL_BYTES if max_total_bytes is None else int(max_total_bytes)
    file_count_limit = MAX_PLUGIN_DATA_FILES if max_files is None else int(max_files)
    new_size = int(size)
    if new_size < 0:
        raise PluginDataQuotaError("plugin data size cannot be negative")
    if new_size > file_limit:
        raise PluginDataQuotaError(f"plugin data file exceeds {file_limit} bytes")

    base = Path(root).resolve()
    resolved = Path(target).resolve()
    if not _is_within(base, resolved) or resolved == base:
        raise PermissionError("path escapes plugin data directory")

    total, files = plugin_data_usage(base)
    old_size = 0
    existed = False
    try:
        if resolved.exists():
            if not resolved.is_file():
                raise PluginDataQuotaError("plugin data target is not a regular file")
            old_size = max(0, int(resolved.stat().st_size))
            existed = True
    except OSError as exc:
        raise PluginDataQuotaError(f"cannot inspect plugin data target: {exc}") from exc

    projected_total = total - old_size + new_size
    projected_files = files + (0 if existed else 1)
    # If an externally modified directory is already over quota, allow an
    # existing file to shrink so the plugin can move back toward compliance.
    if projected_total > total_limit and new_size > old_size:
        raise PluginDataQuotaError(
            f"plugin private data exceeds total quota: {projected_total} > {total_limit} bytes"
        )
    if projected_files > file_count_limit and not existed:
        raise PluginDataQuotaError(
            f"plugin private data exceeds file-count quota: {projected_files} > {file_count_limit}"
        )
    return projected_total, projected_files


def write_plugin_data(
    root: Path,
    target: Path,
    data: bytes,
    *,
    max_file_bytes: int | None = None,
    max_total_bytes: int | None = None,
    max_files: int | None = None,
) -> None:
    payload = bytes(data)
    with _QUOTA_LOCK:
        validate_plugin_data_write(
            root,
            target,
            len(payload),
            max_file_bytes=max_file_bytes,
            max_total_bytes=max_total_bytes,
            max_files=max_files,
        )
        resolved = Path(target).resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)
        resolved.write_bytes(payload)


def read_plugin_data(
    root: Path,
    target: Path,
    *,
    max_file_bytes: int | None = None,
) -> bytes:
    file_limit = MAX_PLUGIN_DATA_FILE_BYTES if max_file_bytes is None else int(max_file_bytes)
    base = Path(root).resolve()
    resolved = Path(target).resolve()
    if not _is_within(base, resolved) or resolved == base:
        raise PermissionError("path escapes plugin data directory")
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    size = max(0, int(resolved.stat().st_size))
    if size > file_limit:
        raise PluginDataQuotaError(f"plugin data file exceeds {file_limit} bytes")
    data = resolved.read_bytes()
    if len(data) > file_limit:
        raise PluginDataQuotaError(f"plugin data file exceeds {file_limit} bytes")
    return data


__all__ = [
    "MAX_PLUGIN_DATA_FILE_BYTES",
    "MAX_PLUGIN_DATA_TOTAL_BYTES",
    "MAX_PLUGIN_DATA_FILES",
    "PluginDataQuotaError",
    "plugin_data_usage",
    "validate_plugin_data_write",
    "write_plugin_data",
    "read_plugin_data",
]
'''
write("apps/server/plugin_data_quota.py", PLUGIN_DATA_QUOTA)

FROZEN_PROBE = r'''from __future__ import annotations

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


def run_frozen_plugin_probe() -> None:
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
'''
write("apps/windows/frozen_plugin_probe.py", FROZEN_PROBE)

AUDIT_GUARD = r'''from __future__ import annotations

import json
import tempfile
from pathlib import Path

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

ROOT = Path(__file__).resolve().parents[1]
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
'''
write("scripts/issue144_release_audit_guard.py", AUDIT_GUARD)

# Python PluginContext: enforce the same bounded private-data API as JavaScript.
replace_once(
    "apps/server/plugin_manager.py",
    "from .danmu_plugins import PLUGIN_API_VERSION, PLUGIN_TYPE, DanmuPlugin, DanmuPluginRegistry\n",
    "from .danmu_plugins import PLUGIN_API_VERSION, PLUGIN_TYPE, DanmuPlugin, DanmuPluginRegistry\nfrom .plugin_data_quota import read_plugin_data, write_plugin_data\n",
)
replace_once(
    "apps/server/plugin_manager.py",
    '''    def read_data(self, relative: str) -> bytes:\n        self._require("filesystem_read")\n        return self._data_path(relative).read_bytes()\n\n    def write_data(self, relative: str, data: bytes) -> None:\n        self._require("filesystem_write")\n        if not isinstance(data, (bytes, bytearray)):\n            raise TypeError("data must be bytes")\n        target = self._data_path(relative)\n        target.parent.mkdir(parents=True, exist_ok=True)\n        target.write_bytes(bytes(data))\n''',
    '''    def read_data(self, relative: str) -> bytes:\n        self._require("filesystem_read")\n        base = self._manager.data_root / self.plugin_id\n        return read_plugin_data(base, self._data_path(relative))\n\n    def write_data(self, relative: str, data: bytes) -> None:\n        self._require("filesystem_write")\n        if not isinstance(data, (bytes, bytearray)):\n            raise TypeError("data must be bytes")\n        base = self._manager.data_root / self.plugin_id\n        write_plugin_data(base, self._data_path(relative), bytes(data))\n''',
)

# JavaScript host: align path bounds and share quota/read helpers.
replace_once(
    "apps/server/javascript_plugin_runtime.py",
    "from .plugin_http_request import perform_plugin_http_request, prepare_plugin_http_request\n",
    "from .plugin_data_quota import read_plugin_data, write_plugin_data\nfrom .plugin_http_request import perform_plugin_http_request, prepare_plugin_http_request\n",
)
replace_once(
    "apps/server/javascript_plugin_runtime.py",
    '''    if (\n        not raw\n        or raw.startswith("/")\n        or path.is_absolute()\n        or ":" in raw\n        or any(part in {"", ".", ".."} for part in path.parts)\n    ):\n        raise PermissionError("invalid plugin data path")\n''',
    '''    if (\n        not raw\n        or raw.startswith("/")\n        or path.is_absolute()\n        or ":" in raw\n        or any(part in {"", ".", ".."} for part in path.parts)\n        or len(path.parts) > 12\n        or len(raw) > 240\n    ):\n        raise PermissionError("invalid plugin data path")\n''',
)
replace_once(
    "apps/server/javascript_plugin_runtime.py",
    '''    def read_data(relative: str) -> str:\n        _require_permission(permissions, "filesystem_read")\n        data = _safe_data_path(data_root, str(relative)).read_bytes()\n        if len(data) > 4 * 1024 * 1024:\n            raise ValueError("plugin data file exceeds safety limit")\n        return base64.b64encode(data).decode("ascii")\n\n    def write_data(relative: str, encoded: str) -> bool:\n        _require_permission(permissions, "filesystem_write")\n        data = base64.b64decode(str(encoded), validate=True)\n        if len(data) > 4 * 1024 * 1024:\n            raise ValueError("plugin data write exceeds safety limit")\n        target = _safe_data_path(data_root, str(relative))\n        target.parent.mkdir(parents=True, exist_ok=True)\n        target.write_bytes(data)\n        return True\n''',
    '''    def read_data(relative: str) -> str:\n        _require_permission(permissions, "filesystem_read")\n        root = Path(data_root).resolve()\n        target = _safe_data_path(data_root, str(relative))\n        data = read_plugin_data(root, target)\n        return base64.b64encode(data).decode("ascii")\n\n    def write_data(relative: str, encoded: str) -> bool:\n        _require_permission(permissions, "filesystem_write")\n        data = base64.b64decode(str(encoded), validate=True)\n        root = Path(data_root).resolve()\n        target = _safe_data_path(data_root, str(relative))\n        write_plugin_data(root, target, data)\n        return True\n''',
)

# Updater/Web: attached release manifest -> latest Release API -> raw now fallback.
replace_once(
    "apps/windows/update_client.py",
    '''    Order:\n    1. update-manifest.json attached to the latest GitHub Release;\n    2. raw ``now/update-manifest.json`` compatibility copy;\n    3. GitHub Release API, using the asset's native ``digest`` when available.\n\n    The third path also keeps old releases and existing tests compatible.\n''',
    '''    Order:\n    1. update-manifest.json attached to the latest GitHub Release;\n    2. GitHub Release API, using the asset's native ``digest`` when available;\n    3. raw ``now/update-manifest.json`` compatibility copy as the last resort.\n\n    Release metadata must win over the raw branch copy because the latter can\n    temporarily be stale while a release is being prepared or mirrored.\n''',
)
replace_once(
    "apps/windows/update_client.py",
    "    for source_url in (LATEST_MANIFEST_URL, RAW_MANIFEST_URL, LATEST_RELEASE_API):\n",
    "    for source_url in (LATEST_MANIFEST_URL, LATEST_RELEASE_API, RAW_MANIFEST_URL):\n",
)
replace_once(
    "apps/server/issue79_guard.py",
    '''def _load_update_manifest() -> dict[str, Any]:\n    errors: list[str] = []\n    for url in (MANIFEST_URL, RAW_MANIFEST_URL):\n        try:\n            payload = _read_json_url(url)\n        except Exception as exc:  # noqa: BLE001\n            errors.append(f"{url}: {exc}")\n            continue\n        if isinstance(payload.get("packages"), dict) and str(payload.get("version", "") or "").strip():\n            return payload\n    try:\n        return _manifest_from_release(_read_json_url(LATEST_RELEASE_API))\n    except Exception as exc:  # noqa: BLE001\n        errors.append(f"{LATEST_RELEASE_API}: {exc}")\n    raise RuntimeError("无法获取更新清单：" + " | ".join(errors[-3:]))\n''',
    '''def _load_update_manifest() -> dict[str, Any]:\n    errors: list[str] = []\n    for url in (MANIFEST_URL, LATEST_RELEASE_API, RAW_MANIFEST_URL):\n        try:\n            payload = _read_json_url(url)\n            if url == LATEST_RELEASE_API:\n                payload = _manifest_from_release(payload)\n        except Exception as exc:  # noqa: BLE001\n            errors.append(f"{url}: {exc}")\n            continue\n        if isinstance(payload.get("packages"), dict) and str(payload.get("version", "") or "").strip():\n            return payload\n        errors.append(f"{url}: 内容不是有效更新清单")\n    raise RuntimeError("无法获取更新清单：" + " | ".join(errors[-3:]))\n''',
)

# Frozen executable self-test entry points.
replace_once(
    "apps/windows/main.py",
    '''def main() -> None:\n    control_panel.main()\n''',
    '''def main() -> None:\n    if "--plugin-runtime-self-test" in sys.argv[1:]:\n        from apps.windows.frozen_plugin_probe import run_frozen_plugin_probe\n\n        run_frozen_plugin_probe()\n        return\n    control_panel.main()\n''',
)
replace_once(
    "apps/web/portable_launcher.py",
    '''def main() -> None:\n    if "--backend" in sys.argv[1:]:\n        _run_backend_mode()\n        return\n\n    root = tk.Tk()\n''',
    '''def main() -> None:\n    if "--backend" in sys.argv[1:]:\n        _run_backend_mode()\n        return\n    if "--plugin-runtime-self-test" in sys.argv[1:]:\n        from apps.windows.frozen_plugin_probe import run_frozen_plugin_probe\n\n        run_frozen_plugin_probe()\n        return\n\n    root = tk.Tk()\n''',
)
replace_once(
    "apps/windows/bilipdj_onedir.spec",
    '''    "apps.server.danmu_plugins", "apps.server.plugin_manager", "apps.server.plugin_runtime_dual", "apps.server.javascript_plugin_runtime",\n''',
    '''    "apps.server.danmu_plugins", "apps.server.plugin_manager", "apps.server.plugin_runtime_dual", "apps.server.javascript_plugin_runtime", "apps.server.plugin_data_quota",\n''',
)
replace_once(
    "apps/windows/bilipdj_onedir.spec",
    '''    "apps.windows.control_panel", "apps.windows.control_panel_bootstrap", "apps.windows.control_panel_guard",\n''',
    '''    "apps.windows.control_panel", "apps.windows.control_panel_bootstrap", "apps.windows.control_panel_guard", "apps.windows.frozen_plugin_probe",\n''',
)
replace_once(
    "apps/web/web_portable.spec",
    '''    "apps.server.danmu_plugins", "apps.server.plugin_manager", "apps.server.plugin_runtime_dual", "apps.server.javascript_plugin_runtime",\n''',
    '''    "apps.server.danmu_plugins", "apps.server.plugin_manager", "apps.server.plugin_runtime_dual", "apps.server.javascript_plugin_runtime", "apps.server.plugin_data_quota",\n''',
)
replace_once(
    "apps/web/web_portable.spec",
    '''    "core.youtube_protocol", "core.twitch_protocol",\n''',
    '''    "apps.windows.frozen_plugin_probe",\n    "core.youtube_protocol", "core.twitch_protocol",\n''',
)

# Plugin API docs: canonical DanmuEvent first, legacy Bilibili-shaped API only for compatibility.
replace_once(
    "docs/PLUGIN_API_V1.md",
    '''## 弹幕进入 BiliPDJ\n\n为了复用现有 QueueManager，外部插件可以把解析后的弹幕转换成 Bilibili 兼容弹幕 JSON，再调用：\n\n```javascript\nhost.processDanmu(payload)\n```\n\n或 Python：\n\n```python\ncontext.process_danmu_json(payload)\n```\n\n平台自己的状态/原始事件可通过 `emit()` 广播给 Web/Windows/OBS。\n''',
    '''## 弹幕进入 BiliPDJ\n\n新插件应把平台消息规范化为平台无关 `DanmuEvent`，不要伪造 Bilibili `DANMU_MSG/info`：\n\n```javascript\nhost.processDanmuEvent({\n  user_id: "platform-native-id",\n  username: "Alice",\n  content: "排队",\n  is_room_admin: false\n})\n```\n\n或 Python：\n\n```python\ncontext.process_danmu_event({\n    "user_id": "platform-native-id",\n    "username": "Alice",\n    "content": "排队",\n    "is_room_admin": False,\n})\n```\n\n旧 `host.processDanmu(payload)` / `context.process_danmu_json(payload)` 仅作为 Bilibili `DANMU_MSG` 兼容接口保留。平台自己的状态/原始事件仍可通过 `emit()` 广播给 Web/Windows/OBS。\n\n### 插件私有数据限额\n\n`filesystem_read` / `filesystem_write` 访问的插件独立 data 目录实行统一资源限制：单文件最多 **4 MiB**、每个插件常规文件总大小最多 **64 MiB**、最多 **1024** 个常规文件；路径最多 12 层且最长 240 字符。Python `context.read_data/write_data` 与 JavaScript `host.readData/writeData` 使用相同限制。超限会拒绝操作，不会静默截断。拥有 `subprocess` 权限的插件仍属于高权限插件，该权限不是 OS 沙箱。\n''',
)

print("Issue #144 audit source patch applied")
