from __future__ import annotations

import base64
import json
import sys
import tempfile
import threading
import time
from pathlib import Path
from types import MethodType, SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.server import plugin_manager as pm  # noqa: E402
from apps.server import plugin_mutation_guard  # noqa: E402
from apps.server.danmu_plugins import DanmuPluginRegistry  # noqa: E402

plugin_mutation_guard.install_plugin_mutation_guard(pm)


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


class FakeRecord:
    def __init__(self, plugin_id: str) -> None:
        self.plugin_id = plugin_id
        self.verified = True
        self.error = ""
        self.enabled = False

    def public_info(self) -> dict[str, Any]:
        return {
            "id": self.plugin_id,
            "enabled": self.enabled,
            "verified": self.verified,
            "error": self.error,
        }


def test_all_mutating_methods_are_wrapped() -> None:
    for name in ("set_enabled", "uninstall", "add_trusted_key", "remove_trusted_key"):
        method = getattr(pm.PluginManager, name)
        assert getattr(method, "_issue134_plugin_mutation_lock", False), name


def test_concurrent_enabled_state_updates_do_not_lose_changes() -> None:
    with tempfile.TemporaryDirectory() as temp:
        manager = pm.PluginManager(fake_module(Path(temp)), DanmuPluginRegistry())
        records = {
            "example.concurrent.one": FakeRecord("example.concurrent.one"),
            "example.concurrent.two": FakeRecord("example.concurrent.two"),
        }
        manager._records = records  # type: ignore[assignment]
        state_store: dict[str, bool] = {}
        start = threading.Barrier(3)
        errors: list[BaseException] = []

        def slow_state(self: Any) -> dict[str, bool]:
            time.sleep(0.03)
            return dict(state_store)

        def slow_save(self: Any, value: dict[str, bool]) -> None:
            time.sleep(0.03)
            state_store.clear()
            state_store.update(value)

        def fake_discover(self: Any) -> list[dict[str, Any]]:
            for plugin_id, record in records.items():
                record.enabled = bool(state_store.get(plugin_id, False))
            self._records = records
            return []

        manager._state = MethodType(slow_state, manager)  # type: ignore[method-assign]
        manager._save_state = MethodType(slow_save, manager)  # type: ignore[method-assign]
        manager.discover = MethodType(fake_discover, manager)  # type: ignore[method-assign]

        def worker(plugin_id: str) -> None:
            try:
                start.wait()
                manager.set_enabled(plugin_id, True)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=("example.concurrent.one",)),
            threading.Thread(target=worker, args=("example.concurrent.two",)),
        ]
        for thread in threads:
            thread.start()
        start.wait()
        for thread in threads:
            thread.join(timeout=3)
        assert not any(thread.is_alive() for thread in threads)
        assert not errors, errors
        assert state_store == {
            "example.concurrent.one": True,
            "example.concurrent.two": True,
        }


def test_concurrent_trusted_key_updates_do_not_lose_changes() -> None:
    with tempfile.TemporaryDirectory() as temp:
        manager = pm.PluginManager(fake_module(Path(temp)), DanmuPluginRegistry())
        original_load = manager._load_json_file
        original_write = manager._atomic_write_json
        start = threading.Barrier(3)
        errors: list[BaseException] = []

        def slow_load(self: Any, path: Path, default: Any) -> Any:
            time.sleep(0.03)
            return original_load(path, default)

        def slow_write(self: Any, path: Path, value: Any) -> None:
            time.sleep(0.03)
            original_write(path, value)

        manager._load_json_file = MethodType(slow_load, manager)  # type: ignore[method-assign]
        manager._atomic_write_json = MethodType(slow_write, manager)  # type: ignore[method-assign]

        def worker(key_id: str, fill: int) -> None:
            try:
                start.wait()
                public_key = base64.b64encode(bytes([fill]) * 32).decode("ascii")
                manager.add_trusted_key(key_id, public_key)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=("publisher.concurrent.one", 1)),
            threading.Thread(target=worker, args=("publisher.concurrent.two", 2)),
        ]
        for thread in threads:
            thread.start()
        start.wait()
        for thread in threads:
            thread.join(timeout=3)
        assert not any(thread.is_alive() for thread in threads)
        assert not errors, errors

        payload = json.loads(manager.trusted_keys_path.read_text(encoding="utf-8"))
        assert set(payload.get("keys", {})) == {
            "publisher.concurrent.one",
            "publisher.concurrent.two",
        }


def test_lock_is_reentrant_for_existing_discover_paths() -> None:
    with tempfile.TemporaryDirectory() as temp:
        manager = pm.PluginManager(fake_module(Path(temp)), DanmuPluginRegistry())
        # discover() already acquires manager._lock internally. Calling it while the
        # Issue #134 wrapper owns the same lock must remain safe, hence RLock.
        acquired: list[bool] = []

        class Record(FakeRecord):
            pass

        record = Record("example.reentrant.test")
        manager._records = {record.plugin_id: record}  # type: ignore[assignment]
        manager._state = MethodType(lambda self: {record.plugin_id: False}, manager)  # type: ignore[method-assign]
        manager._save_state = MethodType(lambda self, value: None, manager)  # type: ignore[method-assign]

        def discover(self: Any) -> list[dict[str, Any]]:
            with self._lock:
                acquired.append(True)
                record.enabled = False
                self._records = {record.plugin_id: record}
                return []

        manager.discover = MethodType(discover, manager)  # type: ignore[method-assign]
        manager.set_enabled(record.plugin_id, False)
        assert acquired == [True]


def test_web_unsigned_approval_is_one_shot() -> None:
    web = (ROOT / "apps/web/static/control_plugins.js").read_text(encoding="utf-8")
    assert "每次安装都需要重新确认来源可信" in web
    assert "本次授权只用于这一次安装" in web
    assert "const allowUnsigned = Boolean(unsignedCheckbox?.checked);" in web
    assert "window.confirm(" in web
    assert "allow_unsigned: allowUnsigned" in web
    assert "finally {" in web
    assert "clearUnsignedApproval();" in web
    assert "unsignedCheckbox.checked = false" in web


def main() -> None:
    tests = [
        test_all_mutating_methods_are_wrapped,
        test_concurrent_enabled_state_updates_do_not_lose_changes,
        test_concurrent_trusted_key_updates_do_not_lose_changes,
        test_lock_is_reentrant_for_existing_discover_paths,
        test_web_unsigned_approval_is_one_shot,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print("issue #134 plugin mutation guard: OK")


if __name__ == "__main__":
    main()
