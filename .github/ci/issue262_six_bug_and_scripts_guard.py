from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    assert not (ROOT / "scripts").exists(), "root scripts/ must not be a runtime or CI dependency"

    from apps.versioning import same_release_version, version_key

    assert version_key("3.0.13-feiqi") < version_key("3.0.13-test")
    assert version_key("3.0.13-test") < version_key("3.0.13-cx")
    assert version_key("3.0.13-cx") < version_key("3.0.13-gc")
    assert version_key("3.0.13-gc") < version_key("3.0.13")
    assert same_release_version("v3.0.13-test", "3.0.13-test+build.1")
    assert not same_release_version("3.0.13-test", "3.0.13-gc")
    assert not same_release_version("3.0.13-gc", "3.0.13")

    pm = importlib.import_module("apps.server.plugin_manager")
    assert pm._version_key("3.0.13-test") < pm._version_key("3.0.13-gc")
    selector = importlib.import_module("apps.server.issue189_release_selector")
    assert selector._version_key("3.0.13-test") < selector._version_key("3.0.13-gc")

    updater_source = (ROOT / "apps/web/web_updater.py").read_text(encoding="utf-8")
    assert "not same_release_version(base, current)" in updater_source

    server_source = (ROOT / "apps/server/server.py").read_text(encoding="utf-8")
    assert 'load_quanxian(updated.get("quanxian", {}))' not in server_source
    assert "queue_manager.load_quanxian(load_quanxian())" in server_source

    storage_source = (ROOT / "apps/server/settings_storage_guard.py").read_text(encoding="utf-8")
    backup_pos = storage_source.index("service.backup_now()")
    done_pos = storage_source.index("backup_module._EXIT_BACKUP_DONE = True", backup_pos)
    assert done_pos > backup_pos

    from apps.server.issue79_guard import MultiPlatformRelayManager

    events: list[str] = []
    fail_second = {"value": True}

    class Relay:
        def __init__(self, platform: str) -> None:
            self.platform = platform

        def start(self) -> None:
            events.append(f"start:{self.platform}")
            if self.platform == "douyin" and fail_second["value"]:
                raise RuntimeError("synthetic relay start failure")

        def stop(self) -> None:
            events.append(f"stop:{self.platform}")

        def join(self, timeout=None) -> None:
            events.append(f"join:{self.platform}")

    fake_module = SimpleNamespace(_create_danmu_relay=lambda proxy: Relay(proxy.platform))
    fake_server = SimpleNamespace(runtime_config={}, logger=logging.getLogger("issue262"))
    manager = MultiPlatformRelayManager(fake_module, fake_server, ("bilibili", "douyin"))
    try:
        manager.start()
    except RuntimeError:
        pass
    else:
        raise AssertionError("synthetic relay failure was not propagated")
    assert manager._started is False
    assert manager._relays == {}
    assert manager._proxies == {}
    assert "stop:bilibili" in events and "join:bilibili" in events

    fail_second["value"] = False
    manager.start()
    assert manager._started is True
    assert set(manager._relays) == {"bilibili", "douyin"}
    manager.stop()

    forbidden = ("scripts/", ".github/ci/")
    for base_name in ("apps", "core"):
        for path in (ROOT / base_name).rglob("*"):
            if not path.is_file() or path.suffix.lower() not in {".py", ".spec", ".ps1", ".sh"}:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for token in forbidden:
                assert token not in text, f"production runtime references CI-only path {token}: {path}"

    for workflow in (ROOT / ".github/workflows").glob("*.yml"):
        text = workflow.read_text(encoding="utf-8", errors="ignore")
        assert "scripts/" not in text, f"workflow still depends on scripts/: {workflow.name}"

    print("issue #262 six bug fixes and scripts decoupling guard: OK")


if __name__ == "__main__":
    main()
