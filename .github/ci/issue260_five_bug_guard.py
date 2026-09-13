from __future__ import annotations

import sys
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def check_post_origin_boundary() -> None:
    from apps.server import security_hardening_guard as guard

    class FakeHandler:
        def do_POST(self) -> None:  # noqa: N802
            self.called = True

    fake_module = SimpleNamespace(ApiHandler=FakeHandler)
    guard._patch_post_request_boundary(fake_module)

    blocked = FakeHandler()
    blocked.called = False
    blocked.headers = {
        "Host": "127.0.0.1:9816",
        "Origin": "https://evil.example",
        "Content-Length": "0",
    }
    blocked.responses = []
    blocked._write_json = lambda payload, status=200: blocked.responses.append((int(status), dict(payload)))
    blocked.do_POST()
    assert blocked.called is False
    assert blocked.responses and blocked.responses[-1][0] == 403

    allowed = FakeHandler()
    allowed.called = False
    allowed.headers = {
        "Host": "127.0.0.1:9816",
        "Origin": "http://127.0.0.1:9816",
        "Content-Length": "0",
    }
    allowed.responses = []
    allowed._write_json = lambda payload, status=200: allowed.responses.append((int(status), dict(payload)))
    allowed.do_POST()
    assert allowed.called is True
    assert allowed.responses == []


def check_gift_uniqueness() -> None:
    from apps.server import gift_compatibility as gifts
    from apps.server import security_hardening_guard as guard

    fake_module = SimpleNamespace(GiftCompatibilityService=gifts.GiftCompatibilityService)
    guard._patch_gift_rule_uniqueness(fake_module)

    with tempfile.TemporaryDirectory() as tmp:
        service = gifts.GiftCompatibilityService(SimpleNamespace(_YAML_DIR=Path(tmp)))

        try:
            service.save_rules([
                {"platform": "douyin", "gift_id": "7", "gift_name": "玫瑰", "value": 1, "enabled": True},
                {"platform": "douyin", "gift_id": "7", "gift_name": "大玫瑰", "value": 2, "enabled": True},
            ])
        except gifts.GiftCompatibilityError as exc:
            assert "礼物 ID" in str(exc)
        else:
            raise AssertionError("duplicate platform + gift_id must be rejected")

        try:
            service.save_rules([
                {"platform": "douyin", "gift_id": "1", "gift_name": "Rose", "value": 1, "enabled": True},
                {"platform": "douyin", "gift_id": "2", "gift_name": "rose", "value": 2, "enabled": False},
            ])
        except gifts.GiftCompatibilityError as exc:
            assert "礼物名称" in str(exc)
        else:
            raise AssertionError("duplicate platform + casefold(gift_name) must be rejected")

        rows = service.save_rules([
            {"platform": "bilibili", "gift_id": "7", "gift_name": "玫瑰", "value": 1, "enabled": True},
            {"platform": "douyin", "gift_id": "7", "gift_name": "玫瑰", "value": 2, "enabled": True},
        ])
        assert len(rows) == 2


def check_language_uninstall_reset() -> None:
    from apps.server import security_hardening_guard as guard

    class FakeService:
        def __init__(self) -> None:
            self.selected = "fr-FR"
            self.reconciles = 0

        def _read_selected(self) -> str:
            return self.selected

        def _write_selected(self, language: str) -> None:
            self.selected = language

        def reconcile(self) -> str:
            self.reconciles += 1
            return self.selected

    class FakeManager:
        def __init__(self) -> None:
            self._lock = threading.RLock()
            self._records = {
                "example.fr.language": SimpleNamespace(
                    manifest={"type": "language", "language": "fr-FR"}
                )
            }

        def uninstall(self, plugin_id: str) -> None:
            self._records.pop(str(plugin_id).lower())

    manager = FakeManager()
    service = FakeService()
    fake_server = SimpleNamespace(plugin_manager=manager, LANGUAGE_SERVICE=service)
    guard._patch_language_uninstall(fake_server)
    manager.uninstall("example.fr.language")
    assert service.selected == "zh-CN"
    assert service.reconciles == 1
    assert "example.fr.language" not in manager._records


def check_release_channel_order() -> None:
    from apps.windows.release_selector import _operation_text
    from apps.windows.update_channel import version_key

    assert version_key("3.0.12-feiqi") < version_key("3.0.12-test")
    assert version_key("3.0.12-test") < version_key("3.0.12-cx")
    assert version_key("3.0.12-cx") < version_key("3.0.12-gc")
    assert version_key("3.0.12-gc") < version_key("3.0.12")
    assert version_key("3.0.12-g") == version_key("3.0.12-gc")
    assert version_key("3.0.12-t") == version_key("3.0.12-test")
    assert version_key("3.0.13-test") > version_key("3.0.12")
    assert _operation_text("3.0.12-gc", "3.0.12-test") == "升级"


def check_web_i18n_data_boundary() -> None:
    source = (ROOT / "apps/web/static/i18n.js").read_text(encoding="utf-8")
    assert "INITIAL_TEXT_NODES = new WeakSet()" in source
    assert "INITIAL_ELEMENTS = new WeakSet()" in source
    assert "DYNAMIC_UI_SELECTOR" in source
    assert "isTextTranslatable" in source
    assert "markInitialUi(document.body)" in source
    assert "data-i18n-ui" in source
    assert "queue names, blacklist entries, plugin/user" in source


def main() -> None:
    check_post_origin_boundary()
    check_gift_uniqueness()
    check_language_uninstall_reset()
    check_release_channel_order()
    check_web_i18n_data_boundary()
    print("issue #260 five bug guard: OK")


if __name__ == "__main__":
    main()
