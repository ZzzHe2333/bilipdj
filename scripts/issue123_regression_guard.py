from __future__ import annotations

import io
import sys
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.server import issue123_guard  # noqa: E402
from apps.server import settings_backup  # noqa: E402
from apps.server import youtube_protocol  # noqa: E402

VIDEO_ID = "xKtKV9wSPQk"


def test_runtime_status_preserves_multi_platform_details() -> None:
    class Handler:
        def do_GET(self) -> None:  # noqa: N802
            self.fallback_called = True

        def _require_loopback(self) -> bool:
            return True

    fake_module = SimpleNamespace(
        ApiHandler=Handler,
        _get_runtime_platform=lambda cfg: str(cfg.get("platform", "bilibili")),
    )
    issue123_guard._patch_runtime_status_and_probe(fake_module, youtube_protocol)

    relay_status = {
        "platform": "multi",
        "connected": True,
        "roomid": VIDEO_ID,
        "active_platforms": ["bilibili", "youtube"],
        "platforms": {
            "bilibili": {"platform": "bilibili", "connected": True, "roomid": 123},
            "youtube": {"platform": "youtube", "connected": True, "roomid": VIDEO_ID},
        },
    }
    server = SimpleNamespace(
        runtime_config={"platform": "bilibili"},
        danmu_relay=SimpleNamespace(get_runtime_status=lambda: relay_status),
        ws_hub=SimpleNamespace(client_count=2, last_message_at="now"),
    )
    handler = Handler()
    handler.path = "/api/runtime-status"
    handler.server = server
    captured: dict[str, object] = {}
    handler._write_json = lambda payload: captured.update(payload)
    handler.do_GET()

    assert captured["danmu_roomid"] == 0
    assert captured["danmu_room_id"] == VIDEO_ID
    assert captured["active_platforms"] == ["bilibili", "youtube"]
    assert isinstance(captured["platforms"], dict)
    assert captured["platforms"]["youtube"]["roomid"] == VIDEO_ID


def test_backend_probe_endpoint_uses_protocol_probe() -> None:
    class Handler:
        def do_GET(self) -> None:  # noqa: N802
            self.fallback_called = True

        def _require_loopback(self) -> bool:
            return True

    fake_module = SimpleNamespace(
        ApiHandler=Handler,
        _get_runtime_platform=lambda cfg: "bilibili",
    )
    original_probe = youtube_protocol.probe_google_access
    youtube_protocol.probe_google_access = lambda timeout=4.0: True
    try:
        issue123_guard._patch_runtime_status_and_probe(fake_module, youtube_protocol)
        handler = Handler()
        handler.path = "/api/platforms/youtube/probe"
        handler.server = SimpleNamespace()
        captured: dict[str, object] = {}
        handler._write_json = lambda payload: captured.update(payload)
        handler.do_GET()
        assert captured == {"status": "ok", "allowed": True}
    finally:
        youtube_protocol.probe_google_access = original_probe


def test_youtube_end_state_stops_reconnect() -> None:
    session = youtube_protocol.YoutubeChatSession(VIDEO_ID)
    session.api_key = "test-key"
    session.context = {"client": {"clientName": "WEB", "clientVersion": "test"}}
    session.continuation = "same-continuation-token"
    session.mode = "live"

    calls = 0
    original_post = youtube_protocol._http_json_post

    def fake_post(*args, **kwargs):
        nonlocal calls
        calls += 1
        return {
            "continuationContents": {
                "liveChatContinuation": {
                    "actions": [],
                    "continuations": [],
                }
            }
        }

    youtube_protocol._http_json_post = fake_post
    try:
        events, _ = session.fetch_once()
        assert events == []
        assert calls == 1
        try:
            session.fetch_once()
        except youtube_protocol.YoutubeChatEnded:
            pass
        else:
            raise AssertionError("continuation exhaustion must become YoutubeChatEnded")
        assert calls == 1
    finally:
        youtube_protocol._http_json_post = original_post

    statuses: list[dict[str, object]] = []
    logger = SimpleNamespace(info=lambda *args, **kwargs: None, warning=lambda *args, **kwargs: None)
    ws_hub = SimpleNamespace(broadcast_json=lambda _sender, payload: statuses.append(payload))
    relay = youtube_protocol.YoutubeDanmuRelay(SimpleNamespace(logger=logger, ws_hub=ws_hub))
    connect_calls = 0
    relay._load_runtime_cfg = lambda: {"auto_reconnect": True, "reconnect_delay_seconds": 0.01}

    def ended(_cfg):
        nonlocal connect_calls
        connect_calls += 1
        raise youtube_protocol.YoutubeChatEnded("红色小电视聊天已结束")

    relay._connect_once = ended
    relay.run()
    assert connect_calls == 1
    assert any(item.get("status") == "danmu_ended" for item in statuses)


def test_platform_slots_participate_in_settings_backup() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)

        class FakeServer:
            MAX_QUEUE_ARCHIVE_SLOTS = 10
            _YAML_DIR = root
            CONFIG_PATH = root / "config.yaml"
            QUANXIAN_PATH = root / "quanxian.yaml"
            KAIGUAN_PATH = root / "kaiguan.yaml"
            STYLE_PATH = root / "style.json"
            APPEARANCE_PATH = root / "appearance.json"

            @staticmethod
            def platform_config_path(slot: int) -> Path:
                return root / "slots" / f"slot-{int(slot)}.yaml"

        FakeServer.CONFIG_PATH.write_text("platform: bilibili\n", encoding="utf-8")
        FakeServer.QUANXIAN_PATH.write_text("admins: []\n", encoding="utf-8")
        FakeServer.KAIGUAN_PATH.write_text("paidui: true\n", encoding="utf-8")
        FakeServer.STYLE_PATH.write_text("{}\n", encoding="utf-8")
        slot1 = FakeServer.platform_config_path(1)
        slot1.parent.mkdir(parents=True, exist_ok=True)
        slot1.write_text("platform: youtube\n", encoding="utf-8")

        service = settings_backup.SettingsBackupService(FakeServer)
        data, included = service.build_settings_zip()
        assert "platform-slot-1.yaml" in included
        with zipfile.ZipFile(io.BytesIO(data), "r") as archive:
            assert archive.read("platform-slot-1.yaml").decode("utf-8") == "platform: youtube\n"

        # Old backups have config.yaml but no slot files. Stale local slots must
        # be cleared or they would override the restored config immediately.
        slot2 = FakeServer.platform_config_path(2)
        slot2.write_text("platform: twitch\n", encoding="utf-8")
        legacy = io.BytesIO()
        with zipfile.ZipFile(legacy, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("config.yaml", b"platform: bilibili\n")
        service.restore_settings_zip(legacy.getvalue())
        assert not slot1.exists()
        assert not slot2.exists()
        assert FakeServer.CONFIG_PATH.read_text(encoding="utf-8") == "platform: bilibili\n"


def test_web_probe_bridge_is_present() -> None:
    text = (ROOT / "apps/web/static/control_issue123.js").read_text(encoding="utf-8")
    assert "www.google.com" in text
    assert "/generate_204" in text
    assert "/api/platforms/youtube/probe" in text
    assert "payload.allowed !== true" in text


def main() -> None:
    tests = [
        test_runtime_status_preserves_multi_platform_details,
        test_backend_probe_endpoint_uses_protocol_probe,
        test_youtube_end_state_stops_reconnect,
        test_platform_slots_participate_in_settings_backup,
        test_web_probe_bridge_is_present,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print("issue #123 regression guard: OK")


if __name__ == "__main__":
    main()
