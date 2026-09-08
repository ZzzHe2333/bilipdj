from __future__ import annotations

import copy
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.server import youtube_protocol  # noqa: E402
from apps.server import youtube_runtime_guard as guard  # noqa: E402

VIDEO_ID = "xKtKV9wSPQk"


def test_runtime_status_accepts_string_room_id() -> None:
    class Handler:
        def do_GET(self) -> None:  # noqa: N802
            self.fallback_called = True

    fake_module = SimpleNamespace(
        ApiHandler=Handler,
        _get_runtime_platform=lambda cfg: str(cfg.get("platform", "bilibili")),
    )
    guard._patch_runtime_status(fake_module)

    relay = SimpleNamespace(
        get_runtime_status=lambda: {
            "platform": "youtube",
            "connected": True,
            "roomid": VIDEO_ID,
            "host": "www.youtube.com",
            "port": 443,
            "transport": "https-poll",
        }
    )
    ws_hub = SimpleNamespace(client_count=1, last_message_at="")
    server = SimpleNamespace(
        runtime_config={"platform": "youtube"},
        danmu_relay=relay,
        ws_hub=ws_hub,
    )

    handler = Handler()
    handler.path = "/api/runtime-status"
    handler.server = server
    captured: dict[str, object] = {}
    handler._write_json = lambda payload: captured.update(payload)
    handler.do_GET()

    assert captured["status"] == "ok"
    assert captured["danmu_platform"] == "youtube"
    assert captured["danmu_roomid"] == 0
    assert captured["danmu_room_id"] == VIDEO_ID


def test_active_platforms_survive_reload_and_empty_list() -> None:
    state = {
        "global": {
            "platform": "bilibili",
            "youtube": {"extra": {}},
            "platform_config_archive": {"active_slot": 1},
        },
        "slot": {
            "platform": "bilibili",
            "youtube": {"extra": {}},
        },
    }

    def load_config():
        cfg = copy.deepcopy(state["global"])
        cfg["youtube"] = copy.deepcopy(state["slot"].get("youtube", {"extra": {}}))
        return cfg

    def save_config(cfg, *args, **kwargs):
        state["global"] = copy.deepcopy(cfg)

    def load_slot(_slot):
        return copy.deepcopy(state["slot"])

    def save_slot(_slot, payload):
        state["slot"] = copy.deepcopy(payload)
        return copy.deepcopy(payload)

    fake_server = SimpleNamespace(
        load_config=load_config,
        save_config=save_config,
        load_platform_config_slot=load_slot,
        save_platform_config_slot=save_slot,
        MAX_QUEUE_ARCHIVE_SLOTS=10,
    )

    supported = ("bilibili", "douyin", "huya", "youtube")

    def normalize(_module, config):
        raw = config.get("active_platforms") if isinstance(config, dict) else None
        if isinstance(raw, (list, tuple)):
            return tuple(x for x in raw if x in fake_issue.SUPPORTED_ACTIVE_PLATFORMS)
        return ("bilibili",)

    fake_issue = SimpleNamespace(
        SUPPORTED_ACTIVE_PLATFORMS=supported,
        _normalize_active_platforms=normalize,
    )

    guard._patch_active_platform_persistence(fake_server, fake_issue)

    cfg = fake_server.load_config()
    cfg["active_platforms"] = ["bilibili", "youtube"]
    fake_server.save_config(cfg)

    reloaded = fake_server.load_config()
    assert reloaded["active_platforms"] == ["bilibili", "youtube"]
    assert fake_issue._normalize_active_platforms(fake_server, reloaded) == ("bilibili", "youtube")

    reloaded["active_platforms"] = []
    fake_server.save_config(reloaded)
    disabled = fake_server.load_config()
    assert disabled["active_platforms"] == []
    assert fake_issue._normalize_active_platforms(fake_server, disabled) == ()


def test_exhausted_continuation_is_not_reused() -> None:
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
        events, timeout_ms = session.fetch_once()
        assert events == []
        assert timeout_ms <= 250
        assert session.continuation == ""
        assert calls == 1

        try:
            session.fetch_once()
        except youtube_protocol.YoutubeProtocolError as exc:
            assert "已结束" in str(exc)
        else:
            raise AssertionError("exhausted chat must stop before reusing the same continuation")
        assert calls == 1
    finally:
        youtube_protocol._http_json_post = original_post


def test_web_accepts_schemeless_youtube_urls() -> None:
    text = (ROOT / "apps/web/static/control_redtv.js").read_text(encoding="utf-8")
    assert "if (!text.includes('://')" in text
    assert "text = `https://${text.replace" in text
    assert "youtube.com" in text and "youtu.be" in text


def main() -> None:
    tests = [
        test_runtime_status_accepts_string_room_id,
        test_active_platforms_survive_reload_and_empty_list,
        test_exhausted_continuation_is_not_reused,
        test_web_accepts_schemeless_youtube_urls,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print("redtv issue #121 regression guard: OK")


if __name__ == "__main__":
    main()
