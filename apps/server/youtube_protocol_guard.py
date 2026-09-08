"""Offline regression checks for the red-TV chat adapter."""
from __future__ import annotations

try:
    from . import youtube_protocol as yp
except ImportError:
    import youtube_protocol as yp


def _synthetic_initial_data() -> dict:
    return {
        "contents": {
            "videoPrimaryInfoRenderer": {"title": {"runs": [{"text": "测试直播"}]}},
            "videoSecondaryInfoRenderer": {
                "owner": {"videoOwnerRenderer": {"title": {"runs": [{"text": "测试频道"}]}}}
            },
            "liveChatRenderer": {
                "continuations": [
                    {
                        "invalidationContinuationData": {
                            "continuation": "CONTINUATION_TOKEN_1234567890",
                            "timeoutMs": 1500,
                        }
                    }
                ]
            },
        }
    }


def run_checks() -> None:
    video_id = "xKtKV9wSPQk"
    assert yp.extract_video_id(video_id) == video_id
    assert yp.extract_video_id(f"https://www.youtube.com/watch?v={video_id}") == video_id
    assert yp.extract_video_id(f"https://youtu.be/{video_id}") == video_id
    assert yp.extract_video_id(f"https://www.youtube.com/live/{video_id}") == video_id
    assert yp.canonical_room_url(video_id).endswith(video_id)

    renderer = {
        "id": "message-1",
        "authorExternalChannelId": "channel-1",
        "authorName": {"simpleText": "测试用户"},
        "message": {"runs": [{"text": "你好"}, {"emoji": {"shortcuts": [":smile:"]}}]},
    }
    event = yp._event_from_renderer("liveChatTextMessageRenderer", renderer)
    assert event is not None
    assert event.nickname == "测试用户"
    assert event.content == "你好:smile:"
    assert event.event_id == "message-1"
    assert event.uid > 0

    initial = _synthetic_initial_data()
    chat = yp._find_live_chat_renderer(initial)
    assert chat is not None
    continuation, timeout_ms = yp._get_continuation(chat)
    assert continuation == "CONTINUATION_TOKEN_1234567890"
    assert timeout_ms == 1500

    import json
    html = (
        '<script>ytcfg.set({"INNERTUBE_API_KEY":"key123",'
        '"INNERTUBE_CLIENT_VERSION":"2.20260908.00.00"});</script>'
        '<script>var ytInitialData = ' + json.dumps(initial) + ';</script>'
    )
    config = yp._extract_ytcfg(html)
    assert config["INNERTUBE_API_KEY"] == "key123"
    parsed = yp._extract_initial_data(html)
    assert isinstance(parsed, dict)
    assert yp._find_live_chat_renderer(parsed) is not None


if __name__ == "__main__":
    run_checks()
    print("Red-TV protocol guard: OK")
