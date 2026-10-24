"""Issue #327: local gift-collector safety regressions (synthetic data only)."""
from __future__ import annotations

import gzip
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from apps.server import douyin_live_pb2
from apps.server.douyin_headless import _FrameTap, _release_profile, DouyinHeadlessCollector
from apps.server.douyin_ws_frames import (
    FrameDecodeError,
    MAX_PAYLOAD_BYTES,
    iter_messages,
)


def _frame(payload: bytes) -> bytes:
    frame = douyin_live_pb2.PushFrame()
    frame.payload = payload
    return frame.SerializeToString()


def check_frame_bounds() -> None:
    response = douyin_live_pb2.LiveResponse()
    msg = response.messagesList.add()
    msg.method, msg.payload = "WebcastChatMessage", b"synthetic"
    actual = list(iter_messages(_frame(gzip.compress(response.SerializeToString()))))
    assert actual == [("WebcastChatMessage", b"synthetic")]

    # A tiny compressed blob must never cause allocation of a huge response.
    oversized = gzip.compress(b"A" * (MAX_PAYLOAD_BYTES + 2))
    assert len(oversized) < 100 * 1024
    try:
        list(iter_messages(_frame(oversized)))
    except FrameDecodeError:
        pass
    else:
        raise AssertionError("Gzip output limit was ignored")

    try:
        list(iter_messages(_frame(gzip.compress(b"test")[:-3])))
    except FrameDecodeError:
        pass
    else:
        raise AssertionError("Truncated gzip payload was accepted")


def check_browser_profile_safety() -> None:
    with tempfile.TemporaryDirectory() as td:
        profile = Path(td)
        lock = profile / "lockfile"
        lock.write_text("synthetic", encoding="ascii")
        browser = SimpleNamespace(info={
            "name": "chrome.exe",
            "cmdline": ["chrome.exe", "--user-data-dir=" + str(profile)],
        })
        with patch("psutil.process_iter", return_value=[browser]):
            try:
                _release_profile(profile)
            except RuntimeError as exc:
                assert "in use" in str(exc)
            else:
                raise AssertionError("Must never kill active QR browser")
            assert lock.is_file(), "Do not clear an active browser profile lock"
        with patch("psutil.process_iter", return_value=[]):
            _release_profile(profile)
        assert not lock.exists(), "Stale profile lock should be removed"


def check_account_status_redaction() -> None:
    tap = _FrameTap()
    tap({
        "method": "Network.webSocketCreated",
        "params": {"url": "wss://example.test/webcast/im/push/v2/"
                          "?user_unique_id=SYNTHETIC_PRIVATE_ACCOUNT"},
    })
    assert tap.im_session_identified is True
    collector = DouyinHeadlessCollector(SimpleNamespace(), SimpleNamespace())
    state = collector.status()
    assert "im_user_unique_id" not in state
    assert "SYNTHETIC_PRIVATE_ACCOUNT" not in str(state)
    assert state["im_session_identified"] is False


def main() -> None:
    check_frame_bounds()
    check_browser_profile_safety()
    check_account_status_redaction()
    print("Issue #327 review safety regressions: OK")


if __name__ == "__main__":
    main()
