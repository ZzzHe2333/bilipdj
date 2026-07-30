from __future__ import annotations

import sys
import threading
import types
import unittest
from dataclasses import dataclass

from core.douyin_fallback_guard import patch_douyin_module


class ProtocolError(RuntimeError):
    pass


@dataclass
class LiveInfo:
    live_id: str
    room_id: str
    user_id: str
    user_unique_id: str = ""
    anchor_id: str = ""
    sec_uid: str = ""
    ttwid: str = ""
    room_status: str = ""
    room_title: str = ""
    anchor_nickname: str = ""
    raw_html: str = ""


def _search_patterns(text: str, patterns: list[str]) -> str:
    import re

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return str(match.group("value"))
    return ""


class DouyinFallbackGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        module = types.ModuleType("fake_douyin_protocol")
        module.DouyinProtocolError = ProtocolError
        module.DouyinLiveInfo = LiveInfo
        module._search_patterns = _search_patterns

        def strict_parser(_html_text: str, _live_id: str):
            raise ProtocolError("missing ids")

        module.parse_douyin_live_info_html = strict_parser

        class Relay:
            def __init__(self, cfg):
                self.cfg = cfg
                self.result = None

            def _load_runtime_cfg(self):
                return dict(self.cfg)

            def _connect_and_stream(self):
                started = self.cfg.get("_started")
                release = self.cfg.get("_release")
                if isinstance(started, threading.Event):
                    started.set()
                if isinstance(release, threading.Event):
                    release.wait(2)
                info = module.parse_douyin_live_info_html(
                    '{"room_status":"2"}',
                    "123",
                )
                if not info.room_id:
                    info.room_id = self.cfg.get("preset_room_id", "")
                if not info.user_unique_id:
                    info.user_unique_id = self.cfg.get(
                        "preset_user_unique_id",
                        "",
                    )
                if not info.user_id:
                    info.user_id = self.cfg.get("preset_user_id", "")
                self.result = info
                return info

        Relay.__module__ = module.__name__
        module.DouyinDanmuRelay = Relay
        sys.modules[module.__name__] = module
        self.module = module
        self.Relay = Relay
        self.assertTrue(patch_douyin_module(module))

    def tearDown(self) -> None:
        sys.modules.pop(self.module.__name__, None)

    def test_strict_parser_remains_strict_outside_relay(self) -> None:
        with self.assertRaises(ProtocolError):
            self.module.parse_douyin_live_info_html("{}", "123")

    def test_saved_ids_make_relay_fallback_reachable(self) -> None:
        relay = self.Relay(
            {
                "preset_room_id": "room-1",
                "preset_user_id": "user-1",
                "preset_user_unique_id": "unique-1",
            }
        )
        result = relay._connect_and_stream()
        self.assertEqual(result.room_id, "room-1")
        self.assertEqual(result.user_unique_id, "unique-1")
        self.assertEqual(result.user_id, "user-1")
        self.assertEqual(result.room_status, "2")

    def test_missing_saved_ids_does_not_enable_lenient_parser(self) -> None:
        relay = self.Relay({"preset_room_id": "room-only"})
        with self.assertRaises(ProtocolError):
            relay._connect_and_stream()

    def test_thread_local_leniency_does_not_leak_to_other_threads(self) -> None:
        errors: list[type[BaseException]] = []
        started = threading.Event()
        release = threading.Event()
        relay = self.Relay(
            {
                "preset_room_id": "room-1",
                "preset_user_id": "user-1",
                "_started": started,
                "_release": release,
            }
        )

        thread = threading.Thread(target=relay._connect_and_stream)
        thread.start()
        self.assertTrue(started.wait(0.5))
        try:
            self.module.parse_douyin_live_info_html("{}", "123")
        except BaseException as exc:  # noqa: BLE001
            errors.append(type(exc))
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertIn(ProtocolError, errors)


if __name__ == "__main__":
    unittest.main()
