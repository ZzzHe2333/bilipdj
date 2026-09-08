"""Offline regression checks for the purple-mouse chat adapter."""
from __future__ import annotations

try:
    from . import twitch_protocol as tp
except ImportError:
    import twitch_protocol as tp


def run_checks() -> None:
    channel = "gearbaby1010"
    assert tp.extract_channel(channel) == channel
    assert tp.extract_channel(f"https://www.twitch.tv/{channel}") == channel
    assert tp.extract_channel(f"https://twitch.tv/{channel}/videos") == channel
    assert tp.canonical_room_url(channel) == f"https://www.twitch.tv/{channel}"

    for invalid in (
        "https://example.com/gearbaby1010",
        "https://www.twitch.tv/directory",
        "https://www.twitch.tv/",
        "bad-channel-name!",
    ):
        try:
            tp.extract_channel(invalid)
        except tp.TwitchProtocolError:
            pass
        else:
            raise AssertionError(f"invalid target accepted: {invalid}")

    tags = tp.parse_irc_tags(
        r"badge-info=subscriber/12;badges=subscriber/12;color=#9146FF;"
        r"display-name=User\sName;emotes=;first-msg=1;id=msg-1;mod=0;"
        r"subscriber=1;user-id=123456"
    )
    assert tags["display-name"] == "User Name"
    assert tags["badges"] == "subscriber/12"
    assert tags["color"] == "#9146FF"

    line = (
        "@badge-info=subscriber/12;badges=subscriber/12;color=#9146FF;"
        "display-name=User\\sName;emotes=;first-msg=1;id=msg-1;mod=0;"
        "subscriber=1;user-id=123456 "
        ":user_name!user_name@user_name.tmi.twitch.tv "
        "PRIVMSG #gearbaby1010 :hello 紫色老鼠"
    )
    event = tp.parse_twitch_privmsg(line)
    assert event is not None
    assert event.uid == 123456
    assert event.user_id == "123456"
    assert event.login == "user_name"
    assert event.nickname == "User Name"
    assert event.content == "hello 紫色老鼠"
    assert event.badges == "subscriber/12"
    assert event.color == "#9146FF"
    assert event.is_subscriber is True
    assert event.is_mod is False
    assert event.first_msg is True
    assert event.message_id == "msg-1"
    assert event.recv_time

    # Non-chat IRC commands must never enter the queue parser.
    assert tp.parse_twitch_privmsg("PING :tmi.twitch.tv") is None
    assert tp.parse_twitch_privmsg(":tmi.twitch.tv 001 justinfan12345 :Welcome") is None
    assert tp.parse_twitch_privmsg("@room-id=1 :tmi.twitch.tv ROOMSTATE #gearbaby1010") is None

    # Missing numeric user-id receives a stable local positive identity.
    anonymous_line = (
        "@display-name=Anon :anon!anon@anon.tmi.twitch.tv "
        "PRIVMSG #gearbaby1010 :hello"
    )
    first = tp.parse_twitch_privmsg(anonymous_line)
    second = tp.parse_twitch_privmsg(anonymous_line)
    assert first is not None and second is not None
    assert first.uid > 0 and first.uid == second.uid

    proxy = tp._parse_proxy_url("https", "http://127.0.0.1:7890")
    assert proxy is not None
    assert proxy["host"] == "127.0.0.1"
    assert proxy["port"] == 7890
    assert proxy["proxy_type"] == "http"


if __name__ == "__main__":
    run_checks()
    print("Purple-mouse protocol guard: OK")
