"""Offline regression checks for the Huya protocol adapter."""
from __future__ import annotations

try:
    from . import huya_protocol as hp
except ImportError:
    import huya_protocol as hp


def _build_message(*, uid: int, nickname: str, content: str) -> bytes:
    sender = hp.TarsWriter()
    sender.write_int(0, uid)
    sender.write_string(2, nickname)

    bullet = hp.TarsWriter()
    bullet.write_int(0, 0xFFFFFF)

    notice = hp.TarsWriter()
    notice.write_struct(0, sender.data())
    notice.write_string(3, content)
    notice.write_struct(6, bullet.data())

    push = hp.TarsWriter()
    push.write_int(1, 1400)
    push.write_bytes(2, notice.data())

    outer = hp.TarsWriter()
    outer.write_int(0, 7)
    outer.write_bytes(1, push.data())
    return outer.data()


def run_checks() -> None:
    assert hp.normalize_huya_target("https://www.huya.com/lpl")[1] == "lpl"
    assert hp.normalize_huya_target("lpl")[1] == "lpl"
    assert hp.normalize_huya_target("660000")[1] == "660000"

    packet = hp.make_huya_register_packet(1346609715)
    command = hp.tars_parse(packet)
    assert int(command[0]) == 16
    register = hp.tars_parse(command[1])
    assert register[0] == ["live:1346609715", "chat:1346609715"]

    event = hp.decode_huya_chat_message(
        _build_message(uid=123456, nickname="测试用户", content="？？？？？？")
    )
    assert event is not None
    assert event.uid == 123456
    assert event.nickname == "测试用户"
    assert event.content == "？？？？？？"
    assert not hp.is_huya_system_chat(event)

    system_event = hp.decode_huya_chat_message(
        _build_message(uid=1, nickname="系统消息", content="阿达 刚刚下单了 英雄联盟精选礼盒")
    )
    assert system_event is not None
    assert hp.is_huya_system_chat(system_event)

    notice = hp.TarsWriter()
    notice.write_string(3, "ignored")
    push = hp.TarsWriter()
    push.write_int(1, 9999)
    push.write_bytes(2, notice.data())
    outer = hp.TarsWriter()
    outer.write_int(0, 7)
    outer.write_bytes(1, push.data())
    assert hp.decode_huya_chat_message(outer.data()) is None


if __name__ == "__main__":
    run_checks()
    print("Huya protocol guard: OK")
