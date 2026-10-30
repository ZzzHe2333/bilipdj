"""Issue #331: synthetic Protobuf regression for free/unknown price gifts."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from apps.server.douyin_ws_frames import decode_gift_message


def _varint(number: int) -> bytes:
    chunks = []
    while number > 127:
        chunks.append((number & 127) | 128)
        number >>= 7
    return bytes([*chunks, number])


def _integer(field: int, value: int) -> bytes:
    return _varint(field << 3) + _varint(value)


def _message(field: int, payload: bytes) -> bytes:
    return _varint((field << 3) | 2) + _varint(len(payload)) + payload


def _gift(*, diamond: int | None) -> bytes:
    common = _integer(2, 123456) + _integer(3, 987654) + _integer(4, 101010)
    user = _integer(1, 2468) + _message(3, b"synthetic-person")
    info = _integer(5, 3992) + _message(16, b"synthetic-gift")
    if diamond is not None:
        info += _integer(12, diamond)
    return (
        _message(1, common)
        + _integer(2, 3992)
        + _integer(5, 1)
        + _integer(9, 1)
        + _message(7, user)
        + _message(15, info)
    )


def main() -> None:
    for price in (0, 1, None):
        gift = decode_gift_message(_gift(diamond=price))
        assert gift is not None
        assert gift["msgId"] == "123456"
        assert gift["roomId"] == "987654"
        assert gift["gift"]["id"] == 3992
        assert gift["gift"]["diamondCount"] == price
        assert gift["totalDiamond"] == price
    print("Issue #331 free and unknown gift value Protobuf regression: OK")


if __name__ == "__main__":
    main()
