from __future__ import annotations

import struct
from typing import Any


class TarsType:
    INT8 = 0
    INT16 = 1
    INT32 = 2
    INT64 = 3
    FLOAT = 4
    DOUBLE = 5
    STRING1 = 6
    STRING4 = 7
    MAP = 8
    LIST = 9
    STRUCT_BEGIN = 10
    STRUCT_END = 11
    ZERO = 12
    SIMPLE_LIST = 13


class TarsWriter:
    def __init__(self) -> None:
        self.buf = bytearray()

    def head(self, typ: int, tag: int) -> None:
        if tag < 15:
            self.buf.append((tag << 4) | typ)
            return
        if tag > 255:
            raise ValueError("TARS tag > 255 is unsupported")
        self.buf.append(0xF0 | typ)
        self.buf.append(tag)

    def write_int(self, tag: int, value: int) -> None:
        value = int(value)
        if value == 0:
            self.head(TarsType.ZERO, tag)
        elif -128 <= value <= 127:
            self.head(TarsType.INT8, tag)
            self.buf.extend(struct.pack(">b", value))
        elif -32768 <= value <= 32767:
            self.head(TarsType.INT16, tag)
            self.buf.extend(struct.pack(">h", value))
        elif -(2**31) <= value <= (2**31 - 1):
            self.head(TarsType.INT32, tag)
            self.buf.extend(struct.pack(">i", value))
        else:
            self.head(TarsType.INT64, tag)
            self.buf.extend(struct.pack(">q", value))

    def write_string(self, tag: int, value: str) -> None:
        raw = str(value).encode("utf-8")
        if len(raw) <= 255:
            self.head(TarsType.STRING1, tag)
            self.buf.append(len(raw))
        else:
            self.head(TarsType.STRING4, tag)
            self.buf.extend(struct.pack(">I", len(raw)))
        self.buf.extend(raw)

    def write_bytes(self, tag: int, value: bytes) -> None:
        self.head(TarsType.SIMPLE_LIST, tag)
        self.head(TarsType.INT8, 0)
        self.write_int(0, len(value))
        self.buf.extend(value)

    def write_string_list(self, tag: int, values: list[str]) -> None:
        self.head(TarsType.LIST, tag)
        self.write_int(0, len(values))
        for value in values:
            self.write_string(0, value)

    def write_struct(self, tag: int, value: bytes) -> None:
        self.head(TarsType.STRUCT_BEGIN, tag)
        self.buf.extend(value)
        self.head(TarsType.STRUCT_END, 0)

    def data(self) -> bytes:
        return bytes(self.buf)


class TarsReader:
    def __init__(self, data: bytes) -> None:
        self.data = memoryview(data)
        self.pos = 0

    def remaining(self) -> int:
        return len(self.data) - self.pos

    def read_exact(self, size: int) -> bytes:
        if size < 0 or self.pos + size > len(self.data):
            raise ValueError("TARS payload is truncated")
        value = self.data[self.pos : self.pos + size].tobytes()
        self.pos += size
        return value

    def read_head(self) -> tuple[int, int]:
        byte = self.read_exact(1)[0]
        typ = byte & 0x0F
        tag = (byte >> 4) & 0x0F
        if tag == 15:
            tag = self.read_exact(1)[0]
        return tag, typ

    def read_value(self, typ: int) -> Any:
        if typ == TarsType.ZERO:
            return 0
        if typ == TarsType.INT8:
            return struct.unpack(">b", self.read_exact(1))[0]
        if typ == TarsType.INT16:
            return struct.unpack(">h", self.read_exact(2))[0]
        if typ == TarsType.INT32:
            return struct.unpack(">i", self.read_exact(4))[0]
        if typ == TarsType.INT64:
            return struct.unpack(">q", self.read_exact(8))[0]
        if typ == TarsType.FLOAT:
            return struct.unpack(">f", self.read_exact(4))[0]
        if typ == TarsType.DOUBLE:
            return struct.unpack(">d", self.read_exact(8))[0]
        if typ == TarsType.STRING1:
            size = self.read_exact(1)[0]
            return self.read_exact(size).decode("utf-8", errors="replace")
        if typ == TarsType.STRING4:
            size = struct.unpack(">I", self.read_exact(4))[0]
            return self.read_exact(size).decode("utf-8", errors="replace")
        if typ == TarsType.STRUCT_BEGIN:
            return self.read_struct(until_struct_end=True)
        if typ == TarsType.LIST:
            _tag, length_type = self.read_head()
            count = int(self.read_value(length_type))
            result = []
            for _ in range(count):
                _item_tag, item_type = self.read_head()
                result.append(self.read_value(item_type))
            return result
        if typ == TarsType.MAP:
            _tag, length_type = self.read_head()
            count = int(self.read_value(length_type))
            result = []
            for _ in range(count):
                _key_tag, key_type = self.read_head()
                key = self.read_value(key_type)
                _value_tag, value_type = self.read_head()
                value = self.read_value(value_type)
                result.append((key, value))
            return result
        if typ == TarsType.SIMPLE_LIST:
            _sub_tag, sub_type = self.read_head()
            if sub_type != TarsType.INT8:
                raise ValueError("Unsupported TARS SIMPLE_LIST subtype")
            _length_tag, length_type = self.read_head()
            size = int(self.read_value(length_type))
            return self.read_exact(size)
        if typ == TarsType.STRUCT_END:
            return None
        raise ValueError(f"Unknown TARS type: {typ}")

    def read_struct(self, *, until_struct_end: bool = False) -> dict[int, Any]:
        fields: dict[int, Any] = {}
        while self.remaining() > 0:
            tag, typ = self.read_head()
            if typ == TarsType.STRUCT_END:
                if until_struct_end:
                    break
                continue
            fields[tag] = self.read_value(typ)
        return fields


def tars_parse(data: bytes) -> dict[int, Any]:
    return TarsReader(data).read_struct()


__all__ = ["TarsReader", "TarsType", "TarsWriter", "tars_parse"]
