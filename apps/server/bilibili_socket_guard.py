"""Socket framing guard for the Bilibili binary danmu protocol.

A timeout before any bytes arrive is a normal idle poll. A timeout after part of
an exact-length packet has arrived is different: continuing on the same socket
would interpret the remaining body as a new packet header and permanently
misalign the stream. This module makes that partial-read condition explicit.
"""
from __future__ import annotations

import socket
from typing import Any


class PartialSocketReadError(ConnectionError):
    """Raised when an exact-length socket read times out after partial data."""


def recv_exact_guarded(conn: socket.socket, size: int) -> bytes | None:
    if size < 0:
        raise ValueError("size must not be negative")
    if size == 0:
        return b""

    chunks = bytearray()
    while len(chunks) < size:
        try:
            chunk = conn.recv(size - len(chunks))
        except (TimeoutError, socket.timeout) as exc:
            if chunks:
                raise PartialSocketReadError(
                    f"socket timed out after {len(chunks)} of {size} bytes"
                ) from exc
            raise TimeoutError("socket recv timeout") from exc
        if not chunk:
            return None
        chunks.extend(chunk)
    return bytes(chunks)


def install_bilibili_socket_guard(protocol_module: Any) -> bool:
    """Install the guarded exact reader on one protocol module, idempotently."""

    current = getattr(protocol_module, "_ws_recv_exact", None)
    if current is recv_exact_guarded:
        return False
    if not callable(current):
        raise TypeError("protocol module does not expose _ws_recv_exact")
    setattr(protocol_module, "_ws_recv_exact", recv_exact_guarded)
    setattr(protocol_module, "PartialSocketReadError", PartialSocketReadError)
    return True


__all__ = [
    "PartialSocketReadError",
    "install_bilibili_socket_guard",
    "recv_exact_guarded",
]
