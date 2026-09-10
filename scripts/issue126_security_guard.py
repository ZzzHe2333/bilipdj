from __future__ import annotations

import csv
import tempfile
from pathlib import Path
from types import SimpleNamespace

from apps.server import security_hardening_guard as guard
from apps.server import server


def check_config_normalization() -> None:
    cfg = server.normalize_server_config(
        {
            "host": "0.0.0.0",
            "lan_listen": True,
            "lan_readonly": True,
            "lan_readonly_token": " secret ",
            "max_connections": 3,
            "http_socket_timeout": 1,
        }
    )
    assert cfg["lan_readonly"] is True
    assert cfg["lan_readonly_token"] == "secret"
    assert cfg["max_connections"] == 8
    assert cfg["http_socket_timeout"] == 2.0


def check_csv_roundtrip() -> None:
    entries = [
        {"id": "=1+1", "content": "+SUM(A1:A2)", "last_operation_at": "2026-09-10 15:00:00"},
        {"id": "normal", "content": "plain", "last_operation_at": "2026-09-10 15:00:01"},
        {"id": "@cmd", "content": "\t-1", "last_operation_at": "2026-09-10 15:00:02"},
    ]
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "queue.csv"
        server.write_queue_archive_entries(path, entries, meta={"actor": "=actor", "message": "normal"})
        raw = path.read_text(encoding="utf-8-sig")
        assert "'=1+1" in raw
        assert "'+SUM(A1:A2)" in raw
        assert "'@cmd" in raw
        assert "'\t-1" in raw
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            meta, restored = server.parse_queue_archive_rows(list(csv.reader(handle)))
        assert meta["actor"] == "=actor"
        assert restored[0]["id"] == "=1+1"
        assert restored[0]["content"] == "+SUM(A1:A2)"
        assert restored[1]["id"] == "normal"
        assert restored[2]["id"] == "@cmd"
        assert restored[2]["content"] == "\t-1"


def _handler(*, loopback: bool, config: dict, path: str, token: str = ""):
    return SimpleNamespace(
        path=path,
        server=SimpleNamespace(runtime_config={"server": config}),
        headers={"X-BiliPDJ-Read-Token": token},
        _is_loopback_client=lambda: loopback,
    )


def check_lan_read_policy() -> None:
    assert guard._remote_read_allowed(_handler(loopback=True, config={}, path="/api/queue/state"))
    assert not guard._remote_read_allowed(_handler(loopback=False, config={}, path="/api/queue/state"))
    assert guard._remote_read_allowed(
        _handler(loopback=False, config={"lan_readonly": True}, path="/api/queue/state")
    )
    assert not guard._remote_read_allowed(
        _handler(
            loopback=False,
            config={"lan_readonly": True, "lan_readonly_token": "abc"},
            path="/api/queue/state",
            token="wrong",
        )
    )
    assert guard._remote_read_allowed(
        _handler(
            loopback=False,
            config={"lan_readonly": True, "lan_readonly_token": "abc"},
            path="/api/queue/state?token=abc",
        )
    )


def check_connection_guard_installed() -> None:
    assert getattr(server.BackendServer.process_request, "_issue126_hardened", False)
    assert getattr(server.BackendServer.process_request_thread, "_issue126_hardened", False)
    assert getattr(server.ApiHandler.do_GET, "_issue126_hardened", False)


def main() -> None:
    check_config_normalization()
    check_csv_roundtrip()
    check_lan_read_policy()
    check_connection_guard_installed()
    print("issue126 security regression guard: ok")


if __name__ == "__main__":
    main()
