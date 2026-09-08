from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from apps.server import bilibili_protocol, douyin_protocol  # noqa: E402


def _result(name: str, status: str, detail: str, **extra: object) -> dict[str, object]:
    return {"name": name, "status": status, "detail": detail, **extra}


def check_bilibili_room() -> dict[str, object]:
    try:
        room = bilibili_protocol.resolve_room_id(3049445)
        return _result("bilibili_room", "ok", f"room={room}")
    except Exception as exc:  # noqa: BLE001
        return _result("bilibili_room", "error", str(exc))


def check_bilibili_danmu() -> dict[str, object]:
    try:
        info = bilibili_protocol.fetch_danmu_info(3049445)
        hosts = info.get("host_list", []) if isinstance(info, dict) else []
        return _result("bilibili_danmu", "ok", f"hosts={len(hosts)}")
    except Exception as exc:  # noqa: BLE001
        return _result("bilibili_danmu", "error", str(exc))


def check_douyin_parser() -> dict[str, object]:
    try:
        normalized = douyin_protocol.normalize_live_id("https://live.douyin.com/123456")
        if normalized != "123456":
            raise RuntimeError(f"unexpected normalized id: {normalized}")
        return _result("douyin_parser", "ok", f"live_id={normalized}")
    except Exception as exc:  # noqa: BLE001
        return _result("douyin_parser", "error", str(exc))


def main() -> int:
    results = [check_bilibili_room(), check_bilibili_danmu(), check_douyin_parser()]
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 1 if any(item["status"] == "error" for item in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
