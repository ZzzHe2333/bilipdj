#!/usr/bin/env python3
"""Non-secret, layered health check for the public Bilibili/Douyin APIs."""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core import bilibili_protocol, douyin_protocol  # noqa: E402


def _result(name: str, status: str, detail: str, **extra: object) -> dict[str, object]:
    return {"name": name, "status": status, "detail": detail, **extra}


def check_bilibili(room_id: int) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    try:
        room = bilibili_protocol._bilibili_room_init(room_id)  # type: ignore[attr-defined]
        ok = int(room.get("code", -1)) == 0 and bool(room.get("data", {}).get("room_id"))
        results.append(_result("bilibili.room_init", "ok" if ok else "failed", f"code={room.get('code')}"))
    except Exception as exc:  # noqa: BLE001
        results.append(_result("bilibili.room_init", "failed", f"{type(exc).__name__}: {exc}"))
        return results

    try:
        info = bilibili_protocol._bilibili_get_danmu_server_info(room_id)  # type: ignore[attr-defined]
        data = info.get("data", {})
        hosts = data.get("host_server_list", []) if isinstance(data, dict) else []
        ok = int(info.get("code", -1)) == 0 and bool(data.get("token")) and bool(hosts)
        results.append(_result("bilibili.danmu", "ok" if ok else "failed", f"code={info.get('code')}, hosts={len(hosts)}"))
    except Exception as exc:  # noqa: BLE001
        results.append(_result("bilibili.danmu", "failed", f"{type(exc).__name__}: {exc}"))
    return results


def check_douyin(live_id: str) -> list[dict[str, object]]:
    try:
        info = douyin_protocol.fetch_douyin_live_info(live_id, timeout=12)
        return [_result("douyin.live_page", "ok", "live page parsed", room_id=info.room_id, live_status=info.room_status)]
    except urllib.error.HTTPError as exc:
        status = "auth_required" if exc.code in {401, 403} else "failed"
        return [_result("douyin.live_page", status, f"HTTP {exc.code}")]
    except douyin_protocol.DouyinProtocolError as exc:
        return [_result("douyin.live_page", "needs_cookie_or_live_room", str(exc))]
    except Exception as exc:  # noqa: BLE001
        return [_result("douyin.live_page", "failed", f"{type(exc).__name__}: {exc}")]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bilibili-room", type=int, default=6, help="Public room id/short id")
    parser.add_argument("--douyin-live", default="1", help="Public live id; no Cookie is read")
    parser.add_argument("--strict-douyin", action="store_true", help="Fail when Douyin needs a real live room/Cookie")
    args = parser.parse_args()
    results = check_bilibili(args.bilibili_room) + check_douyin(args.douyin_live)
    print(json.dumps({"results": results}, ensure_ascii=False, indent=2))
    failed = any(item["status"] == "failed" for item in results)
    if args.strict_douyin:
        failed = failed or any(item["name"].startswith("douyin.") and item["status"] != "ok" for item in results)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
