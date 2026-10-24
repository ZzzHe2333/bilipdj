"""Regression guard for per-gift 排队/插队 rules (Douyin by default).

Covers:
  * rule normalization (platform scoping, dedupe, invalid entries);
  * insert_count > 0 grants 插队 slots, insert_count == 0 grants 排队 slots;
  * 排队 consumes a queue slot (and works past gift-only / queue-length gates);
  * 插队 consumes insert slots and jumps to the configured rank;
  * the legacy Bilibili credit path still works;
  * gift_queue_rules.json persistence + /api/gifts/state fields.
"""
from __future__ import annotations

import importlib
import json
import logging
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

server = importlib.import_module("apps.server.server")
gift_compat = importlib.import_module("apps.server.gift_compatibility")
queue_guard = importlib.import_module("apps.server.queue_logic_guard")


@contextmanager
def temporary_data(root: Path):
    paths = {
        "CONFIG_PATH": root / "core/config.yaml",
        "PD_DIR": root / "core/cd",
        "LOG_DIR": root / "log",
        "QUEUE_STATE_PATH": root / "core/cd/queue_archive_state.json",
        "UI_DIR": root / "ui",
        "LIVE_STYLE_CSS_PATH": root / "ui/moren.css",
        "STYLE_PATH": root / "style.json",
        "QUANXIAN_PATH": root / "core/quanxian.yaml",
        "KAIGUAN_PATH": root / "core/kaiguan.yaml",
        "_CONFIG_LOCK_PATH": root / "core/.config.lock",
    }
    original = {name: getattr(server, name) for name in paths}
    try:
        for key, value in paths.items():
            setattr(server, key, value)
        yield
    finally:
        for key, value in original.items():
            setattr(server, key, value)


class Hub:
    def __init__(self) -> None:
        self.events = []

    def broadcast_json(self, _sender, event):
        self.events.append(event)


def build(root: Path, myjs: dict | None = None, rules: dict | None = None):
    archive = server.QueueArchiveManager(slots=3, enabled=False)
    qm = server.QueueManager(Hub(), archive, logging.getLogger("gift-queue-guard"))
    qm.load_kaiguan(server.DEFAULT_KAIGUAN)
    cfg = {
        "paidui_list_length_max": 100,
        "daily_queue_limit": 0,
        "gift_queue_enabled": False,
        "gift_queue_names": [],
        "gift_queue_only": False,
        "gift_queue_insert_rank": 1,
    }
    cfg.update(myjs or {})
    qm.load_config(cfg)
    if rules is not None:
        qm.save_gift_queue_rules(rules.get("enabled", False), rules.get("rules", []))
    return qm


def gift(qm, platform: str, uid: int, name: str, gift_id: str, gift_name: str) -> dict:
    return qm.process_gift_event({
        "platform": platform,
        "event": "gift",
        "uid": uid,
        "uname": name,
        "gift": {"id": gift_id, "name": gift_name, "count": 1,
                 "value": 1, "coin_type": "diamond"},
        "raw": {"source": "guard"},
    })


def check_normalizer() -> None:
    rules = server._normalize_gift_queue_rules([
        {"gift_name": "粉丝团灯牌", "queue_count": 2},
        {"platform": "douyin", "gift_id": "2001", "insert_count": 3},
        {"platform": "evil", "gift_id": "1", "queue_count": 1},
        {"gift_id": "", "gift_name": "", "queue_count": 1},
        {"platform": "douyin", "gift_id": "2001", "queue_count": 5},
    ])
    assert len(rules) == 2, rules
    assert rules[0]["platform"] == "douyin" and rules[0]["queue_count"] == 2
    assert rules[0]["insert_count"] == 0
    assert rules[1]["gift_id"] == "2001" and rules[1]["insert_count"] == 3
    assert server.DEFAULT_DOUYIN_GIFT_QUEUE_RULES[0]["gift_name"] == "粉丝团灯牌"


def check_queue_mode(root: Path) -> None:
    qm = build(root, rules={"enabled": True, "rules": [
        {"platform": "douyin", "gift_id": "685", "gift_name": "粉丝团灯牌",
         "queue_count": 1, "insert_count": 0},
    ]})
    event = gift(qm, "douyin", 1001, "甲", "685", "粉丝团灯牌")
    credit = event.get("gift_queue_credit") or {}
    assert credit.get("mode") == "queue", credit
    assert credit.get("granted") == 1
    assert qm._gift_queue_join_credits.get(1001) == 1
    ok, note = qm._process(1001, "甲", "排队", False, False, False, 0)
    assert ok and qm.get_queue() == ["甲"], (ok, note, qm.get_queue())
    assert not qm._gift_queue_join_credits.get(1001)


def check_insert_mode(root: Path) -> None:
    qm = build(root, rules={"enabled": True, "rules": [
        {"platform": "douyin", "gift_id": "2001", "gift_name": "玫瑰",
         "queue_count": 0, "insert_count": 2},
    ]})
    event = gift(qm, "douyin", 2002, "乙", "2001", "玫瑰")
    credit = event.get("gift_queue_credit") or {}
    assert credit.get("mode") == "insert" and credit.get("granted") == 2, credit
    qm._process(3003, "丙", "排队", False, False, False, 0)
    ok, _note = qm._process(2002, "乙", "插队", False, False, False, 0)
    assert ok and qm.get_queue()[0] == "乙", qm.get_queue()
    assert qm._gift_queue_credits.get(2002) == 1


def check_gates(root: Path) -> None:
    qm = build(root, {"gift_queue_only": True}, rules={"enabled": True, "rules": [
        {"platform": "douyin", "gift_id": "685", "gift_name": "粉丝团灯牌",
         "queue_count": 1, "insert_count": 0},
    ]})
    ok, note = qm._process(5006, "己", "排队", False, False, False, 0)
    assert not ok and "礼物资格" in (note or ""), (ok, note)
    gift(qm, "douyin", 5006, "己", "685", "粉丝团灯牌")
    ok, _note = qm._process(5006, "己", "排队", False, False, False, 0)
    assert ok and "己" in qm.get_queue()

    qm = build(root, {"paidui_list_length_max": 1}, rules={"enabled": True, "rules": [
        {"platform": "douyin", "gift_id": "685", "gift_name": "粉丝团灯牌",
         "queue_count": 1, "insert_count": 0},
    ]})
    qm._process(7008, "辛", "排队", False, False, False, 0)
    ok, _note = qm._process(7009, "壬", "排队", False, False, False, 0)
    assert not ok
    gift(qm, "douyin", 7009, "壬", "685", "粉丝团灯牌")
    ok, _note = qm._process(7009, "壬", "排队", False, False, False, 0)
    assert ok and qm.get_queue() == ["辛", "壬"], qm.get_queue()


def check_scoping_and_legacy(root: Path) -> None:
    qm = build(root, rules={"enabled": True, "rules": [
        {"platform": "douyin", "gift_id": "685", "gift_name": "粉丝团灯牌",
         "queue_count": 1, "insert_count": 0},
    ]})
    assert not gift(qm, "bilibili", 6007, "庚", "685", "粉丝团灯牌").get("gift_queue_credit")
    qm = build(root, {"gift_queue_enabled": True, "gift_queue_names": ["辣条"],
                      "gift_queue_slots_per_gift": 1},
               rules={"enabled": False, "rules": []})
    event = gift(qm, "bilibili", 8009, "癸", "1", "辣条")
    assert event.get("queue_credit_granted")
    assert qm._gift_queue_credits.get(8009) == 1


def check_persistence_and_state(root: Path) -> None:
    qm = build(root, rules={"enabled": True, "rules": [
        {"platform": "douyin", "gift_id": "685", "gift_name": "粉丝团灯牌",
         "queue_count": 1, "insert_count": 0},
        {"platform": "douyin", "gift_id": "2001", "gift_name": "玫瑰",
         "queue_count": 0, "insert_count": 2},
    ]})
    path = server._gift_queue_rules_path()
    assert path.exists(), path
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["enabled"] is True and len(payload["rules"]) == 2, payload
    state = qm.get_gift_state()
    for key in ("rules_enabled", "rules", "default_rules",
                "pending_insert_credits", "pending_queue_credits"):
        assert key in state, key
    assert state["rules_enabled"] is True
    assert len(state["rules"]) == 2
    # A fresh manager on the same data dir must load the persisted rules.
    reloaded = build(root)
    assert reloaded.get_gift_state()["rules_enabled"] is True
    assert len(reloaded.get_gift_state()["rules"]) == 2
    cleared = reloaded.save_gift_queue_rules(True, [])
    assert cleared["rules"] == []
    after_restart = build(root)
    assert after_restart.get_gift_state()["rules"] == []


def main() -> None:
    assert gift_compat.install_gift_compatibility(server)
    assert queue_guard.patch_queue_manager(server.QueueManager)
    check_normalizer()
    with tempfile.TemporaryDirectory(prefix="bilipdj-gift-queue-") as directory:
        root = Path(directory)
        with temporary_data(root):
            check_queue_mode(root)
            check_insert_mode(root)
            check_gates(root)
            check_scoping_and_legacy(root)
            check_persistence_and_state(root)
    print("douyin gift queue/insert rules guard: OK")


if __name__ == "__main__":
    main()
