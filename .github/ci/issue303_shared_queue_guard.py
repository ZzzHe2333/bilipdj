"""Issue #303: independent platforms, one durable queue/archive with source metadata."""
from __future__ import annotations

import csv
import importlib
import json
import logging
import sys
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@contextmanager
def temporary_data(server, root):
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
    def __init__(self):
        self.events = []

    def broadcast_json(self, _sender, event):
        self.events.append(event)


def send(qm, platform, username, message, user_id="") -> None:
    from apps.server.danmu_event import DanmuEvent
    qm.process_danmu_event(DanmuEvent(
        platform=platform, user_id=user_id or username,
        username=username, content=message,
    ))


def check_shared_archive(server):
    with tempfile.TemporaryDirectory(prefix="bilipdj-issue303-") as directory:
        root = Path(directory)
        with temporary_data(server, root):
            archive = server.QueueArchiveManager(slots=3, enabled=True)
            archive.set_active_slot(2)
            hub = Hub()
            qm = server.QueueManager(hub, archive, logging.getLogger("issue303"))
            qm.load_kaiguan(server.DEFAULT_KAIGUAN)

            # Identical screen names in distinct platforms are separate users.
            send(qm, "bilibili", "同名", "排队", "b:100")
            send(qm, "douyin", "同名", "排队", "dy:100")
            assert qm.get_queue() == ["同名", "同名"], qm.get_queue()
            sources = [item["platform"] for item in qm.get_queue_entries()]
            assert sources == ["bilibili", "douyin"], sources
            assert all("bilibili" not in name and "douyin" not in name for name in qm.get_queue())

            # A second join on one source must not duplicate the queue record.
            send(qm, "bilibili", "同名", "排队")
            assert len(qm.get_queue()) == 2
            send(qm, "douyin", "同名", "取消排队")
            assert qm.get_queue_entries()[0]["platform"] == "bilibili"
            assert len(qm.get_queue()) == 1
            send(qm, "douyin", "同名", "排队")
            assert len(qm.get_queue()) == 2

            # Independent plugin platforms follow the same queue contract.
            send(qm, "huya", "虎牙用户", "排队")
            qm.insert_item(0, "手动观众")
            assert [x["platform"] for x in qm.get_queue_entries()] == ["manual", "bilibili", "douyin", "huya"]
            qm.move_item(1, "down")
            assert [x["platform"] for x in qm.get_queue_entries()] == ["bilibili", "manual", "douyin", "huya"]
            qm.update_item_content(3, "角色A")
            assert qm.get_queue_entries()[2]["platform"] == "douyin"
            # Web 'Next' undo uses the management insert API and must restore
            # the original source instead of relabelling it as manual.
            qm.insert_item(2, "复原用户", platform="douyin")
            assert qm.get_queue_entries()[2]["platform"] == "douyin"
            qm.delete_item(3)

            path = root / "core/cd/queue_archive_slot_2.csv"
            assert path.is_file()
            with path.open(encoding="utf-8-sig", newline="") as source:
                rows = list(csv.reader(source))
            assert "来源平台" in rows[4], rows[:6]
            saved = server.read_queue_archive_entries(path)
            assert [x["platform"] for x in saved] == ["bilibili", "manual", "douyin", "huya"]
            assert saved == qm.get_queue_entries(), (saved, qm.get_queue_entries())
            assert not (root / "core/cd/queue_archive_slot_1.csv").exists()
            assert hub.events[-1]["queue"] == qm.get_queue()

            # Restoring in a fresh QueueManager must preserve the source of each row.
            restored = server.QueueManager(Hub(), archive, logging.getLogger("issue303-restore"))
            restored.restore_from_archive()
            assert restored.get_queue() == qm.get_queue()
            assert restored.get_queue_entries() == qm.get_queue_entries()

            # Legacy CSV rows without a fifth column remain readable.
            legacy = root / "core/cd/legacy.csv"
            with legacy.open("w", encoding="utf-8-sig", newline="") as sink:
                writer = csv.writer(sink)
                writer.writerow(["序号", "id", "内容", "最后操作时间"])
                writer.writerow(["1", "历史用户", "", "2026-10-01 12:00:00"])
                writer.writerow(["2", "另一用户 没平台"])
            parsed = server.read_queue_archive_entries(legacy)
            assert len(parsed) == 2
            assert all(item.get("platform") == "" for item in parsed), parsed

            # Real multi-relay manager uses one queue_manager and one slot.
            issue79 = importlib.import_module("apps.server.issue79_guard")
            class FakeRelay:
                def __init__(self, proxy):
                    self.proxy = proxy
                    self.started = False
                def start(self):
                    self.started = True
                    send(self.proxy.queue_manager, self.proxy.platform, self.proxy.platform + "_加入者", "排队")
                def stop(self):
                    self.started = False
                def join(self, timeout=None):
                    return None
            backend = SimpleNamespace(
                runtime_config={"active_platforms": ["bilibili", "douyin"], "platform": "bilibili"},
                queue_manager=qm,
                logger=logging.getLogger("issue303-relays"),
            )
            module = SimpleNamespace(_create_danmu_relay=lambda proxy: FakeRelay(proxy))
            manager = issue79.MultiPlatformRelayManager(module, backend, ("bilibili", "douyin"))
            manager.start()
            assert list(manager._relays) == ["bilibili", "douyin"]
            assert manager._proxies["bilibili"].queue_manager is manager._proxies["douyin"].queue_manager
            entries = qm.get_queue_entries()
            assert [e["platform"] for e in entries[-2:]] == ["bilibili", "douyin"]
            assert server.read_queue_archive_entries(path) == entries

            # Concurrent arrivals must not overwrite a more recent snapshot.
            barrier = threading.Barrier(2)
            errors = []
            def worker(platform):
                try:
                    barrier.wait(timeout=3)
                    for idx in range(5):
                        send(qm, platform, platform + "_parallel_" + str(idx), "排队")
                except Exception as error:
                    errors.append(error)
            threads = [threading.Thread(target=worker, args=(p,)) for p in ("bilibili", "douyin")]
            for th in threads: th.start()
            for th in threads: th.join(timeout=10)
            assert not errors and not any(th.is_alive() for th in threads), errors
            assert qm.get_queue_entries() == server.read_queue_archive_entries(path)
            assert len(qm.get_queue()) == len(entries) + 10

            qm.clear_queue()
            assert server.read_queue_archive_entries(path) == []
            assert all(len(getattr(qm, field)) == 0 for field in ("_persons", "_entry_timestamps", "_entry_platforms"))


def check_ui_routes():
    windows = (ROOT / "apps/windows/control_panel.py").read_text(encoding="utf-8")
    web = (ROOT / "apps/web/static/control_issue79.js").read_text(encoding="utf-8")
    assert 'heading("platform", text="来源平台")' in windows
    assert 'entry.get("platform", "")' in windows
    assert 'str(entry.get("platform", "") or "未知")' in windows
    assert "entry.platform" in web and "来源平台" in web
    # OBS display must not be changed into "platform:username".
    overlay = (ROOT / "apps/web/static/myjs.js").read_text(encoding="utf-8")
    assert "platform + ':'" not in overlay


def main():
    server = importlib.import_module("apps.server.server")
    check_shared_archive(server)
    check_ui_routes()
    print("issue #303 unified multi-platform queue and archive: OK")


if __name__ == "__main__":
    main()
