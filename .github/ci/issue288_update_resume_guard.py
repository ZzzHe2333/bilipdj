"""Issue #288: remove redundant update copy and recover after old app close timeout."""
from __future__ import annotations

import sys
import tempfile
import types
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from apps.windows import update_page, download_acceleration, release_selector, updater_gui


class Var:
    def __init__(self, value=""):
        self.value = value
    def set(self, value):
        self.value = value
    def get(self):
        return self.value


class Button:
    def __init__(self):
        self.states = []
    def configure(self, **kwargs):
        self.states.append(kwargs)


class Root:
    def __init__(self):
        self.attributes_calls = []
        self.lifts = 0
        self.after_callbacks = []
    def attributes(self, *args):
        self.attributes_calls.append(args)
    def lift(self):
        self.lifts += 1
    def after(self, delay, fn):
        self.after_callbacks.append((delay, fn))


def check_resume():
    app = updater_gui.UpdaterWindow.__new__(updater_gui.UpdaterWindow)
    app.completed = False
    app.failed = False
    app.waiting_for_close = False
    app._worker_running = True
    app.root = Root()
    app.continue_button = Button()
    app.status_var = Var()
    app.progress_value = 0
    app.progress = {"value": 0}
    app.percent_var = Var()
    app.events = updater_gui.queue.Queue()
    app.last_log_path = "log/test.log"
    app.animator = types.SimpleNamespace(stop=lambda: None)
    app.args = types.SimpleNamespace(
        app_dir=str(ROOT), zip_path="dummy.zip", main_exe="main.exe",
        pid=123, target_version="3.0.18"
    )

    # Timeouts must not invoke the updater at all. The session stays intact.
    with patch.object(updater_gui.updater_v2.legacy, "wait_for_process_exit", return_value=False) as wait, \
         patch.object(updater_gui.updater_v2, "perform_update") as install, \
         patch.object(updater_gui.log_manager, "room_token_from_config_file", return_value="test"):
        app._worker()
    wait.assert_called_once()
    install.assert_not_called()
    assert app.events.get_nowait()[0] == "waiting"
    assert app.events.get_nowait()[0] == "wait_for_close"

    with patch.object(updater_gui.messagebox, "showwarning") as warn:
        app.events.put(("wait_for_close", None))
        app._poll_events()
    assert warn.call_count == 1
    assert app.waiting_for_close
    assert ("-topmost", True) in app.root.attributes_calls
    assert app.continue_button.states[-1]["state"] == "normal"
    assert "继续更新" in app.status_var.get()

    with patch.object(updater_gui.threading, "Thread") as thread:
        app._resume_update()
        assert thread.call_count == 1
        app._resume_update()
        assert thread.call_count == 1, "repeat clicks must not duplicate work"
    assert app._worker_running
    assert not app.waiting_for_close
    assert app.continue_button.states[-1]["state"] == "disabled"

    # A retry that finally observes process exit must call the actual updater.
    app.events = updater_gui.queue.Queue()
    with patch.object(updater_gui.updater_v2.legacy, "wait_for_process_exit", return_value=True), \
         patch.object(updater_gui.updater_v2, "perform_update") as install, \
         patch.object(updater_gui.log_manager, "room_token_from_config_file", return_value="test"), \
         patch.object(updater_gui.log_manager, "append_update_log", return_value=ROOT / "log/test.log"):
        app._worker()
    install.assert_called_once()
    kinds = [app.events.get_nowait()[0] for _ in range(app.events.qsize())]
    assert "success" in kinds
    assert "wait_for_close" not in kinds


def check_update_ui():
    page = (ROOT / "apps/windows/update_page.py").read_text(encoding="utf-8")
    polished = (ROOT / "apps/windows/windows_ui.py").read_text(encoding="utf-8")
    accelerator = (ROOT / "apps/windows/download_acceleration.py").read_text(encoding="utf-8")
    selector = (ROOT / "apps/windows/release_selector.py").read_text(encoding="utf-8")
    assert 'text=f"v{current_version}"' in page
    assert 'text=f"当前版本：v{current_version}"' not in page
    assert '"“发行包”显示最近 3 个正式 Release' not in page
    assert 'Selection details are already visible' in polished
    assert 'source_frame.grid(row=1, column=0' in accelerator
    assert '"已选择{kind}' not in selector
    assert '_offer_third_party_on_discovery_failure(app, cloud_error)' in selector
    assert '"releases/latest/download/update-manifest.json"' in selector
    assert "第三方传输的清单没有独立签名" in selector
    assert "tag != f\"v{version}\"" in selector
    assert 'filename != expected_filename' in selector


def check_prompt():
    app = types.SimpleNamespace(root=object(), update_download_source_var=Var("GitHub 官方"))
    with patch.object(release_selector.update_ui.messagebox, "askyesno", return_value=False):
        assert not release_selector._offer_third_party_on_discovery_failure(app, "offline")
    assert app.update_download_source_var.get() == "GitHub 官方"
    with patch.object(release_selector.update_ui.messagebox, "askyesno", return_value=True):
        assert release_selector._offer_third_party_on_discovery_failure(app, "offline")
    assert "GH-Proxy" in app.update_download_source_var.get()


def check_download_retry_prompt():
    app = types.SimpleNamespace(root=object(), update_download_source_var=Var("GitHub 官方"))
    assert not download_acceleration._is_official_download_network_failure(
        "更新包 SHA-256 校验失败"
    )
    assert download_acceleration._is_official_download_network_failure(
        "网络请求失败：HTTP Error 503"
    )
    with patch.object(download_acceleration.messagebox, "askyesno", return_value=False):
        assert not download_acceleration._offer_proxy_after_download_failure(
            app, "网络请求失败：HTTP Error 503"
        )
    assert app.update_download_source_var.get() == "GitHub 官方"
    with patch.object(download_acceleration.messagebox, "askyesno", return_value=True):
        assert download_acceleration._offer_proxy_after_download_failure(
            app, "网络请求失败：HTTP Error 503"
        )
    assert app._update_proxy_approved_for_retry is True
    assert "GH-Proxy" in app.update_download_source_var.get()
    # The per-use confirmation is consumed once, never persisted.
    assert download_acceleration._confirm_download_source(app, "全量更新") == "gh-proxy"
    assert app._update_proxy_approved_for_retry is False


if __name__ == "__main__":
    check_update_ui()
    check_prompt()
    check_download_retry_prompt()
    check_resume()
    print("issue #288 update layout, source fallback, and resume: OK")
