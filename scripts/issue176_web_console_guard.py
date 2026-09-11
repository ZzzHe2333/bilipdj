from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def check_control_extension() -> None:
    js = (ROOT / "apps" / "web" / "static" / "issue167_control_extensions.js").read_text(encoding="utf-8")
    assert "insertAdjacentElement('beforebegin', form)" in js, "backend command box must be above the log output"
    for label in (
        "排队总开关",
        "官服排队",
        "B服排队",
        "超级排队",
        "米服排队",
        "取消排队",
        "修改排队内容",
        "舰长插队",
        "允许房管执行管理命令",
    ):
        assert label in js, f"missing translated switch label: {label}"
    assert "PLATFORM_FIELD_GROUPS" in js and "syncPlatformFields" in js
    assert "platform-huya-url" in js and "platform-douyin-live" in js and "platform-bili-room" in js
    assert "fetch('/api/appearance'" in js and "appearance.mode" in js, "theme toggle must persist through Server appearance API"
    assert "fetch('/api/control/shutdown'" in js
    assert "addEventListener('dblclick'" in js, "shutdown must require a double-click"


def check_shutdown_backend() -> None:
    path = ROOT / "apps" / "server" / "web_control_guard.py"
    spec = importlib.util.spec_from_file_location("issue176_web_control_guard", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module._CONTROL_SHUTDOWN_PATH == "/api/control/shutdown"
    source = path.read_text(encoding="utf-8")
    assert "self._require_loopback()" in source
    assert "_same_origin_or_no_origin" in source
    assert "active_server.shutdown()" in source
    assert 'payload.get("confirm"' in source


def check_transparent_queue() -> None:
    css = (ROOT / "apps" / "web" / "static" / "moren.css").read_text(encoding="utf-8")
    layout = (ROOT / "apps" / "server" / "web_queue_layout.py").read_text(encoding="utf-8")
    for source, name in ((css, "moren.css"), (layout, "web_queue_layout.py")):
        assert "background: transparent" in source, f"{name} must keep the overlay transparent"
        assert "box-shadow: none" in source, f"{name} must remove the default panel shadow"
        assert "border: 0" in source, f"{name} must remove the default panel/card border"
        assert "linear-gradient(145deg" not in source, f"{name} still contains the old default panel gradient"
    assert ".queue-item" in css and "background: transparent" in css


def main() -> None:
    check_control_extension()
    check_shutdown_backend()
    check_transparent_queue()
    print("issue #176 Web console guard: OK")


if __name__ == "__main__":
    main()
