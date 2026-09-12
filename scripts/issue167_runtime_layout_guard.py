from __future__ import annotations

import importlib.util
import os
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]


def _load_file_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_runtime_migration() -> None:
    layout = _load_file_module("runtime_layout_probe", ROOT / "apps/server/runtime_layout.py")
    with tempfile.TemporaryDirectory() as raw:
        app = Path(raw)
        (app / "config.yaml").write_text("server:\n  port: 9988\n", encoding="utf-8")
        (app / "quanxian.yaml").write_text("super_admin:\n  - test\n", encoding="utf-8")
        (app / "kaiguan.yaml").write_text("paidui: false\n", encoding="utf-8")
        (app / "update-result.json").write_text('{"status":"installed"}', encoding="utf-8")
        core, key = layout.ensure_runtime_layout(app)
        for name in ("config.yaml", "quanxian.yaml", "kaiguan.yaml"):
            assert (core / name).is_file(), name
            assert not (app / name).exists(), f"legacy root {name} was not migrated"
        assert (key / "update-result.json").is_file()
        assert not (app / "update-result.json").exists()

    with tempfile.TemporaryDirectory() as raw:
        app = Path(raw)
        core = app / "core"
        core.mkdir()
        target = core / "config.yaml"
        source = app / "config.yaml"
        target.write_text("server:\n  port: 9816\n", encoding="utf-8")
        source.write_text("server:\n  port: 9988\n", encoding="utf-8")
        old = time.time_ns() - 2_000_000_000
        os.utime(target, ns=(old, old))
        now = time.time_ns()
        os.utime(source, ns=(now, now))
        core_dir, _key_dir = layout.ensure_runtime_layout(app)
        assert (core_dir / "config.yaml").read_text(encoding="utf-8") == "server:\n  port: 9988\n"
        backups = list((core_dir / "migration-backup").glob("config.yaml.legacy-root-*"))
        assert backups, "previous core config was not preserved during conflict migration"
        assert "9816" in backups[0].read_text(encoding="utf-8")


def check_command_console() -> None:
    module = _load_file_module("command_console_probe", ROOT / "apps/server/command_console.py")
    source = (ROOT / "apps/server/command_console.py").read_text(encoding="utf-8").lower()
    for forbidden in ("import subprocess", "from subprocess", "subprocess.", "os.system(", "eval(", "exec("):
        assert forbidden not in source, f"unsafe command execution primitive found: {forbidden}"

    captured = []

    class FakeDanmuEvent:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class FakeQueue:
        def get_queue(self):
            return ["existing"]

        def _is_command_like(self, command):
            return command == "暂停排队功能"

        def process_danmu_event(self, event):
            captured.append(event)

    active = SimpleNamespace(queue_manager=FakeQueue(), logger=None)
    result = module._execute_command(SimpleNamespace(DanmuEvent=FakeDanmuEvent), active, "暂停排队功能")
    assert result["accepted"] is True and result["recognized"] is True
    assert captured and captured[0].platform == "console"
    assert captured[0].is_anchor is True
    assert captured[0].metadata.get("source") == "local-control-console"


def check_update_estimates_and_key_layout() -> None:
    estimate = (ROOT / "apps/windows/update_estimate_ui.py").read_text(encoding="utf-8")
    assert "packed_size" in estimate and "range_bytes" in estimate
    assert 'key_dir / "update-estimate.json"' in estimate
    assert "全量更新预估" in estimate and "增量更新预估" in estimate

    web = (ROOT / "apps/web/static/issue167_control_extensions.js").read_text(encoding="utf-8")
    assert "/api/control/command" in web
    assert "/api/control/update-estimate" in web
    assert "update-full-estimate" in web and "update-incremental-estimate" in web

    manifest = (ROOT / "apps/windows/update_manifest.py").read_text(encoding="utf-8")
    assert '"key/"' in manifest
    updater = (ROOT / "apps/windows/updater_v2.py").read_text(encoding="utf-8")
    assert 'KEY_DIR_NAME = "key"' in updater
    assert 'key_dir / "update-result.json"' in updater
    runtime_guard = (ROOT / "core/runtime_guards.py").read_text(encoding="utf-8")
    assert 'root / "key" / "update-result.json"' in runtime_guard

    windows_package = (ROOT / "apps/windows/package.ps1").read_text(encoding="utf-8")
    web_package = (ROOT / "apps/web/package-portable.ps1").read_text(encoding="utf-8")
    assert "dist\\bilipdj\\key" in windows_package
    assert "$outputDir\\key" in web_package


def check_authoritative_core_paths() -> None:
    server_init = (ROOT / "apps/server/__init__.py").read_text(encoding="utf-8")
    for name in ("config.yaml", "quanxian.yaml", "kaiguan.yaml"):
        assert f'runtime_core_dir / "{name}"' in server_init
    windows_main = (ROOT / "apps/windows/main.py").read_text(encoding="utf-8")
    assert "config_dir, key_dir = ensure_runtime_layout(app_dir)" in windows_main
    assert 'control_panel.CONFIG_PATH = config_dir / "config.yaml"' in windows_main


def check_integration_wiring() -> None:
    server_main = (ROOT / "apps/server/main.py").read_text(encoding="utf-8")
    assert "install_command_console(backend)" in server_main
    assert "install_update_estimate_api(backend)" in server_main

    control_html = (ROOT / "apps/web/static/control.html").read_text(encoding="utf-8")
    assert '<script src="issue167_control_extensions.js"></script>' in control_html

    quality = (ROOT / ".github/workflows/quality.yml").read_text(encoding="utf-8")
    assert "python scripts/issue167_runtime_layout_guard.py" in quality
    assert "apps/web/dist/issue167_control_extensions.js" in quality


def main() -> None:
    check_runtime_migration()
    check_command_console()
    check_update_estimates_and_key_layout()
    check_authoritative_core_paths()
    check_integration_wiring()
    print("issue #167 command/update/runtime layout guard: OK")


if __name__ == "__main__":
    main()
