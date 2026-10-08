"""Issue #289: Windows native progress appears before heavy imports and cleans up."""
from __future__ import annotations

import ast
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from apps.windows import startup_splash


def check_arguments() -> None:
    show = startup_splash.should_show_startup_splash
    assert show([], frozen=True, platform="nt")
    assert not show([], frozen=False, platform="nt")
    assert not show([], frozen=True, platform="posix")
    for flag in ("--backend", "--overlay-host", "--gui-startup-self-test",
                 "--gui-close-self-test", "--plugin-runtime-self-test"):
        assert not show([flag], frozen=True, platform="nt"), flag


def check_lifecycle() -> None:
    splash = startup_splash.NativeStartupSplash(enabled=False)
    splash.start()
    assert splash._thread is None
    splash.update("ignored")
    splash.close()
    splash.close()  # multiple close paths never crash
    assert splash._stop.is_set()
    with patch.object(startup_splash.NativeStartupSplash, "_run") as worker:
        live = startup_splash.NativeStartupSplash(enabled=True)
        live.start()
        assert live._thread is not None
        live.update("正在加载组件…")
        assert live._status == "正在加载组件…"
        live.close()
        live.close()
        assert not worker.called or live._stop.is_set()
        live.update("should not show")
        assert live._status == "正在加载组件…"


def check_main_order() -> None:
    code = (ROOT / "apps/windows/main.py").read_text(encoding="utf-8")
    spec = (ROOT / "apps/windows/bilipdj_onedir.spec").read_text(encoding="utf-8")
    module = ast.parse(code)
    assert code.index("open_startup_splash(sys.argv[1:]") < code.index("import tkinter as tk")
    assert code.index("open_startup_splash(sys.argv[1:]") < code.index("from apps.server import server as backend")
    assert code.index("open_startup_splash(sys.argv[1:]") < code.index("from apps.windows import control_panel, update_ui")
    assert "def _initialize_runtime()" in code
    assert "_initialize_runtime()" in ast.get_source_segment(code, next(
        n for n in module.body if isinstance(n, ast.FunctionDef) and n.name == "main"
    ))
    assert "_startup_splash.update" in code
    assert "_startup_splash.close()" in code
    assert code.index("_startup_splash.close()") < code.index("_finish_root_show(root)")
    assert 'name="main"' in spec and "console=False" in spec


if __name__ == "__main__":
    check_arguments()
    check_lifecycle()
    check_main_order()
    print("issue #289 native startup splash and phase lifecycle: OK")
