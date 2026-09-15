from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_shutdown_guard_source() -> None:
    source = read("apps/windows/shutdown_guard.py")
    assert '"taskkill"' in source
    assert '"/T"' in source
    assert '"/F"' in source
    assert "update_stability._terminate_process = terminate_process_tree" in source
    assert "bilipdj-windows-exit-watchdog" in source
    assert "os._exit(0)" in source

    runtime = read("apps/windows/desktop_runtime.py")
    assert "from .shutdown_guard import install_shutdown_guard" in runtime
    assert "install_shutdown_guard(panel_class)" in runtime
    assert runtime.index("patch_control_panel_issue187(panel_class)") < runtime.index("install_shutdown_guard(panel_class)")

    main_source = read("apps/windows/main.py")
    assert '"--gui-close-self-test"' in main_source
    assert "root.after(1200, app.on_close)" in main_source

    package_source = read("apps/windows/package.ps1")
    assert 'Arguments "--gui-close-self-test"' in package_source
    assert "Frozen Windows GUI close self-test" in package_source


def check_non_windows_termination() -> None:
    if sys.platform == "win32":
        return
    from apps.windows.shutdown_guard import terminate_process_tree

    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        terminate_process_tree(process, timeout=1.0)
        assert process.poll() is not None
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=2)


def main() -> None:
    check_shutdown_guard_source()
    check_non_windows_termination()
    print("issue #271 Windows shutdown guard: OK")


if __name__ == "__main__":
    main()
