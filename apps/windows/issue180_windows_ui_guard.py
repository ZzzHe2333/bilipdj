from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent
UI = ROOT / "windows_ui.py"
RUNTIME = ROOT / "desktop_runtime.py"


def require(path: Path, fragments: tuple[str, ...]) -> None:
    text = path.read_text(encoding="utf-8")
    compile(text, str(path), "exec")
    for fragment in fragments:
        if fragment not in text:
            raise AssertionError(f"{path.name} missing guard fragment: {fragment}")


def main() -> None:
    require(
        UI,
        (
            "def _install_stable_update_layout",
            "canvas.after_idle(flush_layout)",
            "本地备份｜v{candidate.version}",
            "来源：本地备份",
            "update_progress_text_var",
            "def _build_plugin_manager_tab",
            '"/api/plugins/manage"',
            '"/api/plugins/install"',
            '"/api/plugins/enable"',
            '"/api/plugins/disable"',
            '"/api/plugins/verify"',
            '"/api/plugins/uninstall"',
            "第三方 Python 插件属于同进程全信任代码",
            "def _build_permissions_page",
            "权限与身份",
            "def _build_performance_page",
            "运行性能",
        ),
    )
    require(
        RUNTIME,
        (
            "from .windows_ui import patch_control_panel_issue180",
            "patch_control_panel_issue180(panel_class)",
            "patch_control_panel_unified_theme(panel_class)",
        ),
    )
    print("Windows UI production module guard: OK")


if __name__ == "__main__":
    main()
