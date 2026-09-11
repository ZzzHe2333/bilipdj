from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent
UI = ROOT / "issue180_windows_ui.py"
BOOTSTRAP = ROOT / "control_panel_bootstrap.py"


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
        BOOTSTRAP,
        (
            "from .issue180_windows_ui import patch_control_panel_issue180",
            "patch_control_panel_issue180(cls)",
            "patch_control_panel_unified_theme(cls)",
        ),
    )
    print("issue #180 Windows UI guard: OK")


if __name__ == "__main__":
    main()
