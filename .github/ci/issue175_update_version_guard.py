from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SELECTOR = ROOT / "apps" / "windows" / "update_version_selector.py"
PAGE = ROOT / "apps" / "windows" / "update_page.py"
UI = ROOT / "apps" / "windows" / "update_ui.py"
RELEASE_SELECTOR = ROOT / "apps" / "windows" / "release_selector.py"


def require(path: Path, fragments: tuple[str, ...]) -> None:
    if not path.is_file():
        raise AssertionError(f"missing file: {path.relative_to(ROOT)}")
    text = path.read_text(encoding="utf-8")
    compile(text, str(path), "exec")
    for fragment in fragments:
        if fragment not in text:
            raise AssertionError(f"{path.relative_to(ROOT)} missing guard fragment: {fragment}")


def main() -> None:
    require(
        SELECTOR,
        (
            "def discover_local_backups",
            'BACKUP_DIR_NAME = "backup"',
            "def prepare_local_backup_restore",
            'skip_top={"plugins"}',
            'Path("plugins")',
            "def install_selected_full",
            "def install_selected_incremental",
        ),
    )
    require(
        PAGE,
        (
            "update_version_selector",
            'text="版本类型"',
            'text="版本"',
            "_update_channel_combo",
            "_update_version_combo",
            "ttk.Combobox(",
            "def _bind_stable_scroll_region",
            "after_idle(refresh_scroll_region)",
            "status_slot = ttk.Frame(update_frame, height=42)",
            "status_slot.grid_propagate(False)",
        ),
    )
    require(
        RELEASE_SELECTOR,
        (
            'CHANNEL_ORDER = ("发行包", "全部")',
            "RELEASE_ONLY_LIMIT = 3",
            "ALL_RELEASE_LIMIT = 10",
            "def on_channel_selected",
            'update_page.RELEASE_CHANNELS = CHANNEL_ORDER',
            'app.update_channel_var.set("发行包")',
        ),
    )
    require(
        UI,
        (
            "def _selected_version_source",
            'source == "local"',
            'hasattr(app, "_update_version_combo")',
            "update_version_selector.check_for_versions(app, silent=True)",
        ),
    )
    print("issue #175 update version selector/jitter guard: OK")


if __name__ == "__main__":
    main()
