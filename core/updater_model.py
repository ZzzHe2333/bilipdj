from __future__ import annotations

from pathlib import Path

WINDOW_SIZE = 520
BOARD_COLUMNS = 10
BOARD_ROWS = 10
CELL_SIZE = 30
BOARD_SIZE = BOARD_COLUMNS * CELL_SIZE
PIECE_COLUMNS = (4, 0, 8, 2, 6)
PIECE_COLORS = ("#00bcd4", "#f59e0b", "#8b5cf6", "#22c55e", "#ef4444")

_PROGRESS_MESSAGES = (
    ("准备更新", 6),
    ("解压更新包", 22),
    ("备份当前版本", 46),
    ("新版本文件替换完成", 78),
    ("正在启动主程序", 90),
    ("启动成功", 100),
    ("开始回滚", 82),
    ("旧版本已恢复", 96),
)


def progress_for_message(message: str, current: int) -> int:
    text = str(message or "")
    for marker, value in _PROGRESS_MESSAGES:
        if marker in text:
            return max(current, value)
    return min(92, max(current, current + 1))


def select_update_log_root(app_dir: Path) -> Path:
    app_dir = Path(app_dir)
    backup = app_dir.parent / f".{app_dir.name or 'bilipdj'}.update-backup"
    if app_dir.is_dir():
        return app_dir
    if backup.is_dir():
        return backup
    return app_dir


__all__ = [
    "BOARD_COLUMNS",
    "BOARD_ROWS",
    "BOARD_SIZE",
    "CELL_SIZE",
    "PIECE_COLORS",
    "PIECE_COLUMNS",
    "WINDOW_SIZE",
    "progress_for_message",
    "select_update_log_root",
]
