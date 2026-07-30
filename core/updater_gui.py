from __future__ import annotations

import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any

try:
    from core import log_manager, updater_v2
except ImportError:
    import log_manager  # type: ignore[no-redef]
    import updater_v2  # type: ignore[no-redef]

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


class TetrisAnimator:
    def __init__(self, canvas: tk.Canvas) -> None:
        self.canvas = canvas
        self.origin_x = (WINDOW_SIZE - BOARD_SIZE) // 2
        self.origin_y = 22
        self.settled: list[tuple[int, int, str]] = []
        self.piece_index = 0
        self.falling_y = -2.0
        self.flash_frames = 0
        self.running = True
        self._job: str | None = None
        self.tick()

    def stop(self) -> None:
        self.running = False
        if self._job is not None:
            try:
                self.canvas.after_cancel(self._job)
            except tk.TclError:
                pass
            self._job = None

    def _cell_box(self, column: int, row: float) -> tuple[float, float, float, float]:
        left = self.origin_x + column * CELL_SIZE
        top = self.origin_y + row * CELL_SIZE
        return left + 2, top + 2, left + CELL_SIZE - 2, top + CELL_SIZE - 2

    def _draw_cell(self, column: int, row: float, color: str) -> None:
        self.canvas.create_rectangle(
            *self._cell_box(column, row),
            fill=color,
            outline="#e8f4ff",
            width=1,
        )
        left, top, right, bottom = self._cell_box(column, row)
        self.canvas.create_line(left + 4, top + 4, right - 4, top + 4, fill="#ffffff", width=1)
        self.canvas.create_line(left + 4, top + 4, left + 4, bottom - 4, fill="#ffffff", width=1)

    def _draw(self) -> None:
        self.canvas.delete("all")
        self.canvas.create_rectangle(
            self.origin_x - 2,
            self.origin_y - 2,
            self.origin_x + BOARD_SIZE + 2,
            self.origin_y + BOARD_SIZE + 2,
            outline="#334155",
            width=2,
        )
        for column in range(BOARD_COLUMNS + 1):
            x = self.origin_x + column * CELL_SIZE
            self.canvas.create_line(x, self.origin_y, x, self.origin_y + BOARD_SIZE, fill="#172033")
        for row in range(BOARD_ROWS + 1):
            y = self.origin_y + row * CELL_SIZE
            self.canvas.create_line(self.origin_x, y, self.origin_x + BOARD_SIZE, y, fill="#172033")

        flash = self.flash_frames > 0 and self.flash_frames % 2 == 0
        for column, row, color in self.settled:
            self._draw_cell(column, row, "#ffffff" if flash else color)

        if self.flash_frames <= 0:
            column = PIECE_COLUMNS[self.piece_index]
            color = PIECE_COLORS[self.piece_index]
            for dx, dy in ((0, 0), (1, 0), (0, 1), (1, 1)):
                row = self.falling_y + dy
                if row > -1.2:
                    self._draw_cell(column + dx, row, color)

    def tick(self) -> None:
        if not self.running:
            return
        if self.flash_frames > 0:
            self.flash_frames -= 1
            if self.flash_frames == 0:
                self.settled.clear()
                self.piece_index = 0
                self.falling_y = -2.0
        else:
            self.falling_y += 0.42
            if self.falling_y >= BOARD_ROWS - 2:
                self.falling_y = float(BOARD_ROWS - 2)
                column = PIECE_COLUMNS[self.piece_index]
                color = PIECE_COLORS[self.piece_index]
                self.settled.extend(
                    (column + dx, BOARD_ROWS - 2 + dy, color)
                    for dx, dy in ((0, 0), (1, 0), (0, 1), (1, 1))
                )
                if self.piece_index >= len(PIECE_COLUMNS) - 1:
                    self.flash_frames = 8
                else:
                    self.piece_index += 1
                    self.falling_y = -2.0
        self._draw()
        self._job = self.canvas.after(42, self.tick)


class UpdaterWindow:
    def __init__(self, args: Any) -> None:
        self.args = args
        self.events: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.progress_value = 0
        self.completed = False
        self.failed = False

        self.root = tk.Tk()
        self.root.title("弹幕排队姬更新器")
        self.root.resizable(False, False)
        self.root.geometry(self._center_geometry())
        self.root.configure(bg="#080d18")
        self.root.protocol("WM_DELETE_WINDOW", self._request_close)

        container = ttk.Frame(self.root, padding=12)
        container.pack(fill="both", expand=True)
        ttk.Label(container, text="正在更新弹幕排队姬", font=("Microsoft YaHei UI", 14, "bold")).pack(pady=(2, 5))
        self.canvas = tk.Canvas(
            container,
            width=WINDOW_SIZE - 24,
            height=350,
            bg="#080d18",
            highlightthickness=0,
            bd=0,
        )
        self.canvas.pack(fill="x")
        self.animator = TetrisAnimator(self.canvas)

        self.status_var = tk.StringVar(value=f"准备更新到 v{args.target_version}")
        ttk.Label(container, textvariable=self.status_var, anchor="w", wraplength=480).pack(fill="x", pady=(7, 5))
        self.progress = ttk.Progressbar(container, mode="determinate", maximum=100, length=470)
        self.progress.pack(fill="x", pady=(0, 5))
        self.percent_var = tk.StringVar(value="0%")
        ttk.Label(container, textvariable=self.percent_var, anchor="e").pack(fill="x")

        threading.Thread(target=self._worker, name="bilipdj-updater-worker", daemon=True).start()
        self.root.after(50, self._poll_events)

    def _center_geometry(self) -> str:
        self.root.update_idletasks()
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        x = max(0, (screen_width - WINDOW_SIZE) // 2)
        y = max(0, (screen_height - WINDOW_SIZE) // 2)
        return f"{WINDOW_SIZE}x{WINDOW_SIZE}+{x}+{y}"

    def _request_close(self) -> None:
        if not self.completed:
            return
        self.animator.stop()
        self.root.destroy()

    def _emit(self, kind: str, payload: Any) -> None:
        self.events.put((kind, payload))

    def _worker(self) -> None:
        app_dir = Path(self.args.app_dir).resolve()
        room_token = log_manager.room_token_from_config_file(app_dir)
        legacy = updater_v2.legacy
        original_write_log = legacy._write_log

        def categorized_write_log(_legacy_path: Path, message: str) -> None:
            target_root = select_update_log_root(app_dir)
            try:
                log_path = log_manager.append_update_log(target_root, str(message), room_token=room_token)
            except Exception:
                log_path = target_root / "log" / f"update_unknown_{room_token}.log"
            self._emit("status", (str(message), str(log_path)))

        legacy._write_log = categorized_write_log
        try:
            updater_v2.perform_update(
                pid=self.args.pid,
                app_dir=app_dir,
                zip_path=Path(self.args.zip_path),
                main_exe_name=self.args.main_exe,
                target_version=self.args.target_version,
            )
        except Exception as exc:
            self._emit("error", str(exc))
        else:
            self._emit("success", self.args.target_version)
        finally:
            legacy._write_log = original_write_log

    def _set_progress(self, value: int) -> None:
        self.progress_value = max(self.progress_value, min(100, int(value)))
        self.progress["value"] = self.progress_value
        self.percent_var.set(f"{self.progress_value}%")

    def _poll_events(self) -> None:
        while True:
            try:
                kind, payload = self.events.get_nowait()
            except queue.Empty:
                break
            if kind == "status":
                message, log_path = payload
                self.status_var.set(str(message))
                self._set_progress(progress_for_message(str(message), self.progress_value))
                self.last_log_path = str(log_path)
            elif kind == "success":
                self.completed = True
                self.status_var.set(f"v{payload} 更新完成，正在启动新版本……")
                self._set_progress(100)
                self.root.after(900, self._request_close)
            elif kind == "error":
                self.completed = True
                self.failed = True
                self.status_var.set(f"更新失败：{payload}")
                self._set_progress(max(1, self.progress_value))
                messagebox.showerror(
                    "更新失败",
                    f"更新失败：\n{payload}\n\n日志：\n{getattr(self, 'last_log_path', 'log/update_*.log')}",
                    parent=self.root,
                )
        if not self.completed:
            if self.progress_value < 92:
                self._set_progress(self.progress_value + 1)
            self.root.after(140, self._poll_events)

    def run(self) -> int:
        self.root.mainloop()
        return 1 if self.failed else 0


def main() -> int:
    args = updater_v2.legacy.parse_args()
    return UpdaterWindow(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
