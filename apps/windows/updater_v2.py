"""More resilient Windows updater entry point.

This module delegates the update transaction to ``updater`` and only supplies
Windows-specific directory-move retries plus a short post-exit settle delay.
Keeping the transaction in one place prevents the GUI updater and legacy entry
point from drifting on backup, rollback, and result-state behavior.
"""
from __future__ import annotations

import ctypes
import os
import time
from pathlib import Path

try:  # Script execution from the core directory (PyInstaller/source).
    import updater as legacy
except ImportError:  # Package import used by tests.
    from core import updater as legacy

DIRECTORY_MOVE_ATTEMPTS = 60
DIRECTORY_MOVE_DELAY_SECONDS = 0.5
POST_EXIT_SETTLE_SECONDS = 1.0


def replace_path_with_retry(
    source: Path,
    destination: Path,
    *,
    attempts: int = DIRECTORY_MOVE_ATTEMPTS,
    delay: float = DIRECTORY_MOVE_DELAY_SECONDS,
) -> None:
    """Move ``source`` to ``destination`` with bounded retry on Windows locks."""

    source = Path(source)
    destination = Path(destination)
    last_error: OSError | None = None
    total = max(1, int(attempts))
    for attempt in range(total):
        try:
            source.replace(destination)
            return
        except OSError as exc:
            last_error = exc
            if attempt + 1 >= total:
                break
            time.sleep(max(0.01, float(delay)))
    raise legacy.UpdaterError(
        f"无法移动程序目录：{source} -> {destination}。"
        "请确认后端、透明窗口和杀毒软件没有占用程序目录。"
        f"原始错误：{last_error}"
    ) from last_error


def rollback(app_dir: Path, rollback_dir: Path, log_path: Path) -> None:
    """Compatibility wrapper using the Windows retry mover."""

    legacy.rollback(
        app_dir,
        rollback_dir,
        log_path,
        move_path=replace_path_with_retry,
    )


def perform_update(
    *,
    pid: int,
    app_dir: Path,
    zip_path: Path,
    main_exe_name: str,
    target_version: str,
) -> None:
    legacy.perform_update_core(
        pid=pid,
        app_dir=app_dir,
        zip_path=zip_path,
        main_exe_name=main_exe_name,
        target_version=target_version,
        move_path=replace_path_with_retry,
        post_exit_settle_seconds=POST_EXIT_SETTLE_SECONDS,
    )


def main() -> int:
    args = legacy.parse_args()
    try:
        perform_update(
            pid=args.pid,
            app_dir=args.app_dir,
            zip_path=args.zip_path,
            main_exe_name=args.main_exe,
            target_version=args.target_version,
        )
        return 0
    except Exception as exc:
        app_dir = Path(args.app_dir).resolve()
        log_path = app_dir.parent / f"{app_dir.name or 'bilipdj'}-update.log"
        legacy._write_log(log_path, f"更新失败：{exc}")
        if os.name == "nt":
            try:
                ctypes.windll.user32.MessageBoxW(
                    0,
                    f"更新失败：\n{exc}\n\n详细日志：\n{log_path}",
                    "弹幕排队姬更新器",
                    0x10,
                )
            except Exception:
                pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
