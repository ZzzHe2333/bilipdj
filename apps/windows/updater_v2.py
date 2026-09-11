"""More resilient Windows updater entry point.

This module delegates full updates to ``updater`` and incremental patch
transactions to ``incremental_apply``.  It only supplies Windows-specific
settle/retry behavior so the packaged GUI updater remains the single updater
entry point for both modes.
"""
from __future__ import annotations

import argparse
import ctypes
import os
import time
from pathlib import Path

try:  # Script execution from the core directory (PyInstaller/source).
    import updater as legacy
    import incremental_apply
except ImportError:  # Package import used by tests.
    from core import updater as legacy
    from apps.windows import incremental_apply

DIRECTORY_MOVE_ATTEMPTS = 60
DIRECTORY_MOVE_DELAY_SECONDS = 0.5
POST_EXIT_SETTLE_SECONDS = 1.0
INCREMENTAL_PLAN_NAME = "incremental-plan.json"


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
    zip_path = Path(zip_path)
    plan_path = zip_path.parent / INCREMENTAL_PLAN_NAME
    if plan_path.is_file():
        incremental_apply.perform_incremental_update_core(
            pid=pid,
            app_dir=app_dir,
            zip_path=zip_path,
            plan_path=plan_path,
            main_exe_name=main_exe_name,
            target_version=target_version,
            post_exit_settle_seconds=POST_EXIT_SETTLE_SECONDS,
        )
        return

    legacy.perform_update_core(
        pid=pid,
        app_dir=app_dir,
        zip_path=zip_path,
        main_exe_name=main_exe_name,
        target_version=target_version,
        move_path=replace_path_with_retry,
        post_exit_settle_seconds=POST_EXIT_SETTLE_SECONDS,
    )


def parse_args() -> argparse.Namespace:
    """Accept the legacy CLI plus explicit incremental-mode metadata.

    The GUI updater still calls ``legacy.parse_args``.  We replace that parser
    below so old full-update commands remain valid while new clients may pass
    ``--mode incremental`` and ``--plan``.  The actual routing additionally
    requires the prepared plan file next to the downloaded ZIP.
    """

    parser = argparse.ArgumentParser(description="弹幕排队姬独立更新器")
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--app-dir", type=Path, required=True)
    parser.add_argument("--zip", dest="zip_path", type=Path, required=True)
    parser.add_argument("--main-exe", default="main.exe")
    parser.add_argument("--target-version", required=True)
    parser.add_argument("--mode", choices=("full", "incremental"), default="full")
    parser.add_argument("--plan", type=Path)
    args = parser.parse_args()
    if args.mode == "incremental":
        expected = Path(args.zip_path).resolve().parent / INCREMENTAL_PLAN_NAME
        if args.plan is None or Path(args.plan).resolve() != expected:
            parser.error("incremental mode requires the prepared incremental-plan.json next to --zip")
    return args


# updater_gui historically calls updater_v2.legacy.parse_args().  Keep that
# public behavior compatible while extending the packaged updater CLI.
legacy.parse_args = parse_args


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
