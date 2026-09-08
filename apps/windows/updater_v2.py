"""More resilient Windows updater entry point.

This module reuses the validated archive and rollback helpers from ``updater``
but retries directory moves that Windows may temporarily reject while child
processes, antivirus software, or filesystem filters release their handles.
"""
from __future__ import annotations

import ctypes
import os
import subprocess
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


def rollback(app_dir: Path, backup_dir: Path, log_path: Path) -> None:
    legacy._write_log(log_path, "开始回滚旧版本")
    if app_dir.exists():
        legacy.remove_path_with_retry(app_dir)
    if backup_dir.exists():
        replace_path_with_retry(backup_dir, app_dir, attempts=40, delay=0.5)
    legacy._write_log(log_path, "旧版本已恢复")


def perform_update(
    *,
    pid: int,
    app_dir: Path,
    zip_path: Path,
    main_exe_name: str,
    target_version: str,
) -> None:
    executable_name = legacy.validate_executable_name(main_exe_name)
    app_dir = Path(app_dir).resolve()
    zip_path = Path(zip_path).resolve()
    if app_dir == app_dir.parent:
        raise legacy.UpdaterError("拒绝将文件系统根目录作为程序目录")

    parent = app_dir.parent
    safe_name = app_dir.name or "bilipdj"
    staging_dir = parent / f".{safe_name}.update-staging"
    backup_dir = parent / f".{safe_name}.update-backup"
    cleanup_dir = zip_path.parent
    log_path = parent / f"{safe_name}-update.log"

    legacy._write_log(log_path, f"准备更新到 v{target_version}")
    if not app_dir.is_dir():
        raise legacy.UpdaterError(f"程序目录不存在：{app_dir}")
    if not zip_path.is_file():
        raise legacy.UpdaterError(f"更新包不存在：{zip_path}")
    if not legacy.wait_for_process_exit(pid):
        raise legacy.UpdaterError("等待主程序退出超时，请完全关闭程序后重试")

    # Wait briefly after the main process handle is signalled.  Windows may
    # still be releasing DLL mappings and child-process handles at this point.
    time.sleep(POST_EXIT_SETTLE_SECONDS)

    backup_created = False
    preflight_completed = False
    try:
        legacy.remove_path_with_retry(staging_dir)
        staging_dir.mkdir(parents=True, exist_ok=True)
        legacy._write_log(log_path, f"解压更新包：{zip_path}")
        legacy.safe_extract(zip_path, staging_dir)
        legacy.validate_staging(staging_dir, executable_name)
        preflight_completed = True

        legacy.remove_path_with_retry(backup_dir)
        legacy._write_log(log_path, f"备份当前版本到：{backup_dir}")
        replace_path_with_retry(app_dir, backup_dir)
        backup_created = True

        replace_path_with_retry(staging_dir, app_dir, attempts=40, delay=0.25)
        legacy.copy_preserved_data(backup_dir, app_dir)
        legacy._write_update_result(
            app_dir,
            status="installed",
            target_version=target_version,
            backup_dir=backup_dir,
            cleanup_dir=cleanup_dir,
        )
        legacy._write_log(log_path, "新版本文件替换完成，正在启动主程序")
        process = legacy.launch_main(app_dir, executable_name)
        deadline = time.monotonic() + legacy.STARTUP_GRACE_SECONDS
        while time.monotonic() < deadline:
            return_code = process.poll()
            if return_code is not None:
                raise legacy.UpdaterError(
                    f"新版主程序启动后提前退出，退出码 {return_code}"
                )
            time.sleep(0.25)
        legacy._write_log(
            log_path,
            f"v{target_version} 启动成功，保留上一版本备份：{backup_dir}",
        )
    except Exception as exc:
        if backup_created:
            try:
                rollback(app_dir, backup_dir, log_path)
            except Exception as rollback_error:
                legacy._write_log(log_path, f"恢复旧版本失败：{rollback_error}")
        elif preflight_completed:
            legacy._write_log(log_path, "更新替换失败，原程序目录未被替换")
        else:
            legacy._write_log(log_path, "更新预检失败，原程序目录未被替换")

        legacy._write_update_result(
            app_dir,
            status="rolled_back" if backup_created else (
                "replace_failed" if preflight_completed else "preflight_failed"
            ),
            target_version=target_version,
            backup_dir=backup_dir,
            cleanup_dir=cleanup_dir,
            error=str(exc),
        )
        if app_dir.is_dir() and (app_dir / executable_name).is_file():
            try:
                legacy.launch_main(app_dir, executable_name)
                legacy._write_log(log_path, "已重新启动旧版本")
            except Exception as restart_error:
                legacy._write_log(log_path, f"重新启动旧版本失败：{restart_error}")
        else:
            legacy._write_log(log_path, "没有可重新启动的主程序")
        raise
    finally:
        try:
            legacy.remove_path_with_retry(staging_dir)
        except OSError as cleanup_error:
            legacy._write_log(log_path, f"清理更新暂存目录失败：{cleanup_error}")


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
