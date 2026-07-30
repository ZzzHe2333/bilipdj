from __future__ import annotations

import argparse
import ctypes
import json
import os
import shutil
import subprocess
import time
import zipfile
from datetime import datetime
from pathlib import Path

PRESERVE_PATHS = (
    Path("config.yaml"),
    Path("quanxian.yaml"),
    Path("kaiguan.yaml"),
    Path("style.json"),
    Path("core/config.yaml"),
    Path("core/quanxian.yaml"),
    Path("core/kaiguan.yaml"),
    Path("core/style.json"),
    Path("core/cd"),
    Path("log"),
)
WAIT_TIMEOUT_SECONDS = 45.0
STARTUP_GRACE_SECONDS = 8.0
MAX_ARCHIVE_MEMBERS = 20_000
MAX_ARCHIVE_UNCOMPRESSED_BYTES = 2 * 1024 * 1024 * 1024


class UpdaterError(RuntimeError):
    pass


def _timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _write_log(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{_timestamp()}] {message}\n")


def _write_update_result(
    app_dir: Path,
    *,
    status: str,
    target_version: str,
    backup_dir: Path,
    cleanup_dir: Path,
    error: str = "",
) -> None:
    if not app_dir.is_dir():
        return
    result = {
        "status": str(status),
        "version": str(target_version),
        "installed_at": _timestamp(),
        "backup_dir": str(backup_dir),
        "cleanup_dir": str(cleanup_dir),
    }
    if error:
        result["error"] = str(error)
    (app_dir / "update-result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def validate_executable_name(name: str) -> str:
    """Return a safe executable basename used only inside the application dir."""

    text = str(name or "").strip()
    if (
        not text
        or text in {".", ".."}
        or "\x00" in text
        or "/" in text
        or "\\" in text
        or Path(text).is_absolute()
        or Path(text).name != text
    ):
        raise UpdaterError(f"主程序文件名不安全：{name!r}")
    return text


def wait_for_process_exit(pid: int, timeout: float = WAIT_TIMEOUT_SECONDS) -> bool:
    if pid <= 0:
        return True
    if os.name == "nt":
        synchronize = 0x00100000
        handle = ctypes.windll.kernel32.OpenProcess(synchronize, False, pid)
        if not handle:
            return True
        try:
            wait_object_0 = 0x00000000
            result = ctypes.windll.kernel32.WaitForSingleObject(
                handle,
                int(timeout * 1000),
            )
            return result == wait_object_0
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            pass
        time.sleep(0.2)
    return False


def remove_path_with_retry(
    path: Path,
    *,
    attempts: int = 20,
    delay: float = 0.25,
) -> None:
    for attempt in range(attempts):
        try:
            if path.is_symlink() or path.is_file():
                path.unlink(missing_ok=True)
            elif path.exists():
                shutil.rmtree(path)
            return
        except OSError:
            if attempt == attempts - 1:
                raise
            time.sleep(delay)


def copy_preserved_data(backup_dir: Path, app_dir: Path) -> None:
    for relative in PRESERVE_PATHS:
        source = backup_dir / relative
        destination = app_dir / relative
        if source.is_dir():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(source, destination, dirs_exist_ok=True)
        elif source.is_file():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)


def safe_extract(zip_path: Path, destination: Path) -> None:
    """Extract an update archive after validating paths and expansion limits."""

    destination_root = destination.resolve()
    seen_targets: set[str] = set()
    total_uncompressed = 0

    with zipfile.ZipFile(zip_path, "r") as archive:
        members = archive.infolist()
        if len(members) > MAX_ARCHIVE_MEMBERS:
            raise UpdaterError(
                f"更新包文件数量过多：{len(members)} > {MAX_ARCHIVE_MEMBERS}"
            )

        for member in members:
            filename = str(member.filename or "")
            if not filename or "\x00" in filename:
                raise UpdaterError("更新包包含空文件名或非法字符")

            target = (destination / filename).resolve()
            try:
                target.relative_to(destination_root)
            except ValueError as exc:
                raise UpdaterError(
                    f"更新包包含不安全路径：{member.filename}"
                ) from exc

            target_key = str(target).casefold()
            if target_key in seen_targets:
                raise UpdaterError(
                    f"更新包包含重复路径：{member.filename}"
                )
            seen_targets.add(target_key)

            if not member.is_dir():
                total_uncompressed += max(0, int(member.file_size))
                if total_uncompressed > MAX_ARCHIVE_UNCOMPRESSED_BYTES:
                    raise UpdaterError("更新包解压后体积超过安全上限")

        archive.extractall(destination)


def validate_staging(staging_dir: Path, main_exe_name: str) -> None:
    executable_name = validate_executable_name(main_exe_name)
    if not (staging_dir / executable_name).is_file():
        raise UpdaterError(f"更新包中缺少主程序：{executable_name}")
    if not (staging_dir / "updater.exe").is_file():
        raise UpdaterError("更新包中缺少 updater.exe")


def launch_main(app_dir: Path, main_exe_name: str) -> subprocess.Popen[bytes]:
    executable_name = validate_executable_name(main_exe_name)
    main_path = app_dir / executable_name
    creationflags = (
        getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        if os.name == "nt"
        else 0
    )
    return subprocess.Popen(
        [str(main_path)],
        cwd=str(app_dir),
        close_fds=True,
        creationflags=creationflags,
    )


def rollback(app_dir: Path, backup_dir: Path, log_path: Path) -> None:
    _write_log(log_path, "开始回滚旧版本")
    if app_dir.exists():
        remove_path_with_retry(app_dir)
    if backup_dir.exists():
        backup_dir.replace(app_dir)
    _write_log(log_path, "旧版本已恢复")


def perform_update(
    *,
    pid: int,
    app_dir: Path,
    zip_path: Path,
    main_exe_name: str,
    target_version: str,
) -> None:
    executable_name = validate_executable_name(main_exe_name)
    app_dir = app_dir.resolve()
    zip_path = zip_path.resolve()
    if app_dir == app_dir.parent:
        raise UpdaterError("拒绝将文件系统根目录作为程序目录")

    parent = app_dir.parent
    safe_name = app_dir.name or "bilipdj"
    staging_dir = parent / f".{safe_name}.update-staging"
    backup_dir = parent / f".{safe_name}.update-backup"
    cleanup_dir = zip_path.parent
    log_path = parent / f"{safe_name}-update.log"

    _write_log(log_path, f"准备更新到 v{target_version}")
    if not app_dir.is_dir():
        raise UpdaterError(f"程序目录不存在：{app_dir}")
    if not zip_path.is_file():
        raise UpdaterError(f"更新包不存在：{zip_path}")
    if not wait_for_process_exit(pid):
        raise UpdaterError("等待主程序退出超时，请完全关闭程序后重试")

    backup_created = False
    try:
        remove_path_with_retry(staging_dir)
        staging_dir.mkdir(parents=True, exist_ok=True)
        _write_log(log_path, f"解压更新包：{zip_path}")
        safe_extract(zip_path, staging_dir)
        validate_staging(staging_dir, executable_name)

        remove_path_with_retry(backup_dir)
        _write_log(log_path, f"备份当前版本到：{backup_dir}")
        app_dir.replace(backup_dir)
        backup_created = True

        staging_dir.replace(app_dir)
        copy_preserved_data(backup_dir, app_dir)
        _write_update_result(
            app_dir,
            status="installed",
            target_version=target_version,
            backup_dir=backup_dir,
            cleanup_dir=cleanup_dir,
        )
        _write_log(log_path, "新版本文件替换完成，正在启动主程序")
        process = launch_main(app_dir, executable_name)
        deadline = time.monotonic() + STARTUP_GRACE_SECONDS
        while time.monotonic() < deadline:
            return_code = process.poll()
            if return_code is not None:
                raise UpdaterError(
                    f"新版主程序启动后提前退出，退出码 {return_code}"
                )
            time.sleep(0.25)
        _write_log(
            log_path,
            f"v{target_version} 启动成功，保留上一版本备份：{backup_dir}",
        )
    except Exception as exc:
        if backup_created:
            try:
                rollback(app_dir, backup_dir, log_path)
            except Exception as rollback_error:
                _write_log(log_path, f"恢复旧版本失败：{rollback_error}")
        else:
            _write_log(log_path, "更新预检失败，原程序目录未被替换")

        _write_update_result(
            app_dir,
            status="rolled_back" if backup_created else "preflight_failed",
            target_version=target_version,
            backup_dir=backup_dir,
            cleanup_dir=cleanup_dir,
            error=str(exc),
        )
        if app_dir.is_dir() and (app_dir / executable_name).is_file():
            try:
                launch_main(app_dir, executable_name)
                _write_log(log_path, "已重新启动旧版本")
            except Exception as restart_error:
                _write_log(log_path, f"重新启动旧版本失败：{restart_error}")
        else:
            _write_log(log_path, "没有可重新启动的主程序")
        raise
    finally:
        try:
            remove_path_with_retry(staging_dir)
        except OSError as cleanup_error:
            _write_log(log_path, f"清理更新暂存目录失败：{cleanup_error}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="弹幕排队姬独立更新器")
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--app-dir", type=Path, required=True)
    parser.add_argument("--zip", dest="zip_path", type=Path, required=True)
    parser.add_argument("--main-exe", default="main.exe")
    parser.add_argument("--target-version", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
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
        _write_log(log_path, f"更新失败：{exc}")
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
