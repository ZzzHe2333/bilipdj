from __future__ import annotations

import os
import shutil
import time
from pathlib import Path, PurePosixPath
from typing import Any

from apps.update_workspace import validate_update_session

try:
    from . import incremental_apply, updater as legacy
except ImportError:  # standalone updater bundle
    import incremental_apply  # type: ignore[no-redef]
    import updater as legacy  # type: ignore[no-redef]

COPY_ATTEMPTS = 40
COPY_DELAY = 0.25


def _preserve_paths() -> tuple[str, ...]:
    values = {"backup", "update", "key", "log", "logs", "plugins", "core/cd"}
    for item in tuple(getattr(legacy, "PRESERVE_PATHS", ())):
        values.add(Path(item).as_posix().strip("/"))
    return tuple(sorted((value.casefold() for value in values if value), key=len))


def _normalize_relative(value: Path | str) -> str:
    return PurePosixPath(str(value).replace("\\", "/")).as_posix().strip("/")


def _is_preserved(relative: Path | str) -> bool:
    text = _normalize_relative(relative).casefold()
    return any(text == item or text.startswith(item + "/") for item in _preserve_paths())


def _has_preserved_descendant(relative: Path | str) -> bool:
    text = _normalize_relative(relative).casefold().rstrip("/") + "/"
    return any(item.startswith(text) for item in _preserve_paths())


def _remove_managed(root: Path, relative: Path = Path()) -> None:
    base = root / relative
    if not base.exists():
        return
    for child in list(base.iterdir()):
        rel = relative / child.name
        if _is_preserved(rel):
            continue
        if child.is_dir() and not child.is_symlink() and _has_preserved_descendant(rel):
            _remove_managed(root, rel)
            try:
                child.rmdir()
            except OSError:
                pass
            continue
        legacy.remove_path_with_retry(child)


def _copy_with_retry(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    last: OSError | None = None
    for attempt in range(COPY_ATTEMPTS):
        try:
            shutil.copy2(source, destination)
            return
        except OSError as exc:
            last = exc
            if attempt + 1 < COPY_ATTEMPTS:
                time.sleep(COPY_DELAY)
    raise legacy.UpdaterError(f"无法写入更新文件：{destination}；原始错误：{last}") from last


def _copy_package_tree(source_root: Path, app_dir: Path, relative: Path = Path()) -> None:
    source = source_root / relative
    for child in source.iterdir():
        rel = relative / child.name
        destination = app_dir / rel
        if _is_preserved(rel) and destination.exists():
            continue
        if child.is_symlink():
            raise legacy.UpdaterError(f"更新包不支持符号链接：{rel}")
        if child.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
            _copy_package_tree(source_root, app_dir, rel)
        elif child.is_file():
            _copy_with_retry(child, destination)


def _copy_snapshot_contents(snapshot: Path, app_dir: Path) -> None:
    for child in snapshot.iterdir():
        target = app_dir / child.name
        if child.is_dir():
            legacy._copy_snapshot_entry(child, target)  # noqa: SLF001
        elif child.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(child, target)


def _create_snapshot(app_dir: Path, target_version: str) -> Path:
    snapshot = legacy._allocate_backup_snapshot_dir(app_dir, target_version)  # noqa: SLF001
    snapshot.mkdir(parents=True, exist_ok=False)
    try:
        for source in app_dir.iterdir():
            if source.name.casefold() in {"backup", "update"}:
                continue
            legacy._copy_snapshot_entry(source, snapshot / source.name)  # noqa: SLF001
    except Exception:
        legacy.remove_path_with_retry(snapshot)
        raise
    return snapshot


def _restore_snapshot(app_dir: Path, snapshot: Path) -> None:
    for entry in list(app_dir.iterdir()):
        if entry.name.casefold() in {"backup", "update"}:
            continue
        legacy.remove_path_with_retry(entry)
    _copy_snapshot_contents(snapshot, app_dir)


def _log_path(app_dir: Path) -> Path:
    return app_dir / "log" / "update.log"


def _wait_new_process(app_dir: Path, executable_name: str) -> None:
    process = legacy.launch_main(app_dir, executable_name)
    deadline = time.monotonic() + legacy.STARTUP_GRACE_SECONDS
    while time.monotonic() < deadline:
        code = process.poll()
        if code is not None:
            raise legacy.UpdaterError(f"新版主程序启动后提前退出，退出码 {code}")
        time.sleep(0.25)


def _full_update(
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
    session = validate_update_session(app_dir, zip_path.parent)
    staging = session / "full-staging"
    log_path = _log_path(app_dir)
    snapshot: Path | None = None

    legacy._write_log(log_path, f"准备在程序目录 update 会话中全量更新到 v{target_version}")  # noqa: SLF001
    if not app_dir.is_dir() or not zip_path.is_file():
        raise legacy.UpdaterError("程序目录或完整更新包不存在")

    try:
        legacy.remove_path_with_retry(staging)
        staging.mkdir(parents=True, exist_ok=True)
        legacy.safe_extract(zip_path, staging)
        legacy.validate_staging(staging, executable_name)
        if not legacy.wait_for_process_exit(pid):
            raise legacy.UpdaterError("等待主程序退出超时，请完全关闭程序后重试")
        time.sleep(1.0)

        snapshot = _create_snapshot(app_dir, target_version)
        legacy._write_log(log_path, f"已保存更新前备份：{snapshot}")  # noqa: SLF001
        _remove_managed(app_dir)
        _copy_package_tree(staging, app_dir)
        if not (app_dir / executable_name).is_file():
            raise legacy.UpdaterError(f"替换后缺少主程序：{executable_name}")
        legacy._write_update_result(  # noqa: SLF001
            app_dir,
            status="installed",
            target_version=target_version,
            backup_dir=snapshot,
            cleanup_dir=session,
        )
        legacy._write_log(log_path, "全量文件替换完成，正在启动新版本")  # noqa: SLF001
        _wait_new_process(app_dir, executable_name)
        legacy._write_log(log_path, f"v{target_version} 全量更新成功")  # noqa: SLF001
    except Exception as exc:
        if snapshot is not None and snapshot.is_dir():
            try:
                _restore_snapshot(app_dir, snapshot)
                status = "rolled_back"
                legacy._write_log(log_path, "全量更新失败，已恢复更新前快照")  # noqa: SLF001
            except Exception as rollback_exc:
                status = "rollback_failed"
                legacy._write_log(log_path, f"全量回滚失败：{rollback_exc}")  # noqa: SLF001
        else:
            status = "preflight_failed"
        legacy._write_update_result(  # noqa: SLF001
            app_dir,
            status=status,
            target_version=target_version,
            backup_dir=snapshot,
            cleanup_dir=session,
            error=str(exc),
        )
        if (app_dir / executable_name).is_file():
            try:
                legacy.launch_main(app_dir, executable_name)
            except Exception:
                pass
        raise
    finally:
        try:
            legacy.remove_path_with_retry(staging)
        except OSError:
            pass


def _incremental_update(
    *,
    pid: int,
    app_dir: Path,
    zip_path: Path,
    plan_path: Path,
    main_exe_name: str,
    target_version: str,
) -> None:
    executable_name = legacy.validate_executable_name(main_exe_name)
    app_dir = Path(app_dir).resolve()
    zip_path = Path(zip_path).resolve()
    plan_path = Path(plan_path).resolve()
    session = validate_update_session(app_dir, zip_path.parent)
    if plan_path.parent.resolve() != session:
        raise legacy.UpdaterError("增量更新计划不在程序目录 update 会话中")
    staging = session / "incremental-staging"
    rollback_root = session / "incremental-rollback"
    log_path = _log_path(app_dir)
    snapshot: Path | None = None
    touched: list[str] = []
    existed: set[str] = set()

    replace, remove = incremental_apply._load_plan(plan_path, target_version)  # noqa: SLF001
    touched = [str(item["path"]) for item in replace] + remove
    try:
        legacy.remove_path_with_retry(staging)
        legacy.remove_path_with_retry(rollback_root)
        staging.mkdir(parents=True, exist_ok=True)
        rollback_root.mkdir(parents=True, exist_ok=True)
        legacy.safe_extract(zip_path, staging)
        incremental_apply._validate_staging(staging, replace)  # noqa: SLF001
        for relative in touched:
            incremental_apply.resolve_managed_path(app_dir, relative)
        if not legacy.wait_for_process_exit(pid):
            raise legacy.UpdaterError("等待主程序退出超时，请完全关闭程序后重试")
        time.sleep(1.0)

        snapshot = _create_snapshot(app_dir, target_version)
        for relative in touched:
            target = incremental_apply.resolve_managed_path(app_dir, relative)
            if target.is_symlink() or target.is_dir():
                raise legacy.UpdaterError(f"增量目标类型不安全：{relative}")
            if target.is_file():
                backup = incremental_apply.resolve_managed_path(rollback_root, relative)
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(target, backup)
                existed.add(relative)

        for item in replace:
            relative = str(item["path"])
            source = incremental_apply.resolve_managed_path(staging, relative)
            destination = incremental_apply.resolve_managed_path(app_dir, relative)
            _copy_with_retry(source, destination)
            legacy._write_log(log_path, f"增量替换：{relative}")  # noqa: SLF001
        for relative in remove:
            destination = incremental_apply.resolve_managed_path(app_dir, relative)
            if destination.exists() or destination.is_symlink():
                incremental_apply._remove_target(destination)  # noqa: SLF001

        legacy._write_update_result(  # noqa: SLF001
            app_dir,
            status="installed",
            target_version=target_version,
            backup_dir=snapshot,
            cleanup_dir=session,
        )
        _wait_new_process(app_dir, executable_name)
        legacy._write_log(log_path, f"v{target_version} 增量更新成功")  # noqa: SLF001
    except Exception as exc:
        rollback_ok = False
        if rollback_root.is_dir() and touched:
            try:
                for relative in reversed(touched):
                    destination = incremental_apply.resolve_managed_path(app_dir, relative)
                    if relative in existed:
                        source = incremental_apply.resolve_managed_path(rollback_root, relative)
                        _copy_with_retry(source, destination)
                    elif destination.exists() or destination.is_symlink():
                        incremental_apply._remove_target(destination)  # noqa: SLF001
                rollback_ok = True
            except Exception as rollback_exc:
                legacy._write_log(log_path, f"增量回滚失败：{rollback_exc}")  # noqa: SLF001
        status = "rolled_back" if rollback_ok else ("rollback_failed" if snapshot is not None else "preflight_failed")
        legacy._write_update_result(  # noqa: SLF001
            app_dir,
            status=status,
            target_version=target_version,
            backup_dir=snapshot,
            cleanup_dir=session,
            error=str(exc),
        )
        if (app_dir / executable_name).is_file():
            try:
                legacy.launch_main(app_dir, executable_name)
            except Exception:
                pass
        raise
    finally:
        for path in (staging, rollback_root):
            try:
                legacy.remove_path_with_retry(path)
            except OSError:
                pass


def perform_update(
    *,
    pid: int,
    app_dir: Path,
    zip_path: Path,
    main_exe_name: str,
    target_version: str,
) -> None:
    zip_path = Path(zip_path).resolve()
    plan_path = zip_path.parent / "incremental-plan.json"
    if plan_path.is_file():
        _incremental_update(
            pid=pid,
            app_dir=app_dir,
            zip_path=zip_path,
            plan_path=plan_path,
            main_exe_name=main_exe_name,
            target_version=target_version,
        )
    else:
        _full_update(
            pid=pid,
            app_dir=app_dir,
            zip_path=zip_path,
            main_exe_name=main_exe_name,
            target_version=target_version,
        )


def patch_updater_v2(module: Any) -> bool:
    module.perform_update = perform_update
    module.legacy.PRESERVE_PATHS = tuple(module.legacy.PRESERVE_PATHS) + tuple(
        path for path in (Path("update"),) if path not in module.legacy.PRESERVE_PATHS
    )
    module._issue222_local_apply_installed = True
    return True


__all__ = ["patch_updater_v2", "perform_update"]
