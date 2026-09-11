from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path

try:  # Standalone updater.exe/source execution.
    import updater as legacy
    from update_manifest import (
        ManifestPathError,
        calculate_sha256,
        is_preserved_path,
        normalize_manifest_path,
        resolve_managed_path,
    )
except ImportError:  # Package import used by the main app/tests.
    from . import updater as legacy
    from .update_manifest import (
        ManifestPathError,
        calculate_sha256,
        is_preserved_path,
        normalize_manifest_path,
        resolve_managed_path,
    )

FILE_REPLACE_ATTEMPTS = 30
FILE_REPLACE_DELAY_SECONDS = 0.2


def _load_plan(plan_path: Path, target_version: str) -> tuple[list[dict[str, object]], list[str]]:
    try:
        payload = json.loads(Path(plan_path).read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise legacy.UpdaterError("增量更新计划无法解析") from exc
    if not isinstance(payload, dict) or payload.get("kind") != "bilipdj-incremental-plan":
        raise legacy.UpdaterError("增量更新计划格式无效")
    if str(payload.get("version", "") or "").strip() != str(target_version).strip():
        raise legacy.UpdaterError("增量更新计划版本与目标版本不一致")

    raw_replace = payload.get("replace", [])
    raw_remove = payload.get("remove", [])
    if not isinstance(raw_replace, list) or not isinstance(raw_remove, list):
        raise legacy.UpdaterError("增量更新计划 replace/remove 格式无效")

    replace: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in raw_replace:
        if not isinstance(raw, dict):
            raise legacy.UpdaterError("增量更新计划包含无效替换记录")
        try:
            relative = normalize_manifest_path(str(raw.get("path", "") or ""))
        except ManifestPathError as exc:
            raise legacy.UpdaterError(str(exc)) from exc
        if is_preserved_path(relative):
            raise legacy.UpdaterError(f"拒绝增量覆盖用户数据：{relative}")
        key = relative.casefold()
        if key in seen:
            raise legacy.UpdaterError(f"增量更新计划包含重复路径：{relative}")
        seen.add(key)
        try:
            size = int(raw.get("size", -1))
        except (TypeError, ValueError) as exc:
            raise legacy.UpdaterError(f"增量文件大小无效：{relative}") from exc
        digest = str(raw.get("sha256", "") or "").strip().lower()
        if size < 0 or len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise legacy.UpdaterError(f"增量文件元数据无效：{relative}")
        replace.append({"path": relative, "size": size, "sha256": digest})

    remove: list[str] = []
    for raw in raw_remove:
        try:
            relative = normalize_manifest_path(str(raw))
        except ManifestPathError as exc:
            raise legacy.UpdaterError(str(exc)) from exc
        if is_preserved_path(relative):
            raise legacy.UpdaterError(f"拒绝增量删除用户数据：{relative}")
        key = relative.casefold()
        if key in seen:
            raise legacy.UpdaterError(f"路径同时出现在 replace/remove：{relative}")
        seen.add(key)
        remove.append(relative)
    return replace, remove


def _validate_staging(
    staging_dir: Path,
    replace: list[dict[str, object]],
) -> None:
    for entry in replace:
        relative = str(entry["path"])
        source = resolve_managed_path(staging_dir, relative)
        if source.is_symlink() or not source.is_file():
            raise legacy.UpdaterError(f"增量包缺少文件：{relative}")
        actual_size = source.stat().st_size
        if actual_size != int(entry["size"]):
            raise legacy.UpdaterError(
                f"增量文件大小不一致：{relative}，预期 {entry['size']}，实际 {actual_size}"
            )
        actual_digest = calculate_sha256(source)
        if actual_digest.lower() != str(entry["sha256"]).lower():
            raise legacy.UpdaterError(f"增量文件 SHA-256 校验失败：{relative}")


def _replace_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_name(f".{destination.name}.bilipdj-update-{os.getpid()}.part")
    temp.unlink(missing_ok=True)
    try:
        shutil.copy2(source, temp)
        last_error: OSError | None = None
        for attempt in range(FILE_REPLACE_ATTEMPTS):
            try:
                os.replace(temp, destination)
                return
            except OSError as exc:
                last_error = exc
                if attempt + 1 >= FILE_REPLACE_ATTEMPTS:
                    break
                time.sleep(FILE_REPLACE_DELAY_SECONDS)
        raise legacy.UpdaterError(
            f"无法替换增量文件：{destination}；原始错误：{last_error}"
        ) from last_error
    finally:
        temp.unlink(missing_ok=True)


def _remove_target(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        legacy.remove_path_with_retry(path)
        return
    if path.exists():
        raise legacy.UpdaterError(f"拒绝将目录作为增量文件删除：{path}")


def _prepare_rollback(
    app_dir: Path,
    rollback_dir: Path,
    touched: list[str],
) -> set[str]:
    legacy.remove_path_with_retry(rollback_dir)
    files_root = rollback_dir / "files"
    files_root.mkdir(parents=True, exist_ok=True)
    existed: set[str] = set()
    for relative in touched:
        target = resolve_managed_path(app_dir, relative)
        if target.is_symlink():
            raise legacy.UpdaterError(f"拒绝增量更新符号链接：{relative}")
        if target.is_dir():
            raise legacy.UpdaterError(f"增量更新目标意外为目录：{relative}")
        if target.is_file():
            backup = resolve_managed_path(files_root, relative)
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(target, backup)
            existed.add(relative)
    (rollback_dir / "state.json").write_text(
        json.dumps({"existed": sorted(existed)}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return existed


def _rollback_files(
    app_dir: Path,
    rollback_dir: Path,
    touched: list[str],
    existed: set[str],
) -> None:
    files_root = rollback_dir / "files"
    for relative in reversed(touched):
        target = resolve_managed_path(app_dir, relative)
        if relative in existed:
            backup = resolve_managed_path(files_root, relative)
            if not backup.is_file():
                raise legacy.UpdaterError(f"增量回滚备份缺失：{relative}")
            _replace_file(backup, target)
        else:
            _remove_target(target)


def perform_incremental_update_core(
    *,
    pid: int,
    app_dir: Path,
    zip_path: Path,
    plan_path: Path,
    main_exe_name: str,
    target_version: str,
    post_exit_settle_seconds: float = 0.0,
) -> None:
    executable_name = legacy.validate_executable_name(main_exe_name)
    app_dir = Path(app_dir).resolve()
    zip_path = Path(zip_path).resolve()
    plan_path = Path(plan_path).resolve()
    if app_dir == app_dir.parent:
        raise legacy.UpdaterError("拒绝将文件系统根目录作为程序目录")

    parent = app_dir.parent
    safe_name = app_dir.name or "bilipdj"
    staging_dir = parent / f".{safe_name}.incremental-staging"
    rollback_dir = parent / f".{safe_name}.incremental-rollback"
    cleanup_dir = zip_path.parent
    log_path = parent / f"{safe_name}-update.log"
    persistent_backup_dir: Path | None = None
    rollback_ready = False
    rollback_succeeded = False
    touched: list[str] = []
    existed: set[str] = set()

    legacy._write_log(log_path, f"准备增量更新到 v{target_version}")
    if not app_dir.is_dir():
        raise legacy.UpdaterError(f"程序目录不存在：{app_dir}")
    if not zip_path.is_file():
        raise legacy.UpdaterError(f"增量更新包不存在：{zip_path}")
    if not plan_path.is_file():
        raise legacy.UpdaterError(f"增量更新计划不存在：{plan_path}")

    replace, remove = _load_plan(plan_path, target_version)
    touched = [str(entry["path"]) for entry in replace] + remove

    try:
        legacy.remove_path_with_retry(staging_dir)
        staging_dir.mkdir(parents=True, exist_ok=True)
        legacy._write_log(log_path, f"解压增量资源包：{zip_path}")
        legacy.safe_extract(zip_path, staging_dir)
        _validate_staging(staging_dir, replace)

        # Validate all destination paths before stopping/replacing anything.
        for relative in touched:
            resolve_managed_path(app_dir, relative)

        if not legacy.wait_for_process_exit(pid):
            raise legacy.UpdaterError("等待主程序退出超时，请完全关闭程序后重试")
        if post_exit_settle_seconds > 0:
            time.sleep(float(post_exit_settle_seconds))

        persistent_backup_dir = legacy.create_update_snapshot(app_dir, target_version)
        legacy._write_log(log_path, f"已保存增量更新前备份：{persistent_backup_dir}")
        existed = _prepare_rollback(app_dir, rollback_dir, touched)
        rollback_ready = True

        for entry in replace:
            relative = str(entry["path"])
            source = resolve_managed_path(staging_dir, relative)
            destination = resolve_managed_path(app_dir, relative)
            _replace_file(source, destination)
            legacy._write_log(log_path, f"增量替换：{relative}")
        for relative in remove:
            destination = resolve_managed_path(app_dir, relative)
            if destination.exists() or destination.is_symlink():
                _remove_target(destination)
                legacy._write_log(log_path, f"增量删除旧文件：{relative}")

        legacy._write_update_result(
            app_dir,
            status="installed",
            target_version=target_version,
            backup_dir=persistent_backup_dir,
            cleanup_dir=cleanup_dir,
        )
        legacy._write_log(
            log_path,
            f"增量替换完成：更新 {len(replace)} 个文件，删除 {len(remove)} 个旧文件；正在启动主程序",
        )
        process = legacy.launch_main(app_dir, executable_name)
        deadline = time.monotonic() + legacy.STARTUP_GRACE_SECONDS
        while time.monotonic() < deadline:
            return_code = process.poll()
            if return_code is not None:
                raise legacy.UpdaterError(f"新版主程序启动后提前退出，退出码 {return_code}")
            time.sleep(0.25)

        legacy.remove_path_with_retry(rollback_dir)
        rollback_ready = False
        legacy._write_log(
            log_path,
            f"v{target_version} 增量更新成功，更新前备份保留于：{persistent_backup_dir}",
        )
    except Exception as exc:
        if rollback_ready:
            try:
                legacy._write_log(log_path, "增量更新失败，开始恢复被修改文件")
                _rollback_files(app_dir, rollback_dir, touched, existed)
                rollback_succeeded = True
                rollback_ready = False
                legacy._write_log(log_path, "增量更新回滚完成")
            except Exception as rollback_error:
                legacy._write_log(log_path, f"增量更新回滚失败：{rollback_error}")
        else:
            legacy._write_log(log_path, "增量更新预检失败，程序文件未被修改")

        status = "rolled_back" if rollback_succeeded else ("rollback_failed" if rollback_ready else "preflight_failed")
        legacy._write_update_result(
            app_dir,
            status=status,
            target_version=target_version,
            backup_dir=persistent_backup_dir,
            cleanup_dir=cleanup_dir,
            error=str(exc),
        )
        if app_dir.is_dir() and (app_dir / executable_name).is_file():
            try:
                legacy.launch_main(app_dir, executable_name)
                legacy._write_log(log_path, "已重新启动原版本")
            except Exception as restart_error:
                legacy._write_log(log_path, f"重新启动原版本失败：{restart_error}")
        raise
    finally:
        try:
            legacy.remove_path_with_retry(staging_dir)
        except OSError as cleanup_error:
            legacy._write_log(log_path, f"清理增量暂存目录失败：{cleanup_error}")
        if not rollback_ready:
            try:
                legacy.remove_path_with_retry(rollback_dir)
            except OSError as cleanup_error:
                legacy._write_log(log_path, f"清理增量回滚目录失败：{cleanup_error}")
