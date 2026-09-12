from __future__ import annotations

from pathlib import Path
from typing import Any

from apps.update_workspace import validate_update_session
from apps.windows import issue222_update_apply as apply


def _safe_incremental_update(
    *,
    pid: int,
    app_dir: Path,
    zip_path: Path,
    plan_path: Path,
    main_exe_name: str,
    target_version: str,
) -> None:
    """Apply an incremental update transactionally using the full snapshot.

    The issue-222 implementation created a per-file rollback tree before any
    mutation.  If that preparation failed part-way through, its generic rollback
    path treated every unrecorded target as a newly-created file and could delete
    untouched original files.  This implementation never rolls back from a
    partially prepared per-file set: before the first mutation it creates one
    complete snapshot, and every failure after that restores the snapshot.
    """

    legacy = apply.legacy
    incremental_apply = apply.incremental_apply
    executable_name = legacy.validate_executable_name(main_exe_name)
    app_dir = Path(app_dir).resolve()
    zip_path = Path(zip_path).resolve()
    plan_path = Path(plan_path).resolve()
    session = validate_update_session(app_dir, zip_path.parent)
    if plan_path.parent.resolve() != session:
        raise legacy.UpdaterError("增量更新计划不在程序目录 update 会话中")

    staging = session / "incremental-staging"
    log_path = apply._log_path(app_dir)  # noqa: SLF001
    snapshot: Path | None = None
    replace, remove = incremental_apply._load_plan(plan_path, target_version)  # noqa: SLF001
    touched = [str(item["path"]) for item in replace] + list(remove)

    try:
        legacy.remove_path_with_retry(staging)
        staging.mkdir(parents=True, exist_ok=True)
        legacy.safe_extract(zip_path, staging)
        incremental_apply._validate_staging(staging, replace)  # noqa: SLF001

        # Resolve and validate every target while the original process is still
        # untouched. Any failure here is a pure preflight failure.
        for relative in touched:
            target = incremental_apply.resolve_managed_path(app_dir, relative)
            if target.is_symlink() or target.is_dir():
                raise legacy.UpdaterError(f"增量目标类型不安全：{relative}")

        if not legacy.wait_for_process_exit(pid):
            raise legacy.UpdaterError("等待主程序退出超时，请完全关闭程序后重试")
        apply.time.sleep(1.0)

        # This is the transaction boundary. No application file is modified
        # before a complete snapshot exists.
        snapshot = apply._create_snapshot(app_dir, target_version)  # noqa: SLF001
        legacy._write_log(log_path, f"增量更新前快照已完成：{snapshot}")  # noqa: SLF001

        for item in replace:
            relative = str(item["path"])
            source = incremental_apply.resolve_managed_path(staging, relative)
            destination = incremental_apply.resolve_managed_path(app_dir, relative)
            apply._copy_with_retry(source, destination)  # noqa: SLF001
            legacy._write_log(log_path, f"增量替换：{relative}")  # noqa: SLF001

        for relative in remove:
            destination = incremental_apply.resolve_managed_path(app_dir, relative)
            if destination.exists() or destination.is_symlink():
                incremental_apply._remove_target(destination)  # noqa: SLF001
                legacy._write_log(log_path, f"增量删除：{relative}")  # noqa: SLF001

        legacy._write_update_result(  # noqa: SLF001
            app_dir,
            status="installed",
            target_version=target_version,
            backup_dir=snapshot,
            cleanup_dir=session,
        )
        apply._wait_new_process(app_dir, executable_name)  # noqa: SLF001
        legacy._write_log(log_path, f"v{target_version} 增量更新成功")  # noqa: SLF001
    except Exception as exc:
        if snapshot is not None and snapshot.is_dir():
            try:
                apply._restore_snapshot(app_dir, snapshot)  # noqa: SLF001
                status = "rolled_back"
                legacy._write_log(log_path, "增量更新失败，已从完整快照恢复")  # noqa: SLF001
            except Exception as rollback_exc:
                status = "rollback_failed"
                legacy._write_log(log_path, f"增量完整快照回滚失败：{rollback_exc}")  # noqa: SLF001
        else:
            # Crucially, do not delete/copy any target here. Snapshot creation
            # failed before the mutation boundary, so the original files are
            # already the correct rollback state.
            status = "preflight_failed"
            legacy._write_log(log_path, "增量更新在修改文件前失败，原文件保持不变")  # noqa: SLF001

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


def patch_issue226_windows_update_safety() -> bool:
    if getattr(apply, "_issue226_update_safety_installed", False):
        return True
    apply._incremental_update = _safe_incremental_update  # type: ignore[attr-defined]  # noqa: SLF001
    apply._issue226_update_safety_installed = True
    return True


__all__ = ["patch_issue226_windows_update_safety"]
