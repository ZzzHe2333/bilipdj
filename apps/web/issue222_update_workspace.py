from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import zlib
from pathlib import Path
from typing import Any

from apps.update_workspace import validate_update_session


def _session(request: dict[str, Any], work: Path) -> tuple[Path, Path]:
    app_dir = Path(request["app_dir"]).resolve()
    session = validate_update_session(app_dir, Path(work).resolve().parent)
    return app_dir, session


def _remove_managed(module: Any, root: Path, relative: Path = Path()) -> None:
    base = root / relative
    if not base.exists():
        return
    for child in list(base.iterdir()):
        rel = relative / child.name
        text = rel.as_posix()
        if module._is_preserved(text):  # noqa: SLF001
            continue
        prefix = text.casefold().rstrip("/") + "/"
        preserved_children = any(
            item.startswith(prefix)
            for item in tuple(module.PRESERVED_PREFIXES) + tuple(module.PRESERVED_FILES)
        )
        if child.is_dir() and not child.is_symlink() and preserved_children:
            _remove_managed(module, root, rel)
            try:
                child.rmdir()
            except OSError:
                pass
        else:
            module._remove(child)  # noqa: SLF001


def _copy_package(module: Any, source_root: Path, app_dir: Path, relative: Path = Path()) -> None:
    source = source_root / relative
    for child in source.iterdir():
        rel = relative / child.name
        destination = app_dir / rel
        if module._is_preserved(rel.as_posix()) and destination.exists():  # noqa: SLF001
            continue
        if child.is_symlink():
            raise module.WebUpdaterError(f"更新包不支持符号链接：{rel}")
        if child.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
            _copy_package(module, source_root, app_dir, rel)
        elif child.is_file():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(child, destination)


def _snapshot(module: Any, app_dir: Path, target_version: str, state: Any) -> Path:
    backup_root = app_dir / "backup"
    backup_root.mkdir(parents=True, exist_ok=True)
    base = f"update-{time.strftime('%Y%m%d-%H%M%S')}-to-v{module._safe_component(target_version)}"  # noqa: SLF001
    snapshot = backup_root / base
    index = 2
    while snapshot.exists():
        snapshot = backup_root / f"{base}-{index}"
        index += 1
    snapshot.mkdir()
    entries = [entry for entry in app_dir.iterdir() if entry.name.casefold() not in {"backup", "update"}]
    try:
        for i, entry in enumerate(entries, 1):
            state.set(
                stage="backup",
                percent=68 + min(8, int(i * 8 / max(1, len(entries)))),
                current_file=entry.name,
                message="正在保存更新前版本快照…",
            )
            module._copy_entry(entry, snapshot / entry.name)  # noqa: SLF001
    except Exception:
        module._remove(snapshot)  # noqa: SLF001
        raise
    return snapshot


def _restore_snapshot(module: Any, app_dir: Path, snapshot: Path) -> None:
    for entry in list(app_dir.iterdir()):
        if entry.name.casefold() in {"backup", "update"}:
            continue
        module._remove(entry)  # noqa: SLF001
    for child in snapshot.iterdir():
        module._copy_entry(child, app_dir / child.name)  # noqa: SLF001


def _write_result(module: Any, app_dir: Path, *, status: str, version: str, backup: Path | None, session: Path, error: str = "") -> None:
    payload = {
        "status": status,
        "version": version,
        "installed_at": module._timestamp(),  # noqa: SLF001
        "backup_dir": str(backup or ""),
        "cleanup_dir": str(session),
        "mode": "web-portable",
    }
    if error:
        payload["error"] = str(error)
    (app_dir / "update-result.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _apply_full_tree(module: Any, request: dict[str, Any], state: Any, staging: Path, session: Path) -> None:
    app_dir = Path(request["app_dir"]).resolve()
    target_version = str(request.get("target_version", "") or "unknown")
    main_exe = str(request["main_exe"])
    updater_exe = str(request["updater_exe"])
    if not (staging / main_exe).is_file() or not (staging / updater_exe).is_file():
        raise module.WebUpdaterError(f"目标版本缺少 {main_exe} 或 {updater_exe}")
    snapshot = _snapshot(module, app_dir, target_version, state)
    try:
        state.set(stage="installing", percent=78, message="正在程序目录内切换版本文件…", current_file="")
        _remove_managed(module, app_dir)
        _copy_package(module, staging, app_dir)
        _write_result(module, app_dir, status="installed", version=target_version, backup=snapshot, session=session)
        process = module._launch_main(app_dir, main_exe)  # noqa: SLF001
        module._wait_started(process, state)  # noqa: SLF001
    except Exception as exc:
        try:
            _restore_snapshot(module, app_dir, snapshot)
            _write_result(module, app_dir, status="rolled_back", version=target_version, backup=snapshot, session=session, error=str(exc))
            try:
                module._launch_main(app_dir, main_exe)  # noqa: SLF001
            except Exception:
                pass
        except Exception as rollback_exc:
            _write_result(module, app_dir, status="rollback_failed", version=target_version, backup=snapshot, session=session, error=f"{exc}; rollback: {rollback_exc}")
            raise module.WebUpdaterError(f"更新失败且自动回滚失败：{exc}；回滚错误：{rollback_exc}") from exc
        raise


def install_issue222_update_workspace(module: Any) -> bool:
    if bool(getattr(module, "_issue222_update_workspace_installed", False)):
        return True
    if "update/" not in module.PRESERVED_PREFIXES:
        module.PRESERVED_PREFIXES = tuple(module.PRESERVED_PREFIXES) + ("update/",)

    def full_update(request: dict[str, Any], state: Any, work: Path) -> None:
        _app_dir, session = _session(request, work)
        package = request.get("package")
        if not isinstance(package, dict):
            raise module.WebUpdaterError("缺少 Web 完整包元数据")
        zip_path = Path(work) / str(package.get("filename", "web-update.zip"))
        module._download(str(package.get("url", "")), zip_path, expected_size=int(package.get("size", 0) or 0), state=state, start_pct=3, end_pct=56, label=zip_path.name)  # noqa: SLF001
        state.set(stage="verifying", percent=58, message="正在校验完整更新包 SHA-256…")
        module._verify(zip_path, str(package.get("sha256", "")), "完整更新包")  # noqa: SLF001
        staging = Path(work) / "full-staging"
        module._remove(staging)  # noqa: SLF001
        staging.mkdir(parents=True, exist_ok=True)
        state.set(stage="extracting", percent=61, message="正在 update 目录安全解压完整更新包…")
        module._safe_extract(zip_path, staging)  # noqa: SLF001
        module._stop_app(request, state)  # noqa: SLF001
        _apply_full_tree(module, request, state, staging, session)

    def restore_update(request: dict[str, Any], state: Any, work: Path) -> None:
        app_dir, session = _session(request, work)
        backup_root = (app_dir / "backup").resolve()
        backup = Path(str(request.get("backup_path", ""))).resolve()
        try:
            backup.relative_to(backup_root)
        except ValueError as exc:
            raise module.WebUpdaterError("恢复备份路径不在 backup 目录内") from exc
        if backup == backup_root or not backup.is_dir() or backup.is_symlink():
            raise module.WebUpdaterError("恢复备份不存在")
        state.set(stage="preparing", percent=10, message="正在 update 目录准备本地备份恢复…")
        staging = Path(work) / "restore-staging"
        module._remove(staging)  # noqa: SLF001
        staging.mkdir(parents=True, exist_ok=True)
        module._copy_tree_contents(backup, staging)  # noqa: SLF001
        state.set(percent=55, message="本地备份已准备完成，等待切换版本…")
        module._stop_app(request, state)  # noqa: SLF001
        _apply_full_tree(module, request, state, staging, session)

    def incremental_update(request: dict[str, Any], state: Any, work: Path) -> None:
        app_dir, session = _session(request, work)
        package = request.get("package")
        if not isinstance(package, dict):
            raise module.WebUpdaterError("缺少 Web 增量元数据")
        manifest_asset = package.get("file_manifest")
        pack_asset = package.get("incremental")
        if not isinstance(manifest_asset, dict) or not isinstance(pack_asset, dict):
            raise module.WebUpdaterError("当前 Release 不支持 Web 增量更新")
        if str(pack_asset.get("transport", "") or "").lower() != "http-range":
            raise module.WebUpdaterError("Web 增量资源不是 HTTP Range 格式")
        manifest_path = Path(work) / "web-files.json"
        module._download(str(manifest_asset.get("url", "")), manifest_path, expected_size=int(manifest_asset.get("size", 0) or 0), state=state, start_pct=3, end_pct=7, label="逐文件清单")  # noqa: SLF001
        module._verify(manifest_path, str(manifest_asset.get("sha256", "")), "逐文件清单")  # noqa: SLF001
        files, removed = module._load_file_manifest(manifest_path, request, int(pack_asset.get("size", 0) or 0))  # noqa: SLF001
        state.set(stage="scanning", percent=9, message="正在扫描本地文件 SHA-256…", downloaded=0, total=0, speed=0)
        needed: list[str] = []
        for i, (relative, metadata) in enumerate(files.items(), 1):
            target = module._resolve_managed(app_dir, relative)  # noqa: SLF001
            same = False
            if target.is_file() and not target.is_symlink():
                try:
                    same = target.stat().st_size == int(metadata["size"]) and module._sha256(target).lower() == str(metadata["sha256"])
                except OSError:
                    same = False
            if not same:
                needed.append(relative)
            if i % 20 == 0 or i == len(files):
                state.set(percent=9 + int(6 * i / max(1, len(files))), current_file=relative)
        total_download = sum(int(files[path]["packed_size"]) for path in needed)
        patch_root = Path(work) / "patch"
        patch_root.mkdir(parents=True, exist_ok=True)
        done = 0
        started = time.monotonic()
        for relative in needed:
            metadata = files[relative]
            packed = module._download_range(str(pack_asset.get("url", "")), int(metadata["offset"]), int(metadata["packed_size"]))  # noqa: SLF001
            if hashlib.sha256(packed).hexdigest().lower() != str(metadata["packed_sha256"]):
                raise module.WebUpdaterError(f"增量片段校验失败：{relative}")
            raw = packed if metadata["compression"] == "store" else zlib.decompress(packed)
            if len(raw) != int(metadata["size"]) or hashlib.sha256(raw).hexdigest().lower() != str(metadata["sha256"]):
                raise module.WebUpdaterError(f"增量文件校验失败：{relative}")
            target = module._resolve_managed(patch_root, relative)  # noqa: SLF001
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
            done += len(packed)
            elapsed = max(0.001, time.monotonic() - started)
            ratio = done / total_download if total_download else 1.0
            state.set(stage="downloading", percent=15 + int(45 * min(1.0, ratio)), current_file=relative, downloaded=done, total=total_download, speed=int(done / elapsed), message=f"增量下载：{len(needed)} 个文件，仅传输本地发生变化的内容")
        module._stop_app(request, state)  # noqa: SLF001
        snapshot = _snapshot(module, app_dir, str(request.get("target_version", "")), state)
        remove_existing = [path for path in removed if module._resolve_managed(app_dir, path).exists() or module._resolve_managed(app_dir, path).is_symlink()]  # noqa: SLF001
        touched = needed + remove_existing
        rollback_root = Path(work) / "rollback-files"
        existed: set[str] = set()
        for relative in touched:
            target = module._resolve_managed(app_dir, relative)  # noqa: SLF001
            if target.is_symlink() or target.is_dir():
                raise module.WebUpdaterError(f"增量目标类型不安全：{relative}")
            if target.is_file():
                backup = module._resolve_managed(rollback_root, relative)  # noqa: SLF001
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(target, backup)
                existed.add(relative)
        try:
            total_ops = max(1, len(touched))
            for i, relative in enumerate(needed, 1):
                source = module._resolve_managed(patch_root, relative)  # noqa: SLF001
                destination = module._resolve_managed(app_dir, relative)  # noqa: SLF001
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)
                state.set(stage="installing", percent=78 + int(14 * i / total_ops), current_file=relative, message="正在应用增量文件…")
            for j, relative in enumerate(remove_existing, len(needed) + 1):
                module._remove(module._resolve_managed(app_dir, relative))  # noqa: SLF001
                state.set(stage="installing", percent=78 + int(14 * j / total_ops), current_file=relative, message="正在清理旧版本文件…")
            _write_result(module, app_dir, status="installed", version=str(request.get("target_version", "")), backup=snapshot, session=session)
            process = module._launch_main(app_dir, str(request["main_exe"]))  # noqa: SLF001
            module._wait_started(process, state)  # noqa: SLF001
        except Exception as exc:
            for relative in reversed(touched):
                target = module._resolve_managed(app_dir, relative)  # noqa: SLF001
                if relative in existed:
                    source = module._resolve_managed(rollback_root, relative)  # noqa: SLF001
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, target)
                else:
                    module._remove(target)  # noqa: SLF001
            _write_result(module, app_dir, status="rolled_back", version=str(request.get("target_version", "")), backup=snapshot, session=session, error=str(exc))
            try:
                module._launch_main(app_dir, str(request["main_exe"]))  # noqa: SLF001
            except Exception:
                pass
            raise

    module._full_update = full_update
    module._restore_update = restore_update
    module._incremental_update = incremental_update
    module._issue222_update_workspace_installed = True
    return True


__all__ = ["install_issue222_update_workspace"]
