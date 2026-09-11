from __future__ import annotations

import shutil
import sys
import tempfile
import threading
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tkinter import messagebox
from typing import Any, Literal

from . import update_client, update_ui

BACKUP_DIR_NAME = "backup"


@dataclass(frozen=True)
class LocalBackup:
    version: str
    path: Path
    created_at: str


@dataclass(frozen=True)
class VersionCandidate:
    label: str
    source: Literal["cloud", "local"]
    version: str
    release: update_client.ReleaseInfo | None = None
    backup: LocalBackup | None = None


def _safe_mtime_text(path: Path) -> str:
    try:
        stamp = path.stat().st_mtime
    except OSError:
        return "时间未知"
    return datetime.fromtimestamp(stamp).strftime("%Y-%m-%d %H:%M:%S")


def _read_backup_version(path: Path) -> str | None:
    version_file = path / "VERSION"
    try:
        version = version_file.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    try:
        update_client.normalize_version(version)
    except ValueError:
        return None
    return version


def discover_local_backups(app_dir: Path) -> list[LocalBackup]:
    """Return restorable update snapshots from ``app_dir/backup`` newest first."""

    app_dir = Path(app_dir).resolve()
    backup_root = app_dir / BACKUP_DIR_NAME
    if not backup_root.is_dir():
        return []

    found: list[LocalBackup] = []
    for candidate in backup_root.iterdir():
        if not candidate.is_dir() or candidate.is_symlink():
            continue
        version = _read_backup_version(candidate)
        if not version:
            continue
        # Persistent updater snapshots are full portable copies. Requiring these
        # executables keeps partial/manual directories out of the version list.
        if not (candidate / "main.exe").is_file() or not (candidate / update_client.UPDATER_EXE_NAME).is_file():
            continue
        found.append(
            LocalBackup(
                version=version,
                path=candidate.resolve(),
                created_at=_safe_mtime_text(candidate),
            )
        )

    def sort_key(item: LocalBackup) -> float:
        try:
            return item.path.stat().st_mtime
        except OSError:
            return 0.0

    found.sort(key=sort_key, reverse=True)
    return found


def build_version_candidates(
    app_dir: Path,
    cloud_release: update_client.ReleaseInfo | None,
) -> list[VersionCandidate]:
    candidates: list[VersionCandidate] = []
    if cloud_release is not None:
        candidates.append(
            VersionCandidate(
                label=f"云端最新 · v{cloud_release.version}",
                source="cloud",
                version=cloud_release.version,
                release=cloud_release,
            )
        )

    used_labels = {candidate.label for candidate in candidates}
    for backup in discover_local_backups(app_dir):
        base = f"本地备份 · v{backup.version} · {backup.created_at}"
        label = base
        suffix = 2
        while label in used_labels:
            label = f"{base} · #{suffix}"
            suffix += 1
        used_labels.add(label)
        candidates.append(
            VersionCandidate(
                label=label,
                source="local",
                version=backup.version,
                backup=backup,
            )
        )
    return candidates


def _selected_candidate(app: Any) -> VersionCandidate | None:
    mapping = getattr(app, "_update_version_candidates", {})
    label = str(getattr(app, "update_version_var", None).get() if hasattr(app, "update_version_var") else "")
    return mapping.get(label)


def _set_version_picker(app: Any, candidates: list[VersionCandidate]) -> None:
    picker = getattr(app, "_update_version_combo", None)
    mapping = {candidate.label: candidate for candidate in candidates}
    app._update_version_candidates = mapping
    values = tuple(mapping)
    if picker is not None:
        picker.configure(values=values, state="readonly" if values else "disabled")
    if values:
        app.update_version_var.set(values[0])
    else:
        app.update_version_var.set("暂无可用版本")
    app._selected_version_label = app.update_version_var.get()


def _configure_buttons_for_candidate(app: Any, candidate: VersionCandidate | None) -> None:
    if getattr(app, "_update_busy", False):
        return
    full_button = getattr(app, "_update_full_button", None)
    incremental_button = getattr(app, "_update_incremental_button", None)
    if candidate is None:
        if full_button is not None:
            full_button.configure(text="全量更新", state="disabled")
        if incremental_button is not None:
            incremental_button.configure(state="disabled")
        return
    if candidate.source == "local":
        if full_button is not None:
            full_button.configure(text="恢复旧版", state="normal")
        if incremental_button is not None:
            incremental_button.configure(state="disabled")
    else:
        if full_button is not None:
            full_button.configure(text="全量更新", state="normal")
        if incremental_button is not None:
            incremental_button.configure(state="normal")


def on_version_selected(app: Any) -> None:
    if getattr(app, "_update_busy", False):
        previous = getattr(app, "_selected_version_label", "")
        if previous:
            app.update_version_var.set(previous)
        return

    candidate = _selected_candidate(app)
    if candidate is None:
        app._available_update = None
        _configure_buttons_for_candidate(app, None)
        return

    app._selected_version_label = candidate.label
    if candidate.source == "cloud" and candidate.release is not None:
        release = candidate.release
        app._available_update = release
        update_ui._set_notes(app, release.body)  # noqa: SLF001 - shared update UI state
        try:
            relation = update_client.normalize_version(release.version) > update_client.normalize_version(app._update_current_version)
            equal = update_client.normalize_version(release.version) == update_client.normalize_version(app._update_current_version)
        except ValueError:
            relation = False
            equal = False
        size_mb = release.zip_asset.size / 1024**2
        if relation:
            text = f"已选择云端最新 v{release.version}，完整包约 {size_mb:.1f} MB。"
        elif equal:
            text = f"云端最新 v{release.version} 与当前版本一致；可重新全量安装或执行增量校验修复。"
        else:
            text = f"已选择云端 v{release.version}；可按所选版本执行更新。"
        app.update_status_var.set(text)
    else:
        backup = candidate.backup
        app._available_update = None
        if backup is not None:
            update_ui._set_notes(  # noqa: SLF001 - shared update UI state
                app,
                "本地备份恢复不会重新联网下载。恢复前仍会由独立更新器为当前版本再创建一份备份，"
                "并保留当前配置、日志、backup 以及当前 plugins 目录。\n\n"
                f"备份目录：{backup.path}",
            )
            app.update_status_var.set(f"已选择本地备份 v{backup.version}（{backup.created_at}）。")
    _configure_buttons_for_candidate(app, candidate)


def check_for_versions(app: Any, *, silent: bool = False) -> None:
    if getattr(app, "_update_busy", False):
        return
    update_ui._discard_all_prepared(app)  # noqa: SLF001 - shared update UI state
    app._available_update = None
    update_ui._set_busy(app, True)  # noqa: SLF001 - shared update UI state
    app.update_progress_var.set(0)
    app.update_status_var.set("正在读取云端最新版本与本地备份…")

    picker = getattr(app, "_update_version_combo", None)
    if picker is not None:
        picker.configure(state="disabled")

    def worker() -> None:
        release: update_client.ReleaseInfo | None = None
        cloud_error = ""
        try:
            release = update_client.fetch_latest_release()
        except Exception as exc:  # noqa: BLE001
            cloud_error = str(exc)
        candidates = build_version_candidates(app._update_app_dir, release)
        app.root.after(0, lambda: _finish_version_check(app, candidates, release, cloud_error, silent))

    threading.Thread(target=worker, name="bilipdj-version-check", daemon=True).start()


def _finish_version_check(
    app: Any,
    candidates: list[VersionCandidate],
    release: update_client.ReleaseInfo | None,
    cloud_error: str,
    silent: bool,
) -> None:
    update_ui._set_busy(app, False)  # noqa: SLF001 - shared update UI state
    _set_version_picker(app, candidates)
    if candidates:
        on_version_selected(app)
        local_count = sum(candidate.source == "local" for candidate in candidates)
        if cloud_error:
            app.update_status_var.set(
                f"云端版本读取失败：{cloud_error}；已加载 {local_count} 个本地备份，可直接恢复。"
            )
        elif release is not None and update_client.is_newer_version(release.version, app._update_current_version) and not silent:
            messagebox.showinfo(
                "发现新版本",
                f"当前版本：v{app._update_current_version}\n最新版本：v{release.version}\n\n可在“选择版本”中切换云端或本地备份。",
                parent=app.root,
            )
        return

    app._available_update = None
    _configure_buttons_for_candidate(app, None)
    if cloud_error:
        app.update_status_var.set(f"检查更新失败：{cloud_error}；未发现可恢复的本地备份。")
        if not silent:
            messagebox.showerror("检查更新失败", cloud_error, parent=app.root)
    else:
        app.update_status_var.set("未发现可用云端版本或本地备份。")


def _validated_backup_path(app_dir: Path, backup: LocalBackup) -> Path:
    app_dir = Path(app_dir).resolve()
    backup_root = (app_dir / BACKUP_DIR_NAME).resolve()
    path = Path(backup.path).resolve()
    try:
        path.relative_to(backup_root)
    except ValueError as exc:
        raise update_client.UpdateError("本地备份目录不在程序 backup 目录内") from exc
    if path == backup_root or not path.is_dir():
        raise update_client.UpdateError("本地备份目录不存在")
    if _read_backup_version(path) != backup.version:
        raise update_client.UpdateError("本地备份 VERSION 已变化，请重新检查版本")
    for required in ("main.exe", update_client.UPDATER_EXE_NAME):
        if not (path / required).is_file():
            raise update_client.UpdateError(f"本地备份缺少 {required}")
    return path


def _iter_archive_files(root: Path, *, skip_top: set[str] | None = None) -> list[Path]:
    skip = {name.casefold() for name in (skip_top or set())}
    files: list[Path] = []
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if relative.parts and relative.parts[0].casefold() in skip:
            continue
        if path.is_symlink():
            raise update_client.UpdateError(f"备份包含不支持的符号链接：{relative}")
        if path.is_file():
            files.append(path)
    return files


def prepare_local_backup_restore(
    backup: LocalBackup,
    *,
    app_dir: Path,
    progress: update_client.ProgressCallback | None = None,
) -> update_client.PreparedUpdate:
    """Package a local snapshot for the existing verified full updater path."""

    app_dir = Path(app_dir).resolve()
    snapshot = _validated_backup_path(app_dir, backup)
    work_dir = Path(tempfile.mkdtemp(prefix="bilipdj-local-restore-"))
    zip_path = work_dir / f"BiliPDJ-local-backup-v{backup.version}.zip"
    checksum_path = zip_path.with_suffix(zip_path.suffix + ".sha256")

    try:
        snapshot_files = _iter_archive_files(snapshot, skip_top={"plugins"})
        current_plugins = app_dir / "plugins"
        plugin_files = _iter_archive_files(current_plugins) if current_plugins.is_dir() else []
        total = sum(path.stat().st_size for path in snapshot_files + plugin_files)
        written = 0
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
            for path in snapshot_files:
                relative = path.relative_to(snapshot)
                archive.write(path, relative.as_posix())
                written += path.stat().st_size
                if progress is not None:
                    progress(written, total)
            for path in plugin_files:
                relative = Path("plugins") / path.relative_to(current_plugins)
                archive.write(path, relative.as_posix())
                written += path.stat().st_size
                if progress is not None:
                    progress(written, total)

        digest = update_client.calculate_sha256(zip_path)
        checksum_path.write_text(f"{digest}  {zip_path.name}\n", encoding="ascii")
        asset = update_client.ReleaseAsset(
            name=zip_path.name,
            download_url="",
            size=zip_path.stat().st_size,
            sha256=digest,
        )
        release = update_client.ReleaseInfo(
            version=backup.version,
            tag_name=f"local-v{backup.version}",
            name=f"本地备份 v{backup.version}",
            body=f"Local backup: {snapshot}",
            page_url=str(snapshot),
            zip_asset=asset,
            checksum_asset=update_client.ReleaseAsset(
                name=checksum_path.name,
                download_url="",
                size=checksum_path.stat().st_size,
                sha256=digest,
            ),
            sha256=digest,
            manifest_url="local-backup",
        )
        return update_client.PreparedUpdate(
            release=release,
            work_dir=work_dir,
            zip_path=zip_path,
            checksum_path=checksum_path,
            sha256=digest,
        )
    except Exception:
        shutil.rmtree(work_dir, ignore_errors=True)
        raise


def _show_local_prepare_progress(app: Any, done: int, total: int) -> None:
    if total > 0:
        percent = min(100.0, done * 100.0 / total)
        app.update_progress_var.set(percent)
        app.update_status_var.set(f"本地恢复：正在准备备份包 {percent:.1f}%")
    else:
        app.update_status_var.set("本地恢复：正在准备备份包…")


def install_selected_full(app: Any) -> None:
    candidate = _selected_candidate(app)
    if candidate is None or getattr(app, "_update_busy", False):
        return
    if candidate.source == "cloud":
        if candidate.release is None:
            return
        app._available_update = candidate.release
        update_ui.install_available_update(app)
        return

    backup = candidate.backup
    if backup is None:
        return
    if sys.platform != "win32" or not getattr(sys, "frozen", False):
        messagebox.showerror("无法自动恢复", "本地版本恢复仅支持 Windows 打包版。", parent=app.root)
        return
    updater_exe = Path(app._update_app_dir) / update_client.UPDATER_EXE_NAME
    if not updater_exe.is_file():
        messagebox.showerror("缺少更新器", f"找不到独立更新器：\n{updater_exe}", parent=app.root)
        return
    if not messagebox.askyesno(
        "恢复本地版本",
        f"将从本地备份恢复到 v{backup.version}，无需重新联网下载。\n\n"
        "恢复前会先为当前版本再创建一份备份；当前配置、日志、backup 与 plugins 会保留。\n\n"
        "是否继续？",
        parent=app.root,
    ):
        return

    update_ui._discard_prepared_update(app)  # noqa: SLF001 - shared update UI state
    update_ui._set_busy(app, True)  # noqa: SLF001 - shared update UI state
    app.update_progress_var.set(0)
    app.update_status_var.set(f"本地恢复：正在准备 v{backup.version} 备份包…")

    def progress(done: int, total: int) -> None:
        app.root.after(0, lambda: _show_local_prepare_progress(app, done, total))

    def worker() -> None:
        try:
            prepared = prepare_local_backup_restore(backup, app_dir=app._update_app_dir, progress=progress)
        except Exception as exc:  # noqa: BLE001
            app.root.after(0, lambda error=str(exc): update_ui._download_failed(app, error, "本地版本恢复"))  # noqa: SLF001
        else:
            app.root.after(0, lambda: update_ui._download_ready(app, prepared, updater_exe))  # noqa: SLF001

    threading.Thread(target=worker, name="bilipdj-local-restore", daemon=True).start()


def install_selected_incremental(app: Any) -> None:
    candidate = _selected_candidate(app)
    if candidate is None or getattr(app, "_update_busy", False):
        return
    if candidate.source == "local":
        messagebox.showinfo(
            "本地备份恢复",
            "本地备份是完整快照，只支持“恢复旧版”；请选择“恢复旧版”按钮。",
            parent=app.root,
        )
        return
    if candidate.release is None:
        return
    app._available_update = candidate.release
    update_ui.install_incremental_update(app)


__all__ = [
    "LocalBackup",
    "VersionCandidate",
    "build_version_candidates",
    "check_for_versions",
    "discover_local_backups",
    "install_selected_full",
    "install_selected_incremental",
    "on_version_selected",
    "prepare_local_backup_restore",
]
