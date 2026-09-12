from __future__ import annotations

import shutil
import sys
import threading
import zipfile
from pathlib import Path
from typing import Any

from apps.update_workspace import allocate_update_session, cleanup_update_session

_PATCH_LOCK = threading.RLock()


def _application_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def install_windows_update_workspace() -> bool:
    """Keep full, incremental and local-restore preparation under app_dir/update."""

    # The frozen JavaScript probe launches an isolated plugin runtime and must
    # stay free of updater monkeypatches/import side effects. Normal desktop
    # startup and the GUI startup probe still install this workspace patch.
    if "--plugin-runtime-self-test" in sys.argv[1:]:
        return True

    from . import incremental_update, update_client, update_version_selector

    with _PATCH_LOCK:
        if bool(getattr(update_client, "_issue222_update_workspace_installed", False)):
            return True

        original_full = update_client.prepare_release_download
        original_incremental = incremental_update.prepare_incremental_download

        def prepare_full_local(
            release: Any,
            *,
            progress: Any = None,
            work_dir: Path | None = None,
        ) -> Any:
            created: Path | None = None
            target = Path(work_dir) if work_dir is not None else allocate_update_session(_application_dir(), "windows-full")
            if work_dir is None:
                created = target
            try:
                return original_full(release, progress=progress, work_dir=target)
            except Exception:
                if created is not None:
                    cleanup_update_session(_application_dir(), created)
                raise

        def prepare_incremental_local(
            release: Any,
            *,
            app_dir: Path,
            progress: Any = None,
            work_dir: Path | None = None,
        ) -> Any:
            app_root = Path(app_dir).resolve()
            created: Path | None = None
            target = Path(work_dir) if work_dir is not None else allocate_update_session(app_root, "windows-incremental")
            if work_dir is None:
                created = target
            try:
                return original_incremental(
                    release,
                    app_dir=app_root,
                    progress=progress,
                    work_dir=target,
                )
            except Exception:
                if created is not None:
                    cleanup_update_session(app_root, created)
                raise

        def prepare_local_backup_restore(
            backup: Any,
            *,
            app_dir: Path,
            progress: Any = None,
        ) -> Any:
            app_root = Path(app_dir).resolve()
            snapshot = update_version_selector._validated_backup_path(app_root, backup)  # noqa: SLF001
            work_dir = allocate_update_session(app_root, "windows-restore")
            zip_path = work_dir / f"BiliPDJ-local-backup-v{backup.version}.zip"
            checksum_path = zip_path.with_suffix(zip_path.suffix + ".sha256")
            try:
                snapshot_files = update_version_selector._iter_archive_files(snapshot, skip_top={"plugins", "update"})  # noqa: SLF001
                current_plugins = app_root / "plugins"
                plugin_files = update_version_selector._iter_archive_files(current_plugins) if current_plugins.is_dir() else []  # noqa: SLF001
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
                asset = update_client.ReleaseAsset(zip_path.name, "", zip_path.stat().st_size, digest)
                release = update_client.ReleaseInfo(
                    version=backup.version,
                    tag_name=f"local-v{backup.version}",
                    name=f"本地备份 v{backup.version}",
                    body=f"Local backup: {snapshot}",
                    page_url=str(snapshot),
                    zip_asset=asset,
                    checksum_asset=update_client.ReleaseAsset(
                        checksum_path.name,
                        "",
                        checksum_path.stat().st_size,
                        digest,
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
                cleanup_update_session(app_root, work_dir)
                raise

        update_client.prepare_release_download = prepare_full_local
        incremental_update.prepare_incremental_download = prepare_incremental_local
        update_version_selector.prepare_local_backup_restore = prepare_local_backup_restore
        update_client._issue222_update_workspace_installed = True
        return True


__all__ = ["install_windows_update_workspace"]
