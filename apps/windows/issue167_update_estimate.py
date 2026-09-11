from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

from apps.server.runtime_layout import ensure_runtime_layout

from . import incremental_update, update_client


def _write_sha_sidecar(path: Path, sha256: str) -> None:
    sidecar = path.with_name(path.name + ".sha256")
    sidecar.write_text(f"{sha256}  {path.name}\n", encoding="ascii")


def estimate_incremental_download(
    release: update_client.ReleaseInfo,
    *,
    app_dir: Path,
) -> dict[str, Any]:
    """Scan local managed files and estimate bytes needed by Range transport.

    The cloud file manifest is cached under ``key/`` together with its SHA-256
    sidecar and the resulting estimate. User-owned configuration/archive paths
    are already rejected by the shared incremental manifest parser.
    """

    app_root = Path(app_dir).resolve()
    _core_dir, key_dir = ensure_runtime_layout(app_root)
    assets = incremental_update.fetch_incremental_assets(release)
    manifest_path = key_dir / assets.file_manifest.name
    update_client.download_file(
        assets.file_manifest.download_url,
        manifest_path,
        expected_size=assets.file_manifest.size,
    )
    update_client.verify_sha256(manifest_path, assets.file_manifest.sha256)
    _write_sha_sidecar(manifest_path, assets.file_manifest.sha256)

    files, removed, manifest_base = incremental_update._read_and_validate_file_manifest(  # noqa: SLF001
        manifest_path,
        release=release,
        pack_size=assets.resource_pack.size,
    )
    base_version = manifest_base or assets.base_version
    if assets.base_version and manifest_base:
        if update_client.normalize_version(assets.base_version) != update_client.normalize_version(manifest_base):
            raise incremental_update.IncrementalUpdateError("增量清单的基线版本信息不一致")
    incremental_update._require_exact_base_version(app_root, base_version)  # noqa: SLF001
    needed, unchanged = incremental_update._scan_local(app_root, files)  # noqa: SLF001
    range_bytes = sum(int(files[path]["packed_size"]) for path in needed)
    estimated_bytes = int(assets.file_manifest.size) + range_bytes

    payload = {
        "schema": 1,
        "version": release.version,
        "tag_name": release.tag_name,
        "base_version": base_version,
        "full_download_bytes": int(release.zip_asset.size),
        "incremental_manifest_bytes": int(assets.file_manifest.size),
        "incremental_range_bytes": int(range_bytes),
        "incremental_download_bytes": int(estimated_bytes),
        "replace_count": len(needed),
        "remove_count": len(removed),
        "unchanged_count": int(unchanged),
        "scanned_count": len(files),
        "package_sha256": release.sha256 or release.zip_asset.sha256,
        "file_manifest_sha256": assets.file_manifest.sha256,
        "resource_pack_sha256": assets.resource_pack.sha256,
    }
    (key_dir / "update-estimate.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_sha_sidecar(key_dir / "update-estimate.json", update_client.calculate_sha256(key_dir / "update-estimate.json"))
    return payload


def _mib(value: int | float) -> str:
    return f"{float(value) / 1024**2:.1f} MiB"


def patch_update_ui(update_ui_module: Any) -> bool:
    if bool(getattr(update_ui_module, "_issue167_estimate_patched", False)):
        return True

    original_build = update_ui_module.build_about_tab
    original_success = update_ui_module._check_succeeded
    original_failed = update_ui_module._check_failed

    def build_about_tab(app: Any, frame: Any, app_name: str, current_version: str, app_dir: Path) -> None:
        original_build(app, frame, app_name, current_version, app_dir)
        update_frame = getattr(getattr(app, "_update_progress", None), "master", None)
        if update_frame is None:
            return
        import tkinter as tk
        from tkinter import ttk

        app.update_full_estimate_var = tk.StringVar(value="全量更新预估：等待检测")
        app.update_incremental_estimate_var = tk.StringVar(value="增量更新预估：等待检测")
        row = ttk.Frame(update_frame)
        row.pack(fill="x", pady=(2, 8), before=app._update_progress)
        ttk.Label(row, textvariable=app.update_full_estimate_var).pack(side="left", padx=(0, 24))
        ttk.Label(row, textvariable=app.update_incremental_estimate_var).pack(side="left")
        app._issue167_estimate_row = row

    def _set_estimate_text(app: Any, *, full: str | None = None, incremental: str | None = None) -> None:
        if full is not None and hasattr(app, "update_full_estimate_var"):
            app.update_full_estimate_var.set(full)
        if incremental is not None and hasattr(app, "update_incremental_estimate_var"):
            app.update_incremental_estimate_var.set(incremental)

    def _estimate_worker(app: Any, release: update_client.ReleaseInfo) -> None:
        try:
            result = estimate_incremental_download(release, app_dir=Path(app._update_app_dir))
        except incremental_update.IncrementalUnavailable as exc:
            app.root.after(
                0,
                lambda text=str(exc): _set_estimate_text(
                    app,
                    incremental=f"增量更新预估：不可用（{text}）",
                ),
            )
        except Exception as exc:  # noqa: BLE001
            app.root.after(
                0,
                lambda text=str(exc): _set_estimate_text(
                    app,
                    incremental=f"增量更新预估：计算失败（{text}）",
                ),
            )
        else:
            app.root.after(
                0,
                lambda: _set_estimate_text(
                    app,
                    incremental=(
                        f"增量更新预估：{_mib(result['incremental_download_bytes'])} "
                        f"（需更新 {result['replace_count']} / {result['scanned_count']} 个文件）"
                    ),
                ),
            )

    def check_succeeded(app: Any, release: update_client.ReleaseInfo, silent: bool) -> None:
        original_success(app, release, silent)
        current = getattr(app, "_update_current_version", "")
        is_newer = update_client.is_newer_version(release.version, current)
        _set_estimate_text(
            app,
            full=(f"全量更新预估：{_mib(release.zip_asset.size)}" if is_newer else "全量更新预估：无需更新"),
            incremental=("增量更新预估：正在扫描本地文件…" if is_newer else "增量更新预估：0.0 MiB"),
        )
        if is_newer:
            threading.Thread(target=_estimate_worker, args=(app, release), daemon=True).start()

    def check_failed(app: Any, error: str, silent: bool) -> None:
        _set_estimate_text(app, full="全量更新预估：检测失败", incremental="增量更新预估：检测失败")
        original_failed(app, error, silent)

    update_ui_module.build_about_tab = build_about_tab
    update_ui_module._check_succeeded = check_succeeded
    update_ui_module._check_failed = check_failed
    update_ui_module._issue167_estimate_patched = True
    return True


__all__ = ["estimate_incremental_download", "patch_update_ui"]
