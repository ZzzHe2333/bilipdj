from __future__ import annotations

import functools
import sys
import tkinter as tk
from dataclasses import replace
from tkinter import messagebox, ttk
from typing import Any

from apps.update_download_source import (
    GH_PROXY_SOURCE,
    OFFICIAL_SOURCE,
    SOURCE_LABELS,
    normalize_download_source,
    official_url_from_accelerated,
    rewrite_download_url,
)

from . import incremental_update, update_client, update_page, update_ui

_RELEASE_SOURCES: dict[int, str] = {}


def _selected_source(app: Any) -> str:
    variable = getattr(app, "update_download_source_var", None)
    if variable is None:
        return OFFICIAL_SOURCE
    try:
        return normalize_download_source(variable.get())
    except Exception:
        return OFFICIAL_SOURCE


def _register_release_source(release: update_client.ReleaseInfo, source: str) -> update_client.ReleaseInfo:
    selected = normalize_download_source(source)
    if selected == OFFICIAL_SOURCE:
        return release
    clone = replace(
        release,
        zip_asset=replace(
            release.zip_asset,
            download_url=rewrite_download_url(release.zip_asset.download_url, selected),
        ),
        checksum_asset=replace(
            release.checksum_asset,
            download_url=rewrite_download_url(release.checksum_asset.download_url, selected),
        ),
    )
    _RELEASE_SOURCES[id(clone)] = selected
    return clone


def _release_source(release: update_client.ReleaseInfo) -> str:
    return _RELEASE_SOURCES.get(id(release), OFFICIAL_SOURCE)


def _confirm_download_source(app: Any, mode_label: str) -> str:
    selected = _selected_source(app)
    if selected != GH_PROXY_SOURCE:
        return OFFICIAL_SOURCE
    if bool(getattr(app, "_update_proxy_approved_for_retry", False)):
        app._update_proxy_approved_for_retry = False
        return GH_PROXY_SOURCE
    approved = messagebox.askyesno(
        "确认使用第三方加速",
        f"你选择了 {SOURCE_LABELS[GH_PROXY_SOURCE]}。\n\n"
        f"本次{mode_label}的 GitHub Release 文件会经过第三方服务 gh-proxy.com 传输。\n"
        "GitHub API/版本信息仍从官方读取，下载完成后仍执行现有文件大小与 SHA-256 校验。\n"
        "本次确认不会保存，也不会建立长期信任；下次使用第三方加速仍会再次询问。\n\n"
        "选择“否”不会取消更新，而是仅本次改用 GitHub 官方下载。\n\n"
        "是否仅本次使用第三方加速？",
        parent=getattr(app, "root", None),
    )
    if approved:
        return GH_PROXY_SOURCE
    status = getattr(app, "update_status_var", None)
    if status is not None:
        try:
            status.set("已取消第三方加速，本次将使用 GitHub 官方下载。")
        except Exception:
            pass
    return OFFICIAL_SOURCE


def _find_labeled_frame(root: Any, text: str) -> Any | None:
    try:
        children = list(root.winfo_children())
    except Exception:
        return None
    for child in children:
        try:
            if str(child.cget("text") or "") == text:
                return child
        except Exception:
            pass
        nested = _find_labeled_frame(child, text)
        if nested is not None:
            return nested
    return None


def _install_source_picker() -> None:
    current = update_page.build_update_tab
    if bool(getattr(current, "_bilipdj_download_source_picker", False)):
        return

    @functools.wraps(current)
    def build_update_tab_with_download_source(app: Any, frame: Any, *args: Any, **kwargs: Any) -> Any:
        result = current(app, frame, *args, **kwargs)
        app.update_download_source_var = tk.StringVar(
            master=getattr(app, "root", None),
            value=SOURCE_LABELS[OFFICIAL_SOURCE],
        )
        settings_content = getattr(app, "_update_settings_content", None)
        if settings_content is None or getattr(app, "_update_download_source_frame", None) is not None:
            return result
        source_frame = ttk.Frame(settings_content)
        source_frame.grid(row=0, column=0, sticky="ew", padx=(10, 0), pady=(6, 10))
        source_frame.columnconfigure(1, weight=1)
        ttk.Label(source_frame, text="下载线路").grid(row=0, column=0, sticky="w", padx=(0, 10))
        combo = ttk.Combobox(
            source_frame,
            textvariable=app.update_download_source_var,
            values=(SOURCE_LABELS[OFFICIAL_SOURCE], SOURCE_LABELS[GH_PROXY_SOURCE]),
            state="readonly",
            width=24,
        )
        combo.grid(row=0, column=1, sticky="w")
        app._update_download_source_frame = source_frame
        app._update_download_source_combo = combo
        return result

    setattr(build_update_tab_with_download_source, "_bilipdj_download_source_picker", True)
    update_page.build_update_tab = build_update_tab_with_download_source


def _install_confirmation_wrappers() -> None:
    full_current = update_ui.install_available_update
    if not bool(getattr(full_current, "_bilipdj_download_source_confirm", False)):
        @functools.wraps(full_current)
        def install_available_update_with_source(app: Any) -> Any:
            release = getattr(app, "_available_update", None)
            if (
                release is None
                or getattr(app, "_update_busy", False)
                or sys.platform != "win32"
                or not getattr(sys, "frozen", False)
            ):
                return full_current(app)
            selected = _confirm_download_source(app, "全量更新")
            if selected == OFFICIAL_SOURCE:
                return full_current(app)
            accelerated = _register_release_source(release, selected)
            app._available_update = accelerated
            try:
                return full_current(app)
            finally:
                app._available_update = release

        setattr(install_available_update_with_source, "_bilipdj_download_source_confirm", True)
        update_ui.install_available_update = install_available_update_with_source

    incremental_current = update_ui.install_incremental_update
    if not bool(getattr(incremental_current, "_bilipdj_download_source_confirm", False)):
        @functools.wraps(incremental_current)
        def install_incremental_update_with_source(app: Any) -> Any:
            release = getattr(app, "_available_update", None)
            if (
                release is None
                or getattr(app, "_update_busy", False)
                or sys.platform != "win32"
                or not getattr(sys, "frozen", False)
            ):
                return incremental_current(app)
            selected = _confirm_download_source(app, "增量更新")
            if selected == OFFICIAL_SOURCE:
                return incremental_current(app)
            accelerated = _register_release_source(release, selected)
            app._available_update = accelerated
            try:
                return incremental_current(app)
            finally:
                app._available_update = release

        setattr(install_incremental_update_with_source, "_bilipdj_download_source_confirm", True)
        update_ui.install_incremental_update = install_incremental_update_with_source


def _install_transport_wrappers() -> None:
    fetch_assets_current = incremental_update.fetch_incremental_assets
    if not bool(getattr(fetch_assets_current, "_bilipdj_download_source", False)):
        @functools.wraps(fetch_assets_current)
        def fetch_incremental_assets_with_source(release: update_client.ReleaseInfo, *args: Any, **kwargs: Any):
            assets = fetch_assets_current(release, *args, **kwargs)
            source = _release_source(release)
            if source == OFFICIAL_SOURCE:
                return assets
            return replace(
                assets,
                file_manifest=replace(
                    assets.file_manifest,
                    download_url=rewrite_download_url(assets.file_manifest.download_url, source),
                ),
                resource_pack=replace(
                    assets.resource_pack,
                    download_url=rewrite_download_url(assets.resource_pack.download_url, source),
                ),
            )

        setattr(fetch_incremental_assets_with_source, "_bilipdj_download_source", True)
        incremental_update.fetch_incremental_assets = fetch_incremental_assets_with_source

    download_current = update_client.download_file
    if not bool(getattr(download_current, "_bilipdj_download_source_fallback", False)):
        @functools.wraps(download_current)
        def download_file_with_official_fallback(url: str, *args: Any, **kwargs: Any):
            try:
                return download_current(url, *args, **kwargs)
            except Exception:
                official = official_url_from_accelerated(url)
                if official == str(url or "").strip():
                    raise
                return download_current(official, *args, **kwargs)

        setattr(download_file_with_official_fallback, "_bilipdj_download_source_fallback", True)
        update_client.download_file = download_file_with_official_fallback

    range_current = incremental_update._download_range  # noqa: SLF001
    if not bool(getattr(range_current, "_bilipdj_download_source_fallback", False)):
        @functools.wraps(range_current)
        def download_range_with_official_fallback(url: str, *args: Any, **kwargs: Any):
            try:
                return range_current(url, *args, **kwargs)
            except incremental_update.IncrementalUpdateError:
                official = official_url_from_accelerated(url)
                if official == str(url or "").strip():
                    raise
                return range_current(official, *args, **kwargs)

        setattr(download_range_with_official_fallback, "_bilipdj_download_source_fallback", True)
        incremental_update._download_range = download_range_with_official_fallback  # type: ignore[attr-defined]  # noqa: SLF001


def _is_official_download_network_failure(error: str) -> bool:
    """Do not offer a different transport for checksum or validation errors."""
    value = str(error or "").casefold()
    if any(term in value for term in ("sha-256", "sha256", "校验", "版本不兼容", "增量基线", "格式无效", "签名")):
        return False
    return any(term in value for term in (
        "网络请求失败", "网络错误", "连接失败", "连接超时",
        "下载失败", "无法下载", "urlopen", "timed out", "timeout",
        "connection reset", "connection refused", "http error",
        "http 403", "http 404", "http 502", "http 503", "http 504",
    ))


def _offer_proxy_after_download_failure(app: Any, error: str) -> bool:
    """Ask in the Tk thread; consent is scoped to exactly the next attempt."""
    if _selected_source(app) != OFFICIAL_SOURCE or not _is_official_download_network_failure(error):
        return False
    if not messagebox.askyesno(
        "GitHub 官方下载失败",
        f"GitHub 官方源下载失败：\n{error}\n\n"
        "是否仅这一次使用 gh-proxy.com 重新尝试？\n"
        "下载内容会经过第三方服务，完成后仍会验证 SHA-256；"
        "拒绝不会更改默认下载线路。",
        parent=getattr(app, "root", None),
    ):
        return False
    variable = getattr(app, "update_download_source_var", None)
    if variable is not None:
        variable.set(SOURCE_LABELS[GH_PROXY_SOURCE])
    app._update_proxy_approved_for_retry = True
    return True


def _install_official_failure_prompt() -> None:
    original_failed = update_ui._download_failed  # noqa: SLF001
    if bool(getattr(original_failed, "_bilipdj_official_failure_prompt", False)):
        return

    @functools.wraps(original_failed)
    def failed_with_proxy_offer(app: Any, error: str, mode: str = "更新") -> Any:
        if mode in {"全量更新", "增量更新"} and _selected_source(app) == OFFICIAL_SOURCE:
            # This callback runs on the Tk thread after the worker reported
            # failure. Restore idle state before offering a fresh attempt.
            update_ui._set_busy(app, False)  # noqa: SLF001
            if _offer_proxy_after_download_failure(app, error):
                app.update_status_var.set("已确认本次第三方加速，正在重新开始下载…")
                if mode == "增量更新":
                    return update_ui.install_incremental_update(app)
                return update_ui.install_available_update(app)
        return original_failed(app, error, mode)

    setattr(failed_with_proxy_offer, "_bilipdj_official_failure_prompt", True)
    update_ui._download_failed = failed_with_proxy_offer  # type: ignore[attr-defined]  # noqa: SLF001


def install_download_acceleration() -> bool:
    _install_transport_wrappers()
    _install_confirmation_wrappers()
    _install_official_failure_prompt()
    _install_source_picker()
    return True


__all__ = ["install_download_acceleration"]
