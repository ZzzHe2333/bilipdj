from __future__ import annotations

import json
import threading
from typing import Any

from . import update_client, update_ui, update_version_selector
from .update_channel import RELEASES_API, is_prerelease_version, version_key

RECENT_RELEASE_LIMIT = 10
_RELEASE_INCREMENTAL_META: dict[str, tuple[str, str]] = {}


def _release_version(payload: dict[str, Any]) -> str:
    tag = str(payload.get("tag_name", "") or "").strip()
    return tag[1:] if tag.lower().startswith("v") else tag


def _manifest_asset_url(payload: dict[str, Any]) -> str:
    assets = payload.get("assets", [])
    if not isinstance(assets, list):
        return ""
    for raw in assets:
        if not isinstance(raw, dict):
            continue
        if str(raw.get("name", "") or "").strip() == "update-manifest.json":
            return str(raw.get("browser_download_url", "") or "").strip()
    return ""


def _read_release_list(*, timeout: float = 15.0) -> list[dict[str, Any]]:
    with update_client._request(RELEASES_API, timeout=timeout) as response:  # noqa: SLF001
        try:
            payload = json.loads(response.read().decode("utf-8-sig"))
        except (AttributeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise update_client.UpdateError("GitHub Release 列表无法解析") from exc
    if not isinstance(payload, list):
        raise update_client.UpdateError("GitHub Release 列表格式无效")
    releases = [item for item in payload if isinstance(item, dict) and not bool(item.get("draft"))]
    releases.sort(key=lambda item: str(item.get("published_at") or item.get("created_at") or ""), reverse=True)
    return releases


def _newest_release(releases: list[dict[str, Any]], *, prerelease: bool) -> dict[str, Any] | None:
    candidates: list[tuple[Any, dict[str, Any]]] = []
    for raw in releases:
        if bool(raw.get("draft")) or bool(raw.get("prerelease")) != prerelease:
            continue
        version = _release_version(raw)
        try:
            key = version_key(version)
        except ValueError:
            continue
        candidates.append((key, raw))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def _record_incremental_metadata(raw: dict[str, Any], manifest: dict[str, object]) -> None:
    tag = str(raw.get("tag_name", "") or "").strip()
    if not tag:
        return
    base_version = ""
    transport = ""
    packages = manifest.get("packages")
    if isinstance(packages, dict):
        package = packages.get(update_client.WINDOWS_PACKAGE_KEY)
        if isinstance(package, dict):
            incremental = package.get("incremental")
            if isinstance(incremental, dict):
                base_version = str(incremental.get("base_version", "") or "").strip()
                transport = str(incremental.get("transport", "") or "").strip().lower()
    _RELEASE_INCREMENTAL_META[tag] = (base_version, transport)


def _release_info(raw: dict[str, Any], *, timeout: float = 15.0) -> update_client.ReleaseInfo:
    tag = str(raw.get("tag_name", "") or "").strip()
    if tag:
        _RELEASE_INCREMENTAL_META.pop(tag, None)
    manifest_url = _manifest_asset_url(raw)
    if manifest_url:
        try:
            with update_client._request(manifest_url, timeout=timeout) as response:  # noqa: SLF001
                manifest = update_client._decode_json_response(response, "更新清单")  # noqa: SLF001
            if isinstance(manifest.get("packages"), dict):
                _record_incremental_metadata(raw, manifest)
                return update_client._release_from_manifest(manifest, source_url=manifest_url)  # noqa: SLF001
        except Exception:
            pass
    return update_client._release_from_github_payload(raw)  # noqa: SLF001


def fetch_release_choices(*, timeout: float = 15.0) -> list[update_client.ReleaseInfo]:
    releases = _read_release_list(timeout=timeout)
    _stable_anchor = _newest_release(releases, prerelease=False)
    _test_anchor = _newest_release(releases, prerelease=True)
    del _stable_anchor, _test_anchor

    infos: list[update_client.ReleaseInfo] = []
    errors: list[str] = []
    seen_tags: set[str] = set()
    _RELEASE_INCREMENTAL_META.clear()
    for raw in releases:
        tag = str(raw.get("tag_name", "") or "").strip()
        if not tag or tag in seen_tags:
            continue
        try:
            info = _release_info(raw, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{tag}: {exc}")
            continue
        seen_tags.add(tag)
        infos.append(info)
        if len(infos) >= RECENT_RELEASE_LIMIT:
            break
    if not infos:
        raise update_client.UpdateError("无法读取可用云端版本" + (f"：{'；'.join(errors)}" if errors else ""))
    return infos


def _build_version_candidates(app_dir, cloud_releases):
    candidates: list[update_version_selector.VersionCandidate] = []
    used_labels: set[str] = set()
    for release in cloud_releases or []:
        try:
            prerelease = is_prerelease_version(release.version)
        except ValueError:
            prerelease = "-" in str(release.version)
        prefix = "云端测试版" if prerelease else "云端正式版"
        label = f"{prefix} · v{release.version}"
        if label in used_labels:
            continue
        used_labels.add(label)
        candidates.append(update_version_selector.VersionCandidate(label=label, source="cloud", version=release.version, release=release))

    for backup in update_version_selector.discover_local_backups(app_dir):
        base = f"本地备份 · v{backup.version} · {backup.created_at}"
        label = base
        suffix = 2
        while label in used_labels:
            label = f"{base} · #{suffix}"
            suffix += 1
        used_labels.add(label)
        candidates.append(update_version_selector.VersionCandidate(label=label, source="local", version=backup.version, backup=backup))
    return candidates


def _incremental_eligible_tags(current_version: str, releases: list[update_client.ReleaseInfo]) -> set[str]:
    try:
        current_key = version_key(current_version)
    except ValueError:
        return set()
    eligible: set[str] = set()
    for release in releases:
        base_version, transport = _RELEASE_INCREMENTAL_META.get(release.tag_name, ("", ""))
        if transport != "http-range" or not base_version:
            continue
        try:
            if version_key(base_version) == current_key:
                eligible.add(release.tag_name)
        except ValueError:
            continue
    return eligible


def _operation_text(candidate_version: str, current_version: str) -> str:
    try:
        candidate_key = version_key(candidate_version)
        current_key = version_key(current_version)
    except ValueError:
        return "安装"
    if candidate_key > current_key:
        return "升级"
    if candidate_key < current_key:
        return "降级"
    return "重新安装"


def _install_candidate_button_policy() -> None:
    current = getattr(update_version_selector, "_configure_buttons_for_candidate", None)
    if callable(current) and not bool(getattr(current, "_recent_release_policy", False)):
        def configure_buttons(app: Any, candidate: Any) -> None:
            current(app, candidate)
            if getattr(app, "_update_busy", False) or candidate is None or getattr(candidate, "source", "") != "cloud":
                return
            full_button = getattr(app, "_update_full_button", None)
            incremental_button = getattr(app, "_update_incremental_button", None)
            operation = _operation_text(str(getattr(candidate, "version", "")), str(getattr(app, "_update_current_version", "")))
            if full_button is not None:
                label = {"升级": "全量升级", "降级": "全量降级", "重新安装": "重新安装"}.get(operation, "全量安装")
                full_button.configure(text=label, state="normal")
            release = getattr(candidate, "release", None)
            tag = str(getattr(release, "tag_name", "") or "")
            eligible = tag in set(getattr(app, "_incremental_eligible_tags", set()) or set())
            if incremental_button is not None:
                incremental_button.configure(state="normal" if eligible else "disabled")

        setattr(configure_buttons, "_recent_release_policy", True)
        update_version_selector._configure_buttons_for_candidate = configure_buttons  # type: ignore[attr-defined]

    current_selected = getattr(update_version_selector, "on_version_selected", None)
    if callable(current_selected) and not bool(getattr(current_selected, "_recent_release_status", False)):
        def on_version_selected(app: Any) -> None:
            current_selected(app)
            candidate = update_version_selector._selected_candidate(app)  # noqa: SLF001
            if candidate is None or candidate.source != "cloud" or candidate.release is None:
                return
            operation = _operation_text(candidate.version, str(getattr(app, "_update_current_version", "")))
            incremental = candidate.release.tag_name in set(getattr(app, "_incremental_eligible_tags", set()) or set())
            size_mb = candidate.release.zip_asset.size / 1024**2
            suffix = "增量可用" if incremental else "该目标与当前版本无兼容增量基线，仅支持全量"
            app.update_status_var.set(f"已选择 v{candidate.version} · {operation} · 完整包约 {size_mb:.1f} MB；{suffix}。")
            update_version_selector._configure_buttons_for_candidate(app, candidate)  # noqa: SLF001

        setattr(on_version_selected, "_recent_release_status", True)
        update_version_selector.on_version_selected = on_version_selected


def _check_for_versions(app: Any, *, silent: bool = False) -> None:
    if getattr(app, "_update_busy", False):
        return
    update_ui._discard_all_prepared(app)  # noqa: SLF001
    app._available_update = None
    update_ui._set_busy(app, True)  # noqa: SLF001
    app.update_progress_var.set(0)
    app.update_status_var.set("正在读取最近 10 个云端版本与本地备份…")
    picker = getattr(app, "_update_version_combo", None)
    if picker is not None:
        picker.configure(state="disabled")

    def worker() -> None:
        releases: list[update_client.ReleaseInfo] = []
        cloud_error = ""
        try:
            releases = fetch_release_choices()
        except Exception as exc:  # noqa: BLE001
            cloud_error = str(exc)
        app._incremental_eligible_tags = _incremental_eligible_tags(str(getattr(app, "_update_current_version", "")), releases)
        candidates = _build_version_candidates(app._update_app_dir, releases)
        default_release = releases[0] if releases else None
        app.root.after(
            0,
            lambda: update_version_selector._finish_version_check(  # noqa: SLF001
                app, candidates, default_release, cloud_error, silent
            ),
        )

    threading.Thread(target=worker, name="bilipdj-version-catalog", daemon=True).start()


def install_release_selector() -> bool:
    if bool(getattr(update_version_selector, "_release_selector_installed", False)):
        return True
    update_version_selector.build_version_candidates = _build_version_candidates  # type: ignore[assignment]
    update_version_selector.check_for_versions = _check_for_versions  # type: ignore[assignment]
    update_version_selector._fetch_release_choices = fetch_release_choices
    _install_candidate_button_policy()
    update_version_selector._release_selector_installed = True
    return True


__all__ = ["RECENT_RELEASE_LIMIT", "fetch_release_choices", "install_release_selector"]
