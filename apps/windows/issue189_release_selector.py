from __future__ import annotations

import json
import threading
from typing import Any

from . import update_client, update_ui, update_version_selector
from .issue187_update_channel import RELEASES_API, is_prerelease_version, version_key


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
    return [item for item in payload if isinstance(item, dict) and not bool(item.get("draft"))]


def _newest_release(releases: list[dict[str, Any]], *, prerelease: bool) -> dict[str, Any] | None:
    candidates: list[tuple[Any, dict[str, Any]]] = []
    for raw in releases:
        if bool(raw.get("draft")):
            continue
        if bool(raw.get("prerelease")) != prerelease:
            continue
        version = _release_version(raw)
        try:
            key = version_key(version)
        except ValueError:
            continue
        candidates.append((key, raw))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def _release_info(raw: dict[str, Any], *, timeout: float = 15.0) -> update_client.ReleaseInfo:
    manifest_url = _manifest_asset_url(raw)
    if manifest_url:
        try:
            with update_client._request(manifest_url, timeout=timeout) as response:  # noqa: SLF001
                manifest = update_client._decode_json_response(response, "更新清单")  # noqa: SLF001
            if isinstance(manifest.get("packages"), dict):
                return update_client._release_from_manifest(manifest, source_url=manifest_url)  # noqa: SLF001
        except Exception:
            pass
    return update_client._release_from_github_payload(raw)  # noqa: SLF001


def fetch_release_choices(*, timeout: float = 15.0) -> list[update_client.ReleaseInfo]:
    """Return latest stable first and latest prerelease second when available."""

    releases = _read_release_list(timeout=timeout)
    selected = [
        _newest_release(releases, prerelease=False),
        _newest_release(releases, prerelease=True),
    ]
    infos: list[update_client.ReleaseInfo] = []
    errors: list[str] = []
    seen_tags: set[str] = set()
    for raw in selected:
        if raw is None:
            continue
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
        candidates.append(
            update_version_selector.VersionCandidate(
                label=label,
                source="cloud",
                version=release.version,
                release=release,
            )
        )

    for backup in update_version_selector.discover_local_backups(app_dir):
        base = f"本地备份 · v{backup.version} · {backup.created_at}"
        label = base
        suffix = 2
        while label in used_labels:
            label = f"{base} · #{suffix}"
            suffix += 1
        used_labels.add(label)
        candidates.append(
            update_version_selector.VersionCandidate(
                label=label,
                source="local",
                version=backup.version,
                backup=backup,
            )
        )
    return candidates


def _check_for_versions(app: Any, *, silent: bool = False) -> None:
    if getattr(app, "_update_busy", False):
        return
    update_ui._discard_all_prepared(app)  # noqa: SLF001
    app._available_update = None
    update_ui._set_busy(app, True)  # noqa: SLF001
    app.update_progress_var.set(0)
    app.update_status_var.set("正在读取云端正式版、测试版与本地备份…")
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
        candidates = _build_version_candidates(app._update_app_dir, releases)
        default_release = releases[0] if releases else None
        app.root.after(
            0,
            lambda: update_version_selector._finish_version_check(  # noqa: SLF001
                app,
                candidates,
                default_release,
                cloud_error,
                silent,
            ),
        )

    threading.Thread(target=worker, name="bilipdj-version-catalog", daemon=True).start()


def install_issue189_release_selector() -> bool:
    if bool(getattr(update_version_selector, "_issue189_release_selector_installed", False)):
        return True
    update_version_selector.build_version_candidates = _build_version_candidates  # type: ignore[assignment]
    update_version_selector.check_for_versions = _check_for_versions  # type: ignore[assignment]
    update_version_selector._issue189_fetch_release_choices = fetch_release_choices
    update_version_selector._issue189_release_selector_installed = True
    return True


__all__ = ["fetch_release_choices", "install_issue189_release_selector"]
