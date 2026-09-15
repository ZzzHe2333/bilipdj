from __future__ import annotations

import json
from typing import Any

from apps.versioning import is_prerelease_version, version_key

RELEASES_API = "https://api.github.com/repos/ZzzHe2333/bilipdj/releases?per_page=100"


def _release_version(payload: dict[str, Any]) -> str:
    tag = str(payload.get("tag_name", "") or "").strip()
    return tag[1:] if tag.lower().startswith("v") else tag


def select_channel_release(releases: list[Any]) -> dict[str, Any]:
    """Return the newest published non-prerelease GitHub Release.

    Release status, rather than a version suffix, is the update channel source of
    truth. Historical suffixed versions remain parseable for compatibility, but
    they never make a prerelease eligible for the default update target.
    """
    candidates: list[dict[str, Any]] = []
    for raw in releases:
        if not isinstance(raw, dict) or bool(raw.get("draft")) or bool(raw.get("prerelease")):
            continue
        try:
            version_key(_release_version(raw))
        except ValueError:
            continue
        candidates.append(raw)
    if not candidates:
        raise RuntimeError("没有可用的正式 GitHub Release")
    candidates.sort(
        key=lambda item: str(item.get("published_at") or item.get("created_at") or ""),
        reverse=True,
    )
    return candidates[0]


def _manifest_asset_url(payload: dict[str, Any]) -> str:
    assets = payload.get("assets", [])
    if not isinstance(assets, list):
        return ""
    for raw in assets:
        if not isinstance(raw, dict):
            continue
        if str(raw.get("name", "") or "") == "update-manifest.json":
            return str(raw.get("browser_download_url", "") or "").strip()
    return ""


def install_update_channel_guard() -> bool:
    from . import update_client

    if bool(getattr(update_client, "_issue187_update_channel_installed", False)):
        return True

    original_fetch_latest = update_client.fetch_latest_release

    def normalize_version(value: str):
        return version_key(value)

    def is_newer_version(candidate: str, current: str) -> bool:
        return version_key(candidate) > version_key(current)

    def fetch_latest_release(*, timeout: float = 15.0):
        # Always follow the newest published Release. Pre-release packages are
        # selectable manually from the "全部" catalog but never become the
        # automatic/default update target.
        request = update_client._request(RELEASES_API, timeout=timeout)  # noqa: SLF001
        with request as response:
            try:
                payload = json.loads(response.read().decode("utf-8-sig"))
            except (AttributeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise update_client.UpdateError("GitHub Release 列表无法解析") from exc
        if not isinstance(payload, list):
            raise update_client.UpdateError("GitHub Release 列表格式无效")
        try:
            release = select_channel_release(payload)
        except RuntimeError as exc:
            raise update_client.UpdateError(str(exc)) from exc
        manifest_url = _manifest_asset_url(release)
        if manifest_url:
            try:
                with update_client._request(manifest_url, timeout=timeout) as response:  # noqa: SLF001
                    manifest = update_client._decode_json_response(response, "更新清单")  # noqa: SLF001
                if isinstance(manifest.get("packages"), dict):
                    return update_client._release_from_manifest(manifest, source_url=manifest_url)  # noqa: SLF001
            except Exception:
                pass
        return update_client._release_from_github_payload(release)  # noqa: SLF001

    update_client.normalize_version = normalize_version
    update_client.is_newer_version = is_newer_version
    update_client.fetch_latest_release = fetch_latest_release
    update_client._issue187_original_fetch_latest = original_fetch_latest
    update_client._issue187_update_channel_installed = True
    return True


__all__ = [
    "RELEASES_API",
    "install_update_channel_guard",
    "is_prerelease_version",
    "select_channel_release",
    "version_key",
]
