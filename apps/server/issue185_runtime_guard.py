from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any

REPOSITORY = "ZzzHe2333/bilipdj"
LATEST_RELEASE_API = f"https://api.github.com/repos/{REPOSITORY}/releases/latest"
RELEASES_API = f"https://api.github.com/repos/{REPOSITORY}/releases?per_page=20"
USER_AGENT = "bilipdj-issue185-release-channel"


def _request_json(url: str, *, timeout: float = 10.0) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json, application/json",
            "User-Agent": USER_AGENT,
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8-sig"))


def _is_prerelease_version(version: str) -> bool:
    text = str(version or "").strip().lower().lstrip("v")
    return "-" in text


def _select_release(current_version: str) -> dict[str, Any]:
    if not _is_prerelease_version(current_version):
        payload = _request_json(LATEST_RELEASE_API)
        if not isinstance(payload, dict):
            raise RuntimeError("GitHub latest Release 响应无效")
        return payload

    payload = _request_json(RELEASES_API)
    if not isinstance(payload, list):
        raise RuntimeError("GitHub Release 列表响应无效")
    for item in payload:
        if not isinstance(item, dict) or bool(item.get("draft")):
            continue
        return item
    raise RuntimeError("没有可用的 GitHub Release")


def _asset(release: dict[str, Any], *, exact: str = "", suffix: str = "") -> dict[str, Any] | None:
    assets = release.get("assets", [])
    if not isinstance(assets, list):
        return None
    for item in assets:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "") or "")
        if exact and name == exact:
            return item
        if suffix and name.endswith(suffix):
            return item
    return None


def install_issue185_runtime_guards(server_module: Any) -> bool:
    """Keep Web update discovery on the installed release channel.

    GitHub's ``/releases/latest`` deliberately excludes prereleases.  A
    ``*-test`` Web Portable build therefore used to report an older stable
    release and incorrectly concluded that Web incremental assets were absent.
    Stable builds still follow ``latest``; prerelease builds follow the most
    recently published non-draft release, including prereleases.
    """

    from apps.server import update_estimate_api, web_control_guard, web_update_api

    current_version = lambda: web_update_api._version(server_module)  # noqa: E731, SLF001
    original_load_manifest = web_update_api._load_manifest  # noqa: SLF001

    def load_manifest_for_channel() -> dict[str, Any]:
        version = current_version()
        try:
            release = _select_release(version)
            manifest_asset = _asset(release, exact="update-manifest.json")
            if not isinstance(manifest_asset, dict):
                raise RuntimeError("所选 Release 缺少 update-manifest.json")
            manifest_url = str(manifest_asset.get("browser_download_url", "") or "").strip()
            if not manifest_url:
                raise RuntimeError("update-manifest.json 缺少下载地址")
            payload = _request_json(manifest_url)
            if not isinstance(payload, dict):
                raise RuntimeError("更新清单格式无效")
            packages = payload.get("packages")
            package = packages.get(web_update_api.WEB_PACKAGE_KEY) if isinstance(packages, dict) else None
            if not isinstance(package, dict):
                raise RuntimeError(f"更新清单缺少 {web_update_api.WEB_PACKAGE_KEY}")
            filename = str(package.get("filename", "") or "").strip()
            download_url = str(package.get("url", "") or "").strip()
            if not filename or not download_url:
                raise RuntimeError("Web 更新包缺少 filename/url")
            package = dict(package)
            package["sha256"] = web_update_api._normalize_sha(package.get("sha256"), filename)  # noqa: SLF001
            package["size"] = int(package.get("size", 0) or 0)
            if package["size"] < 0:
                raise RuntimeError("Web 更新包大小无效")
            payload = dict(payload)
            payload["packages"] = dict(packages)
            payload["packages"][web_update_api.WEB_PACKAGE_KEY] = package
            payload["_source_url"] = manifest_url
            payload["_release_prerelease"] = bool(release.get("prerelease"))
            return payload
        except Exception:
            if _is_prerelease_version(version):
                raise
            return original_load_manifest()

    def fetch_release_for_estimate() -> dict[str, Any]:
        return _select_release(current_version())

    def update_payload_for_channel(module: Any) -> dict[str, Any]:
        current = web_control_guard._read_version(module)  # noqa: SLF001
        try:
            payload = _select_release(current)
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": f"检查更新失败：{exc}", "current_version": current}
        return {
            "status": "ok",
            "current_version": current,
            "latest_version": str(payload.get("tag_name", "") or "").lstrip("v"),
            "tag_name": str(payload.get("tag_name", "") or ""),
            "name": str(payload.get("name", "") or ""),
            "published_at": str(payload.get("published_at", "") or ""),
            "html_url": str(payload.get("html_url", "") or ""),
            "body": str(payload.get("body", "") or "")[:12000],
            "prerelease": bool(payload.get("prerelease")),
        }

    web_update_api._load_manifest = load_manifest_for_channel  # type: ignore[attr-defined]  # noqa: SLF001
    update_estimate_api._fetch_latest_release = fetch_release_for_estimate  # type: ignore[attr-defined]  # noqa: SLF001
    web_control_guard._update_payload = update_payload_for_channel  # type: ignore[attr-defined]  # noqa: SLF001
    return True


__all__ = ["install_issue185_runtime_guards"]
