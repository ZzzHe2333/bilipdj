from __future__ import annotations

import re
import threading
from typing import Any

from apps.server import issue185_runtime_guard, update_estimate_api, web_control_guard, web_update_api

_PATCH_LOCK = threading.RLock()
_LAUNCH_LOCK = threading.RLock()


def _release_version(payload: dict[str, Any]) -> str:
    tag = str(payload.get("tag_name", "") or "").strip()
    return tag[1:] if tag.lower().startswith("v") else tag


def _version_key(value: str) -> tuple[tuple[int, ...], int, tuple[tuple[int, int, str], ...]]:
    text = str(value or "").strip().lstrip("vV").split("+", 1)[0]
    if "-" in text:
        core_text, prerelease = text.split("-", 1)
    else:
        core_text, prerelease = text, ""
    if not re.fullmatch(r"\d+(?:\.\d+)*", core_text):
        raise ValueError(value)
    core = tuple(int(part) for part in core_text.split("."))
    core = core + (0,) * max(0, 3 - len(core))
    if not prerelease:
        return core, 1, ()
    tokens: list[tuple[int, int, str]] = []
    for raw in prerelease.split("."):
        token = raw.strip()
        if not token or not re.fullmatch(r"[0-9A-Za-z-]+", token):
            raise ValueError(value)
        tokens.append((0, int(token), "") if token.isdigit() else (1, 0, token.casefold()))
    return core, 0, tuple(tokens)


def _latest(releases: list[dict[str, Any]], *, prerelease: bool) -> dict[str, Any] | None:
    candidates: list[tuple[Any, dict[str, Any]]] = []
    for release in releases:
        if bool(release.get("draft")) or bool(release.get("prerelease")) != prerelease:
            continue
        try:
            key = _version_key(_release_version(release))
        except ValueError:
            continue
        candidates.append((key, release))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def _release_list() -> list[dict[str, Any]]:
    payload = issue185_runtime_guard._request_json(issue185_runtime_guard.RELEASES_API)  # noqa: SLF001
    if not isinstance(payload, list):
        raise RuntimeError("GitHub Release 列表响应无效")
    return [item for item in payload if isinstance(item, dict) and not bool(item.get("draft"))]


def _validated_manifest(release: dict[str, Any]) -> dict[str, Any]:
    asset = issue185_runtime_guard._asset(release, exact="update-manifest.json")  # noqa: SLF001
    if not isinstance(asset, dict):
        raise RuntimeError(f"{release.get('tag_name', 'Release')} 缺少 update-manifest.json")
    url = str(asset.get("browser_download_url", "") or "").strip()
    if not url:
        raise RuntimeError("update-manifest.json 缺少下载地址")
    payload = issue185_runtime_guard._request_json(url)  # noqa: SLF001
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
    payload["_source_url"] = url
    payload["_release_prerelease"] = bool(release.get("prerelease"))
    return payload


def _catalog() -> list[dict[str, Any]]:
    releases = _release_list()
    chosen = [_latest(releases, prerelease=False), _latest(releases, prerelease=True)]
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    errors: list[str] = []
    for release in chosen:
        if release is None:
            continue
        tag = str(release.get("tag_name", "") or "").strip()
        if not tag or tag in seen:
            continue
        try:
            manifest = _validated_manifest(release)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{tag}: {exc}")
            continue
        seen.add(tag)
        result.append({"release": release, "manifest": manifest})
    if not result:
        raise RuntimeError("无法读取可用 Release" + (f"：{'；'.join(errors)}" if errors else ""))
    return result


def _summary(entry: dict[str, Any]) -> dict[str, Any]:
    release = entry["release"]
    manifest = entry["manifest"]
    package = manifest["packages"][web_update_api.WEB_PACKAGE_KEY]
    incremental = package.get("incremental") if isinstance(package, dict) else None
    file_manifest = package.get("file_manifest") if isinstance(package, dict) else None
    return {
        "version": str(manifest.get("version", "") or _release_version(release)),
        "tag_name": str(release.get("tag_name", "") or manifest.get("tag_name", "") or ""),
        "release_url": str(release.get("html_url", "") or manifest.get("release_url", "") or ""),
        "prerelease": bool(release.get("prerelease")),
        "filename": str(package.get("filename", "") or ""),
        "full_download_bytes": int(package.get("size", 0) or 0),
        "incremental_available": isinstance(incremental, dict) and isinstance(file_manifest, dict),
        "incremental_max_bytes": int(incremental.get("size", 0) or 0) if isinstance(incremental, dict) else 0,
        "incremental_base_version": str(incremental.get("base_version", "") or "") if isinstance(incremental, dict) else "",
    }


def install_issue189_release_selector(server_module: Any) -> bool:
    with _PATCH_LOCK:
        if bool(getattr(web_update_api, "_issue189_release_selector_installed", False)):
            return True

        original_launch = web_update_api._launch_update  # noqa: SLF001

        def state_payload(_module: Any) -> dict[str, Any]:
            app_dir = web_update_api.Path(getattr(server_module, "APP_DIR", ".")).resolve()
            updater = app_dir / web_update_api.WEB_UPDATER_EXE
            result: dict[str, Any] = {
                "status": "ok",
                "current_version": web_update_api._version(server_module),  # noqa: SLF001
                "frozen": bool(getattr(web_update_api.sys, "frozen", False)),
                "updater_available": updater.is_file(),
                "backups": web_update_api._discover_backups(app_dir),  # noqa: SLF001
                "cloud": None,
                "releases": [],
                "default_tag": "",
                "default_version": "",
                "cloud_error": "",
            }
            try:
                entries = _catalog()
                summaries = [_summary(entry) for entry in entries]
                # _catalog is deliberately stable first, prerelease second.
                default = summaries[0]
                result["releases"] = summaries
                result["cloud"] = default
                result["default_tag"] = default["tag_name"]
                result["default_version"] = default["version"]
            except Exception as exc:  # noqa: BLE001
                result["cloud_error"] = str(exc)
            return result

        def launch_update(module: Any, active_server: Any, payload: dict[str, Any]) -> dict[str, Any]:
            mode = str(payload.get("mode", "") or "").strip().lower()
            if mode == "restore":
                return original_launch(module, active_server, payload)
            if mode not in {"full", "incremental"}:
                return original_launch(module, active_server, payload)

            entries = _catalog()
            target_tag = str(payload.get("target_tag", "") or "").strip()
            selected = None
            if target_tag:
                for entry in entries:
                    if str(entry["release"].get("tag_name", "") or "").strip() == target_tag:
                        selected = entry
                        break
                if selected is None:
                    raise ValueError("所选云端版本已不存在，请刷新版本列表后重试")
            else:
                selected = entries[0]

            manifest = selected["manifest"]
            with _LAUNCH_LOCK:
                previous_loader = web_update_api._load_manifest  # noqa: SLF001
                web_update_api._load_manifest = lambda: manifest  # type: ignore[attr-defined]  # noqa: SLF001
                try:
                    return original_launch(module, active_server, payload)
                finally:
                    web_update_api._load_manifest = previous_loader  # type: ignore[attr-defined]  # noqa: SLF001

        def stable_update_payload(module: Any) -> dict[str, Any]:
            current = web_control_guard._read_version(module)  # noqa: SLF001
            try:
                entry = _catalog()[0]
                release = entry["release"]
            except Exception as exc:  # noqa: BLE001
                return {"status": "error", "message": f"检查更新失败：{exc}", "current_version": current}
            return {
                "status": "ok",
                "current_version": current,
                "latest_version": _release_version(release),
                "tag_name": str(release.get("tag_name", "") or ""),
                "name": str(release.get("name", "") or ""),
                "published_at": str(release.get("published_at", "") or ""),
                "html_url": str(release.get("html_url", "") or ""),
                "body": str(release.get("body", "") or "")[:12000],
                "prerelease": False,
            }

        def stable_estimate_release() -> dict[str, Any]:
            return _catalog()[0]["release"]

        web_update_api._state_payload = state_payload  # type: ignore[attr-defined]  # noqa: SLF001
        web_update_api._launch_update = launch_update  # type: ignore[attr-defined]  # noqa: SLF001
        web_control_guard._update_payload = stable_update_payload  # type: ignore[attr-defined]  # noqa: SLF001
        update_estimate_api._fetch_latest_release = stable_estimate_release  # type: ignore[attr-defined]  # noqa: SLF001
        web_update_api._issue189_release_selector_installed = True
        return True


__all__ = ["install_issue189_release_selector"]
