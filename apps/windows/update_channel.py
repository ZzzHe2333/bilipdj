from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

RELEASES_API = "https://api.github.com/repos/ZzzHe2333/bilipdj/releases?per_page=30"


def version_key(value: str) -> tuple[tuple[int, ...], int, tuple[tuple[int, int, str], ...]]:
    """Return a comparable SemVer-like key that preserves prerelease identity.

    Stable releases sort after prereleases with the same numeric core. Numeric
    prerelease identifiers sort before textual identifiers, matching SemVer.
    Build metadata is intentionally ignored for precedence.
    """

    text = str(value or "").strip()
    if text.lower().startswith("v"):
        text = text[1:]
    text = text.split("+", 1)[0]
    if "-" in text:
        core_text, prerelease = text.split("-", 1)
    else:
        core_text, prerelease = text, ""
    if not re.fullmatch(r"\d+(?:\.\d+)*", core_text):
        raise ValueError(f"无法识别版本号：{value}")
    core = tuple(int(piece) for piece in core_text.split("."))
    core = core + (0,) * max(0, 3 - len(core))
    if not prerelease:
        return core, 1, ()

    tokens: list[tuple[int, int, str]] = []
    for raw in prerelease.split("."):
        token = raw.strip()
        if not token or not re.fullmatch(r"[0-9A-Za-z-]+", token):
            raise ValueError(f"无法识别版本号：{value}")
        if token.isdigit():
            tokens.append((0, int(token), ""))
        else:
            tokens.append((1, 0, token.casefold()))
    return core, 0, tuple(tokens)


def is_prerelease_version(value: str) -> bool:
    return version_key(value)[1] == 0


def _installed_version() -> str:
    candidates: list[Path] = []
    if bool(getattr(sys, "frozen", False)):
        try:
            candidates.append(Path(sys.executable).resolve().parent / "VERSION")
        except Exception:
            pass
    try:
        candidates.append(Path(__file__).resolve().parents[2] / "VERSION")
    except Exception:
        pass
    for path in candidates:
        try:
            value = path.read_text(encoding="utf-8-sig").strip()
        except OSError:
            continue
        if value:
            return value
    return ""


def _release_version(payload: dict[str, Any]) -> str:
    tag = str(payload.get("tag_name", "") or "").strip()
    return tag[1:] if tag.lower().startswith("v") else tag


def select_channel_release(releases: list[Any]) -> dict[str, Any]:
    candidates: list[tuple[tuple[tuple[int, ...], int, tuple[tuple[int, int, str], ...]], dict[str, Any]]] = []
    for raw in releases:
        if not isinstance(raw, dict) or bool(raw.get("draft")):
            continue
        version = _release_version(raw)
        try:
            key = version_key(version)
        except ValueError:
            continue
        candidates.append((key, raw))
    if not candidates:
        raise RuntimeError("没有可用的 GitHub Release")
    return max(candidates, key=lambda item: item[0])[1]


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

    def fetch_prerelease_channel(*, timeout: float = 15.0):
        request = update_client._request(RELEASES_API, timeout=timeout)  # noqa: SLF001
        with request as response:
            try:
                payload = json.loads(response.read().decode("utf-8-sig"))
            except (AttributeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise update_client.UpdateError("GitHub Release 列表无法解析") from exc
        if not isinstance(payload, list):
            raise update_client.UpdateError("GitHub Release 列表格式无效")
        release = select_channel_release(payload)
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

    def fetch_latest_release(*, timeout: float = 15.0):
        current = _installed_version()
        try:
            prerelease = bool(current) and is_prerelease_version(current)
        except ValueError:
            prerelease = False
        if prerelease:
            return fetch_prerelease_channel(timeout=timeout)
        return original_fetch_latest(timeout=timeout)

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
