from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

OFFICIAL_SOURCE = "official"
GH_PROXY_SOURCE = "gh-proxy"
GH_PROXY_PREFIX = "https://gh-proxy.com/"
# Allowlisted third-party transports for public GitHub Release assets only.
SOURCE_PREFIXES = {
    GH_PROXY_SOURCE: GH_PROXY_PREFIX,
    "github.akams.cn": "https://github.akams.cn/",
    "ghfile.geekertao.top": "https://ghfile.geekertao.top/",
    "github.dpik.top": "https://github.dpik.top/",
    "gh.dpik.top": "https://gh.dpik.top/",
}
SOURCE_LABELS = {
    OFFICIAL_SOURCE: "GitHub 官方",
    GH_PROXY_SOURCE: "第三方加速（GH-Proxy）",
    "github.akams.cn": "公益加速 · github.akams.cn",
    "ghfile.geekertao.top": "公益加速 · ghfile.geekertao.top",
    "github.dpik.top": "公益加速 · github.dpik.top",
    "gh.dpik.top": "公益加速 · gh.dpik.top",
}


def normalize_download_source(value: Any) -> str:
    text = str(value or "").strip().casefold()
    aliases = {
        "": OFFICIAL_SOURCE,
        OFFICIAL_SOURCE: OFFICIAL_SOURCE,
        "github": OFFICIAL_SOURCE,
        "github 官方": OFFICIAL_SOURCE,
        SOURCE_LABELS[OFFICIAL_SOURCE].casefold(): OFFICIAL_SOURCE,
        GH_PROXY_SOURCE: GH_PROXY_SOURCE,
        "ghproxy": GH_PROXY_SOURCE,
        "gh-proxy.com": GH_PROXY_SOURCE,
        SOURCE_LABELS[GH_PROXY_SOURCE].casefold(): GH_PROXY_SOURCE,
    }
    for source, prefix in SOURCE_PREFIXES.items():
        aliases[source] = source
        aliases[prefix.rstrip("/").casefold()] = source
        aliases[SOURCE_LABELS[source].casefold()] = source
    return aliases.get(text, OFFICIAL_SOURCE)


def source_label(value: Any) -> str:
    return SOURCE_LABELS[normalize_download_source(value)]


def is_github_release_download_url(url: str) -> bool:
    text = str(url or "").strip()
    try:
        parsed = urlsplit(text)
    except ValueError:
        return False
    try:
        permitted = (
            parsed.scheme.lower() == "https"
            and (parsed.hostname or "").lower() == "github.com"
            and parsed.username is None
            and parsed.password is None
            and parsed.port is None
            and not parsed.fragment
        )
    except ValueError:
        return False
    if not permitted:
        return False
    # Only an actual GitHub release asset under /owner/repo/releases/download.
    # A lookalike path, credentials, or nonstandard authority must never be proxied.
    parts = parsed.path.split("/")
    return (
        len(parts) == 7
        and bool(parts[1]) and bool(parts[2]) and bool(parts[6])
        and (
            (parts[3:5] == ["releases", "download"] and bool(parts[5]))
            or parts[3:6] == ["releases", "latest", "download"]
        )
    )


def rewrite_download_url(url: str, source: Any) -> str:
    text = str(url or "").strip()
    selected = normalize_download_source(source)
    prefix = SOURCE_PREFIXES.get(selected)
    if not prefix or not is_github_release_download_url(text):
        return text
    return f"{prefix}{text}"


def official_url_from_accelerated(url: str) -> str:
    text = str(url or "").strip()
    for prefix in SOURCE_PREFIXES.values():
        if text.startswith(prefix):
            candidate = text[len(prefix):]
            if is_github_release_download_url(candidate):
                return candidate
    return text


def rewrite_package_download_urls(package: dict[str, Any], source: Any) -> dict[str, Any]:
    """Copy an update-package mapping and rewrite only GitHub Release asset URLs.

    GitHub API and Raw metadata URLs are intentionally outside this function.
    The accelerator is therefore only a transport path for already-resolved,
    hash-pinned release files.
    """

    selected = normalize_download_source(source)

    def rewrite(value: Any) -> Any:
        if isinstance(value, dict):
            result: dict[str, Any] = {}
            for key, item in value.items():
                if key in {"url", "checksum_url"} and isinstance(item, str):
                    result[key] = rewrite_download_url(item, selected)
                else:
                    result[key] = rewrite(item)
            return result
        if isinstance(value, list):
            return [rewrite(item) for item in value]
        return value

    return rewrite(dict(package))


__all__ = [
    "GH_PROXY_PREFIX",
    "GH_PROXY_SOURCE",
    "OFFICIAL_SOURCE",
    "SOURCE_LABELS",
    "SOURCE_PREFIXES",
    "is_github_release_download_url",
    "normalize_download_source",
    "official_url_from_accelerated",
    "rewrite_download_url",
    "rewrite_package_download_urls",
    "source_label",
]
