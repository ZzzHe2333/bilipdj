from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.update_download_source import (  # noqa: E402
    GH_PROXY_PREFIX,
    GH_PROXY_SOURCE,
    OFFICIAL_SOURCE,
    official_url_from_accelerated,
    rewrite_download_url,
    rewrite_package_download_urls,
)


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def check_url_policy() -> None:
    release_url = "https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.16/example.zip"
    manifest_url = "https://github.com/ZzzHe2333/bilipdj/releases/latest/download/update-manifest.json"
    api_url = "https://api.github.com/repos/ZzzHe2333/bilipdj/releases/latest"
    raw_url = "https://raw.githubusercontent.com/ZzzHe2333/bilipdj/now/update-manifest.json"

    accelerated = rewrite_download_url(release_url, GH_PROXY_SOURCE)
    assert accelerated == f"{GH_PROXY_PREFIX}{release_url}"
    assert rewrite_download_url(manifest_url, GH_PROXY_SOURCE) == f"{GH_PROXY_PREFIX}{manifest_url}"
    assert rewrite_download_url(api_url, GH_PROXY_SOURCE) == api_url
    assert rewrite_download_url(raw_url, GH_PROXY_SOURCE) == raw_url
    assert rewrite_download_url(release_url, OFFICIAL_SOURCE) == release_url
    assert official_url_from_accelerated(accelerated) == release_url

    package = {
        "url": release_url,
        "file_manifest": {"url": release_url.replace("example.zip", "files.json")},
        "incremental": {"url": release_url.replace("example.zip", "incremental.pack")},
        "metadata": {"api": api_url},
    }
    rewritten = rewrite_package_download_urls(package, GH_PROXY_SOURCE)
    assert str(rewritten["url"]).startswith(GH_PROXY_PREFIX)
    assert str(rewritten["file_manifest"]["url"]).startswith(GH_PROXY_PREFIX)
    assert str(rewritten["incremental"]["url"]).startswith(GH_PROXY_PREFIX)
    assert rewritten["metadata"]["api"] == api_url
    assert package["url"] == release_url, "rewrite must not mutate the official manifest object"


def check_windows_policy() -> None:
    source = read("apps/windows/download_acceleration.py")
    runtime = read("apps/windows/desktop_runtime.py")
    assert "install_download_acceleration" in runtime
    assert "GitHub 官方" in source
    assert "第三方加速（GH-Proxy）" not in source, "labels should come from the shared source registry"
    assert "messagebox.askyesno" in source
    assert "本次确认不会保存" in source
    assert "选择“否”不会取消更新" in source
    assert "tk.StringVar" in source and "SOURCE_LABELS[OFFICIAL_SOURCE]" in source
    assert "save_update_network" not in source, "download source/trust must not be persisted"
    assert "official_url_from_accelerated" in source, "transport errors should be able to retry official URLs"

    update_client = read("apps/windows/update_client.py")
    incremental = read("apps/windows/incremental_update.py")
    assert "verify_sha256(zip_path, expected_digest)" in update_client
    assert "packed_sha256" in incremental and "sha256" in incremental
    assert "status != 206" in incremental, "Range transport validation must remain strict"


def check_web_policy() -> None:
    web = read("apps/web/static/web_updater_control.js")
    server = read("apps/server/issue187_web_update_guard.py")
    updater = read("apps/web/web_updater.py")

    assert 'id="web-update-download-source"' in web
    assert '<option value="official">GitHub 官方</option>' in web
    assert '<option value="gh-proxy">第三方加速（GH-Proxy）</option>' in web
    assert "确认使用第三方加速" in web
    assert "本次确认不会保存" in web
    assert "third_party_confirmed" in web and "download_source" in web
    assert "localStorage" not in web and "sessionStorage" not in web

    assert "normalize_download_source" in server
    assert "rewrite_package_download_urls" in server
    assert 'payload.get("third_party_confirmed") is not True' in server
    assert "第三方加速必须由用户针对本次下载明确确认" in server

    assert "_verify(zip_path" in updater
    assert "packed_sha256" in updater and "Content-Range" in updater


def main() -> None:
    check_url_policy()
    check_windows_policy()
    check_web_policy()
    print("issue279 third-party download acceleration guard: OK")


if __name__ == "__main__":
    main()
