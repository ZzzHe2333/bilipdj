"""Issue #316: allowlisted community mirrors, per-use consent, and official fallback."""
from __future__ import annotations

import hashlib
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.update_download_source import (
    OFFICIAL_SOURCE, GH_PROXY_SOURCE, SOURCE_LABELS, SOURCE_PREFIXES,
    is_github_release_download_url, normalize_download_source,
    official_url_from_accelerated, rewrite_download_url,
    rewrite_package_download_urls, source_label,
)
from apps.web import web_updater
from issue285_gh_proxy_update_guard import Response, State

SOURCES = {
    "github.akams.cn": "https://github.akams.cn/",
    "ghfile.geekertao.top": "https://ghfile.geekertao.top/",
    "github.dpik.top": "https://github.dpik.top/",
    "gh.dpik.top": "https://gh.dpik.top/",
}
OFFICIAL = "https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.23/example.zip"
LATEST = "https://github.com/ZzzHe2333/bilipdj/releases/latest/download/update-manifest.json"


def check_urls() -> None:
    assert set(SOURCES.items()).issubset(set(SOURCE_PREFIXES.items()))
    assert SOURCE_PREFIXES[GH_PROXY_SOURCE] == "https://gh-proxy.com/"
    for source, prefix in SOURCES.items():
        assert source_label(source) == SOURCE_LABELS[source]
        for value in (source, source.upper(), prefix.rstrip("/"), SOURCE_LABELS[source]):
            assert normalize_download_source(value) == source, value
        for official in (OFFICIAL, LATEST):
            mirrored = rewrite_download_url(official, source)
            assert mirrored == prefix + official, mirrored
            assert official_url_from_accelerated(mirrored) == official
        package = {
            "url": OFFICIAL,
            "file_manifest": {"url": LATEST},
            "incremental": {"url": OFFICIAL.replace("example.zip", "files.pack")},
            "meta": {"api": "https://api.github.com/repos/ZzzHe2333/bilipdj/releases/latest"},
        }
        copy = rewrite_package_download_urls(package, source)
        assert copy["url"] == prefix + OFFICIAL
        assert copy["file_manifest"]["url"] == prefix + LATEST
        assert copy["incremental"]["url"].startswith(prefix)
        assert copy["meta"] == package["meta"] and package["url"] == OFFICIAL
        for bad in (
            "https://api.github.com/repos/a/b/releases/latest",
            "https://raw.githubusercontent.com/a/b/main/file.py",
            "https://github.com.evil.invalid/a/b/releases/download/v1/a",
            "http://github.com/a/b/releases/download/v1/a",
            "https://github.com:444/a/b/releases/download/v1/a",
        ):
            assert not is_github_release_download_url(bad)
            assert rewrite_download_url(bad, source) == bad
    assert normalize_download_source("malicious.invalid") == OFFICIAL_SOURCE
    assert official_url_from_accelerated("https://evil.invalid/" + OFFICIAL) != OFFICIAL


def check_ui_and_consent() -> None:
    win = (ROOT / "apps/windows/download_acceleration.py").read_text(encoding="utf-8")
    js = (ROOT / "apps/web/static/web_updater_control.js").read_text(encoding="utf-8")
    server = (ROOT / "apps/server/issue187_web_update_guard.py").read_text(encoding="utf-8")
    assert "values=tuple(SOURCE_LABELS.values())" in win
    assert "本次确认不会保存" in win and "SOURCE_PREFIXES[selected]" in win
    assert "if selected == OFFICIAL_SOURCE:" in win
    assert 'if download_source != OFFICIAL_SOURCE and payload.get("third_party_confirmed") is not True:' in server
    for source in SOURCES:
        assert f'<option value="{source}">' in js
        assert f"'{source}': '{source}'" in js
    assert "currentDownloadSource() !== 'official'" in js
    assert "if (thirdPartyConfirmed) downloadSource = currentDownloadSource()" in js
    assert "本次确认不会保存" in js


def check_corrupt_download_fallback() -> None:
    valid = b"PK\x03\x04" + b"release-test-payload"
    invalid = b"x" * len(valid)
    digest = hashlib.sha256(valid).hexdigest()
    for source in SOURCES:
        mirrored = rewrite_download_url(OFFICIAL, source)
        calls: list[str] = []

        def open_zip(req, timeout=45):
            calls.append(req.full_url)
            return Response(invalid if len(calls) == 1 else valid)

        with tempfile.TemporaryDirectory() as folder:
            dest = Path(folder) / "sample.zip"
            with patch.object(web_updater.urllib.request, "urlopen", side_effect=open_zip):
                web_updater._download(
                    mirrored, dest, expected_size=len(valid), state=State(),
                    start_pct=0, end_pct=50, label="ZIP", expected_sha256=digest,
                )
            assert dest.read_bytes() == valid
            assert calls == [mirrored, OFFICIAL], calls

            calls.clear()

            def open_range(req, timeout=45):
                calls.append(req.full_url)
                assert req.get_header("Range") == f"bytes=10-{10 + len(valid) - 1}"
                return Response(
                    invalid if len(calls) == 1 else valid, status=206,
                    content_range=f"bytes 10-{10 + len(valid) - 1}/500",
                )

            with patch.object(web_updater.urllib.request, "urlopen", side_effect=open_range):
                data = web_updater._download_range(
                    mirrored, 10, len(valid), expected_sha256=digest,
                )
            assert data == valid and calls == [mirrored, OFFICIAL], calls


if __name__ == "__main__":
    check_urls()
    check_ui_and_consent()
    check_corrupt_download_fallback()
    print("issue316 community mirrors, consent and fallback: OK")
