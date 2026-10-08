"""Issue #285: reject proxy HTML/corruption, then retry the original GitHub asset."""
from __future__ import annotations

import hashlib
import io
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from apps.update_download_source import (
    GH_PROXY_PREFIX, GH_PROXY_SOURCE, is_github_release_download_url,
    rewrite_download_url, official_url_from_accelerated,
)
from apps.web import web_updater
from apps.windows import update_client


class Response(io.BytesIO):
    def __init__(self, payload: bytes, *, status: int = 200, content_range: str = "", mime: str = "application/octet-stream"):
        super().__init__(payload)
        self.status = status
        self.headers = {"Content-Length": str(len(payload)), "Content-Type": mime}
        if content_range:
            self.headers["Content-Range"] = content_range


class State:
    def set(self, **_kwargs):
        pass


def main():
    official = "https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.16/example.zip"
    proxy = rewrite_download_url(official, GH_PROXY_SOURCE)
    assert proxy == GH_PROXY_PREFIX + official
    assert official_url_from_accelerated(proxy) == official
    for invalid in (
        "http://github.com/owner/repo/releases/download/v1/a.zip",
        "https://github.com.evil.net/owner/repo/releases/download/v1/a.zip",
        "https://user:pass@github.com/owner/repo/releases/download/v1/a.zip",
        "https://github.com:444/owner/repo/releases/download/v1/a.zip",
        "https://github.com/owner/repo/foo/releases/download/v1/a.zip",
        "https://api.github.com/repos/owner/repo/releases/latest",
    ):
        assert not is_github_release_download_url(invalid), invalid
        assert rewrite_download_url(invalid, GH_PROXY_SOURCE) == invalid

    valid = b"PK\x03\x04" + b"fake zip bytes"
    bad = b"x" * len(valid)
    digest = hashlib.sha256(valid).hexdigest()
    calls = []

    def open_response(req, timeout=45):
        calls.append(req.full_url)
        return Response(bad if len(calls) == 1 else valid)

    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "example.zip"
        with patch.object(web_updater.urllib.request, "urlopen", side_effect=open_response):
            web_updater._download(proxy, target, expected_size=len(valid), state=State(),
                                  start_pct=0, end_pct=50, label="ZIP", expected_sha256=digest)
        assert target.read_bytes() == valid
        assert calls == [proxy, official], calls

        calls.clear()
        def open_html(req, timeout=45):
            calls.append(req.full_url)
            return Response(b"<html>maintenance</html>", mime="text/html") if len(calls) == 1 else Response(valid)
        with patch.object(web_updater.urllib.request, "urlopen", side_effect=open_html):
            web_updater._download(proxy, target, expected_size=len(valid), state=State(),
                                  start_pct=0, end_pct=50, label="ZIP", expected_sha256=digest)
        assert calls == [proxy, official]

        calls.clear()
        def open_range(req, timeout=45):
            calls.append(req.full_url)
            assert req.get_header("Range") == f"bytes=10-{10 + len(valid) - 1}"
            return Response(bad if len(calls) == 1 else valid, status=206, content_range=f"bytes 10-{10 + len(valid) - 1}/500")
        with patch.object(web_updater.urllib.request, "urlopen", side_effect=open_range):
            data = web_updater._download_range(proxy, 10, len(valid), expected_sha256=digest)
        assert data == valid and calls == [proxy, official]

        # The legacy Windows prepare path now also retries after a correctly
        # sized but hash-mismatched proxy ZIP. It must verify the official bytes.
        release = update_client.ReleaseInfo(
            version="3.0.16", tag_name="v3.0.16", name="test", body="", page_url="",
            zip_asset=update_client.ReleaseAsset("example.zip", proxy, len(valid), digest),
            checksum_asset=update_client.ReleaseAsset("example.zip.sha256", "", 0, digest),
            sha256=digest,
        )
        calls.clear()
        def fake_download(url, destination, **kwargs):
            calls.append(url)
            Path(destination).write_bytes(bad if url == proxy else valid)
            return Path(destination)
        with patch.object(update_client, "download_file", side_effect=fake_download):
            prepared = update_client.prepare_release_download(release, work_dir=Path(tmp) / "win")
        assert calls == [proxy, official], calls
        assert prepared.zip_path.read_bytes() == valid and prepared.sha256 == digest

    print("issue285 gh-proxy fallback and SHA256 guard: OK")


if __name__ == "__main__":
    main()
