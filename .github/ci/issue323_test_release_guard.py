"""Issue #323: explicit test-tag release contract (offline, no publishing)."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_version() -> None:
    version = read("VERSION").strip()
    assert re.fullmatch(r"\d+\.\d+\.\d+", version), version
    # Historical -test support remains in release workflow, but the current
    # release notes must match the version selected for THIS build.
    notes = read("core/RELEASE_NOTES.md")
    heading = notes.splitlines()[0]
    prerelease = f"v{version}-test" in heading
    tag = f"v{version}-test" if prerelease else f"v{version}"
    assert tag in heading
    if prerelease:
        assert "测试版" in heading and "Pre-release" in notes
        assert "正式版" not in heading
    else:
        assert "正式版" in heading
        assert "测试版" not in heading
    assert f"BiliPDJ-{tag}-Windows-Tk-Portable-x64.zip" in notes
    assert f"BiliPDJ-{tag}-Web-Portable-x64.zip" in notes


def check_workflow() -> None:
    win = read(".github/workflows/package-windows-x64.yml")
    bridge = read(".github/workflows/release-dispatch-bridge.yml")
    assert r'^v[0-9]+(\.[0-9]+){2}(-test)?$' in win
    assert r'^release-publish/v[0-9]+(\.[0-9]+){2}(-test)?$' in bridge
    assert "TAG_NAME%-test" in win
    assert 'if [[ "$TAG_NAME" == *-test ]]' in win
    assert 'if [ "$PRERELEASE" != "true" ]' in win
    assert 'ASSET_TAG="$TAG_NAME"' in win
    assert 'BiliPDJ-$ASSET_TAG-Windows-Tk-Portable-x64.zip' in win
    assert 'BiliPDJ-$ASSET_TAG-Web-Portable-x64.zip' in win
    assert "BILIPDJ_DATA_DIR = $probeDataRoot" in win
    assert "Remove-Item -LiteralPath $probeDataRoot" in win
    assert "bodyFile: core/RELEASE_NOTES.md" in win
    assert "prerelease == 'true'" in win


def check_frozen_probe_entry() -> None:
    script = read("apps/web/portable_launcher_entry.py")
    freeze = script.index("mp.freeze_support()")
    selftest = script.index('if "--plugin-runtime-self-test" in sys.argv[1:]:')
    imported = script.index("from apps.web import portable_launcher")
    assert freeze < selftest < imported
    assert "run_frozen_plugin_probe()" in script


def main() -> None:
    check_version()
    check_workflow()
    check_frozen_probe_entry()
    print("Issue #323 explicit test-release regression: OK")


if __name__ == "__main__":
    main()
