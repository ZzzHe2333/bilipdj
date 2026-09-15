from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _assert_release_links(label: str, text: str, *, version: str) -> None:
    windows_name = f"BiliPDJ-v{version}-Windows-Tk-Portable-x64.zip"
    web_name = f"BiliPDJ-v{version}-Web-Portable-x64.zip"
    release_base = f"https://github.com/ZzzHe2333/bilipdj/releases/download/v{version}"
    windows_url = f"{release_base}/{windows_name}"
    web_url = f"{release_base}/{web_name}"

    assert windows_name in text, f"{label} does not name the Windows client package"
    assert web_name in text, f"{label} does not name the Web portable package"
    assert windows_url in text, f"{label} does not contain a direct Windows download link"
    assert web_url in text, f"{label} does not contain a direct Web download link"


def _assert_readme_release_links(readme: str) -> str:
    windows = re.search(r"BiliPDJ-v(?P<version>\d+\.\d+\.\d+)-Windows-Tk-Portable-x64\.zip", readme)
    web = re.search(r"BiliPDJ-v(?P<version>\d+\.\d+\.\d+)-Web-Portable-x64\.zip", readme)
    assert windows, "README does not name a stable Windows client package"
    assert web, "README does not name a stable Web portable package"
    windows_version = windows.group("version")
    web_version = web.group("version")
    assert windows_version == web_version, "README stable Windows/Web package versions differ"
    _assert_release_links("README", readme, version=windows_version)
    return windows_version


def main() -> None:
    version = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    release_notes = (ROOT / "core/RELEASE_NOTES.md").read_text(encoding="utf-8")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    workflow = (ROOT / ".github/workflows/sync-existing-release-notes.yml").read_text(encoding="utf-8")

    # RELEASE_NOTES follows the current build version. README follows the latest
    # promoted Release and therefore may intentionally lag behind VERSION while
    # VERSION is represented by a GitHub Pre-release.
    _assert_release_links("release notes", release_notes, version=version)
    readme_version = _assert_readme_release_links(readme)

    assert "Windows-Tk-files.json" in release_notes
    assert "Windows-Tk-Incremental-x64.pack" in release_notes
    assert "普通用户无需手动下载" in release_notes

    assert "release-notes/v*" in workflow
    assert "gh release view" in workflow
    assert "gh release edit" in workflow
    assert "--notes-file core/RELEASE_NOTES.md" in workflow
    assert "contents: write" in workflow

    print(f"release download guidance guard: OK (build v{version}, README Release v{readme_version})")


if __name__ == "__main__":
    main()
