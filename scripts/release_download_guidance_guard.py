from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    version = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    release_notes = (ROOT / "core/RELEASE_NOTES.md").read_text(encoding="utf-8")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    workflow = (ROOT / ".github/workflows/sync-existing-release-notes.yml").read_text(encoding="utf-8")

    windows_name = f"BiliPDJ-v{version}-Windows-Tk-Portable-x64.zip"
    web_name = f"BiliPDJ-v{version}-Web-Portable-x64.zip"
    release_base = f"https://github.com/ZzzHe2333/bilipdj/releases/download/v{version}"
    windows_url = f"{release_base}/{windows_name}"
    web_url = f"{release_base}/{web_name}"

    for label, text in (("release notes", release_notes), ("README", readme)):
        assert windows_name in text, f"{label} does not name the Windows client package"
        assert web_name in text, f"{label} does not name the Web portable package"
        assert windows_url in text, f"{label} does not contain a direct Windows download link"
        assert web_url in text, f"{label} does not contain a direct Web download link"

    assert "Windows-Tk-files.json" in release_notes
    assert "Windows-Tk-Incremental-x64.pack" in release_notes
    assert "普通用户无需手动下载" in release_notes

    assert "release-notes/v*" in workflow
    assert "gh release view" in workflow
    assert "gh release edit" in workflow
    assert "--notes-file core/RELEASE_NOTES.md" in workflow
    assert "contents: write" in workflow

    print(f"release download guidance guard: OK (v{version})")


if __name__ == "__main__":
    main()
