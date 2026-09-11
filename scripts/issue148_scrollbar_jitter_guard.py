from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_CSS = ROOT / "apps" / "web" / "static" / "control.css"
BUILT_CSS = ROOT / "apps" / "web" / "dist" / "control.css"
STABLE_GUTTER_RULE = "html{overflow-y:scroll;scrollbar-gutter:stable}"


def check_css(path: Path) -> None:
    if not path.is_file():
        raise AssertionError(f"missing Web control stylesheet: {path.relative_to(ROOT)}")
    text = path.read_text(encoding="utf-8")
    if STABLE_GUTTER_RULE not in text:
        raise AssertionError(
            f"{path.relative_to(ROOT)} must keep a stable root vertical scrollbar gutter "
            "to prevent ~17px horizontal layout shifts on Windows Chromium"
        )
    if ".content{padding:20px;min-width:0}" not in text:
        raise AssertionError(
            f"{path.relative_to(ROOT)} lost the main content min-width guard"
        )


def main() -> None:
    check_css(SOURCE_CSS)
    if BUILT_CSS.exists():
        check_css(BUILT_CSS)
    print("issue #148 scrollbar jitter guard: OK")


if __name__ == "__main__":
    main()
