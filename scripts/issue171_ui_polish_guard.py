from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM_SHA = "d23d7f88a2e21c9e4b1418c7abe420f5c1052ba7"
EXPECTED_SKILLS = (
    "animate-expo",
    "animate",
    "animation-vocabulary",
    "apple-design",
    "ask-sonner",
    "emil-design-eng",
    "find-animation-opportunities",
    "improve-animations",
    "pick-ui-library",
    "prototype",
    "review-animations",
    "write-swift",
)


def check_installed_skills() -> None:
    root = ROOT / ".agents" / "skills"
    assert root.is_dir(), "project-local .agents/skills is missing"
    for name in EXPECTED_SKILLS:
        skill = root / name / "SKILL.md"
        assert skill.is_file(), f"missing vendored Emil skill: {skill.relative_to(ROOT)}"
    upstream = (root / "UPSTREAM.md").read_text(encoding="utf-8")
    assert UPSTREAM_SHA in upstream, "UPSTREAM.md does not pin the reviewed upstream commit"
    assert (root / "emilkowalski-LICENSE").is_file(), "upstream MIT license was not preserved"


def check_web_motion_and_accessibility() -> None:
    css = (ROOT / "apps" / "web" / "static" / "control.css").read_text(encoding="utf-8")
    unified = (ROOT / "apps" / "web" / "static" / "unified_theme.css").read_text(encoding="utf-8")
    combined = f"{css}\n{unified}"

    assert "scrollbar-gutter: stable" in css, "stable scrollbar gutter regression"
    assert "--ease-out: cubic-bezier(.23, 1, .32, 1)" in css
    assert ":focus-visible" in combined, "keyboard focus treatment is missing"
    assert "prefers-reduced-motion: reduce" in css, "reduced-motion handling is missing"
    assert ".button:active" in css and "scale(.97)" in css, "press feedback is missing"
    assert "@media (hover:hover) and (pointer:fine)" in unified, "hover styling must be pointer-gated"

    assert not re.search(r"transition\s*:\s*all\b", combined, flags=re.I), "transition: all is not allowed"
    assert not re.search(r"(?<![-\w])ease-in(?!-out)(?![-\w])", combined, flags=re.I), "standalone ease-in is not allowed for UI motion"


def check_command_and_estimate_polish() -> None:
    js = (ROOT / "apps" / "web" / "static" / "issue167_control_extensions.js").read_text(encoding="utf-8")
    assert "backend-command-copy" in js and "backend-command-controls" in js
    assert 'aria-live="polite"' in js
    assert "aria-busy" in js
    assert "ensureStyles" not in js, "issue #171 moved extension styling into canonical CSS"
    assert "update-estimate-full" in js and "update-estimate-incremental" in js

    windows = (ROOT / "apps" / "windows" / "command_console_ui.py").read_text(encoding="utf-8")
    assert "ttk.LabelFrame" in windows, "Windows command area should have a clear visual group"
    assert 'text="后端指令"' in windows
    assert "wraplength=720" in windows


def main() -> None:
    check_installed_skills()
    check_web_motion_and_accessibility()
    check_command_and_estimate_polish()
    print("issue #171 Emil skills / UI polish guard: OK")


if __name__ == "__main__":
    main()
