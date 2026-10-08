from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "package-windows-x64.yml"


def _section(text: str, start: str, end: str | None = None) -> str:
    start_at = text.index(start)
    if end is None:
        return text[start_at:]
    end_at = text.index(end, start_at + len(start))
    return text[start_at:end_at]


def check_release_policy() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    header = text[: text.index("jobs:")]
    build = _section(text, "  build-portable-bundles:", "  release-portable-bundles:")
    release = _section(text, "  release-portable-bundles:")

    assert "permissions:\n  contents: read" in header, "workflow default permission must remain read-only"
    assert "ncipollo/release-action" not in build, "build job must never publish a GitHub Release"
    assert "contents: write" not in build, "build job must not receive repository write permission"

    # Only explicit dispatch, a version tag, or a release-titled push on now may publish.
    # Ordinary pushes and pull requests remain build-only.
    expected_if = (
        "if: >-\n"
        "      github.event_name == 'workflow_dispatch' ||\n"
        "      startsWith(github.ref, 'refs/tags/v') ||\n"
        "      (github.event_name == 'push' && github.ref == 'refs/heads/now' "
        "&& startsWith(github.event.head_commit.message, 'release: v'))"
    )
    assert expected_if in release, "release job must use only explicitly authorized release triggers"
    assert "RELEASE_COMMIT_MESSAGE: ${{ github.event.head_commit.message }}" in build
    assert 'TAG_NAME="${BASH_REMATCH[1]}"' in build
    assert "EXPLICIT_RELEASE=true\n            PRERELEASE=false" in build
    assert "needs: build-portable-bundles" in release, "release must consume the validated build job"
    assert "permissions:\n      contents: write" in release, "write permission must exist only inside the release job"
    assert "actions/download-artifact@v4" in release, "release job must publish validated build artifacts"
    assert "ncipollo/release-action@v1" in release, "release action is missing from the explicit release job"

    assert "EXPLICIT_RELEASE=false" in build
    assert 'TAG_NAME="build-now-${GITHUB_SHA:0:8}"' in build, "ordinary now pushes must use a non-release build label"
    assert 'if [ "$EXPLICIT_RELEASE" = "true" ]; then' in build, "release tag validation must only run for explicit release events"


def main() -> None:
    check_release_policy()
    print("issue #138 release policy guard: OK")


if __name__ == "__main__":
    main()
