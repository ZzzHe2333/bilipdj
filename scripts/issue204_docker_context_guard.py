from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCKERIGNORE = ROOT / ".dockerignore"
DOCKERFILE = ROOT / "apps/server/Dockerfile"


def _active_lines(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def check_dockerignore() -> None:
    assert DOCKERIGNORE.is_file(), ".dockerignore is required"
    lines = set(_active_lines(DOCKERIGNORE))

    required = {
        "data/",
        "log/",
        "plugins/",
        "backup/",
        "key/",
        "config.yaml",
        "kaiguan.yaml",
        "quanxian.yaml",
        "core/config.yaml",
        "core/kaiguan.yaml",
        "core/quanxian.yaml",
        "core/platform_config_archive*.json",
        "core/*cookie*",
        "core/*token*",
        "core/cd/",
        ".env",
        ".env.*",
        "**/__pycache__/",
        "node_modules/",
        "build/",
        "dist/",
    }
    missing = sorted(required - lines)
    assert not missing, f"missing Docker ignore rules: {missing}"

    dangerous_negations = {
        "!data/",
        "!core/config.yaml",
        "!core/cd/",
        "!.env",
        "!.env.*",
    }
    present = sorted(dangerous_negations & lines)
    assert not present, f"runtime/private paths must not be re-included: {present}"


def check_dockerfile_copy_scope() -> None:
    text = DOCKERFILE.read_text(encoding="utf-8")
    normalized = "\n".join(line.strip() for line in text.splitlines())
    assert "COPY . " not in normalized
    assert "COPY .\n" not in normalized
    assert "COPY ./ " not in normalized
    for required in (
        "COPY requirements.txt /app/requirements.txt",
        "COPY VERSION README.md /app/",
        "COPY core /app/core",
        "COPY apps /app/apps",
    ):
        assert required in text, f"missing expected selective copy: {required}"


def check_ci_wiring() -> None:
    quality = (ROOT / ".github/workflows/quality.yml").read_text(encoding="utf-8")
    server = (ROOT / ".github/workflows/server.yml").read_text(encoding="utf-8")
    assert "python scripts/issue204_docker_context_guard.py" in quality
    assert "'.dockerignore'" in quality
    assert "'.dockerignore'" in server
    assert "context-leak-sentinel" in server


def main() -> None:
    check_dockerignore()
    check_dockerfile_copy_scope()
    check_ci_wiring()
    print("issue #204 Docker context guard: OK")


if __name__ == "__main__":
    main()
