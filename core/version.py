from __future__ import annotations

import re
import sys
from pathlib import Path

_VERSION_PATTERN = re.compile(r"^\d+(?:\.\d+){2,}$")


def _version_candidates() -> tuple[Path, ...]:
    project_root = Path(__file__).resolve().parents[1]
    bundle_root = Path(getattr(sys, "_MEIPASS", project_root))
    candidates = [bundle_root / "VERSION"]
    if getattr(sys, "frozen", False):
        candidates.append(Path(sys.executable).resolve().parent / "VERSION")
    else:
        candidates.append(project_root / "VERSION")

    unique: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve(strict=False))
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return tuple(unique)


def load_app_version() -> str:
    checked: list[str] = []
    for path in _version_candidates():
        checked.append(str(path))
        try:
            version = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if not _VERSION_PATTERN.fullmatch(version):
            raise RuntimeError(f"VERSION 文件格式无效：{path} -> {version!r}")
        return version
    raise RuntimeError(f"找不到 VERSION 文件，已检查：{', '.join(checked)}")


APP_VERSION = load_app_version()


__all__ = ["APP_VERSION", "load_app_version"]
