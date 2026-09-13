from __future__ import annotations

import re
from typing import TypeAlias

VersionKey: TypeAlias = tuple[tuple[int, ...], int, tuple[tuple[int, int, str], ...]]

_PROJECT_PRERELEASE_RANKS = {
    "feiqi": 0,
    "loss": 0,
    "text": 10,
    "t": 10,
    "dev": 10,
    "test": 10,
    "cx": 20,
    "c": 20,
    "gc": 30,
    "g": 30,
}
_PROJECT_PRERELEASE_CANONICAL = {
    "feiqi": "deprecated",
    "loss": "deprecated",
    "text": "internal",
    "t": "internal",
    "dev": "internal",
    "test": "internal",
    "cx": "experimental",
    "c": "experimental",
    "gc": "public-beta",
    "g": "public-beta",
}


def _split_version(value: str) -> tuple[tuple[int, ...], str]:
    text = str(value or "").strip()
    if text.lower().startswith("v"):
        text = text[1:]
    text = text.split("+", 1)[0]
    if "-" in text:
        core_text, prerelease = text.split("-", 1)
    else:
        core_text, prerelease = text, ""
    if not re.fullmatch(r"\d+(?:\.\d+)*", core_text):
        raise ValueError(f"无法识别版本号：{value}")
    core = tuple(int(piece) for piece in core_text.split("."))
    core = core + (0,) * max(0, 3 - len(core))
    if prerelease:
        for raw in prerelease.split("."):
            token = raw.strip()
            if not token or not re.fullmatch(r"[0-9A-Za-z-]+", token):
                raise ValueError(f"无法识别版本号：{value}")
    return core, prerelease


def version_key(value: str) -> VersionKey:
    """Return a comparable BiliPDJ version key.

    Same numeric version ordering is explicitly:
    deprecated < internal/test < experimental < public beta < stable.
    Unknown prerelease identifiers keep a SemVer-like token ordering.
    """
    core, prerelease = _split_version(value)
    if not prerelease:
        return core, 1, ()

    known = prerelease.casefold()
    if known in _PROJECT_PRERELEASE_RANKS:
        return core, 0, (
            (2, _PROJECT_PRERELEASE_RANKS[known], _PROJECT_PRERELEASE_CANONICAL[known]),
        )

    tokens: list[tuple[int, int, str]] = []
    for raw in prerelease.split("."):
        token = raw.strip()
        if token.isdigit():
            tokens.append((0, int(token), ""))
        else:
            tokens.append((1, 0, token.casefold()))
    return core, 0, tuple(tokens)


def is_prerelease_version(value: str) -> bool:
    return version_key(value)[1] == 0


def normalize_release_identity(value: str) -> tuple[tuple[int, ...], str]:
    """Normalize exact file-set identity for incremental update bases.

    Leading ``v`` and build metadata are ignored, while the complete prerelease
    suffix is retained. Therefore test/gc/cx/stable variants of the same numeric
    core can never be accepted as the same incremental-update base.
    """
    core, prerelease = _split_version(value)
    return core, prerelease.casefold()


def same_release_version(left: str, right: str) -> bool:
    try:
        return normalize_release_identity(left) == normalize_release_identity(right)
    except ValueError:
        return False


__all__ = [
    "VersionKey",
    "is_prerelease_version",
    "normalize_release_identity",
    "same_release_version",
    "version_key",
]
