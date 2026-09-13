from __future__ import annotations

import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def main() -> None:
    # Move all historical test/build helpers into GitHub-only CI space.
    scripts = ROOT / "scripts"
    ci_dir = ROOT / ".github" / "ci"
    if not scripts.is_dir():
        raise RuntimeError("scripts directory is missing before migration")
    if ci_dir.exists():
        raise RuntimeError(".github/ci already exists")
    shutil.move(str(scripts), str(ci_dir))

    # Old helpers discovered ROOT one level above scripts/. They are now two
    # levels below the repository root.
    for path in ci_dir.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        text = text.replace(
            "Path(__file__).resolve().parents[1]",
            "Path(__file__).resolve().parents[2]",
        )
        path.write_text(text, encoding="utf-8")

    # Update maintenance/CI references and historical guards. Vendored agent
    # skills are unrelated and intentionally left untouched.
    text_suffixes = {
        ".py", ".yml", ".yaml", ".md", ".ps1", ".sh", ".spec", ".txt", ".json", ".toml"
    }
    for path in ROOT.rglob("*"):
        if not path.is_file() or ".git" in path.parts or ".agents" in path.parts:
            continue
        if path.suffix.lower() not in text_suffixes:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if "scripts/" in text:
            path.write_text(text.replace("scripts/", ".github/ci/"), encoding="utf-8")

    quality_path = ".github/workflows/quality.yml"
    quality = read(quality_path)
    quality = quality.replace(
        "python -m compileall -q apps core scripts",
        "python -m compileall -q apps core .github/ci",
    )
    marker = (
        "      - name: Validate issue 260 five bug fixes\n"
        "        run: python .github/ci/issue260_five_bug_guard.py\n"
    )
    if marker not in quality:
        raise RuntimeError("quality issue260 marker not found")
    quality = quality.replace(
        marker,
        marker
        + "      - name: Validate issue 262 bug fixes and scripts decoupling\n"
        + "        run: python .github/ci/issue262_six_bug_and_scripts_guard.py\n",
        1,
    )
    write(quality_path, quality)

    # Shared, production release-channel/version semantics.
    write(
        "apps/versioning.py",
        '''from __future__ import annotations

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
    if not re.fullmatch(r"\\d+(?:\\.\\d+)*", core_text):
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
''',
    )

    # Windows updater reuses the shared implementation.
    path = "apps/windows/update_channel.py"
    text = read(path)
    pattern = re.compile(
        r"# Same numeric version: deprecated < internal < experimental < public beta < stable\.\n.*?\ndef _installed_version",
        re.S,
    )
    replacement = (
        "from apps.versioning import is_prerelease_version, version_key\n\n\n"
        "def _installed_version"
    )
    text, count = pattern.subn(replacement, text, count=1)
    if count != 1:
        raise RuntimeError("failed to centralize apps/windows/update_channel.py")
    write(path, text)

    # Plugin min/max compatibility checks use the same channel precedence.
    path = "apps/server/plugin_manager.py"
    text = read(path)
    import_anchor = "from urllib.parse import urlparse\n\n"
    if import_anchor not in text:
        raise RuntimeError("plugin_manager import anchor missing")
    text = text.replace(
        import_anchor,
        import_anchor + "from apps.versioning import version_key as _shared_version_key\n\n",
        1,
    )
    pattern = re.compile(r"def _version_key\(value: str\).*?\n\ndef _canonical_manifest", re.S)
    replacement = '''def _version_key(value: str):
    try:
        return _shared_version_key(value)
    except ValueError as exc:
        raise PluginError(f"invalid version: {value}") from exc


def _canonical_manifest'''
    text, count = pattern.subn(replacement, text, count=1)
    if count != 1:
        raise RuntimeError("failed to patch plugin_manager._version_key")
    write(path, text)

    # Web release selector uses the same latest-prerelease ordering.
    path = "apps/server/issue189_release_selector.py"
    text = read(path)
    anchor = "from typing import Any\n\n"
    if anchor not in text:
        raise RuntimeError("issue189 import anchor missing")
    text = text.replace(
        anchor,
        anchor + "from apps.versioning import version_key as _shared_version_key\n\n",
        1,
    )
    pattern = re.compile(r"def _version_key\(value: str\).*?\n\ndef _latest", re.S)
    text, count = pattern.subn(
        "def _version_key(value: str):\n    return _shared_version_key(value)\n\n\ndef _latest",
        text,
        count=1,
    )
    if count != 1:
        raise RuntimeError("failed to patch issue189 _version_key")
    write(path, text)

    # Web incremental updater must match the complete release identity.
    path = "apps/web/web_updater.py"
    text = read(path)
    anchor = "from typing import Any\n\n"
    if anchor not in text:
        raise RuntimeError("web_updater import anchor missing")
    text = text.replace(
        anchor,
        anchor + "from apps.versioning import same_release_version\n\n",
        1,
    )
    old = "if not base or _version_key(base) != _version_key(current):"
    if text.count(old) != 1:
        raise RuntimeError(f"web updater base comparison match count={text.count(old)}")
    text = text.replace(old, "if not base or not same_release_version(base, current):", 1)
    write(path, text)

    # Ordinary config saves must reload persisted quanxian.yaml, not defaults.
    path = "apps/server/server.py"
    text = read(path)
    old = 'self.server.queue_manager.load_quanxian(updated.get("quanxian", {}))'
    count = text.count(old)
    if count < 1:
        raise RuntimeError("server permission reload bug pattern missing")
    text = text.replace(old, "self.server.queue_manager.load_quanxian(load_quanxian())")
    write(path, text)

    # Transactional relay startup with rollback/retry on partial failures.
    path = "apps/server/issue79_guard.py"
    text = read(path)
    pattern = re.compile(
        r"    def start\(self\) -> None:\n.*?\n    def stop\(self\) -> None:",
        re.S,
    )
    replacement = '''    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            started: list[Any] = []
            try:
                for platform in self.active_platforms:
                    proxy = _RelayServerProxy(self.server, platform)
                    relay = self.server_module._create_danmu_relay(proxy)
                    self._proxies[platform] = proxy
                    self._relays[platform] = relay
                    started.append(relay)
                    relay.start()
                    self.server.logger.info("Danmu relay started platform=%s (multi-platform)", platform)
                if not self.active_platforms:
                    self.server.logger.info("All danmu relays are disabled by active_platforms")
                self._started = True
            except Exception:
                for relay in reversed(started):
                    try:
                        relay.stop()
                    except Exception:  # noqa: BLE001
                        pass
                for relay in reversed(started):
                    try:
                        relay.join(timeout=2.0)
                    except Exception:  # noqa: BLE001
                        pass
                self._relays.clear()
                self._proxies.clear()
                self._started = False
                raise

    def stop(self) -> None:'''
    text, count = pattern.subn(replacement, text, count=1)
    if count != 1:
        raise RuntimeError("failed to patch MultiPlatformRelayManager.start")
    write(path, text)

    # Exit backup completion is recorded only after a successful write.
    path = "apps/server/settings_storage_guard.py"
    text = read(path)
    old = '''                if reason == "exit":
                    with getattr(backup_module, "_PATCH_LOCK", _PATCH_LOCK):
                        if bool(getattr(backup_module, "_EXIT_BACKUP_DONE", False)):
                            return
                        backup_module._EXIT_BACKUP_DONE = True
                service.backup_now()
'''
    new = '''                if reason == "exit":
                    with getattr(backup_module, "_PATCH_LOCK", _PATCH_LOCK):
                        if bool(getattr(backup_module, "_EXIT_BACKUP_DONE", False)):
                            return
                service.backup_now()
                if reason == "exit":
                    with getattr(backup_module, "_PATCH_LOCK", _PATCH_LOCK):
                        backup_module._EXIT_BACKUP_DONE = True
'''
    if text.count(old) != 1:
        raise RuntimeError("settings_storage_guard exit backup pattern missing")
    write(path, text.replace(old, new, 1))

    # New targeted regression/boundary guard. It lives in GitHub CI space only.
    write(
        ".github/ci/issue262_six_bug_and_scripts_guard.py",
        '''from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    assert not (ROOT / "scripts").exists(), "root scripts/ must not be a runtime or CI dependency"

    from apps.versioning import same_release_version, version_key

    assert version_key("3.0.13-feiqi") < version_key("3.0.13-test")
    assert version_key("3.0.13-test") < version_key("3.0.13-cx")
    assert version_key("3.0.13-cx") < version_key("3.0.13-gc")
    assert version_key("3.0.13-gc") < version_key("3.0.13")
    assert same_release_version("v3.0.13-test", "3.0.13-test+build.1")
    assert not same_release_version("3.0.13-test", "3.0.13-gc")
    assert not same_release_version("3.0.13-gc", "3.0.13")

    pm = importlib.import_module("apps.server.plugin_manager")
    assert pm._version_key("3.0.13-test") < pm._version_key("3.0.13-gc")
    selector = importlib.import_module("apps.server.issue189_release_selector")
    assert selector._version_key("3.0.13-test") < selector._version_key("3.0.13-gc")

    updater_source = (ROOT / "apps/web/web_updater.py").read_text(encoding="utf-8")
    assert "not same_release_version(base, current)" in updater_source

    server_source = (ROOT / "apps/server/server.py").read_text(encoding="utf-8")
    assert 'load_quanxian(updated.get("quanxian", {}))' not in server_source
    assert "queue_manager.load_quanxian(load_quanxian())" in server_source

    storage_source = (ROOT / "apps/server/settings_storage_guard.py").read_text(encoding="utf-8")
    backup_pos = storage_source.index("service.backup_now()")
    done_pos = storage_source.index("backup_module._EXIT_BACKUP_DONE = True", backup_pos)
    assert done_pos > backup_pos

    from apps.server.issue79_guard import MultiPlatformRelayManager

    events: list[str] = []
    fail_second = {"value": True}

    class Relay:
        def __init__(self, platform: str) -> None:
            self.platform = platform

        def start(self) -> None:
            events.append(f"start:{self.platform}")
            if self.platform == "douyin" and fail_second["value"]:
                raise RuntimeError("synthetic relay start failure")

        def stop(self) -> None:
            events.append(f"stop:{self.platform}")

        def join(self, timeout=None) -> None:
            events.append(f"join:{self.platform}")

    fake_module = SimpleNamespace(_create_danmu_relay=lambda proxy: Relay(proxy.platform))
    fake_server = SimpleNamespace(runtime_config={}, logger=logging.getLogger("issue262"))
    manager = MultiPlatformRelayManager(fake_module, fake_server, ("bilibili", "douyin"))
    try:
        manager.start()
    except RuntimeError:
        pass
    else:
        raise AssertionError("synthetic relay failure was not propagated")
    assert manager._started is False
    assert manager._relays == {}
    assert manager._proxies == {}
    assert "stop:bilibili" in events and "join:bilibili" in events

    fail_second["value"] = False
    manager.start()
    assert manager._started is True
    assert set(manager._relays) == {"bilibili", "douyin"}
    manager.stop()

    forbidden = ("scripts/", ".github/ci/")
    for base_name in ("apps", "core"):
        for path in (ROOT / base_name).rglob("*"):
            if not path.is_file() or path.suffix.lower() not in {".py", ".spec", ".ps1", ".sh"}:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for token in forbidden:
                assert token not in text, f"production runtime references CI-only path {token}: {path}"

    for workflow in (ROOT / ".github/workflows").glob("*.yml"):
        text = workflow.read_text(encoding="utf-8", errors="ignore")
        assert "scripts/" not in text, f"workflow still depends on scripts/: {workflow.name}"

    print("issue #262 six bug fixes and scripts decoupling guard: OK")


if __name__ == "__main__":
    main()
''',
    )

    # Temporary refactor machinery removes itself before the real commit.
    (ROOT / ".github/workflows/issue262-refactor-once.yml").unlink(missing_ok=True)
    Path(__file__).unlink(missing_ok=True)

    assert not (ROOT / "scripts").exists()
    assert (ROOT / ".github/ci/issue260_five_bug_guard.py").is_file()
    assert (ROOT / ".github/ci/build_incremental_update.py").is_file()
    assert (ROOT / ".github/ci/generate_api_docs.py").is_file()
    assert (ROOT / "apps/versioning.py").is_file()


if __name__ == "__main__":
    main()
