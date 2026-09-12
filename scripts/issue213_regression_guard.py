from __future__ import annotations

import sys
import tempfile
import types
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def check_navigation_geometry_settles_before_return() -> None:
    from apps.windows.issue209_nav_stability import _freeze_navigation_width

    class FakeRow:
        def __init__(self, requested_width: int) -> None:
            self.requested_width = requested_width

        def winfo_reqwidth(self) -> int:
            return self.requested_width

    class FakeShell:
        def __init__(self) -> None:
            self.columns: dict[int, dict[str, int]] = {}

        def columnconfigure(self, index: int, **kwargs) -> None:
            self.columns[index] = dict(kwargs)

    rows = [FakeRow(88), FakeRow(104), FakeRow(96)]

    class FakeNav:
        def __init__(self, master: FakeShell) -> None:
            self.master = master
            self.actual_width = 90
            self.configured_width = 90
            self.propagate = True

        def pack_propagate(self, value: bool) -> None:
            self.propagate = bool(value)

        def configure(self, **kwargs) -> None:
            if "width" in kwargs:
                # Tk does not necessarily change the actual width until the next
                # idle-layout pass. Model that delayed geometry explicitly.
                self.configured_width = int(kwargs["width"])

        def winfo_width(self) -> int:
            return self.actual_width

        def update_idletasks(self) -> None:
            self.settle()

        def settle(self) -> None:
            if self.propagate:
                self.actual_width = max(row.winfo_reqwidth() for row in rows)
            else:
                self.actual_width = self.configured_width

    shell = FakeShell()
    nav = FakeNav(shell)

    class FakeRoot:
        def __init__(self) -> None:
            self.update_calls = 0

        def update_idletasks(self) -> None:
            self.update_calls += 1
            nav.settle()

    panel = types.SimpleNamespace(
        root=FakeRoot(),
        _nav_frame=nav,
        _nav_items=[(row, object(), object()) for row in rows],
    )
    width = _freeze_navigation_width(panel)
    assert width == 116
    assert nav.configured_width == 116
    assert nav.actual_width == 116, "final requested sidebar width must settle before _build_ui returns"
    assert nav.propagate is False
    assert panel.root.update_calls >= 2, "one pre-measure and one post-lock idle pass are required"
    assert shell.columns[0] == {"weight": 0, "minsize": 116}


def check_full_update_preserves_appearance() -> None:
    from apps.windows import updater

    assert Path("appearance.json") in updater.PRESERVE_PATHS
    assert Path("core/appearance.json") in updater.PRESERVE_PATHS

    class FakeProcess:
        def poll(self):
            return None

    original_launch = updater.launch_main
    original_grace = updater.STARTUP_GRACE_SECONDS
    try:
        updater.launch_main = lambda *_args, **_kwargs: FakeProcess()  # type: ignore[assignment]
        updater.STARTUP_GRACE_SECONDS = 0.0
        with tempfile.TemporaryDirectory(prefix="bilipdj-issue213-") as temp_dir:
            root = Path(temp_dir)
            app_dir = root / "BiliPDJ"
            app_dir.mkdir()
            (app_dir / "main.exe").write_bytes(b"old-main")
            (app_dir / "updater.exe").write_bytes(b"old-updater")
            old_root_appearance = '{"mode":"light","light":{"accent":"#123456"}}\n'
            old_core_appearance = '{"mode":"light","light":{"accent":"#654321"}}\n'
            (app_dir / "appearance.json").write_text(old_root_appearance, encoding="utf-8")
            (app_dir / "core").mkdir()
            (app_dir / "core" / "appearance.json").write_text(old_core_appearance, encoding="utf-8")

            zip_path = root / "download" / "update.zip"
            zip_path.parent.mkdir()
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("main.exe", b"new-main")
                archive.writestr("updater.exe", b"new-updater")
                archive.writestr("appearance.json", '{"mode":"dark"}\n')
                archive.writestr("core/appearance.json", '{"mode":"dark"}\n')

            updater.perform_update(
                pid=0,
                app_dir=app_dir,
                zip_path=zip_path,
                main_exe_name="main.exe",
                target_version="3.0.8-test",
            )

            assert (app_dir / "main.exe").read_bytes() == b"new-main"
            assert (app_dir / "appearance.json").read_text(encoding="utf-8") == old_root_appearance
            assert (app_dir / "core" / "appearance.json").read_text(encoding="utf-8") == old_core_appearance
    finally:
        updater.launch_main = original_launch
        updater.STARTUP_GRACE_SECONDS = original_grace


def _fake_release(version: str):
    from apps.windows import update_client

    asset = update_client.ReleaseAsset(
        name=f"BiliPDJ-v{version}-Windows-Tk-Portable-x64.zip",
        download_url=f"https://example.invalid/{version}.zip",
        size=1024,
        sha256="a" * 64,
    )
    return update_client.ReleaseInfo(
        version=version,
        tag_name=f"v{version}",
        name=f"v{version}",
        body="",
        page_url="",
        zip_asset=asset,
        checksum_asset=update_client.ReleaseAsset(
            name=f"{asset.name}.sha256",
            download_url="",
            size=0,
            sha256="a" * 64,
        ),
        sha256="a" * 64,
        manifest_url="",
    )


def check_recent_ten_release_catalog() -> None:
    from apps.windows import issue189_release_selector as selector

    raw = [
        {
            "tag_name": f"v3.0.{index}{'-test' if index % 2 else ''}",
            "draft": False,
            "prerelease": bool(index % 2),
            "published_at": f"2026-09-{30 - index:02d}T00:00:00Z",
        }
        for index in range(12)
    ]
    raw.insert(
        2,
        {
            "tag_name": "v99.0.0-draft",
            "draft": True,
            "prerelease": True,
            "published_at": "2026-12-31T00:00:00Z",
        },
    )

    original_read = selector._read_release_list
    original_info = selector._release_info
    try:
        selector._read_release_list = lambda **_kwargs: [item for item in raw if not item["draft"]]  # type: ignore[assignment]
        selector._release_info = lambda item, **_kwargs: _fake_release(selector._release_version(item))  # type: ignore[assignment]
        releases = selector.fetch_release_choices(timeout=0.1)
    finally:
        selector._read_release_list = original_read
        selector._release_info = original_info

    assert selector.RECENT_RELEASE_LIMIT == 10
    assert len(releases) == 10
    assert [release.tag_name for release in releases] == [item["tag_name"] for item in raw if not item["draft"]][:10]
    assert any("-test" in release.version for release in releases)
    assert any("-test" not in release.version for release in releases)

    current = "3.0.7-test"
    target = releases[0]
    selector._RELEASE_INCREMENTAL_META.clear()
    selector._RELEASE_INCREMENTAL_META[target.tag_name] = (current, "http-range")
    selector._RELEASE_INCREMENTAL_META[releases[1].tag_name] = ("3.0.6", "http-range")
    eligible = selector._incremental_eligible_tags(current, releases)
    assert eligible == {target.tag_name}
    assert selector._operation_text("3.0.8-test", "3.0.7-test") == "升级"
    assert selector._operation_text("3.0.7-test", "3.0.7-test") == "重新安装"
    assert selector._operation_text("3.0.6", "3.0.7-test") == "降级"


def main() -> None:
    check_navigation_geometry_settles_before_return()
    check_full_update_preserves_appearance()
    check_recent_ten_release_catalog()
    print("issue #213 regression guard: OK")


if __name__ == "__main__":
    main()
