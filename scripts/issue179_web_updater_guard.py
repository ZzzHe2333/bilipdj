from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_server_api() -> None:
    source = read("apps/server/web_update_api.py")
    assert 'WEB_PACKAGE_KEY = "web-portable-x64"' in source
    assert 'WEB_UPDATER_EXE = "BiliPDJ-Web-Updater.exe"' in source
    assert '"full", "incremental", "restore"' in source
    assert '"backend_pid": os.getpid()' in source
    assert '"launcher_pid": os.getppid()' in source
    assert "token_urlsafe" in source and "127.0.0.1" in source
    assert "_safe_backup" in source and "relative_to(root)" in source

    main = read("apps/server/main.py")
    assert "install_web_update_api" in main


def check_updater_engine() -> None:
    source = read("apps/web/web_updater.py")
    assert "ThreadingHTTPServer" in source and '"/update.html"' in source and '"/api/status"' in source
    assert 'status != 206' in source and '"Range"' in source, "incremental transport must require HTTP 206 Range"
    assert "_snapshot" in source and 'app_dir / "backup"' in source
    assert "rolled_back" in source and "rollback" in source
    assert "_safe_extract" in source and "MAX_ARCHIVE_UNCOMPRESSED_BYTES" in source
    assert "PRESERVED_PREFIXES" in source and '"plugins/"' in source and '"key/"' in source
    assert "launcher_pid" in source and "backend_pid" in source

    html = read("apps/web/updater_static/update.html")
    assert "俄罗斯方块" in html
    assert "requestAnimationFrame" in html and "autoStep" in html
    assert "/api/status?token=" in html
    assert 'id="percent"' in html and 'id="currentFile"' in html and 'id="speed"' in html


def check_web_control() -> None:
    js = read("apps/web/static/web_updater_control.js")
    assert "/api/control/web-update/state" in js
    assert "/api/control/web-update/start" in js
    for mode in ("full", "incremental", "restore"):
        assert mode in js
    assert "window.open('about:blank'" in js
    assert "俄罗斯方块" in js

    command = read("apps/server/command_console.py")
    assert "/web_updater_control.js" in command and "/web_updater_control.css" in command


def check_packaging() -> None:
    package = read("apps/web/package-portable.ps1")
    assert "web_updater.spec" in package
    assert "BiliPDJ-Web-Updater.exe" in package

    spec = read("apps/web/web_updater.spec")
    assert "web_updater.py" in spec and "updater_static" in spec and 'name="BiliPDJ-Web-Updater"' in spec

    builder = read("scripts/build_web_incremental_update.py")
    assert 'PACKAGE_KEY = "web-portable-x64"' in builder
    assert "packed_sha256" in builder and "removed_from_base" in builder

    workflow = read(".github/workflows/package-windows-x64.yml")
    assert "web_files" in workflow and "web_incremental" in workflow
    assert "build_web_incremental_update.py" in workflow
    assert "BiliPDJ-Web-Updater.exe" in workflow
    assert '"web-portable-x64"' in workflow
    assert 'transport = "http-range"' in workflow


def check_no_implicit_release() -> None:
    workflow = read(".github/workflows/package-windows-x64.yml")
    assert "if: github.event_name == 'workflow_dispatch' || startsWith(github.ref, 'refs/tags/v')" in workflow


def main() -> None:
    check_server_api()
    check_updater_engine()
    check_web_control()
    check_packaging()
    check_no_implicit_release()
    print("issue #179 Web Portable updater guard: OK")


if __name__ == "__main__":
    main()
