"""Issue #298 guard: README latest links, marketing page and live UI controls."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

def get(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")

def main() -> None:
    readme = get("README.md")
    assert "releases/latest" in readme and "releases/download/v3.0.12" not in readme
    site = get("site/index.html")
    for token in ('id="steps"', 'id="faq"', 'id="windows-download"', 'id="web-download"', 'id="theme-toggle"', 'http://127.0.0.1:9816/index'):
        assert token in site, token
    sitejs = get("site/app.js")
    assert "releases/latest" in sitejs and "browser_download_url" in sitejs
    control = get("apps/web/static/control.html")
    for token in ('id="queue-next"', 'data-view="queue"', 'platform-status-bilibili', 'platform-status-douyin'):
        assert token in control
    logic = get("apps/web/static/control.js")
    assert 'async function completeCurrent()' in logic and "'/api/queue/delete', { index: 1 }" in logic
    assert "refreshPlatformConnections" in logic and "event.altKey" in logic
    detailed = get("apps/web/static/control_issue79.js")
    assert "username.focus()" in detailed and "dragstart" in detailed
    assert "contrastRatio" in detailed and "loadPreviewScreenshot" in detailed and "renderPreviewCase" in detailed
    overlay = get("apps/web/static/myjs.js")
    assert "queue.slice(0, 8)" in overlay and "PDJ_UpdateOverlayState" in overlay
    dpi = get("apps/windows/main.py")
    assert dpi.index("enable_system_dpi_awareness()") < dpi.index("open_startup_splash(")
    print("issue #298 landing, queue, preview, DPI guards: OK")

if __name__ == "__main__":
    main()
