"""Issue #286: version-check light effect lifecycle and Web visual feedback."""
from __future__ import annotations

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from apps.windows import update_check_glow as glow
from apps.windows import update_page, update_ui, update_version_selector


class Root:
    def __init__(self):
        self.pending = {}
        self.counter = 0
    def after(self, ms, fn):
        assert ms >= 0 and callable(fn)
        self.counter += 1
        self.pending[self.counter] = fn
        return self.counter
    def after_cancel(self, task):
        self.pending.pop(task, None)


class Canvas:
    def __init__(self):
        self.visible = False
        self.shapes = []
        self.existing = True
    def winfo_exists(self): return self.existing
    def winfo_width(self): return 320
    def delete(self, tag): self.shapes.clear()
    def create_rectangle(self, *args, **kwargs): self.shapes.append((args, kwargs))
    def place(self, **kwargs): self.visible = True
    def place_forget(self): self.visible = False


class Progress:
    def __init__(self):
        self.mode = "determinate"
        self.running = False
    def configure(self, **kwargs):
        self.mode = kwargs.get("mode", self.mode)
    def start(self, delay):
        assert delay > 0
        self.running = True
    def stop(self):
        self.running = False


class App:
    def __init__(self):
        self.root = Root()
        self._update_progress = Progress()
        self._update_check_glow_canvas = Canvas()
        self._update_check_glow_active = False
        self._update_check_glow_after = None
        self._update_busy = False


def main():
    app = App()
    glow._start_check_glow(app)
    assert app._update_check_glow_active
    assert app._update_check_glow_canvas.visible
    assert app._update_progress.mode == "indeterminate" and app._update_progress.running
    assert app._update_check_glow_canvas.shapes
    assert len(app.root.pending) == 1
    glow._start_check_glow(app)
    assert len(app.root.pending) == 1, "second click must not spawn another ticker"
    task = next(iter(app.root.pending))
    fn = app.root.pending.pop(task)
    fn()
    assert len(app.root.pending) == 1, "ticker must advance on Tk's event loop"
    glow._stop_check_glow(app)
    assert not app._update_check_glow_active
    assert not app._update_check_glow_canvas.visible
    assert app._update_progress.mode == "determinate" and not app._update_progress.running
    assert not app.root.pending
    glow._stop_check_glow(app)  # idempotent

    real_busy = update_ui._set_busy
    real_check = update_version_selector.check_for_versions
    try:
        def fake_busy(app, busy):
            app._update_busy = busy
        def fake_check(app, silent=False):
            update_ui._set_busy(app, True)
        update_ui._set_busy = fake_busy
        update_version_selector.check_for_versions = fake_check
        glow.install_update_check_glow()
        glow.install_update_check_glow()
        check = update_version_selector.check_for_versions
        check(app)
        assert app._update_check_glow_active and app._update_busy
        update_ui._set_busy(app, False)
        assert not app._update_check_glow_active and not app._update_progress.running
        assert not app.root.pending
    finally:
        update_ui._set_busy = real_busy
        update_version_selector.check_for_versions = real_check

    js = (ROOT / "apps/web/static/web_updater_control.js").read_text(encoding="utf-8")
    css = (ROOT / "apps/web/static/web_updater_control.css").read_text(encoding="utf-8")
    assert "if (checkInFlight) return;" in js
    assert "container?.classList.add('is-checking')" in js
    assert "container?.classList.remove('is-checking')" in js
    assert "container?.removeAttribute('aria-busy')" in js
    assert "finally {" in js
    assert "@keyframes web-update-glow-sweep" in css
    assert "prefers-reduced-motion:reduce" in css
    assert "pointer-events:none" in css
    print("issue #286 update-check glow lifecycle: OK")


if __name__ == "__main__":
    main()
