from __future__ import annotations

import functools
import threading
from typing import Any

_PATCH_LOCK = threading.RLock()

_LAYOUT_CSS = r'''
/* bilipdj queue layout */
html, body {
  margin: 0;
  min-height: 100%;
  background: transparent !important;
  color: var(--text-color);
  font-family: var(--queue-font-family);
}
.wk {
  position: relative;
  width: min(92%, 720px);
  min-height: 72vh;
  height: auto;
  margin: 4vh auto;
  padding: 22px;
  overflow: hidden;
  text-overflow: clip;
  white-space: normal;
  border: 1px solid rgba(0,229,255,.32);
  border-radius: 24px;
  background: linear-gradient(145deg, rgba(8,18,38,.88), rgba(4,10,24,.72));
  box-shadow: 0 24px 80px rgba(0,0,0,.34), inset 0 1px rgba(255,255,255,.08);
}
.vText { display: none !important; }
.div {
  position: relative;
  display: grid;
  width: auto;
  height: auto;
  max-height: 68vh;
  float: none;
  gap: var(--queue-item-gap);
  margin-top: 0;
  overflow: auto;
  line-height: normal;
  text-align: initial;
  scrollbar-width: thin;
  scrollbar-color: #00e5ff transparent;
}
.queue-item {
  display: grid;
  grid-template-columns: 44px minmax(0, 1fr);
  align-items: center;
  min-height: 54px;
  padding: var(--queue-item-padding-y) var(--queue-item-padding-x);
  border: 1px solid rgba(255,255,255,.08);
  border-radius: 14px;
  background: linear-gradient(90deg, rgba(255,255,255,.08), rgba(255,255,255,.025));
}
.queue-item.no-sequence { grid-template-columns: minmax(0, 1fr); }
.queue-number {
  display: grid;
  place-items: center;
  width: 34px;
  height: 34px;
  border-radius: 10px;
  background: rgba(0,229,255,.14);
  color: #00e5ff;
  font-size: 12px;
  font-weight: 800;
}
.queue-content {
  min-width: 0;
  overflow: hidden;
  color: var(--text-color);
  font-family: var(--queue-font-family);
  font-size: var(--queue-font-size);
  font-weight: var(--queue-font-weight);
  font-style: var(--queue-font-style);
  letter-spacing: var(--queue-letter-spacing);
  word-spacing: var(--queue-word-spacing);
  line-height: var(--queue-line-height);
  text-align: var(--queue-text-align);
  opacity: var(--queue-text-opacity);
  text-overflow: ellipsis;
  white-space: nowrap;
  -webkit-text-stroke: 2px var(--text-stroke);
}
@media (max-width: 520px) {
  .wk { width: auto; margin: 12px; padding: 16px; border-radius: 18px; }
}
'''


def install_web_queue_layout_guard(style_module: Any | None = None) -> bool:
    if style_module is None:
        from . import style_option_guard as style_module
    with _PATCH_LOCK:
        current = getattr(style_module, "_enhance_css", None)
        if not callable(current):
            return False
        if bool(getattr(current, "_bilipdj_modern_queue_layout", False)):
            return True

        @functools.wraps(current)
        def enhance_css_with_layout(css: str, style: dict[str, Any]) -> str:
            result = current(css, style)
            return result + "\n" + _LAYOUT_CSS

        setattr(enhance_css_with_layout, "_bilipdj_modern_queue_layout", True)
        setattr(style_module, "_enhance_css", enhance_css_with_layout)
        return True


__all__ = ["install_web_queue_layout_guard"]
