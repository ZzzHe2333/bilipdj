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
  width: auto;
  min-height: 0;
  height: auto;
  margin: 0;
  padding: 0;
  overflow: visible;
  text-overflow: clip;
  white-space: normal;
  border: 0;
  border-radius: 0;
  background: transparent;
  box-shadow: none;
  backdrop-filter: none;
}
.wk::before { content: none; }
.vText { display: none !important; }
.div {
  position: relative;
  display: grid;
  width: auto;
  height: auto;
  max-height: none;
  float: none;
  gap: var(--queue-item-gap);
  margin-top: 0;
  overflow: visible;
  line-height: normal;
  text-align: initial;
  scrollbar-width: none;
}
.div::-webkit-scrollbar { display: none; }
.queue-item {
  display: grid;
  grid-template-columns: 44px minmax(0, 1fr);
  align-items: center;
  min-height: 0;
  padding: var(--queue-item-padding-y) var(--queue-item-padding-x);
  border: 0;
  border-radius: 0;
  background: transparent;
}
.queue-item.no-sequence { grid-template-columns: minmax(0, 1fr); }
.queue-number {
  display: grid;
  place-items: center;
  width: 34px;
  height: 34px;
  border-radius: 0;
  background: transparent;
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
  .wk { width: auto; margin: 0; padding: 0; border-radius: 0; }
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
