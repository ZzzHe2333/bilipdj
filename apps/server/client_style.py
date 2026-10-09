"""Independent Windows Tk display styling; never modifies Web/OBS CSS."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def install_win_style(server_module: Any) -> None:
    def path() -> Path:
        return Path(getattr(server_module, "STYLE_WIN_PATH", Path(server_module._YAML_DIR) / "style-win.json"))

    def load_win_style() -> dict[str, Any]:
        output = dict(server_module.DEFAULT_STYLE)
        try:
            raw = json.loads(path().read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                output.update(raw)
        except (OSError, ValueError):
            pass
        return output

    def save_win_style(data: dict[str, Any]) -> None:
        merged = load_win_style()
        merged.update(data)
        server_module._atomic_write_text(
            path(), json.dumps(merged, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    server_module.load_win_style = load_win_style
    server_module.save_win_style = save_win_style


__all__ = ["install_win_style"]
