from __future__ import annotations

from apps.windows import updater_gui
from apps.windows.issue222_update_apply import patch_updater_v2
from apps.windows.issue226_update_safety import patch_issue226_windows_update_safety


def main() -> int:
    patch_updater_v2(updater_gui.updater_v2)
    patch_issue226_windows_update_safety()
    return int(updater_gui.main())


if __name__ == "__main__":
    raise SystemExit(main())
