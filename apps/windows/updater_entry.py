from __future__ import annotations

from apps.windows import updater_gui
from apps.windows.updater_apply import patch_updater_v2
from apps.windows.updater_safety import install_updater_safety


def main() -> int:
    patch_updater_v2(updater_gui.updater_v2)
    install_updater_safety()
    return int(updater_gui.main())


if __name__ == "__main__":
    raise SystemExit(main())
