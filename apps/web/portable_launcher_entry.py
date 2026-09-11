from __future__ import annotations

import multiprocessing as mp

from apps.web import portable_launcher
from apps.web.issue187_runtime_guard import patch_portable_launcher


def main() -> None:
    mp.freeze_support()
    patch_portable_launcher(portable_launcher.WebPortableLauncher)
    portable_launcher.main()


if __name__ == "__main__":
    main()
