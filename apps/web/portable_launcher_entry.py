from __future__ import annotations

import multiprocessing as mp
import os
import sys

from apps.web import portable_launcher
from apps.web.issue187_runtime_guard import patch_portable_launcher


def main() -> None:
    mp.freeze_support()
    patch_portable_launcher(portable_launcher.WebPortableLauncher)
    if "--plugin-runtime-self-test" in sys.argv[1:]:
        try:
            from apps.windows.frozen_plugin_probe import run_frozen_plugin_probe

            run_frozen_plugin_probe()
        except BaseException:
            os._exit(1)
        os._exit(0)
    portable_launcher.main()


if __name__ == "__main__":
    main()
