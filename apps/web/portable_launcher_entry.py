from __future__ import annotations

import multiprocessing as mp
import os
import sys


def main() -> None:
    # Handle frozen multiprocessing children before importing Tk/launcher.
    # Keeping the probe independent avoids starting the Web UI during CI.
    mp.freeze_support()
    if "--plugin-runtime-self-test" in sys.argv[1:]:
        try:
            from apps.windows.frozen_plugin_probe import run_frozen_plugin_probe
            run_frozen_plugin_probe()
        except BaseException:
            os._exit(1)
        os._exit(0)

    from apps.web import portable_launcher
    from apps.web.issue187_runtime_guard import patch_portable_launcher
    patch_portable_launcher(portable_launcher.WebPortableLauncher)
    portable_launcher.main()


if __name__ == "__main__":
    main()
