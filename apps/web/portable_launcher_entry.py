from __future__ import annotations

import multiprocessing as mp
import os
import sys

def _selftest_mark(stage: str) -> None:
    if os.environ.get("BILIPDJ_FROZEN_DIAGNOSTIC") != "1":
        return
    from pathlib import Path
    import tempfile
    path = Path(tempfile.gettempdir()) / ("bilipdj-web-probe-" + str(os.getpid()) + ".log")
    with path.open("a", encoding="ascii") as file:
        file.write(stage + "\n")


def main() -> None:
    _selftest_mark("enter-main")
    # A frozen multiprocessing child must be handled before importing the
    # launcher (which imports Tk, server modules and other startup workers).
    mp.freeze_support()
    _selftest_mark("after-freeze-support")
    if "--plugin-runtime-self-test" in sys.argv[1:]:
        _selftest_mark("selftest-branch")
        try:
            _selftest_mark("before-probe-import")
            import faulthandler
            from pathlib import Path
            import tempfile
            diag_handle = None
            if os.environ.get("BILIPDJ_FROZEN_DIAGNOSTIC") == "1":
                diag = Path(tempfile.gettempdir()) / ("bilipdj-web-probe-" + str(os.getpid()) + ".log")
                diag_handle = diag.open("a", encoding="utf-8")
                faulthandler.dump_traceback_later(12, repeat=True, file=diag_handle)
            from apps.windows.frozen_plugin_probe import run_frozen_plugin_probe
            _selftest_mark("after-probe-import")
            if diag_handle is not None:
                faulthandler.cancel_dump_traceback_later()
                diag_handle.close()

            run_frozen_plugin_probe()
            _selftest_mark("probe-returned-success")
        except BaseException:
            _selftest_mark("probe-raised")
            os._exit(1)
        os._exit(0)

    from apps.web import portable_launcher
    from apps.web.issue187_runtime_guard import patch_portable_launcher

    patch_portable_launcher(portable_launcher.WebPortableLauncher)
    portable_launcher.main()


if __name__ == "__main__":
    main()
