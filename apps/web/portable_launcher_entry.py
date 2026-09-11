from __future__ import annotations

from apps.web import portable_launcher
from apps.web.issue187_runtime_guard import patch_portable_launcher


def main() -> None:
    patch_portable_launcher(portable_launcher.WebPortableLauncher)
    portable_launcher.main()


if __name__ == "__main__":
    main()
