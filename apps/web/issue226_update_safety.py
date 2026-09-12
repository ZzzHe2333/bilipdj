from __future__ import annotations

from pathlib import Path
from typing import Any


def install_issue226_web_update_safety(module: Any) -> bool:
    """Guarantee that Web Portable is relaunched after any post-stop failure.

    The issue-222 incremental handler stops the running app before creating its
    snapshot and per-file rollback preparation.  Those preparation steps occur
    before the handler's internal try/except.  If one of them raises, the old
    process is already gone and nothing restarts it.  This wrapper tracks whether
    the app was actually stopped and whether any launch succeeded; if the wrapped
    handler propagates an exception without a successful relaunch, the original
    application is started again.
    """

    if bool(getattr(module, "_issue226_web_update_safety_installed", False)):
        return True

    original_incremental = module._incremental_update  # noqa: SLF001

    def safe_incremental(request: dict[str, Any], state: Any, work: Path) -> None:
        original_stop = module._stop_app  # noqa: SLF001
        original_launch = module._launch_main  # noqa: SLF001
        stopped = False
        launched = False

        def tracked_stop(*args: Any, **kwargs: Any) -> Any:
            nonlocal stopped
            result = original_stop(*args, **kwargs)
            stopped = True
            return result

        def tracked_launch(*args: Any, **kwargs: Any) -> Any:
            nonlocal launched
            result = original_launch(*args, **kwargs)
            launched = True
            return result

        module._stop_app = tracked_stop  # type: ignore[attr-defined]  # noqa: SLF001
        module._launch_main = tracked_launch  # type: ignore[attr-defined]  # noqa: SLF001
        try:
            return original_incremental(request, state, work)
        except Exception:
            # If the failure happened after _stop_app but before the original
            # handler reached its own rollback/relaunch block, restore service by
            # starting the still-unmodified old program. If the original handler
            # already relaunched successfully, do not start a duplicate process.
            if stopped and not launched:
                app_dir = Path(request["app_dir"]).resolve()
                main_exe = str(request.get("main_exe", "") or "")
                if main_exe and (app_dir / main_exe).is_file():
                    try:
                        original_launch(app_dir, main_exe)
                    except Exception:
                        pass
            raise
        finally:
            module._stop_app = original_stop  # type: ignore[attr-defined]  # noqa: SLF001
            module._launch_main = original_launch  # type: ignore[attr-defined]  # noqa: SLF001

    module._incremental_update = safe_incremental  # type: ignore[attr-defined]  # noqa: SLF001
    module._issue226_web_update_safety_installed = True
    return True


__all__ = ["install_issue226_web_update_safety"]
