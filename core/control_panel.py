"""Compatibility entry point; Windows implementation moved to apps.windows."""
from __future__ import annotations

if __name__ == "__main__":
    from apps.windows.main import main
    main()
else:
    from importlib import import_module as _import_module
    import sys as _sys
    _compat_name = __name__
    _impl = _import_module("apps.windows.control_panel")
    _sys.modules[_compat_name] = _impl
    globals().update(_impl.__dict__)
