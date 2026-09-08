"""Compatibility shim for apps.server.log_manager."""
from importlib import import_module as _import_module
import sys as _sys
_compat_name = __name__
_impl = _import_module("apps.server.log_manager")
_sys.modules[_compat_name] = _impl
globals().update(_impl.__dict__)
