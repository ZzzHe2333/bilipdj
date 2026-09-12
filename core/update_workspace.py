from __future__ import annotations

# Compatibility re-export.  The authoritative implementation lives in
# apps.update_workspace so update preparation, apply and cleanup all share one
# path-safety policy.
from apps.update_workspace import (  # noqa: F401
    UPDATE_DIR_NAME,
    allocate_update_session,
    cleanup_update_session,
    update_root,
    validate_update_session,
)

__all__ = [
    "UPDATE_DIR_NAME",
    "allocate_update_session",
    "cleanup_update_session",
    "update_root",
    "validate_update_session",
]
