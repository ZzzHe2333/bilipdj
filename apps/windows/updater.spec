# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

spec_dir = Path(SPECPATH).resolve()
project_root = spec_dir.parents[1]

a = Analysis(
    [str(project_root / "apps" / "windows" / "updater_entry.py")],
    pathex=[str(project_root), str(project_root / "apps" / "windows")], binaries=[], datas=[],
    hiddenimports=[
        "apps.windows.updater_gui", "apps.windows.updater_apply", "apps.windows.updater_safety",
        "apps.windows.updater_v2", "apps.windows.updater", "apps.windows.updater_model",
        "apps.windows.incremental_apply", "apps.windows.update_manifest",
        "apps.server.log_manager", "core.updater_v2", "core.updater", "core.updater_model", "core.log_manager",
        "apps.update_workspace", "core.update_workspace", "core.issue222_update_cleanup",
    ],
    hookspath=[], hooksconfig={}, runtime_hooks=[], excludes=[], noarchive=False, optimize=0,
)
pyz = PYZ(a.pure)
exe = EXE(pyz, a.scripts, a.binaries, a.datas, [], name="updater",
    icon=str(project_root / "apps" / "windows" / "assets" / "256x.ico"),
    debug=False, bootloader_ignore_signals=False, strip=False, upx=True, console=False)
