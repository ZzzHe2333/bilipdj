# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

spec_dir = Path(SPECPATH).resolve()
project_root = spec_dir.parents[1]

a = Analysis(
    [str(project_root / "apps" / "windows" / "overlay_host.py")],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=[
        "apps.windows.overlay_bootstrap",
        "apps.windows.overlay_refresh_guard",
        "apps.windows.overlay_performance_guard",
        "apps.windows.style_option_guard",
        "apps.server.style_option_guard",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[str(project_root / "apps" / "windows" / "pyi_overlay_runtime_hook.py")],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="paiduijitm",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    exclude_binaries=False,
)
