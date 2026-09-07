# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

spec_dir = Path(SPECPATH).resolve()
project_root = spec_dir.parents[1]

a = Analysis(
    [str(project_root / "core" / "overlay_host.py")],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=[
        "core.overlay_bootstrap",
        "core.overlay_refresh_guard",
        "core.overlay_performance_guard",
        "core.style_option_guard",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[str(project_root / "core" / "pyi_overlay_runtime_hook.py")],
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
    icon=str(project_root / "core" / "256x.ico"),
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    exclude_binaries=False,
)
