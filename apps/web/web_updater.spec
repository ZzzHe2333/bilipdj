# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

spec_dir = Path(SPECPATH).resolve()
project_root = spec_dir.parents[1]

datas = [
    (str(project_root / "apps" / "web" / "updater_static" / "update.html"), "apps/web/updater_static"),
]

# web_updater_entry.py imports web_updater.py and requires a confirmed browser
# handoff before the updater is allowed to terminate the running Web service.
a = Analysis(
    [str(project_root / "apps" / "web" / "web_updater_entry.py")],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="BiliPDJ-Web-Updater",
    icon=str(project_root / "apps" / "windows" / "assets" / "256x.ico"),
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
)
