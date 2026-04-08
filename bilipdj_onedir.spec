# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

project_root = Path(__file__).resolve().parent

datas = []
datas += collect_data_files("backend", include_py_files=True)
datas += collect_data_files("models", include_py_files=True)
datas += collect_data_files("toGUI", include_py_files=True)

# 运行时会读写 pd/ 下的归档，打包时放一份初始内容进 dist。
pd_dir = project_root / "pd"
if pd_dir.exists():
    datas.append((str(pd_dir), "pd"))

hiddenimports = []
hiddenimports += collect_submodules("backend")

a = Analysis(
    ["gui/control_panel.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
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
    [],
    exclude_binaries=True,
    name="bilipdj",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="bilipdj",
)
