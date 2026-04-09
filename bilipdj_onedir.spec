# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

project_root = Path(SPECPATH).resolve()

datas = [
    # 前端 UI 页面
    (str(project_root / "core" / "ui"), "core/ui"),
    # 初始模型数据（JSON）
    (str(project_root / "core" / "danmuji_initial_model.json"), "core"),
]

# 排队存档目录（存档初始内容）
cd_dir = project_root / "core" / "cd"
if cd_dir.exists():
    datas.append((str(cd_dir), "core/cd"))

hiddenimports = [
    "core.server",
    "qrcode",
    "qrcode.main",
    "qrcode.constants",
    "qrcode.util",
    "qrcode.image.base",
    "qrcode.image.pil",
    "qrcode.image.pure",
    "PIL",
    "PIL.Image",
    "PIL.PngImagePlugin",
    "brotli",
    "psutil",
]

a = Analysis(
    ["core/control_panel.py"],
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
    name="弹幕排队姬",
    icon=str(project_root / "core" / "256x.ico"),
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
    name="弹幕排队姬",
)
