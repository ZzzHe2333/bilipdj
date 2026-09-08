# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path
project_root = Path(SPECPATH).resolve()
datas=[(str(project_root/'VERSION'),'.'),(str(project_root/'apps'/'web'/'static'),'apps/web/static')]
hiddenimports=["apps.server.server","apps.server.bilibili_protocol","apps.server.bilibili_gifts","apps.server.douyin_protocol","apps.server.douyin_live_pb2","apps.server.log_manager","apps.server.queue_logic_guard","apps.server.queue_rank_query","apps.server.server_runtime_guard","apps.server.style_option_guard","apps.server.web_queue_layout","apps.server.websocket_performance_guard","apps.windows.control_panel","apps.windows.update_client","apps.windows.update_ui","apps.windows.update_network","apps.windows.update_page","apps.windows.gui_log_sink","apps.windows.control_panel_bootstrap","apps.windows.control_panel_guard","apps.windows.control_panel_features","apps.windows.control_panel_ui_finish","apps.windows.about_page","apps.windows.bilibili_qr_dialog","apps.windows.portable_autostart","apps.windows.style_save_transport","apps.windows.support_us","apps.windows.overlay_refresh_guard","apps.windows.overlay_performance_guard","qrcode","qrcode.image.pil","PIL","PIL.ImageTk","brotli","psutil","google.protobuf"]
a=Analysis([str(project_root/'apps'/'windows'/'main.py')],pathex=[str(project_root)],binaries=[],datas=datas,hiddenimports=hiddenimports,hookspath=[],hooksconfig={},runtime_hooks=[],excludes=[],noarchive=False,optimize=0)
pyz=PYZ(a.pure)
exe=EXE(pyz,a.scripts,[],exclude_binaries=True,name='main',icon=str(project_root/'apps'/'windows'/'assets'/'256x.ico'),debug=False,bootloader_ignore_signals=False,strip=False,upx=True,console=False)
coll=COLLECT(exe,a.binaries,a.datas,strip=False,upx=True,upx_exclude=[],name='bilipdj')
