# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

spec_dir = Path(SPECPATH).resolve()
project_root = spec_dir.parents[1]

datas = [
    (str(project_root / "VERSION"), "."),
    (str(project_root / "core" / "appearance.json"), "."),
    (str(project_root / "apps" / "web" / "static"), "apps/web/static"),
]

hiddenimports = [
    "apps.server.server", "apps.server.bilibili_protocol", "apps.server.bilibili_gifts",
    "apps.server.bilibili_socket_guard", "apps.server.douyin_protocol", "apps.server.douyin_live_pb2",
    "apps.server.douyin_fallback_guard", "apps.server.youtube_protocol", "apps.server.youtube_runtime_guard",
    "apps.server.twitch_protocol", "apps.server.twitch_runtime_guard",
    "apps.server.log_manager", "apps.server.login_callback_guard",
    "apps.server.queue_logic_guard", "apps.server.queue_rank_query", "apps.server.server_runtime_guard",
    "apps.server.settings_backup", "apps.server.settings_backup_bugfix_guard", "apps.server.settings_mtime_guard", "apps.server.settings_storage_guard",
    "apps.server.style_option_guard", "apps.server.web_control_guard", "apps.server.web_queue_layout", "apps.server.websocket_performance_guard",
    "apps.server.update_estimate_api", "apps.server.web_update_api",
    "apps.server.issue79_guard", "apps.server.appearance_guard",
    "apps.server.danmu_plugins", "apps.server.plugin_manager", "apps.server.plugin_runtime_dual", "apps.server.javascript_plugin_runtime", "apps.server.plugin_data_quota",
    "apps.windows.frozen_plugin_probe",
    "core.youtube_protocol", "core.twitch_protocol",
    "qrcode", "qrcode.main", "qrcode.constants", "qrcode.util", "qrcode.image.base",
    "qrcode.image.pil", "qrcode.image.pure", "PIL", "PIL.Image", "PIL.ImageDraw", "PIL.PngImagePlugin",
    "pystray", "pystray._win32",
    "brotli", "psutil", "google.protobuf", "google.protobuf.internal.builder",
    "quickjs", "_quickjs",
]

a = Analysis(
    [str(project_root / "apps" / "web" / "portable_launcher.py")],
    pathex=[str(project_root)], binaries=[], datas=datas, hiddenimports=hiddenimports,
    hookspath=[], hooksconfig={}, runtime_hooks=[], excludes=[], noarchive=False, optimize=0,
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz, a.scripts, [], exclude_binaries=True, name="BiliPDJ-Web",
    icon=str(project_root / "apps" / "windows" / "assets" / "256x.ico"),
    debug=False, bootloader_ignore_signals=False, strip=False, upx=True, console=False,
)
coll = COLLECT(
    exe, a.binaries, a.datas, strip=False, upx=True, upx_exclude=[], name="bilipdj-web",
)
