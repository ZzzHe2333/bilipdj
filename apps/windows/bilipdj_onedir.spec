# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

spec_dir = Path(SPECPATH).resolve()
project_root = spec_dir.parents[1]

datas = [
    (str(project_root / "VERSION"), "."),
    (str(project_root / "core" / "appearance.json"), "."),
    (str(project_root / "apps" / "web" / "static"), "apps/web/static"),
]
datas += collect_data_files("customtkinter")

hiddenimports = [
    "apps.server.server", "apps.server.bilibili_protocol", "apps.server.bilibili_gifts",
    "apps.server.douyin_protocol", "apps.server.douyin_live_pb2", "apps.server.log_manager",
    "apps.server.youtube_protocol", "apps.server.youtube_runtime_guard",
    "apps.server.twitch_protocol", "apps.server.twitch_runtime_guard",
    "apps.server.queue_logic_guard", "apps.server.queue_rank_query", "apps.server.server_runtime_guard",
    "apps.server.settings_backup", "apps.server.settings_backup_bugfix_guard", "apps.server.settings_mtime_guard", "apps.server.settings_storage_guard",
    "apps.server.style_option_guard", "apps.server.web_control_guard", "apps.server.web_queue_layout", "apps.server.websocket_performance_guard",
    "apps.server.issue79_guard", "apps.server.appearance_guard",
    "apps.server.danmu_plugins", "apps.server.plugin_manager", "apps.server.plugin_runtime_dual", "apps.server.javascript_plugin_runtime", "apps.server.plugin_data_quota",
    "apps.windows.control_panel", "apps.windows.control_panel_bootstrap", "apps.windows.control_panel_guard", "apps.windows.frozen_plugin_probe",
    "apps.windows.control_panel_features", "apps.windows.control_panel_ui_finish", "apps.windows.gui_log_sink", "apps.windows.customtk_ui",
    "apps.windows.about_page", "apps.windows.bilibili_qr_dialog", "apps.windows.portable_autostart", "apps.windows.style_save_transport",
    "apps.windows.support_us", "apps.windows.webdav_backup_ui", "apps.windows.issue79_features", "apps.windows.redtv_control_guard", "apps.windows.purple_mouse_control_guard", "apps.windows.unified_theme",
    "apps.windows.issue187_update_channel", "apps.windows.issue189_release_selector",
    "apps.windows.update_client", "apps.windows.update_ui", "apps.windows.update_network", "apps.windows.update_page",
    "apps.windows.incremental_update", "apps.windows.update_manifest",
    "apps.windows.overlay_refresh_guard", "apps.windows.overlay_performance_guard", "apps.windows.slider_switches",
    "core.server", "core.update_ui", "core.youtube_protocol", "core.twitch_protocol",
    "qrcode", "qrcode.main", "qrcode.constants", "qrcode.util", "qrcode.image.base",
    "qrcode.image.pil", "qrcode.image.pure", "PIL", "PIL.Image", "PIL.ImageTk", "PIL.PngImagePlugin",
    "brotli", "psutil", "google.protobuf", "google.protobuf.internal.builder",
    "quickjs", "_quickjs",
]
hiddenimports += collect_submodules("customtkinter")

a = Analysis(
    [str(project_root / "apps" / "windows" / "main.py")],
    pathex=[str(project_root)], binaries=[], datas=datas, hiddenimports=hiddenimports,
    hookspath=[], hooksconfig={}, runtime_hooks=[], excludes=[], noarchive=False, optimize=0,
)
pyz = PYZ(a.pure)
exe = EXE(pyz, a.scripts, [], exclude_binaries=True, name="main",
    icon=str(project_root / "apps" / "windows" / "assets" / "256x.ico"),
    debug=False, bootloader_ignore_signals=False, strip=False, upx=True, console=False)
coll = COLLECT(exe, a.binaries, a.datas, strip=False, upx=True, upx_exclude=[], name="bilipdj")
