# BiliPDJ Windows

`apps/windows/` 保存桌面端真实实现，包括 Tk 控制台、Overlay、更新器、原生 Bilibili 扫码弹窗、图标资源和 PyInstaller 配置。

旧的 `core.control_panel`、`core.overlay_host`、`core.update_*` 等路径仍保留兼容转发，但不再承载主要实现。

## v2.0.1 Tk Windows 便携版

正式客户包：

```text
BiliPDJ-v2.0.1-Windows-Tk-Portable-x64.zip
```

完整解压后直接运行 `main.exe`。冻结/便携模式会自动启用内置后端自动启动，因此用户无需安装 Python、无需另外运行 Server 命令。包内同时包含 Server、Web 静态资源、Overlay 和 Updater。

源码开发模式仍保留原来的 `auto_start_backend` 配置行为，不强制自动启动。

## 源码启动

```powershell
python -m apps.windows.main
```

旧命令仍兼容：

```powershell
python core/control_panel.py
```

## 独立打包

```powershell
powershell -ExecutionPolicy Bypass -File .\apps\windows\package.ps1 -InstallDependencies
```

主要产物：

```text
dist\bilipdj\main.exe
dist\bilipdj\paiduijitm.exe
dist\bilipdj\updater.exe
```

`bilipdj_onedir.spec` 从 `apps/web/static` 收集唯一 Web 资源，并在发行包中保持 `apps/web/static` 路径。主程序、Overlay 和 Updater 均直接从 `apps/windows` 源码打包。
