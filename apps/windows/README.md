# BiliPDJ Windows

`apps/windows/` 现在保存桌面端的真实实现，包括 Tk 控制台、Overlay、更新器、图标资源和 PyInstaller 配置。

旧的 `core.control_panel`、`core.overlay_host`、`core.update_*` 等路径仍保留兼容转发，但不再承载主要实现。

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
