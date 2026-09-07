# BiliPDJ Windows

Windows 桌面端的独立入口与打包目录。

当前 Tk 桌面 UI 的成熟实现仍保留在 `core/control_panel.py` 作为兼容实现层，本目录通过 `apps/windows/main.py` 启动它；Windows 发布链已经改为从本目录打包。这样可以先完成仓库边界和独立发布，再逐步把桌面 UI 模块内部迁出 `core/`，避免一次重写导致功能回退。

## 源码启动

```powershell
python -m apps.windows.main
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

`bilipdj_onedir.spec` 会从 `apps/web/static` 收集 Web 资源，并在发行包内放到后端兼容的 `core/ui` 路径。
