# BiliPDJ Desktop Frontend

`apps/windows/` 保存 Tk 桌面前端、Overlay、更新器、原生 Bilibili 扫码界面、图标资源和桌面端 PyInstaller 配置。

后端业务实现位于 `apps/server/`。Windows 前端通过 HTTP / WebSocket 使用 Server 的队列、权限、平台和配置状态，不应在此目录复制后端业务规则。

## Windows x64

源码启动：

```powershell
python -m apps.windows.main
```

唯一正式 Windows 本地打包入口：

```powershell
powershell -ExecutionPolicy Bypass -File .\apps\windows\package.ps1 -InstallDependencies
```

该脚本直接使用：

```text
apps/windows/bilipdj_onedir.spec
apps/windows/paiduijitm.spec
apps/windows/updater.spec
```

主要产物：

```text
dist\bilipdj\main.exe
dist\bilipdj\paiduijitm.exe
dist\bilipdj\updater.exe
```

## macOS

macOS 打包入口也收敛在本目录：

```bash
./apps/windows/package-macos.sh --install-deps --arch arm64
./apps/windows/package-macos.sh --install-deps --arch x86_64
```

对应配置：

```text
apps/windows/bilipdj_onedir_mac.spec
apps/windows/paiduijitm_mac.spec
```

GitHub Actions 的 macOS 工作流直接调用该脚本，不再经过根目录或 `scripts/` 中的架构包装脚本。

## 兼容层

`core.control_panel`、`core.overlay_host`、`core.update_*` 等旧路径仅用于旧导入兼容，不是当前桌面端实现位置。新代码统一引用 `apps.windows.*`。
