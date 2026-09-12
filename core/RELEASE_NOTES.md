# 弹幕排队姬 v3.0.12-test

`v3.0.12-test` 是新的测试发行版，重点验证 Windows 桌面端从 CustomTkinter 回退到原生 Tk/ttk 后的启动稳定性与功能完整性。该版本作为 GitHub Pre-release 发布，不替代当前稳定版 v3.0.6。

## 本次测试内容

- Windows 桌面端生产入口恢复为原生 `tk.Tk()`，不再使用 `BiliPDJCTk` 作为主窗口；
- `desktop_runtime` 不再安装 CustomTkinter shell / page components，恢复以现有 Tk/ttk 页面 builder 为主的稳定路径；
- Windows Portable 已移除 CustomTkinter 的 datas、submodules、hidden import，并从 `requirements.txt` 移除 CustomTkinter 依赖；
- 恢复 Tk/ttk 版设置、权限、性能、插件管理、日志工具栏与“后端指令”区域，同时保留近期已经完成的后端、插件与更新器能力；
- frozen GUI startup self-test 现在明确检查生产 root 为 `tk.Tk`、主要页面已经创建、“后端指令”组件存在，并保持窗口持续可见；
- Windows/Web Portable 继续使用程序目录 `update/<会话>/` 作为下载、解压、增量 patch 与 rollback 工作区；
- 更新前继续创建完整 `backup/` 快照，更新失败继续执行恢复并尽可能重新启动旧版本；
- 插件运行时、平台无关 DanmuEvent、Plugin Config Schema、Plugin HTTP Host API、Web Portable 等近期功能保持不变。

## 建议重点验证

- 双击 Windows `main.exe` 后窗口是否稳定显示，不再出现“一闪而过”；
- 后端是否能够自动启动，关闭/重新打开程序是否正常；
- 日志页是否正常显示“后端指令”区域，并可发送命令；
- 依次打开“日志 / 设置 / 权限 / 性能 / 更新软件”等页面，确认布局和交互正常；
- 设置页插件管理区域是否正常显示、刷新和操作；
- 从 `v3.0.11-test` 更新到 `v3.0.12-test`，确认测试通道增量更新基线识别正确；
- 更新下载、解压、patch 和 rollback 是否仍全部位于程序目录 `update/`；
- 人为中断或制造更新失败后，Windows/Web 是否可以正常回滚并重新启动。

## 直接下载

普通测试用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.12-test-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.12-test-Web-Portable-x64.zip`

发布地址：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.12-test/BiliPDJ-v3.0.12-test-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.12-test/BiliPDJ-v3.0.12-test-Web-Portable-x64.zip

`Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、Web 对应 manifest / incremental pack 及其 `.sha256` 均由内置更新器使用，普通用户无需手动下载。

## 更新与数据安全

- `v3.0.12-test` 为测试通道 Release，不替代 v3.0.6 稳定版；
- Windows/Web 更新下载、解压、增量 patch 与 rollback 工作区继续统一位于程序目录 `update/`；
- 更新前继续创建 `backup/` 版本快照；
- Windows 增量更新只有在完整快照建立后才进入文件修改阶段；
- 更新失败会优先恢复原版本，Web Portable 停止后准备失败也会尝试重新启动旧程序；
- 配置、队列存档、日志、备份、插件、插件私有数据、主题与显示样式等用户数据继续受保护；
- 本测试版增量资源应以 `v3.0.11-test` 作为精确基线。

## 发行文件

- `BiliPDJ-v3.0.12-test-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.12-test-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.12-test-Windows-Tk-files.json`
- `BiliPDJ-v3.0.12-test-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.12-test-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.12-test-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.12-test-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.12-test-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.12-test-Web-files.json`
- `BiliPDJ-v3.0.12-test-Web-files.json.sha256`
- `BiliPDJ-v3.0.12-test-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.12-test-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 测试版说明

本版本按测试通道作为 GitHub **Pre-release** 发布。重点用于验证 Windows GUI 回退到 Tk/ttk 后的启动稳定性、主要页面完整性，以及 `v3.0.11-test → v3.0.12-test` 的增量更新链路。
