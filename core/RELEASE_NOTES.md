# 弹幕排队姬 v3.0.11-test

`v3.0.11-test` 是新的测试发行版，重点验证 Windows CustomTkinter 桌面端组件化后的完整功能、后端指令区恢复，以及测试通道之间的增量更新。该版本作为 GitHub Pre-release 发布，不替代当前稳定版 v3.0.6。

## 本次测试内容

- Windows 桌面端生产运行路径进一步整理：不再依赖历史 `issueXXX` 模块作为正式入口，GUI 功能由正式命名模块和统一 `desktop_runtime` 安装；
- 移除对 Python `__build_class__` 的隐式 GUI 类构造 Hook，改为在创建 `ControlPanelApp` 前显式安装桌面运行时；
- 日志、设置、更新、权限、性能页面进入明确的 CustomTkinter 组件层，复杂 Treeview/ttk 控件继续限制在兼容区域内部；
- 修复组件化后 Windows 日志页的“后端指令”区域静默消失问题，现在由 `LogPageComponent` 显式安装；
- 冻结版 GUI 启动自检新增“后端指令”组件存在性检查，避免以后重构再次漏装；
- 修复测试版增量更新基线选择：打包流程不再使用会忽略 Pre-release 的 GitHub `/releases/latest`，而是选择上一条兼容发行版；
- 因此 `v3.0.10-test → v3.0.11-test` 可正确使用测试通道增量更新，不再错误声明稳定版为基线；
- Windows/Web Portable 继续保持程序目录 `update/<会话>/` 工作区、完整快照事务边界、失败回滚和旧版本自动恢复机制；
- Quality、生产 GUI dependency guard、update workspace guard、updater failure recovery、冻结版 GUI/插件自检以及 Windows/Web Portable 打包均作为发布前验收项。

## 建议重点验证

- 双击 `main.exe` 后 GUI 是否稳定显示，后端是否自动启动；
- 日志页顶部是否正常显示“后端指令”输入框，并能发送后端命令；
- 依次打开“日志 / 设置 / 权限 / 性能 / 更新软件”等页面，确认布局与交互正常；
- 设置页插件管理区域是否正常显示、刷新与操作；
- 从 `v3.0.10-test` 执行增量更新到 `v3.0.11-test`，确认更新器识别的增量基线为 `3.0.10-test`；
- 更新下载、解压、patch 和 rollback 是否都位于程序目录 `update/`，而不是系统 C 盘临时目录；
- 人为中断或制造更新失败后，Windows/Web 是否可以正常回滚并重新启动。

## 直接下载

普通测试用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.11-test-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.11-test-Web-Portable-x64.zip`

发布地址：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.11-test/BiliPDJ-v3.0.11-test-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.11-test/BiliPDJ-v3.0.11-test-Web-Portable-x64.zip

`Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、Web 对应 manifest / incremental pack 及其 `.sha256` 均由内置更新器使用，普通用户无需手动下载。

## 更新与数据安全

- `v3.0.11-test` 为测试通道 Release，不替代 v3.0.6 稳定版；
- Windows/Web 更新下载、解压、增量 patch 与 rollback 工作区继续统一位于程序目录 `update/`；
- 更新前继续创建 `backup/` 版本快照；
- Windows 增量更新只有在完整快照建立后才进入文件修改阶段；
- 更新失败会优先恢复原版本，Web Portable 停止后准备失败也会尝试重新启动旧程序；
- 配置、队列存档、日志、备份、插件、插件私有数据、主题与显示样式等用户数据继续受保护；
- 本测试版增量资源应以 `v3.0.10-test` 作为精确基线。

## 发行文件

- `BiliPDJ-v3.0.11-test-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.11-test-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.11-test-Windows-Tk-files.json`
- `BiliPDJ-v3.0.11-test-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.11-test-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.11-test-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.11-test-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.11-test-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.11-test-Web-files.json`
- `BiliPDJ-v3.0.11-test-Web-files.json.sha256`
- `BiliPDJ-v3.0.11-test-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.11-test-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 测试版说明

本版本按测试通道作为 GitHub **Pre-release** 发布。重点用于验证 Windows GUI 组件化后的功能完整性、日志页后端指令恢复、以及 `v3.0.10-test → v3.0.11-test` 的增量更新链路。
