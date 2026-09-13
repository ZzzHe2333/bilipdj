# 弹幕排队姬 v3.0.12

`v3.0.12` 为正式发行版。该版本在 `v3.0.12-gc` 公测基础上转为稳定版本，保留近期完成的 Windows Tk/ttk 稳定化、更新器、插件体系、备份范围控制、跨平台礼物兼容与语言插件能力。

## 本次正式版内容

- Windows 桌面端生产入口保持原生 `tk.Tk()`，以 Tk/ttk 页面 builder 为稳定路径；
- Windows Portable 不再依赖 CustomTkinter，设置、权限、性能、插件管理、日志工具栏与“后端指令”等功能保持完整；
- Windows/Web Portable 继续使用程序目录 `update/<会话>/` 作为下载、解压、增量 patch 与 rollback 工作区；
- 更新前继续创建完整 `backup/` 快照，更新失败继续执行恢复并尽可能重新启动旧版本；
- 备份支持分别控制“配置 / 存档 / 样式”，三项默认开启，全部关闭时禁止备份；
- 礼物排队新增跨平台礼物兼容层，可按平台、礼物 ID / 名称与价值配置，为后续抖音等平台礼物接入提供统一入口；
- 插件系统新增 `language` 语言类资源插件，内置简体中文与 English（en-US）；英语随包提供但默认不启用，语言严格单选；
- 插件运行时、平台无关 `DanmuEvent`、Plugin Config Schema、Plugin HTTP Host API、Web Portable 等近期功能保持不变。

## 发布前测试重点

- 双击 Windows `main.exe` 后窗口是否稳定显示；
- 后端是否能够自动启动，关闭/重新打开程序是否正常；
- 日志页“后端指令”区域是否正常，并可发送命令；
- “日志 / 设置 / 权限 / 性能 / 更新软件”等页面布局和交互是否正常；
- 设置页插件管理、礼物兼容性和界面语言是否正常显示、刷新和操作；
- 默认启动是否仍为简体中文，切换 English 后是否只有一个语言处于活动状态；
- 备份配置、备份存档、备份样式三项开关是否按预期生效；
- 从 `v3.0.11-test` 更新到 `v3.0.12` 时，增量更新基线识别是否正确；
- 更新下载、解压、patch 和 rollback 是否仍全部位于程序目录 `update/`；
- 人为中断或制造更新失败后，Windows/Web 是否可以正常回滚并重新启动。

## 直接下载

普通用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.12-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.12-Web-Portable-x64.zip`

发布地址：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.12/BiliPDJ-v3.0.12-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.12/BiliPDJ-v3.0.12-Web-Portable-x64.zip

`Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、Web 对应 manifest / incremental pack 及其 `.sha256` 均由内置更新器使用，普通用户无需手动下载。

## 更新与数据安全

- `v3.0.12` 为正式稳定版本；
- Windows/Web 更新下载、解压、增量 patch 与 rollback 工作区继续统一位于程序目录 `update/`；
- 更新前继续创建 `backup/` 版本快照；
- Windows 增量更新只有在完整快照建立后才进入文件修改阶段；
- 更新失败会优先恢复原版本，Web Portable 停止后准备失败也会尝试重新启动旧程序；
- 配置、队列存档、日志、备份、插件、插件私有数据、主题与显示样式等用户数据继续受保护；
- 本正式版增量资源继续以 `v3.0.11-test` 作为精确基线。

## 发行文件

- `BiliPDJ-v3.0.12-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.12-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.12-Windows-Tk-files.json`
- `BiliPDJ-v3.0.12-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.12-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.12-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.12-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.12-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.12-Web-files.json`
- `BiliPDJ-v3.0.12-Web-files.json.sha256`
- `BiliPDJ-v3.0.12-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.12-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 正式版说明

`v3.0.12` 不带预发布后缀，按正式稳定版本处理。仓库内会先完成完整 CI 与便携版测试构建；只有在明确执行发布操作时才创建 GitHub Release。
