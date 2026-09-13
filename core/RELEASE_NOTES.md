# 弹幕排队姬 v3.0.13-test

`v3.0.13-test` 是基于 `v3.0.12` 正式版继续验证的测试版本。本次主要用于验证当前 `now` 分支近期功能、更新链路与发布流程，不替代 `v3.0.12` 正式版。

## 本次测试内容

- 保持 Windows 原生 Tk/ttk 桌面端与现有 Web Portable 架构不变；
- 继续验证备份范围控制：配置 / 存档 / 样式可独立选择，默认全部开启；
- 继续验证跨平台礼物兼容层，可按平台、礼物 ID / 名称和价值处理礼物排队；
- 继续验证 `language` 语言类插件，内置简体中文与 English（en-US），英语随包提供但默认不启用，语言保持严格单选；
- 继续验证插件运行时、平台无关 `DanmuEvent`、Plugin Config Schema、Plugin HTTP Host API；
- Windows / Web 更新器继续使用程序目录 `update/<会话>/` 进行下载、解压、增量 patch 与 rollback；
- 更新前继续创建 `backup/` 快照，失败时继续执行恢复与旧版本重启尝试。

## 建议重点测试

- Windows `main.exe` 是否稳定启动，后端能否正常自动启动和退出；
- 设置、权限、性能、插件管理、日志与“后端指令”等页面是否正常；
- 礼物兼容性窗口新增/修改不同平台礼物价值后，排队价值计算是否正确；
- 默认启动是否仍为简体中文，切换 English 或第三方语言后是否始终只有一个活动语言；
- 备份配置 / 存档 / 样式选择是否正确，三项全关时是否阻止备份；
- 从已有测试版或正式版更新到 `v3.0.13-test` 时，全量与增量更新是否正常；
- 更新失败后的 rollback 与数据保护是否正常。

## 直接下载

测试用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.13-test-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.13-test-Web-Portable-x64.zip`

发布地址：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.13-test/BiliPDJ-v3.0.13-test-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.13-test/BiliPDJ-v3.0.13-test-Web-Portable-x64.zip

`Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、Web 对应 manifest / incremental pack 及其 `.sha256` 均由内置更新器使用，普通用户无需手动下载。

## 更新与数据安全

- `v3.0.13-test` 为测试版 GitHub Pre-release，不替代 `v3.0.12` 正式版；
- Windows/Web 更新工作区继续统一位于程序目录 `update/`；
- 更新前继续创建完整 `backup/` 快照；
- 增量更新只有在必要检查与快照完成后才进入文件修改阶段；
- 更新失败优先恢复旧版本；
- 配置、队列存档、日志、备份、插件、插件私有数据、主题与显示样式继续受保护。

## 发行文件

- `BiliPDJ-v3.0.13-test-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.13-test-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.13-test-Windows-Tk-files.json`
- `BiliPDJ-v3.0.13-test-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.13-test-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.13-test-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.13-test-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.13-test-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.13-test-Web-files.json`
- `BiliPDJ-v3.0.13-test-Web-files.json.sha256`
- `BiliPDJ-v3.0.13-test-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.13-test-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 测试版说明

本版本使用 `-test` 后缀，发布时应标记为 GitHub **Pre-release**。正式稳定版仍为 `v3.0.12`。
