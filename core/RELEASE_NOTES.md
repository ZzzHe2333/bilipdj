# 弹幕排队姬 v3.0.14-test

`v3.0.14-test` 是基于 `v3.0.12` 正式版继续验证的测试版本，包含 `v3.0.13-test` 之后的 Bug 修复、版本渠道逻辑统一以及 CI/测试脚本解耦调整。本版本用于验证当前 `now` 分支，不替代 `v3.0.12` 正式版。

## 本次主要变化

- 修复保存普通配置后，运行中的管理员、舰长、黑名单权限被错误重置为默认值的问题；
- 修复 Web 增量更新把同一数字版本下的 `test/gc/cx/stable` 不同渠道误认为同一基础版本的问题；
- 统一项目发行渠道版本比较逻辑，Windows 更新器、插件兼容判断和 Web Release 选择器不再各自按后缀字符串排序；
- 修复多平台 Relay 启动中途失败后残留 `_started=True`、无法正常重试的问题；
- 修复退出自动备份首次失败后提前标记完成，导致后续退出回调无法再次尝试的问题；
- 修复本地管理 API 同源校验旁路、Web 翻译误伤用户数据、礼物兼容规则冲突、语言插件卸载残留状态等问题；
- 根目录 `scripts/` 已移除，测试、guard、probe 与构建辅助代码迁移至 `.github/ci/`，生产运行路径与测试代码彻底解耦；
- 保持 Windows Tk/ttk、Web Portable、插件系统、语言系统、礼物兼容层、备份与更新恢复架构不变。

## 建议重点测试

- Windows `main.exe`、`paiduijitm.exe`、`updater.exe` 是否正常启动；
- Web Portable 的 `BiliPDJ-Web.exe` 与 `BiliPDJ-Web-Updater.exe` 是否正常；
- 修改普通设置后管理员、舰长、黑名单运行态是否保持正确；
- `3.0.13-test → 3.0.14-test` 的全量/增量更新、备份和 rollback 是否正常；
- 不同预发布渠道版本是否能正确区分，不再跨渠道误用增量基础包；
- 多平台 Relay 在单个平台启动失败后能否正确回滚并再次启动；
- WebDAV/本地退出备份失败时，后续退出路径是否仍有重试机会；
- 默认语言仍为简体中文，English（en-US）随包提供但默认不启用，第三方语言保持严格单选；
- 删除根目录 `scripts/` 后，Windows/Web/Server 主运行路径和 Portable 打包不受影响。

## 直接下载

测试用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.14-test-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.14-test-Web-Portable-x64.zip`

发布地址：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.14-test/BiliPDJ-v3.0.14-test-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.14-test/BiliPDJ-v3.0.14-test-Web-Portable-x64.zip

`Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、Web 对应 manifest / incremental pack 及其 `.sha256` 均由内置更新器使用，普通用户无需手动下载。

## 更新与数据安全

- `v3.0.14-test` 为测试版 GitHub Pre-release，不替代 `v3.0.12` 正式版；
- Windows/Web 更新工作区继续统一位于程序目录 `update/`；
- 更新前继续创建完整 `backup/` 快照；
- 增量更新会校验完整发行版本身份，避免不同渠道之间误用基础版本；
- 更新失败优先恢复旧版本；
- 配置、队列存档、日志、备份、插件、插件私有数据、主题与显示样式继续受保护。

## 发行文件

- `BiliPDJ-v3.0.14-test-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.14-test-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.14-test-Windows-Tk-files.json`
- `BiliPDJ-v3.0.14-test-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.14-test-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.14-test-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.14-test-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.14-test-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.14-test-Web-files.json`
- `BiliPDJ-v3.0.14-test-Web-files.json.sha256`
- `BiliPDJ-v3.0.14-test-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.14-test-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 测试版说明

本版本使用 `-test` 后缀，发布时标记为 GitHub **Pre-release**。正式稳定版仍为 `v3.0.12`，README 稳定版下载入口继续保持 `v3.0.12`。
