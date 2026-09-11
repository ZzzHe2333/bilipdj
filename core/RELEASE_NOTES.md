# 弹幕排队姬 v3.0.4-test

这是基于 v3.0.3-test 的下一版测试发行，集中验证 Windows/Web 更新链路、版本回退、Web Portable 独立更新器，以及 Windows 控制台的更新/插件/权限/性能界面优化。该版本作为 GitHub Pre-release 发布，不替代现有稳定版。

## 本次测试内容

- Windows 更新页新增云端最新版本与本地备份版本选择，可直接从本地 `backup/` 恢复旧版；版本来源、目标版本、备份时间和可用操作显示更明确；
- 更新页进一步合并 Canvas/Frame 的几何刷新，固定状态与进度信息区，减少状态变化导致的控件抖动和位置跳变；
- Web 控制台新增持久化主题切换，后端指令区域上移，功能开关中文化，平台参数改为下拉分组，并支持双击关闭服务器；默认透明队列面板同步调整；
- Web Portable 植入独立更新器，支持完整 ZIP 更新、HTTP Range 逐文件增量更新、本地版本备份恢复和失败回滚；
- Web Portable 更新期间由独立 loopback HTML 页面持续显示实时进度，并提供俄罗斯方块小游戏；主 Web 服务可安全退出并完成文件替换；
- Windows 设置新增原生插件管理页，可读取、安装、启用/禁用、完整性校验、打开动态配置和卸载 `.bilipdj-plugin`；继续复用现有 Plugin API，不复制后端业务规则；
- Windows 权限页改为分组卡片布局，性能页改为固定指标卡片；更新状态、按钮状态和信息层级统一整理；
- Windows Tk / Web Portable 构建继续执行 frozen JavaScript 插件运行时自检，并生成全量 ZIP、逐文件 manifest、增量 `.pack` 与 SHA-256 校验文件。

## 直接下载

普通测试用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：下载 **`BiliPDJ-v3.0.4-test-Windows-Tk-Portable-x64.zip`** → [点击直接下载 Windows 测试版](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.4-test/BiliPDJ-v3.0.4-test-Windows-Tk-Portable-x64.zip)
- **Web 便携版**：下载 **`BiliPDJ-v3.0.4-test-Web-Portable-x64.zip`** → [点击直接下载 Web 测试版](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.4-test/BiliPDJ-v3.0.4-test-Web-Portable-x64.zip)

`*-files.json`、`*-Incremental-x64.pack` 及其 `.sha256` 为内置更新器使用的资源，普通用户无需手动下载或解压。

## 更新器与数据安全

- Windows Tk 与 Web Portable 均保留完整 ZIP 更新；增量更新按本地文件大小与 SHA-256 判断，只读取需要变化的文件片段；
- 更新前继续创建本地版本备份，失败时执行回滚；Windows 更新页可选择已存在的本地备份进行离线恢复；
- Web Portable 使用独立 `BiliPDJ-Web-Updater.exe` 接管更新过程，更新期间主服务退出不影响进度页；
- 配置、队列/运行存档、日志、备份、插件与插件私有数据等用户数据不会被增量更新覆盖；
- 普通 PR 和 `now` push 不创建 Release；本次仅通过用户明确授权的显式发布流程创建 `v3.0.4-test` Pre-release。

## 发行文件

- `BiliPDJ-v3.0.4-test-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.4-test-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.4-test-Windows-Tk-files.json`
- `BiliPDJ-v3.0.4-test-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.4-test-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.4-test-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.4-test-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.4-test-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.4-test-Web-files.json`
- `BiliPDJ-v3.0.4-test-Web-files.json.sha256`
- `BiliPDJ-v3.0.4-test-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.4-test-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 测试版说明

`v3.0.4-test` 主要用于验证本轮更新器与 Windows/Web UI 改动。它是 Pre-release，不作为稳定版替代；若用于生产直播环境，建议先保留现有程序目录或确认 `backup/` 中已有可恢复版本。
