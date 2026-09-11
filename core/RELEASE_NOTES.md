# 弹幕排队姬 v3.0.5

`v3.0.5` 是本轮 Windows / Web 更新链路与控制台改造后的正式稳定版，整合并正式发布此前 `v3.0.4-test` 验证过的功能，同时补充正式版/测试版双通道版本选择。

## 主要更新

- Windows 更新页支持同时读取最新正式版、最新测试/预发行版和本地备份；默认选中最新正式版，可手动切换测试版或本地备份；
- Web Portable 更新页同样支持正式版/测试版双通道选择，默认正式版；选择测试版后会严格使用该 Release 自身的 `update-manifest.json`、全量包与增量资源；
- Windows 与 Web Portable 均支持完整 ZIP 更新、逐文件 SHA-256 增量更新、本地版本备份恢复和失败回滚；
- Web Portable 更新期间使用独立 `BiliPDJ-Web-Updater.exe` 提供实时更新进度页面和俄罗斯方块小游戏，主 Web 服务可安全退出并替换文件；
- Windows 更新页优化 Canvas/Frame 几何刷新与固定状态区，减少控件抖动、跳位；
- Web 控制台支持持久化明暗主题、后端指令区上移、功能开关中文化、平台参数下拉选择、双击关闭服务器，并调整默认透明队列面板；
- Windows 设置新增原生插件管理页，支持 `.bilipdj-plugin` 的读取、安装、启用/禁用、完整性校验、动态配置和卸载；
- Windows 权限页与性能页重新整理信息层级和布局；
- Windows Tk / Web Portable 打包继续执行 frozen JavaScript 插件运行时自检，并生成全量 ZIP、逐文件 manifest、增量 `.pack` 与 SHA-256 校验文件。

## 直接下载

普通用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.5-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.5-Web-Portable-x64.zip`

发布完成后可在：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.5/BiliPDJ-v3.0.5-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.5/BiliPDJ-v3.0.5-Web-Portable-x64.zip

`*-files.json`、`*-Incremental-x64.pack` 及其 `.sha256` 为内置更新器使用的资源，普通用户无需手动下载或解压。

## 更新与数据安全

- 正式版更新器默认选择稳定 Release；测试版仍可在版本下拉中手动选择；
- 全量更新继续下载完整 ZIP；增量更新按本地文件大小与 SHA-256 判断，仅下载实际变化的文件片段；
- 更新前创建本地版本备份，失败时自动回滚；Windows / Web 均可恢复已有本地备份；
- 配置、队列/运行存档、日志、备份、插件与插件私有数据等用户数据不会被正常增量更新覆盖。

## 发行文件

- `BiliPDJ-v3.0.5-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.5-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.5-Windows-Tk-files.json`
- `BiliPDJ-v3.0.5-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.5-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.5-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.5-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.5-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.5-Web-files.json`
- `BiliPDJ-v3.0.5-Web-files.json.sha256`
- `BiliPDJ-v3.0.5-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.5-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 发布说明

本次为正式稳定版，GitHub Release 应设置为 `prerelease=false`。正式发布仅通过仓库既有的显式发布工作流执行。
