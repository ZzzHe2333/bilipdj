# 弹幕排队姬 v3.0.6

`v3.0.6` 是在 v3.0.5 基础上的 Windows 桌面端界面稳定性修正版，重点处理固定窗口后的布局边界与日志页工具栏适配，同时继续沿用现有 Windows / Web Portable 全量、增量与备份恢复更新链路。

## 主要更新

- Windows 主控制台固定为 `1180×720`，关闭用户缩放，并在初始化完成后再次校正尺寸，减少窗口尺寸变化带来的布局抖动；
- 更新页、设置页等长内容继续使用内部滚动，不再依赖主窗口自动扩张；
- Windows 日志页右侧“清空 / 复制”按钮改为紧凑布局，缩小显式宽度、内部 padding 与控件间距，避免在固定窗口下被挤出可视区域；
- 本次按钮调整仅作用于日志页工具栏，不改变其他页面按钮尺寸与行为；
- 保留 v3.0.5 已有的正式版/测试版双通道版本选择、Windows / Web 全量更新、逐文件 SHA-256 增量更新、本地备份恢复与失败回滚；
- Windows Tk / Web Portable 打包继续执行 frozen JavaScript 插件运行时自检，并生成完整 ZIP、逐文件 manifest、增量 `.pack` 与 SHA-256 校验文件。

## 直接下载

普通用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.6-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.6-Web-Portable-x64.zip`

发布完成后可在：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.6/BiliPDJ-v3.0.6-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.6/BiliPDJ-v3.0.6-Web-Portable-x64.zip

`Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、Web 对应 manifest / incremental pack 及其 `.sha256` 为内置更新器使用的资源，普通用户无需手动下载或解压。

## 更新与数据安全

- 正式版更新器默认选择稳定 Release；测试版仍可在版本下拉中手动选择；
- 全量更新继续下载完整 ZIP；增量更新按本地文件大小与 SHA-256 判断，仅下载实际变化的文件片段；
- 更新前创建本地版本备份，失败时自动回滚；Windows / Web 均可恢复已有本地备份；
- 配置、队列/运行存档、日志、备份、插件与插件私有数据等用户数据不会被正常增量更新覆盖。

## 发行文件

- `BiliPDJ-v3.0.6-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.6-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.6-Windows-Tk-files.json`
- `BiliPDJ-v3.0.6-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.6-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.6-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.6-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.6-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.6-Web-files.json`
- `BiliPDJ-v3.0.6-Web-files.json.sha256`
- `BiliPDJ-v3.0.6-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.6-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 发布说明

本次为正式稳定版，GitHub Release 应设置为 `prerelease=false`。正式发布仅通过仓库既有的显式发布工作流执行。
