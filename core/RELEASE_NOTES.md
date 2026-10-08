# 弹幕排队姬 v3.0.21 正式版

本版基于 **v3.0.20 正式版**，合并 **PR #295**：参考 [linux-do/cdk](https://github.com/linux-do/cdk) 的设计语言，对 BiliPDJ **Web 管理界面**进行外观改版。业务功能和对外接口保持不变。

## 本次更新

- **Web 控制台视觉重做（#294，PR #295）**：更新顶部品牌区、侧边导航与分组、页面标题、卡片、按钮、输入框、标签页、表格和运行状态等控件，采用中性配色、圆角及轻量阴影。
- **管理子页面统一**：账号连接与备份配置、扫码登录、插件动态配置页面适配同一风格，包含响应式布局、系统深浅色偏好与减少动态效果设置。
- **功能与展示兼容**：保留原有控制台导航、表单 ID、JavaScript 业务行为、Server API 和主题自定义能力；`/index` OBS 透明队列页面及其展示样式不变。
- **不引入额外前端运行依赖**：继续使用 Web 原生 HTML / CSS / JavaScript，不引入 Next.js/React。
- **此前功能继续保留**：Windows Tk 软件更新/第三方可选下载加速与 SHA-256 校验、独立更新器与启动进度窗口、多平台弹幕 Relay、礼物插队、队列管理及 WebDAV/本地/SMB 设置备份。

## 发行包及更新资源

本次发布以下 x64 便携包，以及各自逐文件清单、差量资源和 SHA-256 校验文件：

- **Windows Tk 便携版：** [BiliPDJ-v3.0.21-Windows-Tk-Portable-x64.zip](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.21/BiliPDJ-v3.0.21-Windows-Tk-Portable-x64.zip)
- **Web 便携版：** [BiliPDJ-v3.0.21-Web-Portable-x64.zip](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.21/BiliPDJ-v3.0.21-Web-Portable-x64.zip)

升级器使用的其他文件包括 `Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、`Web-files.json`、`Web-Incremental-x64.pack`、`update-manifest.json` 及对应 `*.sha256`。普通用户通常只需选择适用的便携包。

## 验证与兼容

- PR #295 的 Web 页面静态功能契约检查 37/37 通过；Web Build、Server、Quality/API 等 CI 通过。发布包的最终构建和校验以本 Release 的 GitHub Actions 结果为准。
- 本次为纯 Web 管理界面视觉更新，Windows Tk 界面和 OBS 透明展示保留原有样式。
- 真实直播间互动、不同浏览器和操作系统的视觉细节仍以实际环境为准。

## 发布状态

**正式版（Release，非 Pre-release）**。标签：`v3.0.21`，补丁号从 3.0.20 更新到 3.0.21，不带测试版后缀。
