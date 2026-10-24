# BiliPDJ v3.0.24｜正式版

**正式发行（Release）**。基于 v3.0.22 的稳定修复和抖音直播支持增强，包含 Windows Tk x64 和 Web Portable x64 便携版。

## 主要更新

- **Windows 后端启动修复**：独立处理冻结程序 `main.exe --backend` 启动入口；修复因桌面初始化或存档迁移确认阻塞导致的后端启动超时；改善错误提示与监听地址/端口同步。
- **抖音官方网页扫码登录**：Windows Tk 和 Web 设置提供扫码入口、登录状态查询，以及通过已登录会话获取直播间 web_rid / room_id 和直播状态。扫码登录使用本机独立 Chrome/Edge Profile。
- **抖音礼物监听增强**：从 Chrome DevTools WebSocket 帧解码 `WebcastGiftMessage`，不必修改网页内 JavaScript；支持后台采集启停、重连、会话 Cookie 本地恢复和诊断统计。
- **礼物连击与排队规则**：支持礼物连击去重、累计数量结算。新增按抖音礼物单独设定**排队次数 / 插队次数**，默认可选「粉丝团灯牌」；规则需管理员显式启用，不会默认为任何礼物授权排队。
- **跨平台兼容**：延续 Bilibili 与抖音的统一排队存档、平台区分、窗口与 Web 控制功能。
- **安全修复**：对礼物规则 API 请求限制类型和大小；后台 WSS 帧/Gzip 解码限制内存；避免误结束仍在使用独立 Profile 的 Chrome；接口不回显登录 Cookie、Token、签名或账号标识；清空礼物规则后不会自动恢复默认规则。
- **更新器**：支持 Windows Tk、Web Portable 便携包的全量 ZIP、逐文件清单、SHA-256 校验及增量更新资源。

## 已完成验证

在 GitHub Actions 上通过 Quality/API、Server/Docker、Web 构建、弹幕事件兼容、更新工作区与失败恢复，以及 Windows Tk/Web 便携打包检查。Windows 打包检查包含**实际启动冻结后的 `main.exe --backend` 并读取后端 HTTP 与抖音登录状态接口**。

## 使用提醒及待实测项

- 新版包含抖音私有 Webcast IM 协议适配。**真实抖音 App 扫码、实际直播间的礼物消息和长期稳定性尚需用户现场验证**，不要将 CI 检查等同于完整直播验收。
- 后台扫码/无头礼物采集需在运行服务端的主机上安装可用 Chrome/Edge；Docker 默认镜像不保证支持图形扫码或无头 Chrome。
- 抖音网页礼物事件可以被伪造，**不能作为付款凭证**；请仅在信任的本地环境下配置礼物排队规则。
- 建议备份现有存档与配置后，将新 ZIP **完整解压至新目录**，不要把两种版本相互覆盖。
- 已发布的 `v3.0.23-test` 仍作为历史测试版保留；本次 `v3.0.24` 为正式发行。

## 下载（v3.0.24 正式版）

- [Windows Tk x64 便携版](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.24/BiliPDJ-v3.0.24-Windows-Tk-Portable-x64.zip)
- [Web x64 便携版](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.24/BiliPDJ-v3.0.24-Web-Portable-x64.zip)
- [GitHub 正式版 Release 页面](https://github.com/ZzzHe2333/bilipdj/releases/tag/v3.0.24)

Release 同时提供 `Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、`Web-files.json`、`Web-Incremental-x64.pack`、`update-manifest.json` 与对应 SHA-256 校验文件，供更新器使用。**普通用户无需手动下载**这些清单和增量文件，只需选择相应 ZIP。以上链接仅在正式发行成功后生效。

相关开发与验收：[Issue #325](https://github.com/ZzzHe2333/bilipdj/issues/325)、[Issue #327](https://github.com/ZzzHe2333/bilipdj/issues/327)、[Issue #329](https://github.com/ZzzHe2333/bilipdj/issues/329)，以及 [PR #328](https://github.com/ZzzHe2333/bilipdj/pull/328)。

**发行类型：正式版 / Release；标签：v3.0.24。**
