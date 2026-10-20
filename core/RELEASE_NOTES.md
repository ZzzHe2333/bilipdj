# BiliPDJ v3.0.23-test｜测试版

**预发行（Pre-release）· 功能验证专用，暂不建议替换稳定使用中的 v3.0.22。**

本次测试版基于 v3.0.22 与后续主分支改动，重点试用 Python 版抖音直播登录、直播间参数提取、礼物监听及兼容性修复。版本号仅递增补丁位至 3.0.23；`-test` 为发行标签后缀，不修改应用基础版本格式。

## 新增与改进

- **抖音官方网页扫码登录**：Windows Tk 与 Web 控制台增加扫码入口；使用后端本机独立 Chrome/Edge 配置目录，扫码操作由用户在抖音官方页面自行完成。
- **抖音参数获取**：登录后可填写直播间链接或 web_rid，一键读取并保存内部 room_id、直播状态等所需信息，请求弹幕监听重连；保留既有 HTTP 弹幕通道。
- **礼物监听**：支持基于浏览器 WebSocket 的礼物事件接入，以及手动启停的无头浏览器后台监听；继续使用统一排队与礼物记录模块。
- **连击处理**：抑制重复消息、按累计数量结算连击，优先在连击结束时结算，并提供超时兜底。抖音与 B 站用户身份按平台隔离。
- **配置与安全**：登录会话凭据保存在本机私有目录，不通过新增登录状态/参数接口回显；没有在源码或发行包内收录真实礼物样本、观众昵称、账号 Cookie、令牌或抓包签名。
- **下载兼容**：延续 Windows Tk 与 Web Portable 的全量 ZIP、逐文件清单、SHA-256 校验和更新下载线路功能。

## 目前仍需测试

1. **真实扫码登录**：在运行后端的 Windows 电脑上打开 Chrome/Edge，扫码后确认登录状态和会话持久性。
2. **直播参数读取**：确认 live_id/web_rid 与内部 room_id 正确匹配，切换直播间后监听可以恢复。
3. **真实礼物连击**：在直播间验证礼物消息、连击结束、重连去重、统计显示与多平台排队兼容。
4. **后台采集稳定性**：验证浏览器关闭、重新登录、空闲重连、不同平台并行和程序退出清理。由于抖音网页是私有接口，随平台变更可能需要再次适配。

**注意**：后台扫码/无头采集依赖后端所在机器具备 Chrome/Edge 运行环境；Docker 默认不保证具备可用图形浏览器。浏览器采集事件不能作为真实礼物消费凭据；礼物触发排队资格默认关闭。

## 下载（测试版）

- [Windows Tk x64 便携版](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.23-test/BiliPDJ-v3.0.23-test-Windows-Tk-Portable-x64.zip)
- [Web x64 便携版](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.23-test/BiliPDJ-v3.0.23-test-Web-Portable-x64.zip)
- [测试版 Release 页面](https://github.com/ZzzHe2333/bilipdj/releases/tag/v3.0.23-test)

Release 同时包含各安装包的 SHA-256、逐文件清单和增量更新资源。普通用户下载相应 ZIP 并**完整解压到新目录**即可；建议先备份原有数据和配置。上述文件链接仅在打包并成功发布后有效。

相关开发记录：[Issue #319](https://github.com/ZzzHe2333/bilipdj/issues/319)、[Issue #321](https://github.com/ZzzHe2333/bilipdj/issues/321)、[Issue #323](https://github.com/ZzzHe2333/bilipdj/issues/323)。

**发行类型：测试版 / Pre-release；标签：v3.0.23-test。**
