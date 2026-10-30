# BiliPDJ v3.0.25-test｜测试版

**Pre-release（测试版）**。本次基于 `v3.0.24` 正式版与后续 [PR #332](https://github.com/ZzzHe2333/bilipdj/pull/332) 的抖音礼物处理修复，面向 Windows Tk 与 Web Portable 用户试用。测试版不替代稳定的 `v3.0.24`。

## 相较 v3.0.24 的修复

- **零钻礼物可被识别**：修复 `WebcastGiftMessage` 中 `diamondCount=0` 被视为无效的情况，合法礼物可进入按礼物 ID / 名称设置的排队、插队规则。
- **价格未知不再导致漏收**：缺失 `diamondCount` 的礼物按未知价格 `None` 处理，不伪造成零元价值；保留礼物 ID、数量及用户信息供事件匹配。
- **连击完成去重**：已完成且具有非空 `groupId` 的连击，不再被迟到的不同 `msgId` / 更大累计数量消息重新打开，避免重复结算。
- **分离连续连击**：没有 `groupId` 时，若出现更晚消息且累计数量回落，将前一轮结算并开始新一轮，不再误合并相邻两次连击。
- **回归测试**：增加合成 Protobuf 礼物、零钻/未知价格、520 累计连击、结束后迟到帧和累计数重置测试。测试数据不包含真实观众昵称、房间、Cookie、Token 或签名。

## 继承自 v3.0.24 的功能

- Windows 后端独立启动、启动就绪与本机 HTTP 接口检查。
- Windows Tk 与 Web 设置中的抖音官方扫码登录和直播间参数获取。
- 抖音 Chrome/Edge WebSocket 礼物监听、统一排队、礼物规则、跨平台队列。
- 礼物帧/Gzip 内存限制、Chrome 独立配置目录保护、登录凭据本地保存与敏感信息脱敏。

## 验证及试用注意事项

此次变更已通过 PR #332 的 Quality/API、Server/Docker、更新工作区及 Windows Tk/Web 打包检查（包含冻结程序自检、实际冻结后端 HTTP / 抖音登录状态接口冒烟）。测试版发布流水线仍须对本次发行文件重新构建并通过相同检查。

**真实抖音 App 扫码、真实直播礼物与长时间重连尚未完成用户实机端到端验证。** 本地存在会话 Cookie 不代表抖音服务器仍接受登录。抖音礼物事件不是支付凭证，请谨慎开启按礼物排队资格。

建议保留稳定版 `v3.0.24`，先备份 Roaming/配置/队列存档，将测试版完全解压到新的程序目录；不要直接覆盖原有程序。Windows 本机扫码和后台采集要求后端所在电脑具有可用 Chrome/Edge。

## 下载（v3.0.25-test 测试版）

- [Windows Tk x64 便携版](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.25-test/BiliPDJ-v3.0.25-test-Windows-Tk-Portable-x64.zip)
- [Web x64 便携版](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.25-test/BiliPDJ-v3.0.25-test-Web-Portable-x64.zip)
- [GitHub 测试版 Release 页面](https://github.com/ZzzHe2333/bilipdj/releases/tag/v3.0.25-test)

Release 同时提供 `Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、`Web-files.json`、`Web-Incremental-x64.pack`、`update-manifest.json` 与对应 SHA-256 校验文件，供更新器使用。**普通用户无需手动下载**这些清单和增量文件，只需选择相应 ZIP。上述链接在打包并发布成功后有效。

对应记录：[Issue #331](https://github.com/ZzzHe2333/bilipdj/issues/331)、[Issue #333](https://github.com/ZzzHe2333/bilipdj/issues/333)、[PR #332](https://github.com/ZzzHe2333/bilipdj/pull/332)。

**发行类型：测试版 / Pre-release；标签：v3.0.25-test。**
