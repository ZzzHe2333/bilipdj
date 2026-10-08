# 弹幕排队姬 v3.0.18 正式版

本次为 **3.0.18 正式版（Release）**，由 `now` 分支打包，版本号使用标准 `x.y.z` 格式。包含近期多平台弹幕、更新下载与界面体验改进。

## 本次更新

- **多平台弹幕激活修复（#283）**：修复“平台参数 → 当前平台”影响“激活平台”保存的缺陷，`active_platforms` 独立持久化；切换参数编辑视图不会取消已激活的其他平台。支持同时激活多个弹幕来源，包含显式全部停用及旧单平台配置兼容。
- **多平台扩展（#284）**：从后端实际可用的弹幕插件注册表构建激活选项，可同时激活 Bilibili、抖音、虎牙、YouTube（红色小电视）及 Twitch（紫色老鼠），不再限于两个平台。YouTube/Twitch 仍需先完成对应直播间配置；快手、斗鱼、微信视频号等未实现的来源不视为已支持。
- **支持页面调整（#282）**：移除流量卡相关的“支持我们”广告和自动推广弹幕，仅保留赞赏码。
- **GitHub 更新下载加速（#285）**：支持主动选择第三方 `gh-proxy.com` 下载 GitHub Release 文件；每次使用前均需确认。加速失败、返回 HTML 或完整性校验失败时按规则尝试官方源，不放宽 SHA-256 校验。GitHub API 版本查询仍走官方。
- **检测更新动态反馈（#286）**：Windows 检测更新时显示紫色流光和活动进度条；Web 更新页显示轻量扫光反馈，成功/失败后自动停止；Web 尊重减少动态效果偏好。

## 全量与增量更新

- 同时打包 Windows Tk 与 Web Portable x64 完整便携版；
- 包含完整包、逐文件清单、HTTP Range 增量资源包、SHA-256 校验和 `update-manifest.json`；
- 更新过程维持原有版本校验、备份/回滚和用户配置、队列存档、插件、日志保护；
- 多平台并发的自动回归使用模拟 Relay；实际直播间账号、网络鉴权和不同平台的并发连通性仍需要在使用环境中检验。

## 下载安装

一般用户仅需下载对应完整便携包：

- **Windows 控制台：** [BiliPDJ-v3.0.18-Windows-Tk-Portable-x64.zip](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.18/BiliPDJ-v3.0.18-Windows-Tk-Portable-x64.zip)
- **Web 便携版：** [BiliPDJ-v3.0.18-Web-Portable-x64.zip](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.18/BiliPDJ-v3.0.18-Web-Portable-x64.zip)

程序内更新可选择官方线路或经明确同意的第三方 GH-Proxy 文件加速；下载后仍会检查 SHA-256。其他 `*-files.json`、`*-Incremental-x64.pack` 和 `*.sha256` 文件供更新器使用。

## 发布状态

**正式版（Release，非 Pre-release）**，标签 `v3.0.18`，不带测试版后缀。
