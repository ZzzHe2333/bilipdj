# 弹幕排队姬 v3.0.20 正式版

本次是 **v3.0.20 正式版（Release）**，在 v3.0.18 基础上完善 Windows 更新界面、独立更新器和主程序启动反馈。保留 3.0.18 的多平台弹幕激活、第三方下载加速和完整性校验能力。

## 更新内容

- **更新页面交互调整（#288）**：精简版本/来源等重复提示，将当前版本号移动到“软件更新”标题右侧，并将 GitHub 官方 / GH-Proxy 下载线路设置移动到更新设置区域。
- **GitHub 不可达时的可选加速（#288）**：官方版本查询失败时可选择经第三方读取最新正式版的更新清单；官方资源网络下载失败时可确认切换 GH-Proxy 重试。使用第三方来源均需用户明确同意。第三方清单缺少独立的发布者签名，只适用于了解并接受该风险的用户；文件仍进行原有 SHA-256 验证及下载路径校验。
- **独立更新器可继续操作（#288）**：主程序未及时关闭、等待超时后弹出置顶提醒，提供“继续更新”按钮，关闭主程序后可重新检查并继续安装已下载的更新包，无需重新发起下载。维持旧有备份/回滚机制。
- **启动进度小窗（#289）**：Windows 版双击主程序后，在加载重量级服务与图形模块之前显示原生小型启动窗口、阶段状态和活动进度条；主窗口就绪后自动关闭。后台进程与自检模式不显示。
- **更新软件二级标签页（#290）**：“版本更新”用于检查、版本选择、全量/增量安装、进度和更新说明；“更新设置”用于 GitHub/GH-Proxy 线路、系统/第三方代理、连接测试及预留设置。两页分别滚动，功能独立。
- **此前功能保留（#283、#284、#285、#286）**：Bilibili、抖音、虎牙、YouTube、Twitch 多平台弹幕 Relay 激活配置，更新包加速与失败回退以及检查更新时的流光反馈。

## 发行包及更新资源

本次生成 Windows Tk 和 Web Portable x64 完整便携包、各自的增量资源、逐文件清单、SHA-256 文件以及 `update-manifest.json`。

- **Windows Tk 便携包：** [BiliPDJ-v3.0.20-Windows-Tk-Portable-x64.zip](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.20/BiliPDJ-v3.0.20-Windows-Tk-Portable-x64.zip)
- **Web 便携包：** [BiliPDJ-v3.0.20-Web-Portable-x64.zip](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.20/BiliPDJ-v3.0.20-Web-Portable-x64.zip)

更新器额外使用 `Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、`Web-files.json`、`Web-Incremental-x64.pack`、`update-manifest.json`、以及 `*.sha256`。**普通用户无需手动下载**这些增量和清单文件。

## 已知验证范围

- 自动测试覆盖 Windows 更新 UI、下载来源切换、超时后继续、启动窗口的 Win32 创建/关闭以及多平台模拟 Relay。
- 真实 Windows 桌面的启动视觉体验、真实直播间并发连通性和具体网络条件下的 GH-Proxy 可用性仍以实际使用为准。
- 更新过程保留配置、队列、插件和日志的数据保护及原有回滚机制。

## 发布状态

**正式版（Release，非 Pre-release）**。标签：`v3.0.20`，不带测试或预发行后缀。
