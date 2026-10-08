# 弹幕排队姬 v3.0.22 正式版

本版基于 **v3.0.21 正式版**，重点发布 **B站/抖音等多平台共用同一活动排队存档** 的修复（Issue [#303](https://github.com/ZzzHe2333/bilipdj/issues/303)、PR [#304](https://github.com/ZzzHe2333/bilipdj/pull/304)）。同时收录 v3.0.21 之后已合并的 Web 直播控制台、Docker 部署及双 UI 维护改进。

## 主要修复：多平台统一队列和存档

- **真正共用一个队列、一个当前存档槽位：** 同时激活 Bilibili、抖音及其他已接入 DanmuEvent 的弹幕平台时，事件进入同一 QueueManager 和活动 CSV 存档，Web、Windows Tk、OBS 查看相同队列。
- **存档可追溯来源：** `queue_archive_slot_N.csv` 新增第 5 列 `platform`，记录每条排队来源；人工添加标记为 `manual`。历史 2–4 列 CSV 可继续读取，缺失来源保持空值，不误认作指定平台。
- **同名不同平台分别排队：** 对相同昵称按来源平台区分，避免误判重复或误取消。移动、编辑、删除、插队、清空、切档及恢复均保留来源。
- **并发存档可靠性：** 独立的归档写锁协调快照读取、写盘和广播，避免多路弹幕同时操作时旧快照覆盖新存档，并修正清空竞态及回归过程中发现的锁顺序死锁。
- **展示兼容：** Web 与 Tk 的管理队列显示来源平台，OBS 透明排队层仍只显示用户名、不增加平台前缀；Web“下一位”撤销保留原条目来源。

## 本版收录的其他改进

- **Web 直播控制台和 OBS 预览（#298）：** 队列优先、连接状态、快速下一位/排序、紧凑模式，以及 OBS 背景与极端数据模拟预览；保留既有 API 和双端功能契约。
- **Web 官网与上手体验（#298）：** 最新版下载入口、操作步骤、常见问题和主题可读性改进。
- **Docker Web 部署（#301 / PR #302）：** 使用既有 Web 界面与 Server，完善持久化、容器隔离、安全边界及 Docker Compose 验收；不单独开发第三套 UI。
- **双 UI 维护（#299 / PR #300）：** Windows Tk 与 Web 独立保留并共同维护，服务端作为队列和配置的单一真实来源。
- 继承 v3.0.21 的 Web 控制台视觉改版、跨平台弹幕、礼物规则、更新器与配置备份能力。

## 下载及发行资源

- **Windows Tk x64 便携包：** [BiliPDJ-v3.0.22-Windows-Tk-Portable-x64.zip](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.22/BiliPDJ-v3.0.22-Windows-Tk-Portable-x64.zip)
- **Web x64 便携包：** [BiliPDJ-v3.0.22-Web-Portable-x64.zip](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.22/BiliPDJ-v3.0.22-Web-Portable-x64.zip)

同一 Release 附带 Windows Tk / Web 各自的逐文件清单、增量资源包及对应 SHA-256，以及 `update-manifest.json`，供全量或增量更新使用。普通用户优先下载对应便携 ZIP；升级前请保留配置及 CSV 排队存档备份。

## 验证与兼容边界

- 多平台统一队列 PR #304 通过 Quality/API、Server、Web 构建、Windows Tk 与 Web Portable 构建、Docker Compose，以及模拟双平台事件、同名用户、并发入队、CSV 兼容和重启读档回归。
- 本正式版构建的最终结果以 v3.0.22 发布工作流及 Release 资产校验为准。
- **尚未在用户真实同时开播的 B站和抖音房间执行端到端验收**；模拟弹幕通过不等于所有网络节点、账号鉴权与直播间环境均已实测。

## 发布状态

**正式版（GitHub Release，不是 Pre-release）**，标签 `v3.0.22`，版本从 `3.0.21` 仅升级补丁号至 `3.0.22`。
