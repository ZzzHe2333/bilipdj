# Bilipdj — Bilibili 直播弹幕排队工具

Bilipdj 是一个用于 Bilibili 直播间的**弹幕排队管理系统**。

观众在直播间发送特定弹幕命令（如"排队"、"取消排队"等），后端自动维护排队列表，主播或管理员可通过桌面 GUI 和浏览器页面实时查看和管理队列。

---

## 功能概览

- **实时弹幕接入**：通过 WebSocket 接入 Bilibili 直播间弹幕流。
- **排队逻辑处理**：根据弹幕命令自动入队、出队、修改、插队。
- **权限控制**：支持超级管理员、管理员、舰长等多级权限，控制谁能执行哪些命令。
- **功能开关**：可按需启用/禁用各类排队命令（官服、B服、超级、米服等）。
- **队列展示**：浏览器端实时显示当前队列，适合 OBS 浮窗等场景。
- **存档管理**：支持最多 5 个存档槽位，可随时切换和恢复历史队列。
- **桌面控制台**：tkinter GUI，支持启动/停止后端、查看日志、配置管理、性能监控。
- **扫码登录**：支持扫描二维码自动获取 Bilibili Cookie。

---

## 快速开始

### 环境要求

- Python 3.10+
- 可选：`pip install psutil`（性能监控标签页）
- 可选：NVIDIA 驱动 / `pip install gputil`（GPU 监控）

### 启动方式

```bash
python bilipdj/core/control_panel.py
```

首次启动会在 `bilipdj/core/` 目录自动生成 `config.yaml`、`quanxian.yaml`、`kaiguan.yaml`。

编辑 `config.yaml`，填入以下关键字段后，点击"启动后端"：

```yaml
api:
  roomid: 123456      # 直播间房间号
  uid: 10001          # 主播 UID
  cookie: "SESSDATA=xxx; bili_jct=xxx; DedeUserID=xxx"
```

也可以在 GUI 的"设置"标签页填写，或使用"扫码自动获取 Cookie"功能。

---

## 配置文件

三个配置文件均位于 `core/` 目录（打包 exe 后在 exe 同级目录）。

### config.yaml — 主配置

| 字段 | 说明 | 默认值 |
|---|---|---|
| `server.host` | 监听地址 | `0.0.0.0` |
| `server.port` | 监听端口 | `9816` |
| `api.roomid` | 直播间房间号 | `0` |
| `api.uid` | 主播 UID | `0` |
| `api.cookie` | Bilibili Cookie | 空 |
| `logging.level` | 日志等级 | `INFO` |
| `logging.retention_days` | 日志保留天数 | `15` |
| `queue_archive.enabled` | 是否启用存档 | `true` |
| `queue_archive.active_slot` | 当前活动存档槽（1~5） | `1` |

### quanxian.yaml — 权限配置

```yaml
super_admin:      # 最高权限，可增删管理员
  - "张三"
admin:            # 管理员
  - "李四"
jianzhang:        # 舰长，仅可插队
  - "舰长A"
member:           # 普通成员（留空即可）
```

### kaiguan.yaml — 功能开关

```yaml
paidui: true              # 普通排队
guanfu_paidui: true       # 官服排队
bfu_paidui: true          # B服排队
chaoji_paidui: true       # 超级排队
mifu_paidui: true         # 米服排队
quxiao_paidui: true       # 取消排队
xiugai_paidui: true       # 修改排队内容
jianzhang_chadui: false   # 舰长插队
fangguan_op: false        # 允许房管执行管理员命令
```

---

## GUI 界面说明

| 标签页 | 功能 |
|---|---|
| 日志 | 查看后端日志，支持实时滚动 |
| 当前排队 | 实时显示排队列表（后端运行时自动刷新） |
| 设置 | 编辑服务器/直播间配置、日志等级、存档槽位 |
| 权限 | 管理各级权限名单 |
| 开关 | 控制各排队功能开关 |
| 性能 | 查看 CPU / 内存 / 磁盘 / GPU 占用 |
| 关于 | 版本信息 |

---

## 说明

- 这是**免费软件**。如果有人向你收费购买（亲手帮安装除外），请立刻退款！
