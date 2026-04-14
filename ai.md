# Bilipdj — AI 项目概览（供 Claude 读取）

本文件记录项目结构、关键设计决策与近期变更，供 AI 助手快速恢复上下文使用。

---

## 1. 项目简介

Bilipdj 是一个 **Bilibili / 抖音直播间弹幕排队管理系统**，由四部分组成：

- **Python 后端**（`core/server.py`）：HTTP + WebSocket 服务器，连接弹幕流、管理排队逻辑、提供 REST API。
- **桌面 GUI**（`core/control_panel.py`）：tkinter 控制台，启动/停止后端、配置管理、日志展示、性能监控。支持暗夜/明亮双主题。
- **Web 前端**（`core/ui/`）：浏览器端排队展示（`index.html`）与配置页（`config.html`），霓虹科技感主题。
- **透明弹窗**（`core/overlay_host.py`）：OBS 窗口捕获兼容的无边框透明 tkinter 窗口，独立进程运行。

---

## 2. 目录结构

```
bilipdj/                             # 仓库根（含 .git）
├── core/
│   ├── control_panel.py         # 桌面 GUI（主入口）
│   ├── server.py                # 后端 HTTP/WS 服务器
│   ├── bilibili_protocol.py     # Bilibili 弹幕协议（WebSocket 二进制）
│   ├── douyin_protocol.py       # 抖音直播轮询协议（HTTPS + Protobuf）
│   ├── douyin_live_pb2.py       # 抖音 Protobuf 定义
│   ├── overlay_host.py          # OBS 透明弹窗宿主
│   ├── style.json               # 队列样式配置（运行时生成）
│   ├── config.yaml              # 主配置（运行时生成）
│   ├── quanxian.yaml            # 权限配置（运行时生成）
│   ├── kaiguan.yaml             # 功能开关（运行时生成）
│   ├── 256x.ico / 128x.ico      # 应用图标
│   ├── ui/                      # Web 前端
│   │   ├── index.html           # 队列展示页
│   │   ├── config.html          # 配置/登录页
│   │   ├── cookie_login.html    # 扫码登录弹窗
│   │   ├── moren.css            # 队列展示样式（霓虹科技感主题）
│   │   ├── myjs.js              # 前端 WebSocket 逻辑
│   │   └── pako.min.js          # 压缩库
│   └── cd/                      # 排队存档 CSV + 状态 JSON（运行时生成）
├── bilipdj_onedir.spec          # PyInstaller 主程序配置
├── paiduijitm.spec              # PyInstaller 透明弹窗配置（onefile）
├── package-windows-local.ps1    # Windows 本地打包脚本
├── ai.md                        # 本文件（AI 上下文）
├── README.md
├── UPDATE.md
└── GUIDE.md                     # 用户教学文档
```

> **注意**：`config.yaml`、`quanxian.yaml`、`kaiguan.yaml`、`style.json` 位于 `core/`（开发模式），打包为 exe 后生成在 exe 同级目录。

---

## 3. 路径常量规则

`server.py` 和 `control_panel.py` 均使用相同逻辑：

```python
CORE_DIR  = Path(__file__).resolve().parent      # bilipdj/core/（开发模式）
APP_DIR   = Path(sys.executable).parent if frozen else REPO_DIR
_YAML_DIR = APP_DIR if frozen else CORE_DIR

CONFIG_PATH   = _YAML_DIR / "config.yaml"
QUANXIAN_PATH = _YAML_DIR / "quanxian.yaml"
KAIGUAN_PATH  = _YAML_DIR / "kaiguan.yaml"
LOG_DIR       = APP_DIR / "log"
PD_DIR        = APP_DIR / "core" / "cd"          # 排队存档目录
```

---

## 4. 配置文件说明

### config.yaml（主配置）

```yaml
server:
  host: 0.0.0.0
  port: 9816

platform: bilibili          # bilibili | douyin

bilibili:
  roomid: 0
  uid: 0
  cookie: ""

douyin:
  enabled: false
  live_id: ""               # 直播间路径 ID（如 833045918808）
  cookie: ""
  signature: ""
  ws:
    auto_reconnect: true
    heartbeat_interval_seconds: 30
    reconnect_delay_seconds: 2.0
  bootstrap:
    cursor: ""
    internal_ext: ""
  live_info:                # 由"获取参数"或 _sync_runtime_live_info 写入
    room_id: ""             # WebSocket 房间 ID（URL 的 ?room_id= 参数）
    user_id: ""
    user_unique_id: ""
    anchor_id: ""
    sec_uid: ""
    ttwid: ""
  extra_query: {}

myjs:
  paidui_list_length_max: 100
  all_suoyourenbukepaidui: false
  fangguan_can_doing: false  # 等同于 kaiguan.fangguan_op
  jianzhangchadui: false

ui:
  auto_start_backend: false
  language: 中文
  overlay_window:
    width: 400
    height: 400
    scale: 50               # 字体缩放 40–250%

logging:
  level: INFO
  retention_days: 15

queue_archive:
  enabled: true
  slots: 10                 # 固定 10 槽
  active_slot: 1

callback:
  enabled: false
  url: ""
  auth_token: ""
```

### quanxian.yaml（权限配置）

字段（均为用户名字符串列表）：`super_admin`、`admin`、`jianzhang`、`member`、`blacklist`

### kaiguan.yaml（功能开关）

| 开关 | 默认 | 说明 |
|---|---|---|
| `paidui` | `true` | 排队总开关 |
| `guanfu_paidui` | `true` | 官服排队（`官\|`） |
| `bfu_paidui` | `true` | B 服排队（`B\|`） |
| `chaoji_paidui` | `true` | 超级排队（`<>`） |
| `mifu_paidui` | `true` | 米服排队（`米\|`） |
| `quxiao_paidui` | `true` | 取消排队 |
| `xiugai_paidui` | `true` | 修改排队内容 |
| `jianzhang_chadui` | `false` | 舰长插队 |
| `fangguan_op` | `false` | 房管拥有管理员权限 |

### style.json（样式配置）

```json
{
  "text_color": "#eaf6ff",
  "text_stroke_color": "#000000",
  "text_stroke_enabled": true,
  "queue_font_size": 50,
  "queue_font_weight": "700",
  "queue_font_style": "italic",
  "text_grad_start": "#00d4ff",
  "text_grad_end": "#0099ff",
  "bg1": "#ffffff",
  "bg2": "#f5f5f5",
  "bg3": "#eeeeee"
}
```

---

## 5. 主要 API 端点

### GET 接口

| 路径 | 说明 |
|---|---|
| `/` | 重定向到 `/index.html` |
| `/index` / `/index.html` | 队列展示页 |
| `/config` / `/config.html` | 配置/登录页 |
| `/cookie-login` | 扫码登录弹窗页 |
| `/health` | 健康检查 `{"status":"ok","service":"bilipdj","port":N}` |
| `/model` | Bilibili 初始模型 JSON（调试） |
| `/api/config/basic` | 基础配置（roomid/uid/platform，无 cookie） |
| `/api/config/login` | 登录态读取（含 cookie） |
| `/api/config` | 完整配置快照 |
| `/api/runtime-status` | 后端连接状态（DEBUG 级不记录） |
| `/api/queue/state` | 当前内存队列 `{queue, entries, size}` |
| `/api/queue/archive` | 存档槽元数据列表 |
| `/api/queue/log` | 队列操作日志 CSV |
| `/api/blacklist/state` | 黑名单列表 |
| `/api/quanxian` | 权限配置 |
| `/api/kaiguan` | 功能开关 |
| `/api/style` | 样式配置 |
| `/api/bili/qr/start` | 开始扫码（返回 QR 图像 + key） |

### POST 接口

| 路径 | 输入 | 说明 |
|---|---|---|
| `/api/config/login` | `{uid, cookie}` | 保存登录态 |
| `/api/config` | 配置 dict | 更新主配置 |
| `/api/queue/reload` | `{}` | 从存档重载队列 |
| `/api/queue/switch` | `{slot: 1~10}` | 切换活动存档槽 |
| `/api/queue/delete` | `{index}` | 删除队列项 |
| `/api/queue/move` | `{index, direction}` | 移动队列项（up/down） |
| `/api/queue/insert` | `{position, content}` | 在指定位置插入 |
| `/api/queue/update` | `{index, content}` | 更新内容 |
| `/api/queue/clear` | `{}` | 清空队列 |
| `/api/blacklist/add` | `{name}` | 加入黑名单 |
| `/api/blacklist/delete` | `{index}` | 移出黑名单 |
| `/api/blacklist/clear` | `{}` | 清空黑名单 |
| `/api/style` | 样式 dict | 保存样式配置 |
| `/api/quanxian` | 权限 dict | 更新权限配置 |
| `/api/kaiguan` | 开关 dict | 更新功能开关 |
| `/api/bili/qr/poll` | `{qrcode_key}` | 轮询扫码状态 |

### WebSocket

- `ws://.../danmu/sub`（别名 `/ws`）：弹幕推送与队列状态实时推送

**消息类型：**

```jsonc
// 队列变化
{"type":"QUEUE_UPDATE","queue":["[官|user1]内容",...],"entries":[{"id":"user1","content":"...","last_operation_at":"2026-04-14 12:00:00"},...]}

// 连接状态变化
{"type":"PDJ_STATUS","status":"danmu_connected","platform":"bilibili","roomid":12345,...}

// 抖音弹幕事件
{"type":"DOUYIN_DANMU","uid":123,"sec_uid":"...","nickname":"用户","content":"弹幕内容","time":"...","platform":"douyin"}
```

---

## 6. GUI 标签页（共 10 个）

| # | 标签 | 说明 |
|---|---|---|
| 0 | 日志 | 后端输出日志，支持按等级过滤，时间戳绿色，URL 可点击 |
| 1 | 当前排队 | 实时队列（Treeview），支持删除/上移/下移/新增/清空，3 秒轮询 |
| 2 | 黑名单 | 黑名单用户名列表管理，4 秒轮询 |
| 3 | 设置 | 配置编辑（Canvas 滚动），含基础/平台/抖音参数一键获取 |
| 4 | 透明窗口 | 启动/关闭/置顶透明弹窗，尺寸/缩放设置 |
| 5 | 权限 | super_admin / admin / jianzhang / member 名单 |
| 6 | 开关 | 9 项功能开关 Checkbutton |
| 7 | 性能 | CPU / 内存 / 磁盘 / GPU 实时占用 |
| 8 | 样式设置 | 队列颜色、字体、描边等（写入 style.json） |
| 9 | 关于 | 版本信息与法律声明 |

---

## 7. 存档槽机制

- 共 10 个槽（`queue_archive_slot_1.csv` ~ `queue_archive_slot_10.csv`）
- `active_slot` 由 GUI 写入 config，后端读取并持久化到 `core/cd/queue_archive_state.json`
- `QueueArchiveManager._read_state / _write_state` 有 `threading.Lock` 保护
- 切换槽位：GUI → POST `/api/queue/switch` → 后端更新内存队列 + 广播 `QUEUE_UPDATE`
- 后端重启：从 `queue_archive_state.json` 读取 `active_slot`，恢复对应 CSV

---

## 8. 权限体系

| 角色 | 权限范围 |
|---|---|
| `super_admin` | 全部操作 + 添加/移除管理员、拉黑/解黑 |
| `admin` | 全部队列操作，不能修改管理员名单 |
| `jianzhang` | 仅插队（需 `jianzhang_chadui=true`） |
| `member` | 仅排队/取消/修改自身 |
| `blacklist` | 全部命令被拦截，写入日志 |

---

## 9. 弹幕指令

### 普通用户

| 弹幕内容 | 效果 |
|---|---|
| `排队` | 加入排队 |
| `官服排` / `官服排队` | 以 `官\|昵称` 加入排队 |
| `B服排` / `排B服` | 以 `B\|昵称` 加入排队 |
| `超级排` / `超级排队` | 以 `<昵称>` 加入排队 |
| `小米排` / `排米服` | 以 `米\|昵称` 加入排队 |
| `排队 [内容]` | 以自定义内容加入排队 |
| `取消排队` | 离开排队 |
| `替换 [内容]` / `修改 [内容]` | 修改已有排队内容 |

### 管理员 / 主播

| 弹幕内容 | 效果 |
|---|---|
| `完成` / `del [ID]` | 删除队列中指定用户 |
| `add [ID] [内容]` | 在队首插入指定内容 |
| `无影插 [ID]` | 静默插队（不广播通知） |
| `暂停排队功能` | 关闭排队总开关 |
| `恢复排队功能` | 开启排队总开关 |
| `设置排队上限 [N]` | 设置最大排队人数 |

### super_admin 专属

| 弹幕内容 | 效果 |
|---|---|
| `添加管理员 [昵称]` | 提升为 admin |
| `取消管理员 [昵称]` | 降级为 member |
| `拉黑 [昵称]` | 加入黑名单 |
| `取消拉黑 [昵称]` | 移出黑名单 |

---

## 10. 技术栈

- **后端**：Python 3.10+，标准库（`http.server`、`threading`、`socket`、`ssl`、`json`、`csv`、`yaml`）
- **协议**：Bilibili 二进制 WebSocket（版本 0/1/2/3，zlib/brotli 压缩）；抖音 HTTPS 轮询（Protobuf）
- **GUI**：`tkinter` + `ttk.Notebook`；`ttk.Style`（clam 主题，暗夜/明亮双模式）；`psutil`；`GPUtil`/`nvidia-smi`（GPU 可选）
- **前端**：HTML5 + CSS3（霓虹科技感主题，直角控件）+ Vanilla JS；WebSocket 实时推送
- **打包**：PyInstaller（Windows x64）；CI/CD via GitHub Actions

---

## 11. UI 样式说明

### tkinter 控制台（暗夜/明亮双模式）

- **切换方式**：顶栏「☀ 明亮 / 🌙 暗夜」按钮，`_toggle_theme()` → `_apply_theme(dark)`
- **暗夜**：`bg=#07101e`（深黑蓝）+ `accent=#00e5ff`（青霓虹）+ `fg=#cce8ff`
- **明亮**：`bg=#eef3fa`（浅蓝灰）+ `accent=#0078c8`（科技蓝）+ `fg=#1a2a40`
- **实现**：`ttk.Style("clam")` 全局配置，所有 tk.Text 手动遍历设色，`focuscolor` 与背景同色消除虚框
- 日志时间戳：绿色 `ts` tag；URL 可点击（`link` tag，`webbrowser.open`）

### Web 前端（霓虹科技感）

- **配色**：深黑/深蓝背景 `#07101e` + 青色霓虹主色 `#00e5ff`
- **纹理**：`repeating-linear-gradient` 扫描线叠加
- **容器**：左侧 3px 青色实线 + 顶角/底角 L 形装饰 + `box-shadow` 发光
- **控件**：`input`/`textarea`/`button` 全部直角；focus/hover 时边框发光扩散
- **队列文字**：`text-shadow` 青色霓虹发光；待确认项青/白闪烁动画

---

## 12. 打包说明

| spec | 说明 | 产物 |
|---|---|---|
| `bilipdj_onedir.spec` | 主程序（onedir） | `dist\bilipdj\main.exe` |
| `paiduijitm.spec` | 透明弹窗独立进程（onefile） | `dist\paiduijitm.exe` |

本地打包：
```powershell
powershell -ExecutionPolicy Bypass -File .\package-windows-local.ps1 -InstallDependencies
```
脚本构建 `dist\bilipdj\main.exe` 并将 `dist\paiduijitm.exe` 复制至主程序目录。
CI（`package-windows-x64.yml`）同样调用此脚本，onedir 打包成 zip 发布。

---

## 13. 近期变更摘要

| 时间 | 变更 |
|---|---|
| 2026-04-14 | 文档全量重写（ai.md / README.md / GUIDE.md）；修复 QueueArchiveManager 竞态、preset 覆盖逻辑、刷新线程积压 |
| 2026-04-13 | 设置页平台参数区可滚动 Canvas；抖音参数一键从链接获取（URL query room_id 优先） |
| 2026-04-13 | 抖音弹幕"没有弹幕"修复：room_status 正则兜底、unknown 状态乐观连接、preset 字段补填 |
| 2026-04-11 | UI 科技感重设计：tkinter 双主题、Web 霓虹青色、扫描线纹理、直角控件 |
| 2026-04-11 | 打包产物名调整为 `main.exe`，新增本地打包脚本 |
| 2026-04-09 | 透明弹窗 OBS 兼容、GUI 置顶/关闭控制 |
| 2026-04-08 | 性能标签页、日志等级过滤、存档槽 10 个、权限/开关独立文件 |
| 2026-04-08 | 排队逻辑后端化（QueueManager），前端仅做展示 |
