# 弹幕排队姬（Bilipdj）
[更新日志](./UPDATE.md) |
[Ai请看](./ai.md) | 
[readme](./README.md) | 
[教程](./GUIDE.md) |
[贡献者](./CONTRIBUTORS.md) |

[![更新日志](https://img.shields.io/badge/更新日志-UPDATE.md-blue)](UPDATE.md) 
[![使用教程](https://img.shields.io/badge/使用教程-GUIDE.md-green)](GUIDE.md) 
[![AI上下文](https://img.shields.io/badge/AI上下文-ai.md-purple)](ai.md)

弹幕排队姬是一个面向 **Bilibili / 抖音**直播间的弹幕排队管理工具。
排队逻辑由 Python 后端统一处理，前端页面只负责展示，避免前后端计算不一致。

## 核心功能

- 弹幕入队、取消、修改、管理员删除等指令统一由后端处理
- **权限体系**：`super_admin` / `admin` / `jianzhang` / `member` / `blacklist`
- **黑名单机制**：黑名单用户无法触发任何指令，拦截行为写入日志
- **功能开关**：总开关与分项开关（官服 / B 服 / 超级 / 米服 / 舰长插队等，共 9 项）
- **排队存档**：10 个槽位，支持切换、恢复、清空；每条记录保留最后操作时间
- **多平台支持**：Bilibili（WebSocket 二进制协议）、抖音（HTTPS 轮询 + Protobuf）
- **GUI 控制台**：10 个标签页，涵盖日志、队列、黑名单、设置、权限、开关、性能、样式等；暗夜/明亮双主题
- **透明弹窗**：支持 OBS 窗口捕获、拖拽移动、置顶控制

## 目录结构（主要）

```
bilipdj/
├── core/
│   ├── control_panel.py        # 桌面 GUI（主入口）
│   ├── server.py               # 后端 HTTP/WS 服务器
│   ├── bilibili_protocol.py    # Bilibili 弹幕协议
│   ├── douyin_protocol.py      # 抖音直播协议
│   ├── overlay_host.py         # OBS 透明弹窗
│   ├── config.yaml             # 主配置（运行时生成）
│   ├── quanxian.yaml           # 权限配置（运行时生成）
│   ├── kaiguan.yaml            # 功能开关（运行时生成）
│   ├── style.json              # 样式配置（运行时生成）
│   ├── ui/                     # Web 前端（index/config/cookie_login）
│   └── cd/                     # 排队存档 CSV + 状态 JSON
├── bilipdj_onedir.spec         # PyInstaller 主程序配置
├── paiduijitm.spec             # PyInstaller 透明弹窗配置（onefile）
├── package-windows-local.ps1   # Windows 本地打包脚本
├── README.md
├── UPDATE.md
└── GUIDE.md                    # 用户教学文档
```

## 快速启动（源码运行）

1. 进入 `bilipdj/` 子目录（本 README 所在位置）。
2. 安装依赖：

```bash
pip install Pillow qrcode brotli psutil pyyaml protobuf
```

3. 启动 GUI：

```bash
python core/control_panel.py
```

启动后可通过 GUI 顶部按钮访问：

- **配置页**（扫码登录）：`http://127.0.0.1:9816/config`
- **展示页**（队列看板）：`http://127.0.0.1:9816/index`

**透明弹窗（OBS 捕获）**：在 OBS 中添加「窗口捕获」，按标题 **排队透明弹窗** 选择窗口，勾选「允许透明」即可。

## 弹幕指令

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

## 配置说明（简要）

- `core/config.yaml`：直播间信息、日志、存档槽位、UI 参数
- `core/quanxian.yaml`：权限名单（每行一个用户名）
- `core/kaiguan.yaml`：9 项功能开关（布尔值）
- `core/style.json`：队列展示颜色、字体、描边等

关闭 `kaiguan.paidui` 后，除管理员命令外所有排队指令不处理；
`恢复排队功能` 弹幕或在 GUI 开关页勾选可重新开启。

## 打包说明（Windows）

| spec | 说明 | 产物 |
|---|---|---|
| `bilipdj_onedir.spec` | 主程序（onedir） | `dist\bilipdj\main.exe` |
| `paiduijitm.spec` | 透明弹窗独立进程（onefile） | `dist\paiduijitm.exe` |

本地打包推荐执行脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\package-windows-local.ps1 -InstallDependencies
```

脚本会构建 `dist\bilipdj\main.exe` 并自动将 `dist\paiduijitm.exe` 复制至主程序目录。
CI（`package-windows-x64.yml`）同样调用此脚本，将 onedir 打包成 zip 发布。

### 按架构打包

构建必须在与目标架构一致的原生 runner 上执行，避免生成“文件名正确但无法运行”的伪跨架构产物：

- `scripts/package-arm64.sh`：macOS Apple Silicon / Linux arm64 runner
- `scripts/package-amd64.sh`：macOS Intel / Linux amd64 runner
- `scripts/package-amd64.ps1`：Windows amd64 runner

GitHub Actions 的 macOS 工作流可选择 `arm64` 或 `x86_64`，两条路径会分别调用对应的架构校验脚本。发布前会运行 `scripts/scan_secrets.py`，拒绝把 Cookie、Token 或私钥打入产物。

### API 健康检查

```bash
python scripts/check_api_health.py
```

此检查不读取本地 Cookie：它会分层验证 Bilibili 房间解析/弹幕服务器接口，以及抖音直播页解析。抖音完整弹幕轮询需要真实开播房间和有效登录态，因此默认不会把“需要 Cookie 或当前未开播”误报成 API 过期。

### 标准化弹幕身份接口

Bilibili `DANMU_MSG` 会被转换为 WebSocket 内部事件 `DANMU_EVENT`，其中 `identity` 包含主播、房管、舰长/提督/总督以及粉丝牌名称和等级。最近一次解析结果也可以从本机接口读取：

```text
GET http://127.0.0.1:9816/api/danmu/identity/latest
```

接口不返回 Cookie、弹幕鉴权 token 等登录凭据。

### 送礼与上舰事件

后端会把 `SEND_GIFT`、`COMBO_SEND`、`GUARD_BUY` 标准化为 `LIVE_GIFT_EVENT`，并通过 `GET /api/gifts/state` 提供最近事件、内置 77 种礼物及运行期间识别到的礼物类型。GUI 的“送礼插队”标签默认关闭，可按一个或多个指定礼物、最低电池数（10 电池 = 1 元）授予排队资格，并设置每次可排人数、插入名次及是否允许重复获得资格。“仅允许礼物排队”会临时把插入名次设为 0，关闭后恢复原名次。

用户发送 `插队 名字1 名字2` 时会按名字的输入顺序处理；不带名字时使用送礼者昵称。未开启重复插队时，每个 UID 只能使用一次资格。

### 每日排队次数限制

“设置 → 基础设置”可限制每个用户在一个统计日内成功加入队列的次数，范围为 1–999，默认不限制。统计日默认每天 `04:00` 重置，也可以自定义重置时刻；计数按 UID（无 UID 时按用户名）保存，重启程序不会清空。

### MirrorChyan 预接入

`core/mirrorchyan.py` 已实现官方 `latest` API 客户端并进入打包隐藏依赖，但 `MirrorChyanSettings.enabled` 默认是 `False`，当前启动流程不会调用它，也不会发送 CDK 或自动下载更新。

预载入

## 运行数据与日志

- 日志目录：`log/`（按日期命名，保留天数可配置）
- 存档目录：`core/cd/`（10 个槽位 CSV + 状态 JSON）

建议将 `core/cd/` 和 `core/*.yaml` 加入 `.gitignore`，避免误提交个人直播数据。

## 许可证

本项目使用仓库内 `LICENSE` 文件所示许可协议。
