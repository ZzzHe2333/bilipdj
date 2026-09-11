# BiliPDJ API contract

默认本地地址：`http://127.0.0.1:9816`。

**后端是唯一业务状态源。** Windows Tk、Web 控制台、OBS 展示与第三方客户端都应通过 HTTP / WebSocket 读取和修改状态，不应在前端重新实现排队、权限、礼物或平台 Relay 业务规则。

## 1. 客户端接入原则

- 本地客户端优先连接 `127.0.0.1:9816`；
- 只读状态与实时事件可按 Server 监听配置用于局域网展示；
- 配置、登录凭据、权限、队列写操作、备份等管理接口按后端现有规则限制为本机管理；
- 不要把当前管理端口直接暴露到公网；
- 第三方客户端应以 `entries`、结构化状态和 WebSocket 事件为主，不要解析界面 HTML；
- 旧字段仅用于兼容，新增客户端应优先使用本文档列出的结构化字段。

## 2. HTTP 基础约定

JSON 写请求：

```http
Content-Type: application/json
```

成功通常返回：

```json
{
  "status": "ok"
}
```

失败通常返回：

```json
{
  "status": "error",
  "message": "错误原因"
}
```

客户端还必须同时检查 HTTP 状态码，不能只看 JSON `status`。

## 3. 访问范围

| 类型 | 典型接口 | 建议访问范围 |
|---|---|---|
| 健康/展示状态 | `/health`、`/api/runtime-status`、`/api/queue/state` | 本机；开启 LAN 后可用于可信局域网只读客户端 |
| WebSocket | `/ws`、`/danmu/sub` | 本机或可信局域网展示客户端 |
| 队列写操作 | `/api/queue/*` POST | 本机管理客户端 |
| 配置/登录 | `/api/config`、`/api/config/login`、Bilibili QR | 仅本机 |
| 主题/样式 | `/api/appearance*`、`/api/style` | 仅本机管理；OBS 可读取 `/api/style` |
| 权限/黑名单/开关 | `/api/quanxian`、`/api/blacklist/*`、`/api/kaiguan` | 仅本机 |
| 备份 | `/api/backup/*` | 仅本机 |
| 控制台日志/性能/更新 | `/api/control/*` | 仅本机 |

如果 Server 配置为 `0.0.0.0`，只代表开始监听局域网地址，**不代表已经具备公网身份认证能力**。

## 4. 基础接口

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 健康检查 |
| GET | `/api/runtime-status` | 后端、弹幕 Relay、WebSocket 运行状态 |
| GET | `/api/queue/state` | 当前队列与结构化 `entries` |
| GET | `/api/queue/archive` | 队列存档槽与当前槽 |
| GET | `/api/blacklist/state` | 黑名单 |
| GET | `/api/gifts/state` | B站礼物规则运行状态 |
| GET | `/api/quanxian` | 权限配置 |
| GET | `/api/kaiguan` | 功能开关 |
| GET | `/api/style` | 当前 OBS/队列展示样式 |
| POST | `/api/style` | 保存 OBS/队列展示样式 |
| GET | `/api/appearance` | 读取 Windows/Web 通用界面主题 |
| POST | `/api/appearance` | 保存 Windows/Web 通用界面主题 |
| GET | `/api/appearance/profile` | 导出 Windows/Web/OBS 通用配置 |
| POST | `/api/appearance/profile` | 导入 Windows/Web/OBS 通用配置 |
| GET | `/api/platforms/active` | 查询激活平台 |
| POST | `/api/platforms/active` | 修改激活平台 |
| GET | `/api/control/update` | 获取 Web 端更新状态/manifest 信息 |
| WS | `/ws`、`/danmu/sub` | 实时事件广播 |

更完整的 HTTP / WebSocket 开发参考由：

```bash
python scripts/generate_api_docs.py
```

生成到本地 `api/` 目录。

## 5. 队列读取

`GET /api/queue/state`

典型返回：

```json
{
  "status": "ok",
  "size": 2,
  "queue": ["用户A 牵丝霖", "用户B"],
  "entries": [
    {
      "id": "用户A",
      "content": "牵丝霖",
      "last_operation_at": "2026-09-08 17:00:00"
    },
    {
      "id": "用户B",
      "content": "",
      "last_operation_at": "2026-09-08 17:00:10"
    }
  ]
}
```

第三方客户端显示队列时：

- `entries[].id`：用户名；
- `entries[].content`：排队内容；
- `entries[].last_operation_at`：最近操作时间；
- `queue[]`：旧客户端兼容文本，不建议新客户端自行拆字符串。

## 6. 手动新增排队

`POST /api/queue/insert`

推荐结构：

```json
{
  "after": 2,
  "username": "用户A",
  "content": "牵丝霖"
}
```

字段：

- `after`：插入到第几项之后，`0` 表示队首；
- `username`：用户名，结构化调用时必填；
- `content`：排队内容，可为空；
- `entry`：旧客户端兼容字段，仍可传 `用户名 内容`。

用户名为空时，新客户端应在前端先阻止提交；后端也会拒绝无有效用户名的结构化新增。

## 7. 其他队列写操作

### 删除

`POST /api/queue/delete`

```json
{"index": 1}
```

### 移动

`POST /api/queue/move`

```json
{"index": 2, "direction": "up"}
```

`direction`：`up` / `down`。

### 修改

`POST /api/queue/update`

```json
{"index": 1, "content": "新内容"}
```

### 清空

`POST /api/queue/clear`

```json
{}
```

### 切换存档槽

`POST /api/queue/switch`

```json
{"slot": 2}
```

### 从当前存档重载

`POST /api/queue/reload`

```json
{}
```

## 8. 激活平台 / 多平台弹幕流

### 查询

`GET /api/platforms/active`

```json
{
  "status": "ok",
  "supported": ["bilibili", "douyin"],
  "active": ["bilibili", "douyin"],
  "one_room_per_platform": true,
  "runtime": {
    "active_platforms": ["bilibili", "douyin"],
    "platforms": {
      "bilibili": {"connected": true},
      "douyin": {"connected": true}
    }
  }
}
```

### 修改

`POST /api/platforms/active`

```json
{
  "active": ["bilibili", "douyin"]
}
```

当前规则：

- Bilibili 与抖音可以同时激活；
- 每个平台当前只允许一个直播间；
- 多个平台的排队消息进入同一个后端 `QueueManager`；
- 同一平台多个直播间暂不支持；
- 虎牙、快手、斗鱼、微信视频号仍为预留配置，不能激活真实弹幕流；
- 保存后后端会重建或重连对应 Relay。

第三方客户端不应自行启动平台协议连接后再维护另一份队列；应让 Server 统一接入平台并读取其状态。

## 9. WebSocket

入口：

```text
ws://127.0.0.1:9816/ws
ws://127.0.0.1:9816/danmu/sub
```

常见消息：

```json
{"type":"QUEUE_UPDATE","queue":["..."],"entries":[...]}
```

```json
{"type":"PDJ_STATUS","status":"danmu_connected","platform":"bilibili","roomid":3049445}
```

```json
{"type":"DOUYIN_DANMU","nickname":"用户","content":"弹幕内容","platform":"douyin"}
```

客户端收到 `QUEUE_UPDATE` 后应直接刷新本地队列状态，不要在前端重新执行入队/出队算法。

WebSocket 断线后建议：

1. 指数或固定间隔重连；
2. 重连成功后重新 GET `/api/queue/state`；
3. 再继续消费增量事件。

## 10. 更新清单

发行版包含 `update-manifest.json`。客户端必须**先读取清单，再下载安装包**，不要根据版本号猜文件名。

稳定入口：

```text
https://github.com/ZzzHe2333/bilipdj/releases/latest/download/update-manifest.json
```

示例：

```json
{
  "schema": 1,
  "version": "3.0.0",
  "tag_name": "v3.0.0",
  "release_url": "https://github.com/ZzzHe2333/bilipdj/releases/tag/v3.0.0",
  "packages": {
    "windows-tk-x64": {
      "filename": "BiliPDJ-v3.0.0-Windows-Tk-Portable-x64.zip",
      "url": "...",
      "sha256": "64位十六进制SHA256",
      "size": 12345678
    },
    "web-portable-x64": {
      "filename": "BiliPDJ-v3.0.0-Web-Portable-x64.zip",
      "url": "...",
      "sha256": "64位十六进制SHA256",
      "size": 12345678
    }
  }
}
```

正确更新流程：

1. GET `update-manifest.json`；
2. 比较 `version`；
3. 根据客户端类型选择 `packages` 条目；
4. 使用 `url` 下载清单指定的 `filename`；
5. 计算文件 SHA-256；
6. 与清单中的 `sha256` 完全一致后才允许安装。

## 11. 登录与敏感配置

Bilibili Cookie、`SESSDATA`、`bili_jct`、备份凭据等属于敏感信息。第三方客户端：

- 不应记录完整 Cookie 到普通日志；
- 不应上传这些字段到第三方服务器；
- 不应在远程浏览器页面直接暴露 `/api/config/login`；
- 如果只做队列展示，不应请求登录/配置接口。

## 12. 远程访问建议

当前版本不把 `9816` 定义为公网管理 API。需要远程使用时，推荐架构是：

```text
Internet
   ↓
VPN / Zero-Trust / 反向代理鉴权层
   ↓
可信内网
   ↓
BiliPDJ Server :9816
```

至少应具备：

- TLS；
- 用户身份认证；
- 来源限制；
- 对管理写接口额外授权；
- 日志中避免泄露 Cookie / Token。

在独立远程鉴权方案完成前，不建议把 `0.0.0.0:9816` 直接映射到公网。

## 13. 兼容性规则

- `core.server` 只是 `apps.server.server` 的兼容导入入口；
- 新客户端与新代码应使用 `apps.server` 和公开 HTTP / WebSocket 契约；
- `queue[]`、旧 `entry` 字段继续兼容，但新代码优先结构化 `entries`、`username`、`content`；
- 旧 `style.json` 保持为 OBS/队列展示样式，不会被新的界面主题系统废弃；
- 旧 Web `localStorage` 主题对象可自动迁移到 `appearance.json`；
- 新主题/样式以 Server 返回为准，Windows/Web 不再维护互不兼容的独立主题文件。

## 14. Windows / Web / OBS 通用主题与样式配置

### 14.1 `appearance.json`

Windows Tk 与 Web 控制台使用同一套 `appearance.json`。Server 是唯一持久化来源。

```json
{
  "schema": 1,
  "design": "aurora",
  "mode": "dark",
  "font_family": "Microsoft YaHei UI",
  "font_size": 10,
  "radius": 10,
  "dark": {
    "background": "#090E1A",
    "sidebar": "#0D1424",
    "surface": "#111A2C",
    "surface_alt": "#18233A",
    "input": "#0D1424",
    "border": "#26334D",
    "text": "#E6EDF7",
    "muted": "#8A9AB3",
    "accent": "#7C6CF2",
    "accent_hover": "#9184FF",
    "selection": "#7C6CF2",
    "success": "#32D583",
    "warning": "#F5B942",
    "danger": "#F97066"
  },
  "light": {
    "background": "#F4F6FB",
    "sidebar": "#EAEDF5",
    "surface": "#FFFFFF",
    "surface_alt": "#F0EFFF",
    "input": "#FBFBFE",
    "border": "#D5D9E7",
    "text": "#20263A",
    "muted": "#687089",
    "accent": "#6757D9",
    "accent_hover": "#5142BC",
    "selection": "#6757D9",
    "success": "#007A40",
    "warning": "#B57600",
    "danger": "#D92D20"
  }
}
```

读取：

```http
GET /api/appearance
```

保存：

```http
POST /api/appearance
Content-Type: application/json

{
  "appearance": { ... }
}
```

### 14.2 通用导入/导出文件

Windows 与 Web 的“导出通用配置”生成同一种 JSON：

```json
{
  "schema": 1,
  "kind": "bilipdj-appearance-profile",
  "appearance": { ... },
  "display_style": { ... }
}
```

- `appearance`：Windows/Web 控制台共同的界面主题；
- `display_style`：现有 `style.json`，用于 OBS/队列展示；
- Windows 导出后可以在 Web 直接导入；
- Web 导出后可以在 Windows 直接导入；
- 配置备份 ZIP/WebDAV 也包含 `appearance.json`。

导出：

```http
GET /api/appearance/profile
```

导入：

```http
POST /api/appearance/profile
Content-Type: application/json

{
  "schema": 1,
  "kind": "bilipdj-appearance-profile",
  "appearance": { ... },
  "display_style": { ... }
}
```

导入接口同时接受旧 `style.json`、直接的 `appearance.json` 和旧 Web 主题对象，便于升级旧版本。
