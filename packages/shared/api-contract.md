# BiliPDJ API contract

默认本地地址：`http://127.0.0.1:9816`。

后端是唯一业务状态源。Windows Tk、Web 控制台、OBS 展示和第三方客户端都应通过 HTTP / WebSocket 读取和修改状态，不应在前端重新实现排队算法。

## 基础接口

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 健康检查 |
| GET | `/api/runtime-status` | 后端、弹幕流和 WebSocket 运行状态 |
| GET | `/api/queue/state` | 当前队列与结构化 entries |
| GET | `/api/blacklist/state` | 黑名单 |
| GET | `/api/gifts/state` | B站礼物规则运行状态 |
| GET | `/api/quanxian` | 权限配置 |
| GET | `/api/kaiguan` | 功能开关 |
| GET | `/api/style` | 当前展示样式 |
| POST | `/api/style` | 保存展示样式 |
| WS | `/ws`、`/danmu/sub` | 实时事件广播 |

## 手动新增排队

`POST /api/queue/insert`

推荐使用结构化参数：

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
- `entry`：旧客户端兼容字段，格式仍可使用 `用户名 内容`。

返回示例：

```json
{
  "status": "ok",
  "size": 2,
  "queue": ["用户A 牵丝霖", "用户B"],
  "entries": [
    {"id": "用户A", "content": "牵丝霖", "last_operation_at": "2026-09-08 17:00:00"},
    {"id": "用户B", "content": "", "last_operation_at": "2026-09-08 17:00:10"}
  ]
}
```

第三方客户端显示队列时应优先使用 `entries[].id` 作为用户名、`entries[].content` 作为内容，而不是自行拆分 `queue[]`。

其他队列管理接口：

- `POST /api/queue/delete`：`{"index": 1}`；
- `POST /api/queue/move`：`{"index": 1, "direction": "up"}`；
- `POST /api/queue/update`：`{"index": 1, "content": "用户A 新内容"}`；
- `POST /api/queue/clear`；
- `POST /api/queue/reload`；
- `POST /api/queue/switch`：`{"slot": 2}`。

## 激活平台 / 多平台弹幕流

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
- 多个平台的排队消息进入同一个后端队列；
- 同一平台多个直播间暂不支持；
- 虎牙、快手、斗鱼、微信视频号仍为预留配置，不能激活实际弹幕流。

保存后后端会重建或重连对应弹幕 Relay。

## 更新清单

发行版包含 `update-manifest.json`。客户端应**先读取清单，再下载安装包**，不要根据版本号猜文件名。

稳定入口：

```text
https://github.com/ZzzHe2333/bilipdj/releases/latest/download/update-manifest.json
```

示例：

```json
{
  "schema": 1,
  "version": "2.0.4",
  "tag_name": "v2.0.4",
  "packages": {
    "windows-tk-x64": {
      "filename": "BiliPDJ-v2.0.4-Windows-Tk-Portable-x64.zip",
      "url": "...",
      "sha256": "64位十六进制SHA256",
      "size": 12345678
    },
    "web-portable-x64": {
      "filename": "BiliPDJ-v2.0.4-Web-Portable-x64.zip",
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
3. 根据客户端类型选择 `packages` 下的条目；
4. 使用 `url` 下载 `filename`；
5. 计算下载文件 SHA-256；
6. 必须与清单中的 `sha256` 完全一致才允许安装。

Web 控制台的 `/api/control/update` 会把 Web 便携包的清单信息转换为 JSON 返回给页面。

## 安全边界

当前管理接口继续遵循后端现有的本机管理限制。第三方本地客户端建议连接 `127.0.0.1:9816`。如需真正的公网/远程管理，需要在独立的远程鉴权方案完成后再开放，不应直接把当前管理端口裸露到公网。
