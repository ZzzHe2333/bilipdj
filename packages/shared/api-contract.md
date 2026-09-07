# BiliPDJ API contract

默认本地地址：`http://127.0.0.1:9816`。

主要接口：

- `GET /health`：健康检查
- `GET /api/runtime-status`：运行状态
- `GET /api/queue/state`：队列状态
- `GET /api/blacklist/state`：黑名单
- `GET /api/gifts/state`：礼物状态
- `POST /api/style`：保存展示样式
- `WS /ws`、`WS /danmu/sub`：实时事件广播

前端应把后端视作唯一状态源。新增前端时优先复用这些接口，不直接连接 Bilibili / 抖音，也不在前端重新实现排队算法。
