# Danmuji Initial Model

该目录提供了基于 `Bilibili_Danmuji_流程与接口整理 (1).docx` 抽取的初始模型：

- `danmuji_initial_model.py`：可扩展的 Python 数据模型（dataclass + Enum）。
- `danmuji_initial_model.json`：由 Python 模型导出的初始 JSON 样例。

## 使用方式

### 1) 打印模型 JSON

```bash
python models/danmuji_initial_model.py
```

执行后会打印模型 JSON，可用于：

- 后续接口适配层生成。
- 事件路由/线程编排可视化。
- 规则聚合与发送限流配置化。

### 2) 启动 Python 后端（默认端口 9816）

```bash
python backend/server.py
```

服务启动后可访问：

- `GET /`：基础欢迎信息。
- `GET /health`：健康检查。
- `GET /model`：读取 `models/danmuji_initial_model.json` 并返回。

如需修改监听地址/端口，可使用环境变量：

```bash
DANMUJI_BACKEND_HOST=0.0.0.0 DANMUJI_BACKEND_PORT=9816 python backend/server.py
```

## 新增配置与日志说明

- 根目录新增 `config.yaml`：集中管理服务端口、API 注入信息、日志级别、日志保留天数、排队存档槽位等配置。
- `config.yaml -> ui.startup_splash_seconds` 可配置 Web 控制台启动提示层展示时长（默认 5 秒），并显示免费软件退款提示。
- 运行日志统一写入 `log/` 目录，服务每次启动时会自动清理 **15 天前** 的旧日志（可在 `config.yaml -> logging.retention_days` 调整）。
- 默认日志等级是 `INFO`（可在 `config.yaml -> logging.level` 修改）。
- 新增 `POST /api/queue/log`：
  - 前端每次处理排队消息都会调用该接口记录一次快照。
  - 排队快照按“游戏存档”思路以 CSV 写入 `log/queue_archive_slot_1.csv` ~ `log/queue_archive_slot_3.csv` 三个槽位循环覆盖。


### 3) 启动桌面 GUI 控制台

```bash
python gui/control_panel.py
```

GUI 支持：

- 启动 GUI 后自动拉起后端服务（也可手动启动/停止后端）。
- 修改并保存 `config.yaml` 中的核心配置（host/port/roomid/uid/cookie/日志/存档槽位）。
- 一键打开 Web 控制页（由后端统一托管前端静态资源）。

## 前后端合并与自动反代说明

- 后端会同时提供 API、WebSocket 和前端静态页面：
  - 页面入口：`/`（自动跳转到 `/index.html`）
  - 配置接口：`GET /api/config`、`POST /api/config`
  - 队列日志接口：`POST /api/queue/log`
  - WebSocket：`/ws`（兼容别名 `/danmu/sub`）
- 因此前端始终走同源地址，不需要手动再配额外反向代理。
