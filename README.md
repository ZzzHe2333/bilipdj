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
