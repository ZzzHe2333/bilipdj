# Danmuji Initial Model

该目录提供了基于 `Bilibili_Danmuji_流程与接口整理 (1).docx` 抽取的初始模型：

- `danmuji_initial_model.py`：可扩展的 Python 数据模型（dataclass + Enum）。
- `danmuji_initial_model.json`：由 Python 模型导出的初始 JSON 样例。

## 使用方式

```bash
python models/danmuji_initial_model.py
```

执行后会打印模型 JSON，可用于：

- 后续接口适配层生成。
- 事件路由/线程编排可视化。
- 规则聚合与发送限流配置化。
