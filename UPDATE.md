# 更新日志

---

## 2026-04-08 09:40 UTC+08:00

### Debug 弹幕日志 + 存档槽位切换联动

**backend/server.py**
- `_log_business_message`：DEBUG 日志由元数据摘要改为输出原始 `uname` + `msg` 内容
- `QueueManager.process_danmu_json`：在处理前新增 DEBUG 日志，记录原始弹幕用户名和消息
- `QueueArchiveManager.read_snapshot_by_slot(slot)`：新增公开方法，按指定槽位号读取存档
- `QueueManager.switch_to_slot(slot)`：新增方法，从指定槽位加载队列到内存并广播 `QUEUE_UPDATE`
- 新增 `POST /api/queue/switch` 接口（`{"slot": N}`），供 GUI 触发存档切换

**gui/control_panel.py**
- `save_to_file()` 保存成功后自动调用 `_switch_queue_slot()`
- 新增 `_switch_queue_slot()`：向 `/api/queue/switch` 发请求，切换后端内存队列并刷新前端显示；后端未运行时静默提示

---

## 2026-04-08 09:07 UTC+08:00

### 排队逻辑后端化 + GUI 多标签页改版

**backend/server.py**
- 新增 `QueueManager` 类，将排队业务逻辑完整迁移至 Python 后端
- 维护线程安全的纯文本队列 `_persons`，支持从 CSV 存档恢复并自动剥离旧 HTML 格式
- `DANMU_MSG` 弹幕不再直接广播，改为路由进 `QueueManager` 处理队列命令
- 队列变化时自动广播 `{"type": "QUEUE_UPDATE", "queue": [...]}` 并写入 CSV 存档
- 新 WebSocket 客户端连接后立即推送当前队列快照
- `POST /api/config` 保存后同步刷新 `QueueManager` 的管理员/黑名单配置
- 新增 `GET /api/queue/state` 接口，返回当前内存队列（供调试使用）
- 新增 `import re` 模块级导入

**toGUI/myjs.js**
- 大幅精简（1334 行 → 120 行），移除全部排队业务逻辑
- 仅保留 WebSocket 连接、接收 `QUEUE_UPDATE` 渲染队列、接收 `PDJ_STATUS` 状态事件
- 从 `/api/config` 加载 `roomid` / `uid` 用于状态显示

**gui/control_panel.py**
- GUI 改为 `ttk.Notebook` 多标签页布局
  - **日志**：直播间连接状态指示 + 实时后端日志
  - **设置**：所有配置字段及保存/刷新按钮
  - **关于**：版本号、工具说明、免费软件声明
- 启动/停止后端和打开 Web 界面按钮固定在顶部，切换标签始终可见
