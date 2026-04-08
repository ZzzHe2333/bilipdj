# 更新日志

---

## 2026-04-08 UTC+08:00

### 修复存档槽位切换 + 权限系统 + 功能开关

**backend/server.py**
- `QueueArchiveManager`：新增 `get_active_slot()` / `set_active_slot(slot)` 方法，以 `active_slot` 字段替代原有 `next_slot` 轮转写入，确保存档始终写入用户选择的槽位
- `QueueArchiveManager.write_snapshot()`：不再轮转，改为写入当前 `active_slot`
- `QueueManager.restore_from_archive()`：启动时从 `active_slot` 恢复，而非按时间戳取最新（**修复**重启后存档不生效的 bug）
- `QueueManager.switch_to_slot()`：切换前先调用 `set_active_slot()` 持久化槽位选择
- 新增常量 `QUANXIAN_PATH` / `KAIGUAN_PATH`
- 新增 `DEFAULT_QUANXIAN` / `DEFAULT_KAIGUAN` 默认值字典
- 新增 `load_quanxian()` / `save_quanxian()` / `load_kaiguan()` / `save_kaiguan()` 函数
- `QueueManager`：新增 `_super_admins` / `_kaiguan` 字段，新增 `load_quanxian()` / `load_kaiguan()` / `_has_super_admin()` 方法
- `QueueManager._has_op_permission()`：super_admin 拥有最高权限
- `QueueManager._process()`：`添加管理员` / `取消管理员` 命令改为仅 super_admin 可用；各排队命令增加 kaiguan 开关判断；舰长插队改为受 kaiguan `jianzhang_chadui` 控制
- 新增 `GET /api/quanxian` / `POST /api/quanxian` 接口
- 新增 `GET /api/kaiguan` / `POST /api/kaiguan` 接口
- `run_server()`：启动时加载并注入 quanxian 和 kaiguan 配置

**gui/control_panel.py**
- 新增 `权限` 标签页（`_build_quanxian_tab()`）：4 个 Text 区域分别对应 super_admin / admin / jianzhang / member，每行一个用户名，支持在线保存/刷新；后端未运行时写本地文件
- 新增 `开关` 标签页（`_build_kaiguan_tab()`）：9 个 Checkbutton 对应各排队命令开关，支持在线保存/刷新；后端未运行时写本地文件

**quanxian.yaml**（新建）
- 权限配置默认文件，包含 super_admin / admin / jianzhang / member 四个级别

**kaiguan.yaml**（新建）
- 功能开关默认文件，控制各排队命令是否启用

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
