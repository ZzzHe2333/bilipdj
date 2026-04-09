# 更新日志

---

## 2026-04-09 UTC+08:00

### 透明弹窗 OBS 兼容 + GUI 操控 + 打包修复 + 配置页简化

**core/overlay_host.py**
- 移除右键关闭（`<Button-3>`）和双击切换置顶（`<Double-Button-1>`）的快捷绑定，改由主 GUI 统一管控
- Windows 下不再使用 `overrideredirect(True)`，改为保留普通 HWND 并通过 Win32 API 移除 `WS_CAPTION | WS_SYSMENU | WS_MINIMIZEBOX | WS_MAXIMIZEBOX | WS_THICKFRAME`，使窗口在 OBS 「窗口捕获」按标题 "排队透明弹窗" 可正常被发现和捕获
- 启动时立即调用 `withdraw()` 隐藏窗口，Win32 样式调整完成后再 `deiconify()`，避免装饰帧闪烁
- 新增 CLI 参数 `--no-topmost`，供主控制台在重启时传入置顶状态

**core/control_panel.py**
- 透明窗口设置区新增控制按钮行：**关闭弹窗 / 置顶 / 取消置顶**
- 新增 `_set_overlay_topmost(topmost)` 方法：更新置顶状态，若弹窗正在运行则自动重启并传入 `--no-topmost` 标志
- `_build_overlay_command()` 在 `self._overlay_topmost == False` 时追加 `--no-topmost` 参数
- 设置页 Cookie 字段右侧新增 **获取** 按钮，点击调用 `open_config()` 直接打开浏览器配置页
- 提示文案更新为 OBS 窗口捕获操作引导

**core/ui/config.html**
- 去除「权限用户」和「扫码回调」两个 section，配置页仅保留 UID、Cookie 字段及扫码获取入口
- 标题改为「弹幕排队姬 - 登录」
- 移除 `readLocalCookie`、`formatUserList`、`parseUserList` 等无关方法
- `saveConfig` 保持回传 `roomid` 和 `callback`（不展示但防止覆盖已有配置）

**bilipdj_onedir.spec**
- 修复高危 bug：`name` 字段恢复为 ASCII `"bilipdj"`，解决 CI 路径失配（`dist\bilipdj\bilipdj.exe`）和非 ASCII 产物名乱码问题

**paiduijitm.spec**
- 从 onedir（含 COLLECT，产物为目录）改为 **onefile**（直接打进 EXE），产物为单文件 `dist\paiduijitm.exe`，与主程序同级放置时可被 `APP_DIR / "paiduijitm.exe"` 直接找到

**.github/workflows/package-windows-x64.yml**
- 新增 `pyinstaller --noconfirm --clean paiduijitm.spec` 构建步骤
- 构建完成后将 `dist\paiduijitm.exe` 复制至 `dist\bilipdj\`，随主包一同发布，确保线上包透明弹窗功能完整

---

## 2026-04-08 （二次修订）UTC+08:00

### 性能优化、打包修复、体验改善

**core/control_panel.py**
- 修复：后端未启动时拖动窗口卡顿问题——`_auto_refresh_queue` / `refresh_runtime_status` 增加 `_backend_is_running()` 守卫，后端未运行时不发送任何 HTTP 请求
- 修复：切换日志级别为 INFO 时 DEBUG 日志仍显示的 bug——新增 `_LEVEL_ORDER` 字典和 `_LOG_LEVEL_RE` 正则，在 `_enqueue_log` 中过滤低级别日志
- 修复：打包后 GUI 日志栏中文乱码——日志 `Text` 控件改用 `font=("Microsoft YaHei UI", 9)`（Windows 专用系统中文字体）
- 修复：启动 GUI 窗口时有隐藏窗口闪烁——`main()` 改为先 `withdraw()` + `wm_attributes("-alpha", 0)` 隐藏，布局完成后再 `deiconify()` + `alpha=1` 显示，避免打断输入法
- 修复：启动子进程（后端）时出现命令行窗口闪烁——`subprocess.Popen` 增加 `creationflags=CREATE_NO_WINDOW`（Windows）
- 新增：`性能` 标签页，显示 CPU 使用率、本进程内存（RSS）、系统内存占用、磁盘占用、GPU 占用（nvidia-smi / GPUtil 双路回退）
- 新增：顶部按钮栏增加 `配置页` 按钮，打开 `/config` 页面
- 修改：`open_config` / `open_web` 不再弹出对话框，改为在日志栏输出红字免费提醒 + 侵权法律后果说明
- 修改：`关于` 标签页补充著作权侵权法律条款（民事赔偿 + 刑事追责，著作权法第五十三条，最高三年有期徒刑）
- 修改：默认直播间号改为 `3049445`

**core/server.py**
- 将 `/api/runtime-status`、`/api/queue/state`、`/api/queue/switch`、`/api/queue/log`、`/favicon.ico`、`/.well-known/...` 的访问日志降至 DEBUG 级别，减少 INFO 日志噪声
- 修复：切换存档槽位后重启丢失选择的 bug——`save_config` 将 `slots`（固定=5）与 `active_slot`（用户选择 1~5）分开写入，`run_server` 先创建 `QueueArchiveManager(slots=5)` 再调用 `set_active_slot(cfg_active_slot)`，彻底解耦
- `ensure_runtime_layout()`：首次启动自动生成 `quanxian.yaml` / `kaiguan.yaml` / 5 个存档 CSV（若缺失）
- 默认超级管理员改为 `一纸轻予梦`
- YAML 配置路径统一：打包 exe 时使用 `APP_DIR`（exe 同级目录），开发模式使用 `CORE_DIR`（`core/` 目录）

**bilipdj_onedir.spec**
- `hiddenimports` 新增 `qrcode.*`（main/constants/util/image.base/image.pil/image.pure）、`PIL.*`（Image/PngImagePlugin）、`brotli`、`psutil`——修复打包后扫码获取 Cookie 功能失效的问题

**配置文件迁移**
- `config.yaml` / `quanxian.yaml` / `kaiguan.yaml` 从项目根目录移入 `core/` 目录，与 `control_panel.py` 同级

**.github/workflows/package-windows-x64.yml**
- "Debug Python env" 步骤从 `shell: cmd` 改为 `shell: pwsh`，`findstr` 改为 `Select-String`，修复 findstr 无匹配返回 exit code 1 导致 CI 失败的问题
- "Install build dependencies"：`pip uninstall` 失败时使用 `2>$null; Write-Host` 替代 `||`，兼容 PowerShell 语义
- "Create release zip"：打包路径改为 `core\quanxian.yaml` / `core\kaiguan.yaml`（适配迁移后路径）
- `pip install` 新增 `psutil`

---

## 2026-04-08 UTC+08:00

### 修复存档槽位切换 + 权限系统 + 功能开关

**core/backend/server.py**
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

**core/gui/control_panel.py**
- 新增 `权限` 标签页（`_build_quanxian_tab()`）：4 个 Text 区域分别对应 super_admin / admin / jianzhang / member，每行一个用户名，支持在线保存/刷新；后端未运行时写本地文件
- 新增 `开关` 标签页（`_build_kaiguan_tab()`）：9 个 Checkbutton 对应各排队命令开关，支持在线保存/刷新；后端未运行时写本地文件

**quanxian.yaml**（新建）
- 权限配置默认文件，包含 super_admin / admin / jianzhang / member 四个级别

**kaiguan.yaml**（新建）
- 功能开关默认文件，控制各排队命令是否启用

---

## 2026-04-08 09:40 UTC+08:00

### Debug 弹幕日志 + 存档槽位切换联动

**core/backend/server.py**
- `_log_business_message`：DEBUG 日志由元数据摘要改为输出原始 `uname` + `msg` 内容
- `QueueManager.process_danmu_json`：在处理前新增 DEBUG 日志，记录原始弹幕用户名和消息
- `QueueArchiveManager.read_snapshot_by_slot(slot)`：新增公开方法，按指定槽位号读取存档
- `QueueManager.switch_to_slot(slot)`：新增方法，从指定槽位加载队列到内存并广播 `QUEUE_UPDATE`
- 新增 `POST /api/queue/switch` 接口（`{"slot": N}`），供 GUI 触发存档切换

**core/gui/control_panel.py**
- `save_to_file()` 保存成功后自动调用 `_switch_queue_slot()`
- 新增 `_switch_queue_slot()`：向 `/api/queue/switch` 发请求，切换后端内存队列并刷新前端显示；后端未运行时静默提示

---

## 2026-04-08 09:07 UTC+08:00

### 排队逻辑后端化 + GUI 多标签页改版

**core/backend/server.py**
- 新增 `QueueManager` 类，将排队业务逻辑完整迁移至 Python 后端
- 维护线程安全的纯文本队列 `_persons`，支持从 CSV 存档恢复并自动剥离旧 HTML 格式
- `DANMU_MSG` 弹幕不再直接广播，改为路由进 `QueueManager` 处理队列命令
- 队列变化时自动广播 `{"type": "QUEUE_UPDATE", "queue": [...]}` 并写入 CSV 存档
- 新 WebSocket 客户端连接后立即推送当前队列快照
- `POST /api/config` 保存后同步刷新 `QueueManager` 的管理员/黑名单配置
- 新增 `GET /api/queue/state` 接口，返回当前内存队列（供调试使用）
- 新增 `import re` 模块级导入

**core/ui/myjs.js**
- 大幅精简（1334 行 → 120 行），移除全部排队业务逻辑
- 仅保留 WebSocket 连接、接收 `QUEUE_UPDATE` 渲染队列、接收 `PDJ_STATUS` 状态事件
- 从 `/api/config` 加载 `roomid` / `uid` 用于状态显示

**core/gui/control_panel.py**
- GUI 改为 `ttk.Notebook` 多标签页布局
  - **日志**：直播间连接状态指示 + 实时后端日志
  - **设置**：所有配置字段及保存/刷新按钮
  - **关于**：版本号、工具说明、免费软件声明
- 启动/停止后端和打开 Web 界面按钮固定在顶部，切换标签始终可见
