from pathlib import Path

panel = Path('core/control_panel.py')
source = panel.read_text(encoding='utf-8')
old = 'APP_VERSION = "1.0.6"'
new = 'APP_VERSION = "1.0.7"'
if source.count(old) != 1:
    raise SystemExit(f'expected exactly one {old!r}, found {source.count(old)}')
panel.write_text(source.replace(old, new, 1), encoding='utf-8')

changelog = Path('UPDATE.md')
existing = changelog.read_text(encoding='utf-8')
section = '''# 更新日志

---

## v1.0.7（2026-07-18）

### 启动体验与响应性
- 点击“启动服务”后立即显示动态按钮文字、阶段提示和不确定进度条，避免用户误以为程序没有响应。
- 启动动作延迟到 Tkinter 完成首次界面重绘后执行，窗口在保存配置和创建后端进程期间仍能正常刷新。
- 启动期间禁用重复启动和停止操作，并显示等待光标；成功、失败或取消后统一恢复界面状态。

### 启动速度优化
- 启动前保存配置时不再访问尚未运行的后端接口，功能开关改为直接写入本地配置。
- 队列存档切换延后到后端健康检查通过后执行，移除启动前无意义的连接失败与超时等待。
- 正常运行期间手动保存配置仍保留后端即时生效逻辑，不影响原有使用方式。

### 状态可信度与错误处理
- 创建后端进程后，在后台线程轮询本地接口，只有服务真正响应才显示“后端已启动”。
- 后端提前退出时报告退出码；等待超过约 15 秒时报告超时和最后一次连接错误。
- 启动失败会清理残留进程、恢复按钮和进度条，并同时写入日志和弹出明确错误提示。

### 测试与发布
- 新增 5 项启动流程回归测试，覆盖动画、界面重绘、后台就绪检测、本地配置保存和 Python 语法解析。
- 连同原有服务器监听配置测试共 10 项，发布前全部通过。
- Windows x64 Release 改为使用 `RELEASE_NOTES.md` 作为详细发布正文，并将说明文件一并打入 ZIP。

'''
if not existing.startswith('# 更新日志'):
    raise SystemExit('UPDATE.md header is unexpected')
rest = existing[len('# 更新日志'):].lstrip('\n')
changelog.write_text(section + rest, encoding='utf-8')
