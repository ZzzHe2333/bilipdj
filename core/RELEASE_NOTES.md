# 弹幕排队姬 v3.0.10-test

`v3.0.10-test` 是新的测试发行版，重点验证 Windows CustomTkinter 桌面端启动稳定性，以及全量/增量更新工作区和失败回滚安全性。该版本作为 GitHub Pre-release 发布，不替代当前稳定版 v3.0.6。

## 本次测试内容

- 修复 Windows 桌面端在部分环境下 GUI 闪一下后不可见、但后端仍继续运行的问题；
- CustomTkinter 主窗口启动不再使用全窗口透明度 `alpha=0 → 1` 切换，改为完成布局后正常显示并置前；
- GUI 启动异常会写入 `log/gui-startup-error.log`，冻结版启动自检加入明确超时和确定性退出；
- Windows 与 Web Portable 的全量更新、增量更新、恢复操作统一在 `<程序目录>/update/<会话>/` 中下载、解压、staging、patch 和 rollback，不再把增量更新工作区放到系统 C 盘临时目录；
- `update/` 被列为运行时保护目录，更新器不会覆盖自身正在使用的更新会话；
- Windows 增量更新以完整快照成功创建作为事务边界：快照完成前不会修改程序文件，失败时不会误删原文件；
- Windows 增量更新在文件替换、删除或新版本启动失败后，会从完整快照恢复；
- Web Portable 在停止旧程序后，如果快照/rollback 准备或安装阶段失败，会自动恢复并重新启动程序；
- `apps/update_workspace.py` 成为唯一权威的 update workspace 实现，避免创建与清理逻辑长期漂移；
- 新增 updater failure recovery CI，覆盖失败路径和重复启动保护。

## 建议重点验证

- 双击 `main.exe` 后 GUI 是否稳定显示，后端是否自动启动；
- 连续关闭、重启 Windows 客户端，确认不会出现 GUI 消失但后台残留的问题；
- 从旧版本执行全量更新时，程序目录中应出现 `update/`，下载和解压操作都位于该目录；
- 从支持的旧版本执行增量更新时，不应再在系统 C 盘临时目录创建主要更新工作区；
- 人为中断或制造更新失败后，旧版本是否能正常恢复并重新启动；
- Windows / Web 的完整包、增量包、SHA-256 和 update manifest 是否均可正常使用。

## 直接下载

普通测试用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.10-test-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.10-test-Web-Portable-x64.zip`

发布地址：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.10-test/BiliPDJ-v3.0.10-test-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.10-test/BiliPDJ-v3.0.10-test-Web-Portable-x64.zip

## 更新与数据安全

- `v3.0.10-test` 为测试通道 Release，不替代 v3.0.6 稳定版；
- 更新下载、解压、增量 patch 与 rollback 工作区统一位于程序目录 `update/`；
- 更新前继续创建 `backup/` 版本快照；
- Windows 增量更新只有在完整快照建立后才进入文件修改阶段；
- 更新失败会优先恢复原版本，Web Portable 停止后准备失败也会尝试重新启动旧程序；
- 配置、队列存档、日志、备份、插件、插件私有数据、主题与显示样式等用户数据继续受保护。

## 发行文件

- `BiliPDJ-v3.0.10-test-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.10-test-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.10-test-Windows-Tk-files.json`
- `BiliPDJ-v3.0.10-test-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.10-test-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.10-test-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.10-test-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.10-test-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.10-test-Web-files.json`
- `BiliPDJ-v3.0.10-test-Web-files.json.sha256`
- `BiliPDJ-v3.0.10-test-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.10-test-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 测试版说明

本版本按测试通道作为 GitHub **Pre-release** 发布。重点用于验证 GUI 启动稳定性、程序目录本地更新工作区，以及 Windows/Web 更新失败回滚安全性。
