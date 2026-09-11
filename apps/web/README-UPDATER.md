# Web Portable 自动更新

Web Portable 从本版本起携带独立的 `BiliPDJ-Web-Updater.exe`。

更新流程：

1. Web 控制台读取 `update-manifest.json` 和本地 `backup/` 快照。
2. 用户选择全量更新、增量更新或本地备份恢复。
3. 主后端复制独立更新器到临时目录，并启动仅监听 `127.0.0.1` 的临时更新服务。
4. 浏览器打开独立 `update.html`。页面包含俄罗斯方块小游戏，并通过带随机 token 的状态接口轮询进度。
5. 更新器完成下载/校验后停止 Web launcher 和 backend，创建更新前快照，再替换程序文件。
6. 全量模式校验完整 ZIP SHA-256；增量模式校验逐文件清单并通过 HTTP Range 只下载 SHA-256 不一致的文件片段。
7. 更新失败时自动回滚，成功后重新启动 `BiliPDJ-Web.exe`。

运行数据（配置、队列档案、插件、日志、备份历史和 key 元数据）不属于普通更新管理文件，更新和恢复时会保留当前数据。

GitHub Release 仍只会在显式 `workflow_dispatch` 或 `v*` 标签触发时创建；普通 push / PR 只构建和验证发行包，不自动发布。
