# BiliPDJ 用户数据存储与迁移

## 各操作系统的默认路径

| 系统 | 持久化用户数据目录 | 日志（不参加 Roaming 漫游） |
| --- | --- | --- |
| Windows | `%APPDATA%\\bilipdj`（通常为 `C:\\Users\\<用户>\\AppData\\Roaming\\bilipdj`） | `%LOCALAPPDATA%\\bilipdj\\log` |
| macOS | `~/Library/Application Support/bilipdj` | `~/Library/Logs/bilipdj` |
| Linux | `$XDG_DATA_HOME/bilipdj`，未设置绝对路径时为 `~/.local/share/bilipdj` | `$XDG_STATE_HOME/bilipdj/log`，否则 `~/.local/state/bilipdj/log` |

以上不是固定 C 盘路径。若 Windows 的用户配置文件位于其他盘，`%APPDATA%` 指向用户实际 Roaming 目录。

选择 Roaming 的原因是用户明确希望数据与 Windows 用户配置关联，且少量账户配置、存档可随 Windows 用户资料保留。不过 Roaming 可能参与企业域配置漫游，因此**持续增长的日志/临时缓存不应存放在 Roaming**。插件与排队存档按用户自有持久数据处理；极大型插件资源另建议通过外部目录参数指定。

## 数据布局

```text
bilipdj/
  core/
    config.yaml
    quanxian.yaml
    kaiguan.yaml
    blacklist.csv
    cd/
      queue_archive_state.json
      queue_archive_slot_1.csv
      ...
      pingtai_config_1.yaml
  style-win.json
  style-web.json
  appearance-win.json
  appearance-web.json
  plugins/
  key/
  backup/
  .storage-choice.json
```

Windows Tk 本地样式只能写入 `*-win.json`，Web 与未来统一的 bilipdj-go 浏览器前端使用 `*-web.json`，**不自动双向同步**。Web/OBS 当前生成的 `moren.css` 和排队样式槽位属于 Web 展示，不反向覆盖 Windows Tk 本地样式。不同客户端可以显式导出/导入主题配置。

## 首次迁移逻辑

1. **仅旧项目/便携目录存在用户数据：** 自动复制到系统用户目录，保留原始文件；之后以新目录为准。不会复制程序源码、打包文件、缓存或日志，也不通过修改时间比较覆盖。
2. **仅系统用户目录有用户数据：** 读取系统用户目录。不会将其同步回程序目录。
3. **两个目录都有数据：** 不自动合并/覆盖。Windows Tk 在初始化后端前弹出选择框；Web 控制台通过 `GET /api/storage/status` 显示选择窗口，`POST /api/storage/choice` 记录 `{"choice":"user"}` 或 `{"choice":"legacy"}`。选择后需要**重启**后端才能切换路径。在做选择之前 Web 会沿用项目目录，原始数据仍留在原位置。
4. **两个目录均无数据：** 首次启动在用户目录初始化新数据。

旧有 Windows Roaming **镜像复制策略已停止自动执行**，避免它以旧/新文件时间作决定并静默覆盖用户文件。旧 Roaming 镜像与项目数据同时存在时也会触发冲突选择，不会被删除。

## 显式自定义目录 / Docker

`BILIPDJ_DATA_DIR` 始终优先于默认用户目录，既有 Docker Compose `./data:/data` 和容器内 `BILIPDJ_DATA_DIR=/data` 完全保留原 `/data` 结构，**不会迁移到容器内 /root 或宿主机 Roaming**。需要 USB 便携部署时也可以将它设置为相对于程序目录的可写目录；此时由用户负责备份。不要将用户存档直接写入只读应用目录。

本机管理 API 不能直接暴露公网；如需跨主机操作，应使用已设置权限边界的可信局域网方式或安全代理。

## 安全注意事项

- 修改/升级前先备份项目目录和用户目录；迁移会**复制不删除**旧数据。
- 若两份数据内容有差别，应在选择之前分别备份并人工比对；本工具不做不可靠的自动合并。
- 选择数据来源是全局设置，不代表支持多进程同时读写两套配置目录。
- 对于部署在同一台机器的 BiliPDJ 与 bilipdj-go：共享同一目录时应协调锁与配置字段；本次仅定义 Web 样式命名，不宣称 Go 后端已经支持这一目录结构。
