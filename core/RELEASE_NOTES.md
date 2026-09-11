# 弹幕排队姬 v3.0.0

v3.0.0 是 BiliPDJ 插件化与多平台弹幕核心的一次主版本升级。重点从“把不同平台伪装成 Bilibili 消息”迁移到统一 `DanmuEvent`，并完善 Python / JavaScript 插件运行时、权限边界、动态配置和发布安全。

## 插件系统

- 支持 `.bilipdj-plugin` 安装包的安装、卸载、启用和禁用；
- manifest 支持版本兼容、文件哈希、签名/完整性校验、权限声明和 capability；
- 同时支持 Python 与 JavaScript 插件；
- JavaScript 插件在独立进程 QuickJS runtime 中执行，保留 watchdog 和 IPC 边界；
- 新增 `config_schema`，插件可以声明动态配置项，Web 设置页自动生成安全配置界面；
- secret 配置不会通过读取 API 回显明文，空值保存可保留现有 secret；
- JavaScript `host.httpRequest()` 支持受限 GET / POST、请求体、状态码和安全响应头；
- Python / JavaScript 插件私有数据统一限制为单文件 4 MiB、每插件 64 MiB、最多 1024 个常规文件，并限制相对路径深度与长度。

## 平台无关 DanmuEvent

- 新增平台无关 `DanmuEvent` 作为 QueueManager 的标准输入；
- Bilibili 原始 `DANMU_MSG` 继续兼容，但只作为适配层；
- 新插件应使用 Python `context.process_danmu_event(...)` 或 JavaScript `host.processDanmuEvent(...)`；
- 原生字符串用户 ID 被完整保留，不再要求第三方平台伪造 Bilibili 数字 UID；
- 房管、主播、舰长/守护、粉丝牌和接收时间使用统一身份字段；
- `DANMU_EVENT` WebSocket 事件提供统一 `platform / message / identity / received_at` 结构。

## 平台 Relay 与兼容

- 抖音、虎牙、Twitch、YouTube 的内建 Relay 已改为直接产生 `DanmuEvent`，去除伪造 Bilibili JSON 的内部耦合；
- 抖音 `role >= 3` 按房管身份映射；
- Twitch 保留 moderator / broadcaster / badge 等原生身份元数据；
- 旧 `processDanmu` / `process_danmu_json` 仍保留为 Bilibili 兼容 API，已有插件不会被强制一次性迁移。

## 安全与稳定性

- 发布前补齐插件 API 权限、文件路径、symlink、mutation、HTTP Host、config schema、DanmuEvent 等专项回归；
- 插件私有数据增加总容量与文件数配额，避免失控插件持续创建文件耗尽磁盘；
- 更新检查顺序调整为 Release 附件 manifest → GitHub latest Release API → `now` 静态 manifest 最后兜底，避免旧静态清单抢占最新 Release；
- 普通 `now` push 和 Pull Request 只构建/校验，不再拥有创建 GitHub Release 的权限；
- GitHub Release 只能通过显式 workflow dispatch 或 `v*` tag 流程创建。

## Windows / Web Portable 验证

- Tk Windows 与 Web Portable 都会真实 PyInstaller 构建；
- 打包 CI 在生成 ZIP 前实际启动两个 frozen GUI EXE 的隐藏插件自检入口；
- 自检会在冻结环境中安装并启动 QuickJS 插件，通过 multiprocessing worker 发送 `DanmuEvent`；
- frozen EXE 返回非 0 时停止打包，不会生成可发布 ZIP；
- Windows GUI 子系统 EXE 使用 `Start-Process -Wait -PassThru` 获取真实退出码。

## 更新与校验

正式发行会同时提供：

- `BiliPDJ-v3.0.0-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.0-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.0-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.0-Web-Portable-x64.zip.sha256`
- `update-manifest.json`

`update-manifest.json` 由发布构建根据真实产物生成，包含包名、下载地址、文件大小和 SHA-256。客户端仍会先读取 manifest 并校验 SHA-256 后再安装。

## 升级建议

- 从 v2.x 升级时请完整解压新 portable 包；
- 原有队列、配置和兼容 API 继续保留；
- 新开发的第三方弹幕插件建议直接使用 `DanmuEvent` API；
- 高权限插件仍应仅安装可信来源，尤其是声明 `subprocess`、网络或文件写权限的插件。
