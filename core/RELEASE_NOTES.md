# 弹幕排队姬 v3.0.1

v3.0.1 在 v3.0.0 基础上完成 Web 控制台稳定性、更新器备份/回滚修复，并新增 Windows 便携版的按文件增量更新。插件 API 与 `DanmuEvent` 的既有兼容约定保持不变。

## 全量更新与增量更新

Windows 更新页现在提供两个明确入口：

- **全量更新**：沿用原有逻辑，下载完整 Windows Portable ZIP，校验整包 SHA-256 后交给独立更新器替换程序目录；
- **增量更新**：先读取正式 Release 的逐文件清单，扫描本地程序资源的大小和 SHA-256，只下载本地缺失或内容变化的程序文件，再由独立更新器原子替换。

增量更新不会扫描后覆盖用户数据。以下内容明确排除在增量资源管理之外：配置文件、`core/cd/` 队列/运行数据、`log/`/`logs/`、`backup/`、`plugins/` 与插件私有数据、`update-result.json` 等运行状态文件。

为避免跨多个版本时遗留已经从程序包删除的旧文件，增量更新要求本地 `VERSION` 与该 Release 声明的增量基线版本一致；不一致时会明确提示改用全量更新。

## 逐文件发布清单与增量资源包

正式发布构建除完整 ZIP 总 SHA-256 外，还会为 Windows Tk Portable 生成：

- `BiliPDJ-v3.0.1-Windows-Tk-files.json`：完整包内部受管文件结构，每个文件记录相对路径、原始大小、SHA-256、增量资源偏移、压缩片段大小和片段 SHA-256；
- `BiliPDJ-v3.0.1-Windows-Tk-Incremental-x64.pack`：同一 Release 内的可随机读取完整程序资源库；每个文件单独压缩/存储并按清单偏移拼接；
- 对上述两个资产也分别生成 `.sha256`。

客户端扫描本地后，只向 `.pack` 发出所需文件对应的 HTTP Range 请求。每个下载片段先校验 `packed_sha256`，解压后再校验目标文件的大小和 SHA-256，最后才进入安装阶段。若 GitHub/CDN/代理没有返回 HTTP 206 Range 响应，客户端不会偷偷下载整个资源包，而是提示用户改用全量更新。

当前实现的全量 ZIP、逐文件 manifest、增量 `.pack` 和总 `update-manifest.json` 均放在 **同一个 `ZzzHe2333/bilipdj` GitHub Release** 中，不需要第二个资源仓库。

## Web 控制台稳定性

- 修复 Windows Chromium 下页面内容高度变化时，纵向滚动条出现/消失导致右侧主内容区约 16–17 px 左右横移的问题；
- 根滚动容器固定预留纵向 scrollbar gutter，页面切换和内容刷新时不再因可用宽度变化产生横向 layout shift；
- 新增 Web scrollbar 回归 guard，避免后续误删稳定 gutter 规则。

## 内置更新器与 backup 备份仓库

- 真正执行覆盖更新前，在程序源目录创建并维护 `backup/`，与 `log/` 平级；
- 每次更新生成独立快照，例如 `backup/update-YYYYMMDD-HHMMSS-to-v3.0.1/`，不会覆盖历史备份；
- 快照复制旧版本文件并明确排除 `backup/` 自身，避免形成 `backup/backup/...` 递归备份；
- 配置、日志和既有备份仓库在更新后继续保留；
- 全量更新的源目录外 `.<app>.update-backup` 仍只作为更新过程中的临时原子回滚目录；
- 增量更新使用文件级临时回滚区，同时仍创建 `backup/` 长期完整快照；
- 新版启动失败时会恢复被修改的旧文件并重新启动原版本。

## GUI 更新路径修复

发布前审计发现，打包后的 `updater.exe` 实际通过 `updater_gui.py -> updater_v2.perform_update()` 执行更新，而最初的 `backup/` 逻辑只加入了 `updater.py`。这会导致真实 GUI 自动更新路径没有使用新的长期备份仓库。

v3.0.1 已将完整更新事务收敛到共享实现，并让同一个 `updater.exe` 同时承接全量/增量安装：

- `updater.py` 与 `updater_v2.py` 不再各维护一套容易漂移的全量更新流程；
- `updater_v2` 保留 Windows 文件占用场景下的目录移动重试和短暂 settle delay，并根据已准备的增量计划路由到文件级安装事务；
- GUI 更新器和兼容入口统一使用同一套快照、启动检测、回滚与结果记录约定。

## 更新状态记录修复

- 修复程序已有历史 `backup/` 时，如果新更新包在预检阶段失败，`update-result.json` 可能误报 `rolled_back` 的问题；
- 未创建本次快照时，`backup_dir` 现在为空，不再错误指向整个 `backup/` 仓库；
- 只有实际完成回滚时才记录 `rolled_back`；
- 回滚本身失败时单独记录 `rollback_failed`；
- 更新包缺文件、解压/预检失败且程序文件尚未修改时记录 `preflight_failed`。

## 发布前验证

- updater 回归测试覆盖全量 updater 与增量 updater；
- 增量专项 guard 覆盖逐文件清单/资源包生成、用户数据排除、成功替换、启动失败回滚、跨版本基线拒绝和配置/存档/插件私有数据不变；
- Windows Tk Portable 与 Web Portable 继续进行 PyInstaller 构建和 frozen 插件自检；
- Web build、API 文档、协议 guard、更新器 guard 与 Release 权限策略检查继续执行；
- 普通 Pull Request 和 `now` push 的 GitHub Release job 保持 skipped，只有显式发布流程可以创建 Release。

## 计划发行文件

正式发布 v3.0.1 时，构建流程将生成：

- `BiliPDJ-v3.0.1-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.1-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.1-Windows-Tk-files.json`
- `BiliPDJ-v3.0.1-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.1-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.1-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.1-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.1-Web-Portable-x64.zip.sha256`
- `update-manifest.json`

`update-manifest.json` 的文件大小和 SHA-256 必须由正式发布构建根据真实产物生成。在 v3.0.1 尚未正式发布前，仓库根目录仍保留当前真实 v3.0.0 Release 的 manifest，不提前伪造 v3.0.1 哈希。

## 升级说明

- v3.0.0 用户可在 v3.0.1 正式发布后选择“增量更新”；也可随时选择原逻辑的“全量更新”；
- 增量更新开始安装前同样会把旧版本写入源目录下的 `backup/`，建议在确认新版本稳定运行前保留该目录；
- 插件格式、`DanmuEvent`、配置文件和现有队列数据保持兼容。
