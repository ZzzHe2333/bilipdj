# 弹幕排队姬 v3.0.1

v3.0.1 是 v3.0.0 之后的稳定性修复版本，主要处理 Web 控制台横向抖动和内置更新器备份/回滚链路，不改变插件 API 与 `DanmuEvent` 的既有兼容约定。

## Web 控制台稳定性

- 修复 Windows Chromium 下页面内容高度变化时，纵向滚动条出现/消失导致右侧主内容区约 16–17 px 左右横移的问题；
- 根滚动容器固定预留纵向 scrollbar gutter，页面切换和内容刷新时不再因可用宽度变化产生横向 layout shift；
- 新增 Web scrollbar 回归 guard，避免后续误删稳定 gutter 规则。

## 内置更新器与 backup 备份仓库

- 真正执行覆盖更新前，在程序源目录创建并维护 `backup/`，与 `log/` 平级；
- 每次更新生成独立快照，例如 `backup/update-YYYYMMDD-HHMMSS-to-v3.0.1/`，不会覆盖历史备份；
- 快照复制旧版本文件并明确排除 `backup/` 自身，避免形成 `backup/backup/...` 递归备份；
- 配置、日志和既有备份仓库在更新后继续保留；
- 源目录外的 `.<app>.update-backup` 只作为更新过程中的临时原子回滚目录，新版确认启动成功后会清理；
- 新版启动失败时仍会恢复旧版本，同时保留本次 `backup/` 长期快照。

## GUI 更新路径修复

发布前审计发现，打包后的 `updater.exe` 实际通过 `updater_gui.py -> updater_v2.perform_update()` 执行更新，而最初的 `backup/` 逻辑只加入了 `updater.py`。这会导致真实 GUI 自动更新路径没有使用新的长期备份仓库。

v3.0.1 已将完整更新事务收敛到共享实现：

- `updater.py` 与 `updater_v2.py` 不再各维护一套容易漂移的更新流程；
- `updater_v2` 只保留 Windows 文件占用场景下的目录移动重试和短暂 settle delay；
- GUI 更新器和兼容入口统一使用同一套快照、替换、启动检测、回滚与结果记录逻辑。

## 更新状态记录修复

- 修复程序已有历史 `backup/` 时，如果新更新包在预检阶段失败，`update-result.json` 可能误报 `rolled_back` 的问题；
- 未创建本次快照时，`backup_dir` 现在为空，不再错误指向整个 `backup/` 仓库；
- 只有实际完成回滚时才记录 `rolled_back`；
- 回滚本身失败时单独记录 `rollback_failed`；
- 更新包缺文件、解压/预检失败且程序目录从未被替换时记录 `preflight_failed`。

## 发布前验证

- updater 回归测试同时覆盖 legacy 与真实 GUI (`updater_v2`) 两条路径；
- 覆盖成功更新、启动失败回滚、已有历史 backup 时预检失败三类场景；
- Windows Tk Portable 与 Web Portable 均重新进行 PyInstaller 构建和 frozen 插件自检；
- Web build、API 文档、协议 guard、更新器 guard 与 Release 权限策略检查通过；
- 普通 Pull Request 和 `now` push 的 GitHub Release job 继续保持 skipped，只有显式发布流程可以创建 Release。

## 计划发行文件

正式发布 v3.0.1 时，构建流程将生成：

- `BiliPDJ-v3.0.1-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.1-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.1-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.1-Web-Portable-x64.zip.sha256`
- `update-manifest.json`

`update-manifest.json` 的文件大小和 SHA-256 必须由正式发布构建根据真实产物生成。在 v3.0.1 尚未正式发布前，仓库根目录仍保留当前真实 v3.0.0 Release 的 manifest，不提前伪造 v3.0.1 哈希。

## 升级说明

- v3.0.0 用户可在 v3.0.1 正式发布后直接使用内置更新器升级；
- 升级时旧版本会先写入源目录下的 `backup/`，建议在确认新版本稳定运行前保留该目录；
- 插件格式、`DanmuEvent`、配置文件和现有队列数据保持兼容。
