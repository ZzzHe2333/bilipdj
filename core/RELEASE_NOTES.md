# 弹幕排队姬 v3.0.2

v3.0.2 汇总了 v3.0.0 之后尚未正式发布的稳定性修复，并正式加入 Windows 便携版按文件增量更新。插件 API 与 `DanmuEvent` 的兼容约定保持不变。

## 直接下载

普通用户只需要下载对应的完整便携版 ZIP，不需要手动下载 `files.json`、`.pack` 或它们的 `.sha256`。

- **Windows 客户端（推荐）**：下载 **`BiliPDJ-v3.0.2-Windows-Tk-Portable-x64.zip`** → [点击直接下载 Windows 客户端](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.2/BiliPDJ-v3.0.2-Windows-Tk-Portable-x64.zip)
- **Web 便携版**：下载 **`BiliPDJ-v3.0.2-Web-Portable-x64.zip`** → [点击直接下载 Web 便携版](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.2/BiliPDJ-v3.0.2-Web-Portable-x64.zip)

> Windows 用户请优先选择 `Windows-Tk-Portable-x64.zip`。`Windows-Tk-files.json` 与 `Windows-Tk-Incremental-x64.pack` 是内置更新器使用的逐文件索引和 Range 资源库，普通用户无需手动下载或解压。

## 更新器：全量更新与增量更新

Windows 更新页现在提供两个明确入口：

- **全量更新**：继续沿用原有流程，下载完整 Windows Portable ZIP，校验整包 SHA-256 后交给独立更新器完成备份、替换、启动检测和失败回滚；
- **增量更新**：读取正式 Release 的逐文件清单，扫描本地受管程序文件，只下载缺失或 SHA-256 不一致的程序资源，再由独立更新器进行文件级替换。

增量更新不会扫描或覆盖用户数据。配置、队列/运行存档、`log/` / `logs/`、`backup/`、`plugins/` 与插件私有数据、`core/cd/`、`update-result.json` 等均被排除。

## 逐文件清单与 Range 资源包

正式发布构建会为 Windows Tk Portable 额外生成：

- `BiliPDJ-v3.0.2-Windows-Tk-files.json`：记录受管文件的相对路径、原始大小、SHA-256、资源偏移、压缩片段大小、片段 SHA-256 与压缩方式；
- `BiliPDJ-v3.0.2-Windows-Tk-Incremental-x64.pack`：同一 Release 内的可随机读取程序资源库；
- 上述两个文件各自对应的 `.sha256`。

客户端会先比较本地文件大小与 SHA-256，只向 `.pack` 发出真正需要文件对应的 HTTP Range 请求。下载片段先校验 `packed_sha256`，解压后再校验最终文件大小和 SHA-256。若 GitHub/CDN/代理没有返回 HTTP 206 Range，客户端会明确提示改用全量更新，不会静默下载整个 `.pack`。

全量 ZIP、逐文件 manifest、增量 `.pack` 和 `update-manifest.json` 均位于同一个 `ZzzHe2333/bilipdj` GitHub Release，不需要第二个资源仓库。

## 更新前长期备份

- 真正覆盖更新前，在程序源目录维护 `backup/`，与 `log/` 平级；
- 每次更新生成独立快照，例如 `backup/update-YYYYMMDD-HHMMSS-to-v3.0.2/`；
- 备份明确排除 `backup/` 自身，避免形成递归备份；
- 全量更新继续使用源目录外的临时原子回滚目录；
- 增量更新使用文件级临时回滚区，同时仍创建完整 `backup/` 快照；
- 新版启动失败时恢复旧版本，并保留长期备份。

## GUI 更新路径与状态修复

- 统一 `updater.py` 与真实打包 GUI 路径 `updater_v2` 的核心更新事务，避免功能漂移；
- 同一个 `updater.exe` 同时承接全量与增量安装；
- 修复存在历史 `backup/` 时，预检失败可能被误记为 `rolled_back` 的问题；
- 预检失败、成功回滚、回滚失败分别记录为 `preflight_failed`、`rolled_back`、`rollback_failed`。

## Web 控制台稳定性

- 修复 Windows Chromium 下纵向滚动条出现/消失导致主内容区约 16–17 px 左右横移的问题；
- 根滚动容器固定预留 scrollbar gutter；
- 增加对应 CI 回归 guard。

## 发布与安全验证

- 全量 updater 与增量 updater 回归均纳入 Quality；
- 增量专项 guard 覆盖逐文件 manifest / `.pack` 生成、用户数据排除、成功替换、启动失败回滚、跨版本基线拒绝和 UI/工作流契约；
- Windows Tk Portable 与 Web Portable 均执行 PyInstaller 构建与 frozen 插件自检；
- Server、QuickJS、插件权限/安全、Web build、API 文档、发布权限策略继续验证；
- 普通 Pull Request 和 `now` push 仍不会创建 Release，只有显式发布流程可以发布。

## 发行文件

v3.0.2 正式发布包含：

- `BiliPDJ-v3.0.2-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.2-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.2-Windows-Tk-files.json`
- `BiliPDJ-v3.0.2-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.2-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.2-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.2-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.2-Web-Portable-x64.zip.sha256`
- `update-manifest.json`

GitHub 另外自动显示 `Source code (zip)` 与 `Source code (tar.gz)`，它们是源码快照，不是普通 Windows/Web 用户需要下载的便携程序包。

## 升级说明

- **v3.0.0 用户升级到 v3.0.2 时需要使用全量更新。** v3.0.0 客户端本身尚未包含新的“增量更新”入口和执行代码；
- 安装 v3.0.2 后，后续兼容版本可使用按文件增量更新；
- 增量更新只允许从 Release 声明的直接基线版本升级，跨版本时会提示改用全量更新；
- 插件格式、`DanmuEvent`、现有配置与队列数据保持兼容。
