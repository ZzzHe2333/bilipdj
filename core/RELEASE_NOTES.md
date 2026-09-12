# 弹幕排队姬 v3.0.7-test

`v3.0.7-test` 是基于 v3.0.6 的测试发行版，主要用于验证 Windows 控制台左侧导航横向抖动修复，以及 v3.0.6 之后加入的 Docker / Compose 基础设施与现有 Windows / Web Portable 更新链路。该版本作为 GitHub Pre-release 发布，不替代当前稳定版 v3.0.6。

## 本次测试内容

- 修复 Windows 控制台页面切换时的横向抖动：左侧导航完成最终文字和主题布局后只测量一次自然宽度，并关闭 `pack` 几何传播；
- 同时固定主内容 `shell` 第 0 列的宽度边界，选中项在 normal / bold 字重之间切换时不再推动右侧内容区重新布局；
- 继续保持 Windows 主窗口 `1180×720` 固定尺寸，更新页、设置页等滚动页面维持现有 Canvas 内部滚动逻辑；
- 新增 Docker Compose 基础设施、`BILIPDJ_DATA_DIR` 持久化目录、`/health` 健康检查以及容器环境下安全的 local-only API 访问适配；
- 新增 Docker 构建上下文隐私保护，使用 `.dockerignore` 和自动化 guard 排除配置、运行数据、日志、备份、插件私有数据与本地缓存，避免被复制进镜像；
- Windows Tk / Web Portable 继续执行 frozen JavaScript 插件运行时自检，并生成完整 ZIP、逐文件 manifest、增量 `.pack` 与 SHA-256 校验文件；
- 正式版 / 测试版双通道版本选择、本地备份恢复、失败回滚等更新机制保持不变。

## 建议重点验证

- 在 Windows 客户端连续切换“日志 → 当前排队 → 设置 → 更新软件 → 关于项目”等页面，观察左侧导航与右侧内容分栏是否保持稳定；
- 重点观察“更新软件”页面，确认左栏不再先向右扩张、右侧 Canvas 不再随后发生第二次横向重排；
- 如使用 Docker，可验证 `docker compose up -d --build` 后服务健康检查、数据持久化和重启后的配置保留情况；
- 测试 Windows / Web 的全量更新、增量更新和本地版本恢复是否正常。

## 直接下载

普通测试用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.7-test-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.7-test-Web-Portable-x64.zip`

发布完成后可在：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.7-test/BiliPDJ-v3.0.7-test-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.7-test/BiliPDJ-v3.0.7-test-Web-Portable-x64.zip

`Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、Web 对应 manifest / incremental pack 及其 `.sha256` 为内置更新器使用的资源，普通用户无需手动下载或解压。

## 更新与数据安全

- `v3.0.7-test` 为测试通道 Release，不替代 v3.0.6 稳定版；稳定通道默认下载仍保持 v3.0.6；
- 全量更新继续下载完整 ZIP；增量更新按本地文件大小与 SHA-256 判断，只读取实际变化文件对应的 Range 数据；
- 更新前创建本地版本备份，失败时自动回滚；Windows / Web 均可恢复已有本地备份；
- 配置、队列/运行存档、日志、备份、插件与插件私有数据等用户数据不会被正常增量更新覆盖；
- Docker 构建上下文显式排除可能包含 Cookie、WebDAV 设置、日志和插件数据的本地持久化目录。

## 发行文件

- `BiliPDJ-v3.0.7-test-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.7-test-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.7-test-Windows-Tk-files.json`
- `BiliPDJ-v3.0.7-test-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.7-test-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.7-test-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.7-test-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.7-test-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.7-test-Web-files.json`
- `BiliPDJ-v3.0.7-test-Web-files.json.sha256`
- `BiliPDJ-v3.0.7-test-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.7-test-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 测试版说明

本次应作为 GitHub **Pre-release** 发布（`prerelease=true`）。仓库默认稳定版入口继续保持 v3.0.6，避免普通用户误装测试版本。显式 Release 仅在本次用户授权后通过仓库既有发布流程触发。
