# 弹幕排队姬 v3.0.15-test

`v3.0.15-test` 是基于 `v3.0.12` 正式版继续验证的测试版本。本次重点增加旧版包清理、Windows AppData 本地存档恢复，以及排队清空二次确认；同时继续包含前序测试版的 Bug 修复与更新链路改进。本版本不替代 `v3.0.12` 正式版。

## 本次主要变化

- Windows“更新软件”页新增 **清理旧版包**：从程序目录 `backup/` 读取可恢复的旧版快照，在独立窗口中展示版本号与实际占用大小，支持清理选中或全部旧版包；
- 新增 Windows 本地恢复存档 `%APPDATA%\bilipdj`。配置、权限/开关、排队存档、黑名单、主题/显示样式、WebDAV 设置、礼物兼容设置、语言设置及插件数据会在启动时进行内部同步；
- 已有安装中，程序/core 数据仍是正常运行时的权威来源，并在启动时同步到 AppData；只有 AppData 存在而本地对应数据缺失时，会从 AppData 恢复；
- 全新/重装后的程序目录若检测到既有 AppData 存档，会优先恢复旧用户数据覆盖新包自带的默认配置，再继续启动；整个过程不增加迁移提示或额外小字；
- “一键清空”排队信息改为独立警告弹窗。正文为“是否需要清空本存档的排队信息？如需清空，请连续点击确定2下（5s内）。”，只有 5 秒内连续确认两次才真正清空；
- AppData 同步不会介入 `BILIPDJ_DATA_DIR` 外部数据模式，因此 Docker / 显式外部数据目录保持原有行为；
- 新增 Issue #268 专项回归检查，覆盖旧数据优先恢复、本地优先同步、缺失单项恢复、目录镜像、旧版包安全删除和 5 秒双确认；
- 开发验收中修复了专项 guard 导入路径与 `runtime_layout.py` 独立文件探针兼容问题，现有 Quality、Server、更新器和 Portable 构建继续通过。

## 建议重点测试

- Windows `main.exe` 启动后，“更新软件”页右侧是否出现“清理旧版包”，旧版列表的版本号和大小是否正确；
- 删除某个本地旧版包后，`backup/` 对应快照是否被删除，当前版本、程序数据和 AppData 存档是否不受影响；
- 已有配置启动后， `%APPDATA%\bilipdj` 是否形成对应镜像；
- 模拟重装时仅保留 `%APPDATA%\bilipdj`，新程序能否恢复旧配置、排队信息、样式和插件数据；
- 同时存在本地数据与 AppData 数据的既有安装是否继续以本地/core 数据为准；
- “一键清空”第一次确认不应删除队列，5 秒内第二次确认才执行；取消或超过 5 秒都不应误清空；
- `3.0.14-test → 3.0.15-test` 的全量/增量更新、备份和 rollback 是否正常；
- Windows Tk Portable 与 Web Portable 是否均能正常启动，Docker 外部数据模式是否保持不变。

## 直接下载

测试用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.15-test-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.15-test-Web-Portable-x64.zip`

发布地址：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.15-test/BiliPDJ-v3.0.15-test-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.15-test/BiliPDJ-v3.0.15-test-Web-Portable-x64.zip

`Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、Web 对应 manifest / incremental pack 及其 `.sha256` 均由内置更新器使用，普通用户无需手动下载。

## 更新与数据安全

- `v3.0.15-test` 为测试版 GitHub Pre-release，不替代 `v3.0.12` 正式版；
- Windows/Web 更新工作区继续统一位于程序目录 `update/`；
- 更新前继续创建完整 `backup/` 快照；
- `backup/` 旧版包清理只允许删除可识别的直接子目录，不触碰当前程序目录和 `%APPDATA%\bilipdj`；
- AppData 本地恢复存档采用明确的数据白名单，不会把 `core/` 中的 Python 源码、文档或程序文件当作用户数据恢复；
- 增量更新继续校验完整发行版本身份，更新失败优先恢复旧版本；
- 正式版下载入口继续保持 `v3.0.12`。

## 发行文件

- `BiliPDJ-v3.0.15-test-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.15-test-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.15-test-Windows-Tk-files.json`
- `BiliPDJ-v3.0.15-test-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.15-test-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.15-test-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.15-test-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.15-test-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.15-test-Web-files.json`
- `BiliPDJ-v3.0.15-test-Web-files.json.sha256`
- `BiliPDJ-v3.0.15-test-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.15-test-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 测试版说明

本版本使用 `-test` 后缀，发布时标记为 GitHub **Pre-release**。正式稳定版仍为 `v3.0.12`，README 稳定版下载入口继续保持 `v3.0.12`。
