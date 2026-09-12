# 弹幕排队姬 v3.0.9-test

`v3.0.9-test` 是基于 v3.0.8-test 的测试发行版，重点将 Windows 桌面端主框架迁移到 CustomTkinter，并继续保留原有后端、排队、插件、更新器与 Web Portable 业务逻辑。该版本作为 GitHub Pre-release 发布，不替代当前稳定版 v3.0.6。

## 本次测试内容

- Windows 桌面主窗口改用 `CustomTkinter.CTk`；
- 顶部品牌区、运行状态、主要操作按钮、左侧导航与主内容外壳迁移到 CustomTkinter；
- 左侧导航宽度从窗口构建阶段固定，不再依赖运行时文字 `winfo_reqwidth()` 测量；
- 导航容器同时关闭 `grid_propagate` / `pack_propagate`，减少选中态字体、页面内容变化对主布局宽度的影响；
- Treeview、复杂设置页等业务控件继续通过 ttk/Tk 兼容层运行，避免一次性重写业务功能造成回归；
- 白昼 / 黑夜模式继续使用现有 `appearance.json`，与 Web/OBS 主题数据保持兼容；
- PyInstaller 构建已加入 CustomTkinter 数据文件和子模块收集；
- 继续包含 v3.0.8-test 的主题保留、最近 10 个云端版本选择、升级/重装/降级以及安全增量基线判断。

## 建议重点验证

- 双击 `main.exe` 后 Windows 桌面控制台能正常显示并自动启动后端；
- 连续切换“日志 / 当前排队 / 设置 / 更新软件 / 关于项目”等页面时左侧导航宽度保持稳定；
- 白昼 / 黑夜切换及重启后的主题状态正常；
- 排队、平台配置、插件、更新器、本地备份恢复和 OBS 透明窗口功能保持可用；
- Windows / Web 全量更新与逐文件增量更新的 SHA-256 校验和失败回滚保持正常。

## 直接下载

普通测试用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.9-test-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.9-test-Web-Portable-x64.zip`

发布地址：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.9-test/BiliPDJ-v3.0.9-test-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.9-test/BiliPDJ-v3.0.9-test-Web-Portable-x64.zip

`Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、Web 对应 manifest / incremental pack 及其 `.sha256` 为内置更新器使用的资源，普通用户无需手动下载或解压。

## 更新与数据安全

- `v3.0.9-test` 为测试通道 Release，不替代 v3.0.6 稳定版；
- Windows 全量更新继续保留 `appearance.json` 与 `core/appearance.json` 等用户配置；
- 全量安装下载完整 ZIP 并校验 SHA-256；
- 增量更新仅在目标 Release 的 `base_version` 与本地当前版本完全匹配时启用，并通过逐文件 SHA-256 + HTTP Range 获取差异内容；
- 更新前创建本地版本备份，失败时自动回滚；
- 配置、队列存档、日志、备份、插件、插件私有数据、主题与显示样式等用户数据不会被正常增量更新覆盖。

## 发行文件

- `BiliPDJ-v3.0.9-test-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.9-test-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.9-test-Windows-Tk-files.json`
- `BiliPDJ-v3.0.9-test-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.9-test-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.9-test-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.9-test-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.9-test-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.9-test-Web-files.json`
- `BiliPDJ-v3.0.9-test-Web-files.json.sha256`
- `BiliPDJ-v3.0.9-test-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.9-test-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 测试版说明

本版本已作为 GitHub **Pre-release** 发布（`prerelease=true`）。后续修复仍按 Issue → PR → CI → 合并流程进行；未收到明确发布请求时不会自动创建新的 Release。
