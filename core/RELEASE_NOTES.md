# 弹幕排队姬 v3.0.8-test

`v3.0.8-test` 是基于 v3.0.7-test 的测试发行版，重点修复 Windows 控制台左侧导航仍会发生的二次横向抖动、全量更新后白昼主题被重置为黑夜主题的问题，并将 Windows 更新页扩展为可选择最近 10 个可安装 GitHub Release 进行升级、重装或降级。该版本作为 GitHub Pre-release 发布，不替代当前稳定版 v3.0.6。

## 本次测试内容

- 修复 Windows 左侧导航锁宽后的延迟 geometry 结算：最终 `nav.width` 与 `shell` 列宽写入后，在窗口首次显示前再执行一次 idle layout，避免下一次 `<Configure>` 才突然向右扩张；
- 保留现有 normal / bold 字重切换锁宽逻辑，同时避免更新页 Canvas 在左栏迟到的尺寸变化后再次横向重排；
- 修复 Windows 全量更新遗漏 `appearance.json` 的问题：根目录与兼容 `core/appearance.json` 均按用户数据保留，更新包内默认 `mode=dark` 不再覆盖用户的白昼模式、自定义配色或 system 模式；
- 增量更新原有的 `appearance.json` preserved-path 规则保持不变；
- Windows 更新页现在读取最近最多 10 个可安装 GitHub Release，同时保留正式版、测试版和本地备份入口；
- 用户可以选择高于、等于或低于当前版本的云端版本，界面会明确显示“升级 / 重新安装 / 降级”；
- 全量安装可用于任意所选云端版本；只有目标 Release 的增量基线与当前版本完全匹配且 transport 为 `http-range` 时，才启用“增量更新”；其余目标自动只允许全量安装，避免跨版本增量遗留旧文件；
- 最近版本目录会跳过 draft、缺少 Windows 可安装包或无法解析的 Release，并继续向后补足，最多展示 10 个有效云端版本；
- 新增 Issue #213 回归测试，覆盖导航 geometry 结算、全量更新主题保留、最近 10 个版本目录和增量基线判断。

## 建议重点验证

- 连续切换“日志 → 当前排队 → 设置 → 更新软件 → 关于项目”等页面，确认左侧导航从首帧开始宽度稳定，不再先向右拉、随后带动右侧区域二次重排；
- 在白昼模式及自定义配色下执行 Windows 全量更新，确认重新启动后模式和颜色仍保持更新前状态；
- 在“更新软件”页面检查最近版本下拉框，确认最多显示最近 10 个云端可安装 Release，并可选择比当前版本更旧或更新的版本；
- 选择旧版本时确认全量按钮显示降级语义；选择与当前版本一致时显示重新安装语义；
- 只有当前版本正好等于目标 Release 的增量基线时，增量更新按钮才可用；否则应保持禁用并提示仅支持全量；
- 本地备份恢复、SHA-256 校验、失败回滚和现有 Web Portable 更新链路继续保持正常。

## 直接下载

普通测试用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.8-test-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.8-test-Web-Portable-x64.zip`

发布完成后可在：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.8-test/BiliPDJ-v3.0.8-test-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.8-test/BiliPDJ-v3.0.8-test-Web-Portable-x64.zip

`Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、Web 对应 manifest / incremental pack 及其 `.sha256` 为内置更新器使用的资源，普通用户无需手动下载或解压。

## 更新与数据安全

- `v3.0.8-test` 为测试通道 Release，不替代 v3.0.6 稳定版；稳定通道默认入口继续保持 v3.0.6；
- Windows 全量更新现在显式保留 `appearance.json` 与 `core/appearance.json`，主题模式和自定义配色不会再被发行包默认值覆盖；
- 全量安装继续完整下载 ZIP 并校验 SHA-256；最近 10 个云端版本均可作为全量目标，包括升级、同版本重装和降级；
- 增量更新仍要求目标 Release 的 `base_version` 与本地当前版本完全一致，并按逐文件 SHA-256 + HTTP Range 读取差异数据；
- 更新前创建本地版本备份，失败时自动回滚；Windows / Web 均继续保留已有本地备份恢复能力；
- 配置、队列/运行存档、日志、备份、插件、插件私有数据、主题与显示样式等用户数据不会被正常增量更新覆盖。

## 发行文件

- `BiliPDJ-v3.0.8-test-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.8-test-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.8-test-Windows-Tk-files.json`
- `BiliPDJ-v3.0.8-test-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.8-test-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.8-test-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.8-test-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.8-test-Web-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.8-test-Web-files.json`
- `BiliPDJ-v3.0.8-test-Web-files.json.sha256`
- `BiliPDJ-v3.0.8-test-Web-Incremental-x64.pack`
- `BiliPDJ-v3.0.8-test-Web-Incremental-x64.pack.sha256`
- `update-manifest.json`

## 测试版说明

本次应作为 GitHub **Pre-release** 发布（`prerelease=true`）。显式 Release 仅在本次用户授权后通过仓库既有发布流程触发。
