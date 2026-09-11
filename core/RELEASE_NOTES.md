# 弹幕排队姬 v3.0.3-test

这是基于 v3.0.2 的测试发行版，主要用于验证 Issue #171 合并后的 UI 优化与当前便携打包/更新链路。该版本作为 GitHub Pre-release 发布，不替代现有稳定版 v3.0.2。

## 本次测试内容

- Web 控制台统一按钮按压反馈、输入框/键盘焦点样式和短过渡动画；
- 增加 `prefers-reduced-motion` 支持，减少动画模式下关闭非必要动效；
- 优化后端指令区域与更新大小预估卡片的视觉层级；
- 调整窄屏布局，并继续保留 `scrollbar-gutter: stable` 与相关防横向抖动约束；
- Windows Tk 后端指令区域重新整理布局与视觉层级，不改变指令处理逻辑；
- `.agents/skills/` 固定集成 `emilkowalski/skills` 上游 commit `d23d7f88a2e21c9e4b1418c7abe420f5c1052ba7`，保留 MIT License 与来源记录；
- 新增 Issue #171 UI regression guard，并继续通过 Quality CI 验证。

## 下载说明

普通测试用户只需要下载对应的完整便携版 ZIP：

- **Windows 客户端**：`BiliPDJ-v3.0.3-test-Windows-Tk-Portable-x64.zip`
- **Web 便携版**：`BiliPDJ-v3.0.3-test-Web-Portable-x64.zip`

`Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack` 及其 `.sha256` 为更新器资源，普通用户无需手动处理。

## 更新器与安全

- Windows Tk Portable 与 Web Portable 均继续执行 frozen JavaScript 插件运行时自检；
- 全量 ZIP、逐文件 manifest、增量 `.pack` 与 `update-manifest.json` 将放在同一个 `v3.0.3-test` Release；
- 增量更新仍按逐文件大小与 SHA-256 判断，仅下载缺失或变化的受管程序文件；
- 配置、队列/运行存档、日志、备份、插件与插件私有数据等用户数据仍不会被增量更新覆盖；
- 普通 PR 和 `now` push 不发布 Release，本次仅通过用户明确授权的显式发布流程创建测试发行版。

## 发行文件

- `BiliPDJ-v3.0.3-test-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.3-test-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.3-test-Windows-Tk-files.json`
- `BiliPDJ-v3.0.3-test-Windows-Tk-files.json.sha256`
- `BiliPDJ-v3.0.3-test-Windows-Tk-Incremental-x64.pack`
- `BiliPDJ-v3.0.3-test-Windows-Tk-Incremental-x64.pack.sha256`
- `BiliPDJ-v3.0.3-test-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.3-test-Web-Portable-x64.zip.sha256`
- `update-manifest.json`

## 测试版说明

`v3.0.3-test` 用于验证当前 UI 与打包链路，不作为稳定版替代。发现问题可继续在仓库 Issue 中反馈；稳定用户可继续使用 v3.0.2。
