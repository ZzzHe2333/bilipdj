# 弹幕排队姬 v3.0.16

`v3.0.16` 是采用统一 Release / Pre-release 版本模型后的新版本。本次按 **Pre-release（预发行包）** 发布，版本号继续使用标准 `x.y.z`，不再添加 `-test`、`-gc`、`-cx`、`-dev` 等渠道后缀。

## 本次版本重点

- 版本号统一为标准 `3.0.16`；
- GitHub 发布状态由 Release 的 `prerelease` 元数据决定，不再从版本后缀推断；
- 未特别说明或明确要求“测试版”时发布为 Pre-release；明确要求“正式版”时发布为 Release；
- “转正式版”使用同一标签直接由 Pre-release 转为 Release，不重新打包、不更换版本号；
- Windows / Web 更新器的云端筛选统一为“发行包 / 全部”；
- “发行包”显示最近 3 个正式 Release；“全部”显示最近 10 个 Release（包含 Pre-release）；
- 自动检查、默认选择和未指定目标的更新操作始终以最新正式 Release 为准；
- 历史带后缀版本继续保留解析、备份与升级兼容。

## 发布与增量更新

- Windows Tk 与 Web Portable 继续同时生成完整便携包；
- 继续生成逐文件 manifest、增量资源包与 SHA-256 校验信息；
- 增量更新基线从此前可用的便携版 Release 中选择，不再依赖目标版本是否带预发布后缀；
- 更新前备份、失败回滚、配置/存档/插件数据保护保持不变。

## 发行文件与下载

普通用户只需要下载对应平台的完整便携版 ZIP：

- Windows 客户端：`BiliPDJ-v3.0.16-Windows-Tk-Portable-x64.zip`
- Web 便携版：`BiliPDJ-v3.0.16-Web-Portable-x64.zip`

对应直接下载地址：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.16/BiliPDJ-v3.0.16-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.16/BiliPDJ-v3.0.16-Web-Portable-x64.zip

内置更新器还会使用 `Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、Web 对应的 manifest / incremental pack、`update-manifest.json` 及校验信息；普通用户无需手动下载这些更新资源。

## 发布状态

本次发布目标为 **Pre-release（预发行包）**。如果后续明确要求“转正式版”，应直接将同一 `v3.0.16` Release 的 `prerelease` 状态切换为正式 Release，而不是创建新的后缀版本。
