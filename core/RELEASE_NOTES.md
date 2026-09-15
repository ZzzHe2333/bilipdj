# 弹幕排队姬 v3.0.15

本版本开始统一版本与 GitHub Release 状态模型：版本号只使用标准 `x.y.z`，不再通过 `-test`、`-gc`、`-cx`、`-dev` 等后缀区分测试版、公测版或其他渠道。

## 发布状态规则

- 版本号统一使用 `x.y.z`，例如 `3.0.15`；
- 未特别说明发布类型时，按 **Pre-release（预发行包）** 发布；
- 明确要求“测试版”时，同样按 **Pre-release（预发行包）** 发布，但版本号仍不增加测试后缀；
- 明确要求“正式版”时，按 **Release（发行包）** 发布；
- “转正式版”表示将同一标签下已有 Pre-release 原地转换为 Release，不重新打包、不更换版本号、不新建 Release。

历史上已经发布的带后缀版本仍可识别，以保证旧包和旧备份兼容；新版本不再产生新的渠道后缀。

## 更新器调整

Windows Tk 与 Web Portable 的云端版本筛选统一为：

- **发行包**：显示最近 3 个非 Pre-release 的正式 Release；
- **全部**：显示最近 10 个 GitHub Release，包含 Release 与 Pre-release；
- 自动检查更新、默认更新目标和未指定目标的更新操作，始终以最新正式 Release 为准；
- Pre-release 只在用户主动切换到“全部”后用于手动选择，不会自动覆盖正式发行包。

本地备份恢复继续保留，不受 Release / Pre-release 筛选影响。

## 发布与增量更新

- 发布工作流不再根据版本号后缀推断 Release 状态，而是使用显式的 `release_status`；
- 默认 `release_status` 为 `prerelease`；
- 明确正式发布时使用 `release`；
- 增量包基线从此前可用的便携版 Release 中选择，不再依赖目标版本是否带预发布后缀；
- 新增已有 Pre-release 原地转为正式 Release 的工作流。

## 发行文件与下载

普通用户只需要下载对应平台的完整便携版 ZIP：

- Windows 客户端：`BiliPDJ-v3.0.15-Windows-Tk-Portable-x64.zip`
- Web 便携版：`BiliPDJ-v3.0.15-Web-Portable-x64.zip`

当 `v3.0.15` 被实际发布到 GitHub Releases 后，对应直接下载地址为：

- Windows：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.15/BiliPDJ-v3.0.15-Windows-Tk-Portable-x64.zip
- Web：https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.15/BiliPDJ-v3.0.15-Web-Portable-x64.zip

内置更新器还会使用 `Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、Web 对应的 manifest / incremental pack、`update-manifest.json` 及校验信息；普通用户无需手动下载这些更新资源。

> 上述文件名和链接描述的是该版本一旦发布后的标准资源位置，不表示当前仓库修改已经创建了 GitHub Release。

## 数据安全

更新前的本地备份、失败回滚、配置与存档保护、插件数据保护以及 Windows/Web Portable 的全量/增量更新能力保持不变。

> 本文件描述仓库当前版本逻辑。是否已经在 GitHub 上发布为 Release 或 Pre-release，以 GitHub Release 页面实际状态为准。
