# 弹幕排队姬 v3.0.23 正式版

本版基于 v3.0.22，新增四个公益 GitHub Release 下载加速节点（Issue [#316](https://github.com/ZzzHe2333/bilipdj/issues/316)）。Windows Tk 和 Web Portable 的更新功能同步支持。

## 下载线路

- GitHub 官方（默认）
- `https://gh-proxy.com/`（原有）
- `https://github.akams.cn/`（新增）
- `https://ghfile.geekertao.top/`（新增）
- `https://github.dpik.top/`（新增）
- `https://gh.dpik.top/`（新增）

Windows Tk：更新设置 →「下载线路」；Web Portable：自动更新页面 →「下载线路」。

## 兼容与安全

- GitHub API 和 Release 版本信息仍从官方读取，仅对合法的公开 GitHub Release 文件下载 URL 增加镜像前缀。
- 每次使用第三方加速均须确认；不保存长期信任，取消确认则走官方源。
- 复用原有下载失败回退官方、大小和 SHA-256 校验；增量 Range 下载仍严格验证 Content-Range 和文件摘要。
- 公益节点由第三方运营，可能限流、故障或改变服务方式；使用前应了解第三方能够看到下载文件 URL。用户个人网络的实际访问速度与节点可用性未承诺。

## 下载

- [Windows Tk x64 便携版](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.23/BiliPDJ-v3.0.23-Windows-Tk-Portable-x64.zip)
- [Web x64 便携版](https://github.com/ZzzHe2333/bilipdj/releases/download/v3.0.23/BiliPDJ-v3.0.23-Web-Portable-x64.zip)

同一正式版 Release 另提供 `Windows-Tk-files.json`、`Windows-Tk-Incremental-x64.pack`、`Web-files.json`、`Web-Incremental-x64.pack`、`update-manifest.json` 与对应 `*.sha256` 附件，供更新器使用。**普通用户无需手动下载**这些清单与增量资源，只需下载相应便携 ZIP。发行包可用性以 GitHub Actions 成功及最终 Release 为准。

**发布版本：v3.0.23 正式版，补丁号 +1。**
