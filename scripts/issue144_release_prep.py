from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    (ROOT / path).write_text(text, encoding="utf-8")


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected exactly one match, got {count}: {old!r}")
    write(path, text.replace(old, new, 1))


# Canonical version.
write("VERSION", "3.0.0\n")

# User-facing current-version references.
replace_once(
    "README.md",
    "BiliPDJ-v2.0.4-Windows-Tk-Portable-x64.zip",
    "BiliPDJ-v3.0.0-Windows-Tk-Portable-x64.zip",
)
replace_once(
    "README.md",
    "BiliPDJ-v2.0.4-Web-Portable-x64.zip",
    "BiliPDJ-v3.0.0-Web-Portable-x64.zip",
)
replace_once("core/ai.md", "```text\n2.0.4\n```", "```text\n3.0.0\n```")

api = read("packages/shared/api-contract.md")
for old, new in [
    ('"version": "2.0.4"', '"version": "3.0.0"'),
    ('"tag_name": "v2.0.4"', '"tag_name": "v3.0.0"'),
    ('releases/tag/v2.0.4', 'releases/tag/v3.0.0'),
    ('BiliPDJ-v2.0.4-Windows-Tk-Portable-x64.zip', 'BiliPDJ-v3.0.0-Windows-Tk-Portable-x64.zip'),
    ('BiliPDJ-v2.0.4-Web-Portable-x64.zip', 'BiliPDJ-v3.0.0-Web-Portable-x64.zip'),
]:
    if api.count(old) != 1:
        raise RuntimeError(f"packages/shared/api-contract.md: expected one {old!r}, got {api.count(old)}")
    api = api.replace(old, new, 1)
write("packages/shared/api-contract.md", api)

release_notes = """# 弹幕排队姬 v3.0.0

v3.0.0 是 BiliPDJ 插件化与多平台弹幕核心的一次主版本升级。重点从“把不同平台伪装成 Bilibili 消息”迁移到统一 `DanmuEvent`，并完善 Python / JavaScript 插件运行时、权限边界、动态配置和发布安全。

## 插件系统

- 支持 `.bilipdj-plugin` 安装包的安装、卸载、启用和禁用；
- manifest 支持版本兼容、文件哈希、签名/完整性校验、权限声明和 capability；
- 同时支持 Python 与 JavaScript 插件；
- JavaScript 插件在独立进程 QuickJS runtime 中执行，保留 watchdog 和 IPC 边界；
- 新增 `config_schema`，插件可以声明动态配置项，Web 设置页自动生成安全配置界面；
- secret 配置不会通过读取 API 回显明文，空值保存可保留现有 secret；
- JavaScript `host.httpRequest()` 支持受限 GET / POST、请求体、状态码和安全响应头；
- Python / JavaScript 插件私有数据统一限制为单文件 4 MiB、每插件 64 MiB、最多 1024 个常规文件，并限制相对路径深度与长度。

## 平台无关 DanmuEvent

- 新增平台无关 `DanmuEvent` 作为 QueueManager 的标准输入；
- Bilibili 原始 `DANMU_MSG` 继续兼容，但只作为适配层；
- 新插件应使用 Python `context.process_danmu_event(...)` 或 JavaScript `host.processDanmuEvent(...)`；
- 原生字符串用户 ID 被完整保留，不再要求第三方平台伪造 Bilibili 数字 UID；
- 房管、主播、舰长/守护、粉丝牌和接收时间使用统一身份字段；
- `DANMU_EVENT` WebSocket 事件提供统一 `platform / message / identity / received_at` 结构。

## 平台 Relay 与兼容

- 抖音、虎牙、Twitch、YouTube 的内建 Relay 已改为直接产生 `DanmuEvent`，去除伪造 Bilibili JSON 的内部耦合；
- 抖音 `role >= 3` 按房管身份映射；
- Twitch 保留 moderator / broadcaster / badge 等原生身份元数据；
- 旧 `processDanmu` / `process_danmu_json` 仍保留为 Bilibili 兼容 API，已有插件不会被强制一次性迁移。

## 安全与稳定性

- 发布前补齐插件 API 权限、文件路径、symlink、mutation、HTTP Host、config schema、DanmuEvent 等专项回归；
- 插件私有数据增加总容量与文件数配额，避免失控插件持续创建文件耗尽磁盘；
- 更新检查顺序调整为 Release 附件 manifest → GitHub latest Release API → `now` 静态 manifest 最后兜底，避免旧静态清单抢占最新 Release；
- 普通 `now` push 和 Pull Request 只构建/校验，不再拥有创建 GitHub Release 的权限；
- GitHub Release 只能通过显式 workflow dispatch 或 `v*` tag 流程创建。

## Windows / Web Portable 验证

- Tk Windows 与 Web Portable 都会真实 PyInstaller 构建；
- 打包 CI 在生成 ZIP 前实际启动两个 frozen GUI EXE 的隐藏插件自检入口；
- 自检会在冻结环境中安装并启动 QuickJS 插件，通过 multiprocessing worker 发送 `DanmuEvent`；
- frozen EXE 返回非 0 时停止打包，不会生成可发布 ZIP；
- Windows GUI 子系统 EXE 使用 `Start-Process -Wait -PassThru` 获取真实退出码。

## 更新与校验

正式发行会同时提供：

- `BiliPDJ-v3.0.0-Windows-Tk-Portable-x64.zip`
- `BiliPDJ-v3.0.0-Windows-Tk-Portable-x64.zip.sha256`
- `BiliPDJ-v3.0.0-Web-Portable-x64.zip`
- `BiliPDJ-v3.0.0-Web-Portable-x64.zip.sha256`
- `update-manifest.json`

`update-manifest.json` 由发布构建根据真实产物生成，包含包名、下载地址、文件大小和 SHA-256。客户端仍会先读取 manifest 并校验 SHA-256 后再安装。

## 升级建议

- 从 v2.x 升级时请完整解压新 portable 包；
- 原有队列、配置和兼容 API 继续保留；
- 新开发的第三方弹幕插件建议直接使用 `DanmuEvent` API；
- 高权限插件仍应仅安装可信来源，尤其是声明 `subprocess`、网络或文件写权限的插件。
"""
write("core/RELEASE_NOTES.md", release_notes)

update = read("core/UPDATE.md")
entry = """## v3.0.0（2026-09-11）\n\n### 插件化与 DanmuEvent\n- 完成 `.bilipdj-plugin` 插件管理器、Python / JavaScript 双运行时、权限和完整性校验。\n- 新增 `config_schema` 动态插件设置，以及受控 `host.httpRequest()`。\n- QueueManager 改为平台无关 `DanmuEvent` 标准输入，第三方平台不再依赖伪造 Bilibili JSON。\n\n### 发布前稳定性与安全\n- Python / JavaScript 插件私有数据增加 4 MiB 单文件、64 MiB 总容量、1024 文件数量限制。\n- 修复更新检查在 Release manifest 不可用时可能被旧 `now/update-manifest.json` 降级的问题。\n- Windows Tk / Web Portable 打包新增 frozen EXE QuickJS 插件端到端自检。\n- 普通 `now` push / PR 的 Release job 保持 skipped，只有显式发布流程可以创建 Release。\n\n### 兼容\n- 旧 Bilibili `DANMU_MSG` 和插件 `processDanmu/process_danmu_json` 接口继续保留。\n- 新插件推荐使用 `process_danmu_event/processDanmuEvent`。\n\n---\n\n"""
anchor = "# 更新日志\n\n---\n\n"
if update.count(anchor) != 1:
    raise RuntimeError("core/UPDATE.md: unexpected changelog header")
update = update.replace(anchor, "# 更新日志\n\n---\n\n" + entry, 1)
write("core/UPDATE.md", update)

# Static update-manifest.json deliberately stays untouched here. It is synced
# from the real v3.0.0 release asset after the release build produces hashes.
print("3.0.0 release metadata prepared")
