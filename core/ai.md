# BiliPDJ AI Context

本文档用于给 AI / 自动化工具提供当前仓库结构与修改边界。若本文档与代码冲突，以 `now` 分支代码和 CI 为准。

## 当前版本

```text
3.0.1
```

## 核心架构

BiliPDJ 是 Monorepo：

```text
apps/server/      Python 后端，唯一业务状态源
apps/windows/     Tk 桌面前端、Overlay、Updater 与桌面打包配置
apps/web/         Web 前端唯一源码与 Web Portable 启动器
packages/shared/  HTTP / WebSocket 共享契约
core/             旧导入兼容层 + 兼容运行数据位置 + 项目文档
scripts/          API 文档生成、安全扫描等仍在使用的维护脚本
```

### 强制边界

- 排队、权限、礼物、平台 Relay、存档和业务状态由 `apps/server/` 统一处理。
- Windows Tk 只负责 UI、用户交互、Overlay 和调用后端 API。
- Web 只负责浏览器 UI、展示和调用后端 API。
- 不在 Windows / Web 中复制一套 QueueManager 或平台协议业务逻辑。
- 第三方客户端优先依据 `packages/shared/api-contract.md` 开发。

## 统一主题 / Design System

Windows 与 Web 不再维护两套互不兼容的主题配置。

唯一持久化主题文件：

```text
core/appearance.json        # 源码模式
appearance.json             # Portable 运行目录
```

Server 统一管理：

```text
GET  /api/appearance
POST /api/appearance
GET  /api/appearance/profile
POST /api/appearance/profile
```

统一主题名为 **BiliPDJ Aurora**，主要 Design Tokens 包括：

```text
mode
font_family
font_size
radius
light.background / sidebar / surface / surface_alt / input / border
light.text / muted / accent / accent_hover / selection / success / warning / danger
dark.background / sidebar / surface / surface_alt / input / border
dark.text / muted / accent / accent_hover / selection / success / warning / danger
```

规则：

- Windows `ttk.Style` 从同一份 appearance 配置映射颜色和字体；
- Web CSS Variables 从同一份 appearance 配置生成；
- `style.json` **继续单独负责 OBS / 队列展示**，不要把它删除或强行合并进 appearance；
- 对用户导入/导出时，使用统一 profile 将两者打包到一个 JSON：

```json
{
  "schema": 1,
  "kind": "bilipdj-appearance-profile",
  "appearance": {},
  "display_style": {}
}
```

该 profile 必须保持 Windows → Web 和 Web → Windows 双向兼容。

不要重新把 Web 主题改回只存 `localStorage`；localStorage 仅允许作为旧版本迁移/启动兼容缓存，Server 才是最终状态源。

## 仓库根目录约束

根目录只保留项目级版本/依赖文件、应用目录和共享目录。打包脚本与 PyInstaller `.spec` 不再放在根目录；桌面打包实现统一归入 `apps/windows/`，Web 打包实现归入 `apps/web/`。不要重新引入仅做一层转发的根目录 `.ps1` / `.sh` / `.spec`。

## 平台

当前真实接入：

- Bilibili
- 抖音

支持同时激活两个平台，每个平台当前最多一个直播间，统一进入同一个后端队列。

虎牙、快手、斗鱼、微信视频号目前仅保留配置位，不应在 UI 或文档中声称已经支持真实弹幕流。

## Windows 打包

唯一 Windows 本地打包入口：

```powershell
powershell -ExecutionPolicy Bypass -File .\apps\windows\package.ps1 -InstallDependencies
```

使用：

```text
apps/windows/bilipdj_onedir.spec
apps/windows/paiduijitm.spec
apps/windows/updater.spec
```

不要重新创建根目录 `package-windows-local.ps1` 或根目录重复 `.spec`。

## macOS 打包

唯一 macOS 打包入口：

```bash
./apps/windows/package-macos.sh --install-deps --arch arm64
./apps/windows/package-macos.sh --install-deps --arch x86_64
```

使用：

```text
apps/windows/bilipdj_onedir_mac.spec
apps/windows/paiduijitm_mac.spec
```

不要重新创建 `package-macos-local.sh`、`scripts/package-arm64.sh`、`scripts/package-amd64.sh` 等纯包装入口。

## Web

唯一静态前端源码：

```text
apps/web/static/
```

构建：

```bash
python apps/web/build.py
```

Web Portable：

```powershell
powershell -ExecutionPolicy Bypass -File .\apps\web\package-portable.ps1 -InstallDependencies
```

## 更新系统

客户端更新必须先读取：

```text
https://github.com/ZzzHe2333/bilipdj/releases/latest/download/update-manifest.json
```

manifest 提供版本、真实文件名、下载地址、大小和 SHA-256。不要根据版本号猜 Release 文件名。

## API

主文档：

```text
packages/shared/api-contract.md
```

本地生成完整参考：

```bash
python scripts/generate_api_docs.py
```

默认本地 Server：

```text
http://127.0.0.1:9816
```

关键接口包括：

```text
GET  /health
GET  /api/runtime-status
GET  /api/queue/state
POST /api/queue/insert
POST /api/queue/delete
POST /api/queue/move
POST /api/queue/update
GET  /api/platforms/active
POST /api/platforms/active
GET  /api/style
POST /api/style
GET  /api/appearance
POST /api/appearance
GET  /api/appearance/profile
POST /api/appearance/profile
WS   /ws
WS   /danmu/sub
```

## tests

GitHub 远端不跟踪 `tests/`。`.gitignore` 保留 `tests/` 作为本地开发测试空间。

远端验收主要依赖：

- Python source compile / smoke checks
- Server 验证
- Web build
- API 文档覆盖检查
- Windows Tk / Web Portable 实际打包

## 必须保留的 scripts

当前只保留并维护有明确用途的脚本：

```text
scripts/generate_api_docs.py
scripts/scan_secrets.py
```

删除脚本前必须先检查 `.github/workflows/`、README 和代码引用。

## 修改流程

仓库修改必须按以下顺序：

```text
Issue → 独立分支 → 修改 → 测试/构建 → 对照验收标准 → PR → CI → squash merge → 记录完成时间 → 关闭 Issue
```

不要直接在 `now` 上提交功能修改。

## 安全

- Cookie、SESSDATA、bili_jct、Token、备份凭据不得提交仓库。
- Server 默认本地监听最安全。
- `0.0.0.0` 只是监听范围扩大，不等于具备公网鉴权。
- 当前管理端口不应直接裸露到公网。
