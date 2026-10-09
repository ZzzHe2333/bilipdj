<div align="center">

# 🎬 BiliPDJ · 弹幕排队姬

面向 Bilibili / 抖音直播间的本地化弹幕排队、权限控制、队列存档与 OBS 展示工具

<p>
  <a href="https://github.com/ZzzHe2333/bilipdj/releases"><img alt="Release" src="https://img.shields.io/github/v/release/ZzzHe2333/bilipdj?style=flat-square&color=6366f1"></a>
  <a href="https://github.com/ZzzHe2333/bilipdj/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/ZzzHe2333/bilipdj?style=flat-square"></a>
  <a href="https://github.com/ZzzHe2333/bilipdj/stargazers"><img alt="Stars" src="https://img.shields.io/github/stars/ZzzHe2333/bilipdj?style=flat-square&color=yellow"></a>
  <a href="https://github.com/ZzzHe2333/bilipdj/issues"><img alt="Issues" src="https://img.shields.io/github/issues/ZzzHe2333/bilipdj?style=flat-square"></a>
  <a href="https://github.com/ZzzHe2333/bilipdj/actions/workflows/quality.yml"><img alt="CI" src="https://github.com/ZzzHe2333/bilipdj/actions/workflows/quality.yml/badge.svg"></a>
</p>

<p>
  <a href="https://github.com/ZzzHe2333/bilipdj/releases">下载发行版</a> ·
  <a href="./core/GUIDE.md">使用教程</a> ·
  <a href="./core/UPDATE.md">更新日志</a> ·
  <a href="./packages/shared/api-contract.md">API 契约</a> ·
  <a href="https://github.com/ZzzHe2333/bilipdj/issues">问题反馈</a>
</p>

</div>

---

## ✨ 这是什么

主播开播时弹幕刷得飞快，想按顺序接观众的连麦 / 点歌 / 点评论需求却很难手动维护一份靠谱的队列。**BiliPDJ** 把「排队」这件事做成了一套完整、可离线运行的本地服务：监听直播间弹幕、按规则自动或手动排队、支持管理员权限与礼物特权、把队列实时推给 OBS 展示，数据全程留在自己的电脑上。

Server 是唯一业务状态源，Windows 客户端、Web 控制台、OBS 展示层和第三方客户端统一通过 HTTP / WebSocket 共享同一份队列、权限、礼物、平台连接、主题与存档状态——不管从哪个界面操作，看到的都是同一个真实状态。

> 当前正式接入的平台是 **Bilibili** 与 **抖音**，虎牙 / 快手 / 斗鱼 / 微信视频号目前仅保留配置位，尚未启用。

## 🚀 快速开始

不熟悉 Python、只想直接用的话，下载便携版即可，双击运行，不需要单独装 Python，也不需要手动起 Server。

| 版本 | 发行包 | 启动方式 | 说明 |
|---|---|---|---|
| 🖥️ Tk Windows 便携版 | [下载最新正式版（选择 Windows Tk Portable）](https://github.com/ZzzHe2333/bilipdj/releases/latest) | 解压后双击 `main.exe` | 普通 Windows 用户请选择这个包 |
| 🌐 Web 便携版 | [下载最新正式版（选择 Web Portable）](https://github.com/ZzzHe2333/bilipdj/releases/latest) | 解压后双击 `BiliPDJ-Web.exe` | 启动内置后端服务器，就绪后自动打开 Web 控制台 |

**始终从 [latest 正式发行页](https://github.com/ZzzHe2333/bilipdj/releases/latest) 下载**，不要使用历史版 ZIP 链接。下载时根据发行资产名称选择 `Windows-Tk-Portable-x64.zip` 或 `Web-Portable-x64.zip`。

**OBS 三步上手：** 下载并解压 → 在 Windows 客户端的“平台参数”填直播间号并启动服务 → OBS 新增“浏览器源”，地址为 `http://127.0.0.1:9816/index`，宽高可从 **800 × 600** 开始按画面调整。默认管理服务只应通过本机访问，不应直接暴露公网。

每个发行包都附带 `.sha256` 与 `update-manifest.json`。客户端更新时会先读取 manifest 拿到准确的包名、下载地址、大小和 SHA-256 再下载校验，不会靠猜版本号拼文件名。`Windows-Tk-files.json` 与 `Windows-Tk-Incremental-x64.pack` 是内置增量更新器使用的资源，普通用户无需手动下载。

## 🐳 Docker：以 Web 控制台为操作界面

Linux / Ubuntu / Docker Desktop 用户可直接部署与 Web 便携版共用的 `apps/web/static/` 和 `apps/server/`，无需 Windows Tk，也不单独维护另一套 Web UI。

```bash
git clone https://github.com/ZzzHe2333/bilipdj.git
cd bilipdj
mkdir -p data
docker compose up -d --build
docker compose ps
```

默认只在本机监听 `127.0.0.1:9816`，浏览器访问 **[Web 控制台](http://127.0.0.1:9816/control)**，平台配置为 `/config`，OBS 浏览器源为 `http://127.0.0.1:9816/index`。队列、设置、主题、插件与日志持久化在项目 `./data`，重建容器不删除。需要改宿主机端口可设置 `BILIPDJ_HOST_PORT`；容器内端口仍为 `9816`。

**注意：** Web 管理 API 默认只允许本机访问，不可直接把管理端口开放到公网；远程管理可通过 SSH 端口转发在客户端使用 `localhost`。Docker 版按容器镜像/源码更新，**不要在容器里执行 Windows Web Portable 的独立更新器**。详细部署、升级、数据迁移与安全边界见 [Docker 使用说明](./docs/DOCKER.md)。

## 🧩 功能一览

- 🎨 Windows 与 Web/未来 Go Web 前端分别保存主题（`-win` / `-web`），共用后端与队列，但不会因修改不同 UI 的主题而相互覆盖；仍支持手动导入/导出主题配置。
- 🖌️ Web 样式页支持可视化设置 + 实时预览，同时保留高级 JSON 编辑
- 📦 Windows / Web 更新逻辑统一走 `update-manifest.json`，校验和下载全自动
- 🙋 Windows / Web 手动排队统一为「用户名必填、内容可选」，减少误操作
- 🔀 后端支持 Bilibili + 抖音同时接入，每个平台当前最多挂一个直播间
- 🧭 Windows / Web 导航统一为「更新软件 / 关于项目 / 支持我们」
- 🌗 Web 支持跟随系统 / 白天 / 夜晚 / 自定义配色
- 🗂️ 系统托盘隐藏与恢复，Web 便携启动器本质是一套本地后端服务器系统

## 🏗️ 项目结构

<details>
<summary>点击展开目录树</summary>

```text
bilipdj/
├─ apps/
│  ├─ server/                 # 后端真实实现，唯一业务状态源
│  │  ├─ server.py
│  │  ├─ appearance_guard.py  # Windows/Web 通用主题与 profile API
│  │  ├─ bilibili_*.py / douyin_*.py
│  │  └─ main.py
│  ├─ web/                    # Web 唯一源码目录
│  │  ├─ static/              # HTML/CSS/JS + Aurora Web 主题层
│  │  ├─ portable_launcher.py # Web 后端服务器系统启动器
│  │  ├─ web_portable.spec
│  │  └─ package-portable.ps1
│  └─ windows/                # Tk 桌面前端真实实现
│     ├─ control_panel.py
│     ├─ unified_theme.py     # Aurora Windows 主题映射与跨端导入/导出
│     ├─ overlay_host.py
│     ├─ update_*.py / updater*.py
│     ├─ main.py
│     └─ *.spec
├─ packages/shared/           # 前端/第三方客户端共享契约
├─ core/                      # 旧兼容层、兼容运行数据 + 项目文档中心
│  └─ appearance.json         # 源码模式默认通用主题配置
├─ .github/ci/                   # 必要构建、API 文档与安全检查脚本
└─ .github/workflows/
```

`core/` 已不再承载 Server 或 Windows 的主业务实现，迁移后的同名 Python 文件仅保留轻量转发，用来兼容旧代码里的 `import core.server`、`import core.control_panel` 之类的入口，真正的业务实现在 `apps/server` 与 `apps/windows`。项目的使用教程、更新日志、发行说明、贡献者说明和 AI 上下文也统一收在 `core/`，入口见 [core/README.md](./core/README.md)。

源码/便携运行时的用户数据优先保存在系统用户目录（Windows `%APPDATA%/bilipdj`、macOS `~/Library/Application Support/bilipdj`、Linux `$XDG_DATA_HOME/bilipdj` 或 `~/.local/share/bilipdj`）。旧项目目录数据首次迁移保留原件，两处冲突会要求用户选择。Docker 仍优先使用 `BILIPDJ_DATA_DIR=/data`。

</details>

## 🛠️ 开发

<details>
<summary>Windows 桌面端</summary>

源码启动：

```powershell
python -m pip install -r requirements.txt
python -m apps.windows.main
```

本地打包：

```powershell
powershell -ExecutionPolicy Bypass -File .\apps\windows\package.ps1 -InstallDependencies
```

主要产物：

```text
dist\bilipdj\main.exe
dist\bilipdj\paiduijitm.exe
dist\bilipdj\updater.exe
```
</details>

<details>
<summary>Web 前端</summary>

静态构建：

```bash
python apps/web/build.py
```

Windows Web 便携版：

```powershell
powershell -ExecutionPolicy Bypass -File .\apps\web\package-portable.ps1 -InstallDependencies
```

主要产物：

```text
dist\web-portable\bilipdj-web\BiliPDJ-Web.exe
```

</details>

<details>
<summary>Server 后端</summary>

```bash
python -m pip install -r apps/server/requirements.txt
python -m apps.server.main
```

局域网监听：

```bash
python -m apps.server.main --host 0.0.0.0 --port 9816
```

Docker：

```bash
docker build -f apps/server/Dockerfile -t bilipdj-server .
docker run --rm -p 9816:9816 bilipdj-server
```

后端一旦停止，Windows / Web / OBS / 第三方客户端都无法继续使用排队、弹幕和管理核心功能——它是名副其实的单一状态源。

</details>

## 🎨 Windows 与 Web 分开保存主题

共用 Server、队列和多平台弹幕不代表两套 UI 必须共用样式。Windows Tk 使用 `appearance-win.json`、`style-win.json`；Web（未来 BiliPDJ-Go Web 前端也使用同一套）使用 `appearance-web.json`、`style-web.json`。旧 `appearance.json`、`style.json` 仅在新文件不存在时作为首次迁移基线，**修改一端不会自动覆盖另一端**。

主题接口默认 Web，可明确指定 Windows：

```text
GET  /api/appearance?client=web
POST /api/appearance?client=web
GET  /api/appearance?client=win
POST /api/appearance?client=win
GET  /api/appearance/profile?client=win
POST /api/appearance/profile?client=win
```

配置文件仍可通过导出/导入手动复制到另一端，但不会自动同步。原有主题与 OBS/队列样式的兼容导入仍保留，设置 ZIP/WebDAV 备份包含两端的专用样式文件。

### 用户数据和迁移

| 系统 | 持久数据 | 日志 |
| --- | --- | --- |
| Windows | `%APPDATA%\\bilipdj` | `%LOCALAPPDATA%\\bilipdj\\log` |
| macOS | `~/Library/Application Support/bilipdj` | `~/Library/Logs/bilipdj` |
| Linux | `$XDG_DATA_HOME/bilipdj`；默认 `~/.local/share/bilipdj` | `$XDG_STATE_HOME/bilipdj/log`；默认 `~/.local/state/bilipdj/log` |
| Docker / 自定义 | `BILIPDJ_DATA_DIR` 指定的绝对或相对路径（例如 `/data`） | 数据目录的 `log/` |

迁移不会删除原始项目文件。当两边存在内容不同的文件时，启动时会要求选择用户目录或项目目录，并在覆写用户目录数据前留下迁移备份。无终端、无可用图形界面时会安全停止，不擅自覆盖任何一份数据。远程/无头环境可在明确决定后临时设置 `BILIPDJ_MIGRATION_CHOICE=user`（使用用户数据目录）或 `BILIPDJ_MIGRATION_CHOICE=project`（导入项目文件），仅在发生数据冲突时使用该设置。

## 📡 默认地址

| 地址 | 用途 |
|---|---|
| `http://127.0.0.1:9816/control` | Web 管理控制台 |
| `http://127.0.0.1:9816/index` | 队列 / OBS 展示 |
| `GET /health` | 后端健康检查 |
| `GET /api/runtime-status` | 运行状态 |
| `GET /api/queue/state` | 队列状态 |
| `GET /api/platforms/active` | 多平台激活状态 |
| `GET /api/appearance` | Windows/Web 通用主题 |
| `ws://127.0.0.1:9816/ws` | WebSocket 实时事件 |

## 🧭 架构

```mermaid
flowchart LR
    B[Bilibili] --> S[apps/server]
    D[Douyin] --> S
    S --> Q[Queue / Permission / Gift / Archive]
    S --> A[Appearance / Style]
    Q --> API[HTTP + WebSocket]
    A --> API
    API --> W[apps/windows]
    API --> WEB[apps/web]
    API --> OBS[Overlay / OBS]
    API --> C[Third-party client]
```

核心原则只有一句话：**前端只负责交互、展示与调用 API，不复制 Server 的排队业务规则，也不维护另一套主题持久化状态。**

### 双 UI 长期维护决议

**Windows Tk 桌面端与 Web 控制台是两套独立、并列、长期共同维护的正式 UI。** 保留各自的界面布局、交互技术与平台专属能力；不再以 WebView2 / pywebview 取代 Tk 或只维护一套前端为目标。共同功能的业务行为、API、数据状态、配置兼容性与安全边界必须保持一致。新增/修改共用功能时同时检查两端并记录测试；不同之处应明确标注为平台专属或待办，而不是静默遗漏。

具体维护约定、共享功能对照和验收清单参见 **[Windows Tk / Web 双 UI 维护规范](./docs/DUAL_UI_MAINTENANCE.md)**。

## ✅ CI / 构建验证

| Workflow | 作用 |
|---|---|
| `quality.yml` | Python 编译、后端兼容入口、API 文档覆盖与 Web 源码 smoke check |
| `server.yml` | 验证 Server 入口与 `core.server` 兼容层 |
| `web.yml` | 独立构建 Web 静态资源 |
| `package-windows-x64.yml` | 实际构建 Tk Windows Portable 与 Web Portable，并生成 ZIP / SHA256 / manifest |

仓库远端不再跟踪 `tests/`，`.gitignore` 会把它保留为本地开发测试空间——已有的本地测试文件可以继续用，但不会随普通提交带回 GitHub。正式合并前，以源码 smoke check、Server/Web 验证和实际 Windows Portable 构建作为远端验收链路。

## 🔌 API 与第三方客户端

想接第三方客户端，从 [packages/shared/api-contract.md](./packages/shared/api-contract.md) 开始看。完整开发参考可以本地生成：

```bash
python .github/ci/generate_api_docs.py
```

会生成到本地 `api/` 目录（该目录默认忽略，不提交仓库）。

## 🔒 安全提示

配置里可能包含 Cookie、`SESSDATA`、`bili_jct` 或其他登录凭据，**请勿提交到仓库或公开截图**。后端默认只监听 `127.0.0.1:9816`；局域网监听不等于公网鉴权，当前管理接口不应该直接暴露到公网。

## 📚 文档

| 文档 | 内容 |
|---|---|
| [core/README.md](./core/README.md) | 文档与兼容层总索引 |
| [core/GUIDE.md](./core/GUIDE.md) | 使用教程 |
| [core/UPDATE.md](./core/UPDATE.md) | 更新日志 |
| [core/RELEASE_NOTES.md](./core/RELEASE_NOTES.md) | 发行说明 |
| [core/CONTRIBUTORS.md](./core/CONTRIBUTORS.md) | 贡献者说明 |
| [core/ai.md](./core/ai.md) | AI / 自动化工具仓库上下文 |
| [packages/shared/api-contract.md](./packages/shared/api-contract.md) | 共享 API 契约、访问边界与第三方客户端说明 |

## 🤝 参与贡献

欢迎提 [Issue](https://github.com/ZzzHe2333/bilipdj/issues) 反馈 Bug 或提需求，详细规范见 [core/CONTRIBUTORS.md](./core/CONTRIBUTORS.md)。

## ⭐ Star History

[![Star History Chart](https://api.star-history.com/svg?repos=ZzzHe2333/bilipdj&type=Date)](https://star-history.com/#ZzzHe2333/bilipdj&Date)

## 📄 许可证

本项目基于 [GNU General Public License v3.0](./LICENSE) 发布。