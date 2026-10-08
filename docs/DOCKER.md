# Docker / Web 控制台部署

> Docker 版是 BiliPDJ 的第三种**部署形态**，不是第三套 UI。它直接使用 `apps/web/static` 的 Web 控制台和 `apps/server` 唯一业务后端；Windows Tk 和 Web Portable 仍独立维护。

## 1. 环境准备和快速安装

要求 Docker Engine 与 Docker Compose v2。Ubuntu / Debian 小主机推荐安装官方 Docker Engine + Compose 插件；本项目不需要 X11、图形桌面或 Windows 运行时。

```bash
git clone https://github.com/ZzzHe2333/bilipdj.git
cd bilipdj
mkdir -p data
docker compose config --quiet
docker compose up -d --build
docker compose ps
```

确认 `docker compose ps` 中 `bilipdj` 为 `healthy`。访问：

| 入口 | 地址 | 用途 |
|---|---|---|
| Web 控制台（默认操作界面） | `http://127.0.0.1:9816/control` | 管理当前队列、权限、样式、更新检查和日志 |
| 平台设置 | `http://127.0.0.1:9816/config` | 配置 B 站/抖音等已接入的平台与房间号 |
| OBS 浏览器源 | `http://127.0.0.1:9816/index` | 在 OBS 内显示直播弹幕队列 |
| 健康检查 | `http://127.0.0.1:9816/health` | Docker 内部健康检查也使用此路径 |

OBS 宽高可从 800×600 开始按版面调整。平台是否能连接取决于直播间配置、Cookie、目标平台的网络访问能力，与运行在 Tk / Web / Docker 无关。容器可能会使用宿主机网络代理或 DNS；代理需正确配置，不要因此放开管理 API。

## 2. 安全与网络（重要）

Compose 默认绑定 `127.0.0.1:9816:9816`，并采用只读容器文件系统、丢弃 Linux capability、`no-new-privileges`、受限进程数和可写的独立临时目录。默认运行 `root` **容器用户**，这是为了兼容已有的 `./data` 宿主机绑定目录权限；它仍不是可以面向公网暴露的安全模型。不要把 `ports` 改为 `0.0.0.0` 或用 `network_mode: host` 绕过本地管理 API 的来源检查。

如果需要从其他设备管理 Ubuntu 上的 Docker，可使用 SSH 隧道：

```bash
ssh -L 9816:127.0.0.1:9816 ubuntu@your-ubuntu-host
```

在 SSH 客户端设备的浏览器打开 `http://127.0.0.1:9816/control`。其他设备上的 OBS 也可通过同类安全隧道获得 `/index`；浏览器源 URL 的 `127.0.0.1` 指向 **运行 OBS 的那台设备**，不是 Docker 宿主机。若宿主机端口 9816 已被占用：

```bash
BILIPDJ_HOST_PORT=19816 docker compose up -d --build
```

改用本机 `http://127.0.0.1:19816/control`；容器内部服务仍使用 9816。因为 API 检查 `Host` / `Origin`，不要添加任意反向代理的“信任整个内网”规则。

## 3. 数据持久化和迁移

默认保留原有 `./data:/data` 绑定：`/data/core/` 保存平台配置、队列和黑名单等，`/data/log/` 为日志、`/data/plugins/` 为插件及私有数据，另外有 `/data/appearance.json`、`/data/style.json`、`/data/key/`、`/data/backup/`。不要将 `/app` 挂载为数据卷；这是应用程序代码。

```bash
docker compose stop
tar -czf "bilipdj-data-$(date +%Y%m%d).tgz" data/
docker compose up -d
```

从现有 Docker 部署升级时不要删除 `./data`；从 Windows 便携版迁移配置则应首先停服务并备份，再使用程序内的配置导出/导入功能，不建议直接覆盖含密钥的目录。

如果选择让容器以非 root UID/GID 运行，需**先**为当前 `./data` 在宿主机设置匹配的所有者和权限，确保队列归档、日志和插件可写；不要递归改为所有用户可读写的 777 权限。

## 4. 升级、日志与故障排查

Docker 镜像必须通过容器重建更新，**不能安装 Windows Web Portable ZIP，也不能调用它的 EXE 自更新流程**。从 Git 更新代码并重新构建：

```bash
git pull --ff-only
docker compose up -d --build
docker compose ps
docker compose logs --tail=100 bilipdj
```

管理界面的“更新软件”可展示 GitHub Release 信息，但 Docker 更新需要在宿主机操作。容器升级后 `./data` 仍保留；若升级不兼容，请先通过之前备份的数据恢复并切回原代码版本。

常见问题：
- `/control` 返回 403：检查是否真的从 `localhost` 打开，以及 Compose 是否保持默认的本机端口映射；不要通过公网转发访问管理 API。
- `docker compose ps` 显示 unhealthy：运行 `docker compose logs --tail=200 bilipdj`，检查 9816 端口、数据目录权限及启动异常。
- 登录凭据失效或弹幕断线：检查平台房间号、Cookie、容器 DNS 与出站网络，不要在日志或 Issue 中公开 Cookie。
- Docker 重启后队列没保留：确认 `./data:/data` 绑定没有更改、存档已开启且磁盘可写。
- 更新 Web 文件后页面仍旧：检查容器是否重新构建以及浏览器缓存；不要只替换宿主机的 `apps/web/static` 而不重建镜像。

## 5. 测试与验证

仓库的 `.github/workflows/server.yml` 在 Linux runner 中执行真正的 Docker Compose 构建、Web/API 健康检查、隔离访问测试及持久化验证。Docker 版与 Web Portable 使用同一套 Server 业务接口，不能自建只在容器里存在的排队实现。

**发布说明：** 只有在明确要求发布时才制作 Docker 镜像或 GitHub Release；本指南使用仓库源码本地构建，不需要连接 Docker Hub。
