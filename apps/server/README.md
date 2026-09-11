# BiliPDJ Server

`apps/server/` 保存后端的真实实现：Bilibili / 抖音 / 虎牙协议、队列逻辑、权限/礼物相关运行时、HTTP API、WebSocket、日志和安全/性能 guard 都从这里加载。

`core.server` 等旧导入仍可用，但只作为兼容转发层。

v2.0.1 的 **Windows Tk 便携版** 与 **Web 便携版** 都直接打包这一套 Server 实现，并同时带上 `apps/web/static`。终端用户启动对应前端后会自动拉起内置后端，不需要单独安装或启动 Server。

## 虎牙弹幕

虎牙使用网页直播间链接作为首选配置，例如：

```text
https://www.huya.com/lpl
```

后端启动虎牙 relay 时会自动解析真实房间号和主播 UID；也可以在平台参数中手动填写 `room_id` 与 `anchor_id` 作为兜底。公开直播间弹幕通过虎牙 WebSocket/TARS 通道接收，第一阶段只把普通文本弹幕送入统一排队逻辑，商城系统消息会在进入 `QueueManager` 前过滤。

## 本地启动

```bash
python -m pip install -r apps/server/requirements.txt
python -m apps.server.main
```

指定监听地址：

```bash
python -m apps.server.main --host 0.0.0.0 --port 9816
```

指定 Web 目录：

```bash
python -m apps.server.main --web-dir apps/web/dist
```

默认 Web 资源来自唯一源码目录 `apps/web/static/`。

## Docker

推荐从仓库根目录直接使用 Compose：

```bash
docker compose up -d --build
```

默认访问地址：

```text
http://127.0.0.1:9816/control
http://127.0.0.1:9816/config
http://127.0.0.1:9816/index
```

Compose 默认把宿主机 `./data` 挂载到容器 `/data`，并设置：

```text
BILIPDJ_DATA_DIR=/data
BILIPDJ_DOCKER=1
BILIPDJ_DOCKER_TRUSTED_CIDRS=auto
```

配置、主题、日志、队列/黑名单存档、`key/` 更新元数据、插件与插件私有数据会写入持久化数据目录；删除或重建容器不会删除 `./data` 中的用户数据。

为了保持管理 API 的本地安全边界，Compose 默认只发布：

```text
127.0.0.1:9816:9816
```

因此宿主机浏览器可以正常访问 local-only API，但局域网和公网不会直接得到管理端口。Docker NAT 会让后端看到网桥网关作为客户端地址；`BILIPDJ_DOCKER_TRUSTED_CIDRS=auto` 只自动信任 Linux 默认路由的**精确网关 IP**，不会把整个 `172.16.0.0/12` 或其他私网段加入信任列表。特殊 Docker 网络环境可以显式填写一个或多个精确 IP/CIDR，例如：

```text
BILIPDJ_DOCKER_TRUSTED_CIDRS=172.30.0.1/32
```

即使来源 IP 位于受信任网关范围，现有管理请求仍继续执行 loopback `Host` 与同源 `Origin/Referer` 检查。不要为了远程管理直接把 Compose 端口改成 `0.0.0.0:9816:9816`；当前 local-only API 的设计目标仍是本机管理，而不是公网鉴权。

容器和 Compose 都使用现有 `/health` 接口做 healthcheck。查看状态：

```bash
docker compose ps
```

如需手动运行镜像，可显式保持相同安全边界：

```bash
docker build -f apps/server/Dockerfile -t bilipdj-server .
docker run --rm \
  -p 127.0.0.1:9816:9816 \
  -v "$(pwd)/data:/data" \
  -e BILIPDJ_DATA_DIR=/data \
  -e BILIPDJ_DOCKER=1 \
  -e BILIPDJ_DOCKER_TRUSTED_CIDRS=auto \
  bilipdj-server
```

## 兼容说明

未设置 `BILIPDJ_DATA_DIR` 时，源码模式和冻结便携版继续使用原有运行目录布局；只有显式设置该变量时，用户数据才会切换到指定数据根目录。这样 Docker 可以安全挂载 `/data`，同时不改变 Windows / 源码现有安装的路径行为。
