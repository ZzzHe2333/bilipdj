# BiliPDJ Server

独立后端入口。直播平台接入、队列状态、权限、礼物、存档、HTTP API 和 WebSocket 仍由现有 `core/` 实现层提供；本目录负责把后端作为独立应用启动，并默认加载 `apps/web/static`。

## 本地启动

```bash
python -m pip install -r apps/server/requirements.txt
python -m apps.server.main
```

指定监听地址：

```bash
python -m apps.server.main --host 0.0.0.0 --port 9816
```

指定已经构建好的 Web 目录：

```bash
python -m apps.server.main --web-dir apps/web/dist
```

## Docker

从仓库根目录执行：

```bash
docker build -f apps/server/Dockerfile -t bilipdj-server .
docker run --rm -p 9816:9816 bilipdj-server
```

> 当前阶段保留 `core/` 作为兼容实现层，以保证旧发行版、测试和 macOS 打包不被破坏。后续可以逐模块把纯后端代码继续迁入本目录，而不用再调整 Web/Windows 的目录边界。
