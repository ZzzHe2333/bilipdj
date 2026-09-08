# BiliPDJ Server

`apps/server/` 现在保存后端的真实实现：Bilibili/抖音协议、队列逻辑、权限/礼物相关运行时、HTTP API、WebSocket、日志和安全/性能 guard 都从这里加载。

`core.server` 等旧导入仍可用，但只作为兼容转发层。

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

从仓库根目录执行：

```bash
docker build -f apps/server/Dockerfile -t bilipdj-server .
docker run --rm -p 9816:9816 bilipdj-server
```

## 兼容说明

本阶段只迁移实现代码，没有同时改变用户运行数据位置。源码模式下配置和队列数据仍兼容原来的 `core/` 路径；后续迁移数据目录不会影响 `apps.server` 的公开入口。
