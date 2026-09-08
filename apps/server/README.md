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

从仓库根目录执行：

```bash
docker build -f apps/server/Dockerfile -t bilipdj-server .
docker run --rm -p 9816:9816 bilipdj-server
```

## 兼容说明

源码模式下配置和队列数据仍兼容原来的 `core/` 路径；冻结的便携发行包把运行数据放在发行目录中，以便整体复制和迁移。
