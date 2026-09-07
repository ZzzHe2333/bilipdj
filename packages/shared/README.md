# Shared contracts

`packages/shared/` 预留给 Windows 与 Web 真正共用的稳定协议，而不是复制业务逻辑。

当前两个前端都通过同一套 HTTP/WebSocket 后端通信，因此首先把接口契约放在这里；等桌面端逐步从 Tk 兼容实现迁出后，可继续加入共享类型、事件名和客户端 SDK。

原则：

- 队列、权限、礼物、平台连接等业务状态只由 Server 计算。
- Windows 与 Web 不各自实现一套排队规则。
- 共用内容只放协议/类型/客户端，不把平台连接代码放入 shared。
