# 抖音网页版礼物桥接（BiliPDJ Python）

Issue #319。仅适用于 BiliPDJ Python 版；不修改 Go 版，不替换现有 HTTP Fetch 弹幕通道。本功能属于用户自己已登录浏览器上的 WSS 消息监听，不是官方抖音开放 API。

## 安装

1. 在 Chrome / Edge 安装 Tampermonkey 或兼容的用户脚本管理器。
2. 依次安装仓库内的两份脚本：
   - [礼物捕获脚本](../tools/douyin/douyin_live_gift_hook.user.js)：监听页面已有的 WebSocket，解析 gzip + protobuf，输出礼物 JSON。
   - [本机桥接脚本](../tools/douyin/bilipdj_douyin_gift_bridge.user.js)：通过用户脚本管理器专用的 GM_xmlhttpRequest，将事件传给本机 Python 后端。
3. 在 BiliPDJ 里启用抖音直播，配置直播间链接，让原有 HTTP 监听获取内部 room_id。内部 room_id 不一定等于浏览器地址栏的 live_id。
4. 在浏览器本机打开 http://127.0.0.1:9816/api/douyin/gift-hook/status ，确认 configured_room_id 不为空。状态响应给出 token_file 路径但不返回令牌。
5. 首次启动自动在 key 目录生成 douyin-gift-bridge-token.txt（Unix 文件模式 0600）；打开并复制令牌，不要提交、共享令牌文件。也可预先通过环境变量 BILIPDJ_DOUYIN_GIFT_TOKEN 指定至少 32 字符的自有随机令牌。
6. 打开 Tampermonkey 第二份脚本的菜单“设置 BiliPDJ 礼物桥接”，输入后端端口（默认 9816）及上述令牌。必须刷新已登录的抖音直播网页，让 WSS Hook 在页面连接创建前生效。
7. 直播收到礼物后检查日志中的 [礼物][douyin]、状态接口 received/settled，以及 /api/gifts/state 的 last_event。控制台可查看 window.__douyinGifts / window.__douyinMsgCounts 排障。

## 通信与权限

本机接口为 POST /api/douyin/gift-hook，要求 localhost、Bearer token、JSON、消息大小不超过 16 KiB、消息中的 roomId 匹配 BiliPDJ 当前已启用的抖音内部房间 ID。接口拒绝未配置房间或任意外部房间 ID；不会存储网页登录 Cookie、WebSocket 签名或抖音账号凭据。桥接脚本仅发送到 127.0.0.1，不允许指定任意公网服务端。

若是 Docker，浏览器必须位于映射 9816 端口的本地主机；远端浏览器连接 localhost 不会自动到达 Docker 主机。请勿为此开放公网管理接口。

## 连击与兼容性

- repeatCount 是累计值，同一轮连击按最高值结算。例如 1 → 57 → 520 结算 520，不得求和。
- 同一 msgId 先去重；repeatEnd=1 结算；5 秒未更新自动结算。对迟到帧根据 createTime 和累计计数做有限抑制。groupId 缺失时同秒连续礼物存在无法完全判断的歧义。
- 内部抖音用户 ID 与弹幕的数字 UID 使用一致的队列标识，并与 B 站命名空间隔离，避免资格串用。
- 礼物显示价值单位为抖音“钻”，不能当作 B 站“电池”；兼容规则应单独配置 douyin 平台。
- 礼物触发排队资格默认关闭，浏览器网页事件可伪造，不能证明真实交易。
- 只有本地操作人员了解风险并主动设置 BILIPDJ_DOUYIN_GIFT_QUEUE_CREDIT=1 时，才会允许桥接数据进入礼物排队资格逻辑。仍需核对现有统一门槛的计量单位，不建议用于任何有经济价值的资格。

## 离线测试

在仓库根目录运行：

    python .github/ci/issue319_douyin_gift_bridge_guard.py
    node --check tools/douyin/douyin_live_gift_hook.user.js
    node --check tools/douyin/bilipdj_douyin_gift_bridge.user.js

原始礼物样本包含直播用户的 ID/头像等数据，因此不复制进公开仓库；测试使用合成的匿名事件覆盖连击、迟到帧、重复消息、鉴权与跨平台身份。

## 已知限制

必须保持已登录的抖音网页开启，才能收到礼物。关闭浏览器后不会继续监听；这个接入并非不依赖浏览器的原生 WSS 客户端。私有 Protobuf 字段随抖音调整可能失效；无法承诺长期兼容。必须使用支持 DecompressionStream 的现代浏览器。
