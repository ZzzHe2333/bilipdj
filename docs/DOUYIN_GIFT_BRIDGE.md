# 抖音网页版礼物桥接（BiliPDJ Python）

Issue #319。仅适用于 BiliPDJ Python 版；不修改 Go 版，不替换现有 HTTP Fetch 弹幕通道。本功能属于用户自己已登录浏览器上的 WSS 消息监听，不是官方抖音开放 API。

## 新增：本机官方扫码登录与参数提取（Issue #321）

仅在具备图形桌面的 Python 后端主机上使用。程序会使用一个**独立于日常 Chrome/Edge 的用户配置目录**打开 `https://live.douyin.com/`，需要用户本人通过抖音 App 在官方页面扫码确认；不会向 BiliPDJ Web API 传输验证码、账号密码、Cookie 或 WebSocket 签名。

在 **Web 控制台 → 设置 → 平台参数** 操作；Windows Tk 桌面端的「平台参数 → 抖音参数」也增加了「扫码登录」和「登录后获取」按钮（需先启动本机后端）：



1. 点击 **抖音扫码登录**；在弹出的本机 Chrome/Edge 窗口中，用抖音 App 扫码。窗口不是浏览器里嵌入的二维码。登录完成后会自动关闭，页面显示「扫码成功」。
2. 在「抖音直播间链接 / web_rid」填写自己需要监听的直播间链接或数字 web_rid，点击 **获取并保存直播参数**。
3. BiliPDJ 会用已保存在**本机私有目录**中的登录会话向官方网页读取参数，回填 `live_id` 和内部 `room_id` 等配置，并请求弹幕监听重连；Web 页面仅显示经过脱敏的房间状态、标题和 ID。
4. 选择 **启动后台礼物采集**，程序将以独立的 Chrome/Edge 登录 Profile 在无头模式下打开配置好的一个直播间，复用原有礼物 WSS 解码脚本，再把数据送入已有 BiliPDJ 礼物兼容与连击结算模块。也可点击「停止采集」。无需另起 8787 看板、无需手动复制 Cookie；仍可选择使用原有两份 Tampermonkey 脚本。**为避免重复统计，针对同一个直播间不要同时运行两种采集方式。**

后台采集遵循**一个抖音直播间对应一个采集器**的现有 BiliPDJ 限制；仅在用户点「启动」后运行，不会自动创建第三方服务器。扫码和无头模式互斥，重新扫码时先停止采集。采集失败会在本机自动重试，浏览器进程退出时会清理。若服务重启，需要在 Web 控制台再次点击「启动后台礼物采集」；当前没有持久化自动启动开关。服务必须运行在能够启动 Chrome/Edge 的主机上，Docker 容器需要另行安装浏览器及依赖，默认镜像不保证支持该模式。

本地状态和参数接口：

- `GET /api/douyin/collector/status`：后台采集状态、数量、非敏感错误文本。
- `POST /api/douyin/collector/start` / `POST /api/douyin/collector/stop`：手动启停后台礼物采集。
- `GET /api/douyin/login/status`：是否已有登录态、扫码流程是否完成；不返回凭据。
- `POST /api/douyin/login/start`，JSON `{}`：开始一次官方扫码登录。
- `POST /api/douyin/login/params`，JSON `{"live_id":"100000000000","apply":true}`：读取/保存房间配置，只返回非敏感字段。

安全约束：仅允许**本机、同源 JSON 管理请求**执行扫码/读取参数；登录状态保存在用户私有的 key 目录下（独立 Chrome Profile 及 `douyin-browser-session.json`，POSIX 权限 `0700/0600`）。不要把私有数据目录、浏览器 Profile 或配置文件复制到仓库/网盘/日志/工单。抖音会话到期后需重新扫码。若有原先手动填写的 Cookie，旧版配置仍可沿用；网页不回显 Cookie，输入框留空保持原值。

注意：这里复用了压缩包中 **官方网页 + 独立 Profile + CDP 读取白名单内的登录 Cookie 并仅在本机私有目录保存** 的机制，但没有提交压缩包中真实房间号、直播观众 ID、礼物样本、用户昵称、签名和本地路径。浏览器调试端口随机且绑定回环地址，扫码完成会关闭。如果 Chrome/Edge 安全策略禁用远程调试，则无法完成自动获取；可回退手动配置。Docker 或远程 Ubuntu 无图形桌面时无法在访问 Web 的电脑上弹出扫码窗口；需在后端所在主机使用本机图形桌面。

## 原始压缩包与隐私清理

本次仅将代码逻辑转译为 Python 后端模块，不打包上传 `samples/gifts_sample.json`、日志/抓包快照、用户昵称、会话/签名数据或原始调试 Profile。**不要直接把上传的 ZIP 复制到公开仓库**。详见独立安全测试 `.github/ci/issue321_douyin_login_guard.py`。

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

## 礼物排队 / 插队（按礼物单独配置）

Web 控制台 → 设置 → **抖音礼物** 页：勾选启用，然后为一种或多种礼物分别填写**排队次数**和**插队次数**。

- **插队次数 > 0**：送礼者获得对应次数的插队资格，发送「插队」即可跳到「B站礼物」里设置的**插入名次**。
- **插队次数 = 0**：该礼物只用于**排队**，送礼者发送「排队」时消耗 1 次资格（这就是「插队=0 就是排队」）。
- 次数按礼物累加（送几次给几次），每发一次「排队」/「插队」指令消耗 1 次。
- 有排队资格时，可以突破「队列已满」和「仅允许礼物排队」两个限制；插队资格沿用原有逻辑。
- 默认规则：抖音 **粉丝团灯牌**（gift_id `685`）→ 排队 1 次、插队 0 次。
- 规则按平台隔离：抖音规则不会命中 B 站礼物，反之亦然；B 站原有的「指定礼物名称 + 每次可排人数」逻辑保持不变。
- 规则文件：`core/gift_queue_rules.json`（与 `config.yaml` 同目录）。`config.yaml` 的 myjs 只保存扁平标量，所以规则单独存放。
- 接口：`GET /api/gifts/state` 返回 `rules_enabled` / `rules` / `default_rules` / `pending_queue_credits` / `pending_insert_credits`；`POST /api/gifts/queue-rules`，JSON `{"enabled":true,"rules":[{"platform":"douyin","gift_id":"685","gift_name":"粉丝团灯牌","queue_count":1,"insert_count":0}]}`，仅本机可访问。
- 安全提示：浏览器上报的礼物事件无法证明真实交易，打开该开关即表示你接受这一风险；不要把它用于有经济价值的资格。

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

### 未登录的网页收不到礼物消息（实测结论）

抖音的网页 IM 通道对**未登录**访客仍会推送弹幕、进场、点赞、房间统计等消息，但**不推送 `WebcastGiftMessage`**。用匿名浏览器在三个不同直播间连续抓取约 8 分钟、共 4000+ 条消息，礼物消息数为 0；同一条连接里弹幕/进场/点赞正常。因此礼物功能必须以扫码登录后的会话为前提，这不是解码脚本的 bug。

反过来验证：把扫码得到的会话 Cookie 回放到无头浏览器后，同一个直播间 70 秒内收到 19 条 `WebcastGiftMessage`，桥接 `received=19 / settled=11`，`/api/gifts/state` 的 `last_event` 与 `observed_catalog` 都正确出现了抖音礼物（小心心=1 钻、大啤酒=2 钻、为你闪耀=9 钻等）。

### 扫码会话必须回放到采集浏览器

扫码流程是通过 CDP `Storage.getCookies` 读取 Cookie 后写入本机私有文件 `douyin-browser-session.json` 的；这些 Cookie **不保证**会被 Chrome 落盘到 `douyin-browser-profile` 的 Cookie 数据库。无头采集器复用同一个 Profile 时，页面可能因此仍是未登录状态（`collector/status` 的 `browser_logged_in=false`），表现为 IM 能连上、弹幕正常，但礼物永远是 0。

因此采集器启动时会检查浏览器里是否存在 `sessionid` Cookie，缺失时用 `Network.setCookie` 把本机保存的会话回放到该浏览器，再访问直播间。回放只发生在本机进程内，不经过任何 HTTP 接口。

排查顺序：

1. `GET /api/douyin/login/status` 确认 `logged_in` 为真；否则先扫码。
2. `GET /api/douyin/collector/status`：先看 `browser_logged_in` 是否为 `true`——为 `false` 时页面是未登录状态，收不到礼物；`im_session_identified` 应为 `true`（不回显实际账号 ID）。
3. `frames`/`messages` 持续增长说明 IM 已连上；`messages` 一直为 0 说明页面没有建立 IM 连接（常见原因：播放器被暂停、直播间已下播）。
4. `methods` 里出现 `WebcastGiftMessage` 说明礼物消息已进入解码；`gifts`/`accepted` 增长说明桥接吃下了。
5. `page_video_seconds` 不增长说明播放器没有真正开始播放，此时也不会建立 IM 连接。
6. `rejected` 持续增长说明桥接房间号不匹配：重新「获取并保存直播参数」，确认 `live_info.room_id` 与当前直播间一致。

### 后台采集不再改写页面脚本

`DouyinHeadlessCollector` 现在通过 DevTools 协议在浏览器层抓取 `Network.webSocketFrameReceived`，再由 `apps/server/douyin_ws_frames.py` 解码 `PushFrame -> LiveResponse -> WebcastGiftMessage`，不再向页面注入改写 `window.WebSocket` 的脚本，因此不受页面脚本变化影响。启动前会先访问 `https://www.douyin.com/` 取得 `ttwid` 等访客 Cookie，并带 `--autoplay-policy=no-user-gesture-required` 启动无头浏览器，避免播放器被静音策略暂停导致 IM 不连接。若 60 秒内仍未出现任何 IM 消息，采集器会明确报错并重试，而不是静默空转。

