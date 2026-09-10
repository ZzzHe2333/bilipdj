# BiliPDJ Plugin API v1

BiliPDJ 的外部插件包扩展名为 `.bilipdj-plugin`，本质是受校验的 ZIP 包。安装阶段只解析和校验文件，不执行插件代码；插件默认安装为禁用状态，只有显式启用后才加载运行时。

## 包结构

最小 Python 插件：

```text
kuaishou-python.bilipdj-plugin
├─ manifest.json
└─ plugin.py
```

最小 JavaScript 插件：

```text
kuaishou-js.bilipdj-plugin
├─ manifest.json
└─ plugin.js
```

包内禁止绝对路径、`..`、Windows 驱动器路径/保留设备名、符号链接、重复文件和异常压缩比。插件包、单文件、文件数量和解压后大小也有安全上限。

## manifest.json

Python 示例：

```json
{
  "schema": 1,
  "id": "example.kuaishou.danmu",
  "name": "快手获取弹幕插件",
  "version": "1.0.0",
  "plugin_api": 1,
  "type": "danmu_source",
  "platform": "kuaishou",
  "runtime": "python",
  "entry": "plugin.py:Plugin",
  "min_bilipdj_version": "2.0.4",
  "permissions": ["network"],
  "capabilities": ["danmu"],
  "files": {
    "plugin.py": "<plugin.py 的 SHA-256>"
  }
}
```

JavaScript 示例：

```json
{
  "schema": 1,
  "id": "example.kuaishou.js.danmu",
  "name": "快手获取弹幕插件（JS）",
  "version": "1.0.0",
  "plugin_api": 1,
  "type": "danmu_source",
  "platform": "kuaishou_js",
  "runtime": "javascript",
  "entry": "plugin.js",
  "min_bilipdj_version": "2.0.4",
  "permissions": ["network"],
  "capabilities": ["danmu"],
  "files": {
    "plugin.js": "<plugin.js 的 SHA-256>"
  }
}
```

可选 `max_bilipdj_version`。`files` 必须列出除 `manifest.json` 外的全部包内文件，且每项必须是 64 位十六进制 SHA-256。

为保证签名验证没有歧义，`id`、`platform`、`entry`、`files` 路径和权限名必须直接使用规范形式：ID/平台使用小写，路径使用 `/`，字段前后不能带空白。BiliPDJ 不会在签名验证前偷偷排序权限数组；`permissions` 的原始数组顺序属于签名内容的一部分。

## 权限

Plugin API v1 识别以下权限：

- `network`：使用 Host HTTP / WebSocket 网络能力。
- `filesystem_read`：读取插件自己的 `plugins/data/<plugin-id>/` 数据目录。
- `filesystem_write`：写入插件自己的数据目录。
- `subprocess`：通过 Host API 启动子进程。
- `secrets`：读取当前平台配置中的 Cookie、Token 等敏感字段。

未知权限会直接拒绝安装。

### Python 权限边界

Python 插件拿到的是 `PluginContext`，不会拿到完整 BiliPDJ Server 对象；Host API 会检查 manifest 声明的权限。但是 Python 本身仍是原生代码运行时，能自行 import Python/系统模块，因此 **Plugin API 权限不是操作系统级 Python 沙箱**。管理接口会把这类插件标记为 `permission_enforcement=python-full-trust`。只应启用来源可信或签名可信的 Python 插件。

### JavaScript 权限边界

JavaScript 插件由内嵌 QuickJS 执行，不依赖系统 Node.js。默认没有 Node/浏览器文件系统、网络、进程 API，只能通过 BiliPDJ 提供的 Host API 使用 manifest 已声明的能力，管理接口会标记为 `permission_enforcement=host-enforced`。

JavaScript 运行在独立工作进程中。QuickJS 设置内存和栈上限；父进程同时对每次 JavaScript 执行阶段设置 watchdog。默认单次纯 JS 执行超过约 0.5 秒会终止插件工作进程并把该插件标记为错误。HTTP、WebSocket 接收、`sleep()`、子进程等明确的阻塞 Host API 会按各自受限超时时间临时延长 watchdog，因此正常网络等待不会被误杀。插件崩溃、死循环或超时不会直接拖垮 BiliPDJ 主后端。

## Python 入口

入口格式：

```text
plugin.py:Plugin
```

插件对象需要实现：

```python
class Plugin:
    def create_relay(self, context, config):
        return Relay(context, config)
```

返回的 Relay 至少实现 `start()`；建议同时实现：

```text
stop()
join(timeout=None)
request_reconnect()
get_runtime_status() -> dict
```

`context` 提供经过权限检查的 Host 能力，包括配置、敏感字段、事件广播、弹幕入口、HTTP、私有数据目录和子进程。

## JavaScript 入口

JavaScript 入口是单个 `.js` 文件，必须定义全局：

```javascript
function createRelay(config, host) {
  let status = { connected: false };

  return {
    start() {
      status.connected = true;
      host.setStatus(status);
    },

    tick() {
      // 保持单次纯 JS 处理很短；网络等待请使用 Host API。
    },

    stop() {
      status.connected = false;
      host.setStatus(status);
    },

    requestReconnect() {},

    getRuntimeStatus() {
      return status;
    }
  };
}
```

BiliPDJ 在独立子进程里创建 QuickJS Context，并周期调用 `tick()`。每次纯 JavaScript 执行由父进程 watchdog 限时；QuickJS Context 同时有内存和栈限制。Windows/Web 便携入口已启用 `multiprocessing.freeze_support()`，不要求用户单独安装 Node.js 或额外启动器。

### JavaScript Host API

核心方法：

```text
host.getConfig()
host.getSecret(key)                   // secrets
host.emit(payload)
host.processDanmu(payload)
host.setStatus(payload)
host.httpRequest(url, options)        // network
host.wsConnect(url, headers)          // network
host.wsRecv(handle, timeoutMs)        // network
host.wsSend(handle, data, binary)     // network
host.wsClose(handle)                  // network
host.readData(path)                   // filesystem_read
host.writeData(path, base64Data)      // filesystem_write
host.runProcess(argv, timeoutSeconds) // subprocess
host.sleep(ms)
host.isStopping()
host.consumeReconnect()
host.base64ToBytes(base64)
```

`wsRecv()` 返回：

```json
{"type":"text","data":"..."}
```

或：

```json
{"type":"binary","data_base64":"..."}
```

超时返回：

```json
{"type":"timeout"}
```

## 弹幕进入 BiliPDJ

为了复用现有 QueueManager，外部插件可以把解析后的弹幕转换成 Bilibili 兼容弹幕 JSON，再调用：

```javascript
host.processDanmu(payload)
```

或 Python：

```python
context.process_danmu_json(payload)
```

平台自己的状态/原始事件可通过 `emit()` 广播给 Web/Windows/OBS。

## SHA-256 与完整性

安装时 BiliPDJ：

1. 校验包结构和 `manifest.files`。
2. 逐文件验证 SHA-256。
3. 计算完整 `.bilipdj-plugin` 包 SHA-256。
4. 保存原始插件包快照。
5. 后续加载/手动校验时重新验证文件，并要求已安装 `manifest.json` 与原始包中的 manifest 完全一致。
6. 检查已安装插件目录，只允许 manifest 声明文件和 BiliPDJ 自己的安装元数据；额外塞入的未声明文件或符号链接会让插件失效。Python 运行产生的 `__pycache__` 会在重新校验时清理。

因此修改入口文件、增加额外代码、替换文件、添加符号链接、修改哈希清单、权限或 manifest 都会使插件失效。

## Ed25519 签名

可在 manifest 增加：

```json
"signature": {
  "algorithm": "ed25519",
  "key_id": "publisher.example",
  "value": "<Base64 Ed25519 signature>"
}
```

签名数据是 **删除 `signature` 字段后的 manifest**，按 UTF-8 JSON 进行确定性序列化：

```text
sort_keys=true
ensure_ascii=false
separators=(",", ":")
```

然后直接对这些字节做 Ed25519 签名。数组顺序保持原样，因此 `permissions`、`capabilities` 等数组的顺序也是签名内容的一部分；发布者应先生成最终 manifest，再签名，不要依赖 BiliPDJ 自动规范化字段。

签名插件必须使用插件管理器中已登记的可信 `key_id`，签名无效或公钥不可信会拒绝安装/加载。删除可信公钥后，重新发现插件时相应签名插件会失去可用状态并从平台注册表移除。

未签名本地插件只能在安装时显式勾选“允许安装未签名的本地插件”，并且仍然经过路径、版本、权限、哈希和完整性校验。

## 生命周期

安装：

```text
选择 .bilipdj-plugin
→ 安全解包检查
→ manifest / 版本 / 权限检查
→ 文件 SHA-256
→ 签名验证（如有）
→ 原子安装
→ 默认禁用
```

启用：

```text
重新校验完整性
→ Python：同进程全信任运行时 / JavaScript：独立 QuickJS 工作进程
→ 注册 DanmuPluginRegistry
→ /api/platforms/active 自动出现平台
```

禁用/卸载会从注册表移除插件；通过管理 API 操作时会同步现有 `active_platforms` 并触发 Relay 重连。

## 管理 API

插件管理接口只允许本机访问：

```text
GET  /api/plugins/manage
POST /api/plugins/install
POST /api/plugins/enable
POST /api/plugins/disable
POST /api/plugins/uninstall
POST /api/plugins/verify
GET  /api/plugins/trusted-keys
POST /api/plugins/trusted-keys
POST /api/plugins/trusted-keys/delete
```

Web 控制台的“设置 → 插件管理”使用同一组 API。

## 平台无关 DanmuEvent（Issue #142）

新的获取弹幕插件应把平台消息规范化后交给统一事件入口，不再伪造 Bilibili `DANMU_MSG/info`：

Python：

```python
context.process_danmu_event({
    "user_id": "platform-native-user-id",
    "username": "Alice",
    "content": "排队",
    "is_room_admin": False,
    "metadata": {"message_id": "abc"},
})
```

`platform` 可以省略，`PluginContext` 会绑定为插件 manifest 自己声明的平台；插件不能替其他平台发事件。也可以直接传入 `apps.server.danmu_event.DanmuEvent`。

JavaScript：

```javascript
host.processDanmuEvent({
  user_id: "platform-native-user-id",
  username: "Alice",
  content: "排队",
  is_room_admin: false,
  metadata: {message_id: "abc"}
});
```

规范字段包括 `platform`、`user_id`、`username`、`content`、`is_room_admin`、`is_anchor`、`is_guard`、`guard_level`、`guard_name`、`fan_medal`、`received_at`、`metadata`。`user_id` 是字符串，可安全保存非数字平台 ID。

旧 Python `context.process_danmu_json(payload)` 和 JavaScript `host.processDanmu(payload)` 继续支持，但它们仅作为 Bilibili `DANMU_MSG` 兼容接口；新插件应使用 `process_danmu_event` / `processDanmuEvent`。
