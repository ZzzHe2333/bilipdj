# JavaScript Plugin `host.httpRequest`

`host.httpRequest(url, options)` 是 JavaScript `.bilipdj-plugin` 在 QuickJS 沙箱中访问 HTTP API 的受控 Host 能力。插件 manifest 必须声明：

```json
"permissions": ["network"]
```

没有 `network` 权限时 Host 会拒绝调用。URL 仅允许 `http://` 与 `https://`。

## 基本 GET

旧写法继续兼容：

```javascript
const response = host.httpRequest("https://example.com/api", {
  headers: {"Accept": "application/json"},
  timeout: 10
});
```

`method` 省略时默认为 `GET`。

## POST 文本 body

```javascript
const response = host.httpRequest("https://example.com/api", {
  method: "POST",
  headers: {"Content-Type": "application/json"},
  body: JSON.stringify({room_id: "12345"}),
  timeout: 10
});
```

`body` 必须是字符串，Host 以 UTF-8 编码发送。

## POST 二进制 body

```javascript
const response = host.httpRequest("https://example.com/api", {
  method: "POST",
  body_base64: "AAEC/w=="
});
```

`body_base64` 必须是合法 Base64。`body` 与 `body_base64` 不能同时出现。

## 返回值

```json
{
  "status": 200,
  "headers": {
    "content-type": "application/json",
    "etag": "\"abc\""
  },
  "data_base64": "eyJvayI6dHJ1ZX0="
}
```

- `status`：HTTP 状态码。
- `data_base64`：响应体原始字节的 Base64。
- `headers`：Host 白名单允许暴露的响应头，键统一为小写。

当前响应头白名单：

```text
cache-control
content-encoding
content-language
content-length
content-type
date
etag
expires
last-modified
location
retry-after
```

`Set-Cookie`、认证头和其他未列出的响应头不会传给插件。

HTTP 4xx/5xx 会作为正常 HTTP 响应返回。例如 429 可返回：

```json
{
  "status": 429,
  "headers": {"retry-after": "10"},
  "data_base64": "Li4u"
}
```

DNS 失败、连接失败、TLS/网络错误、超时等传输层错误仍会作为 Host 调用错误抛出。

## 安全限制

- 方法仅允许 `GET`、`POST`。
- GET 不允许 body。
- 请求 body 最大 128 KiB；该值特意低于 1 MiB Host JSON 信封限制，确保 Base64 和 JSON 转义后仍有安全余量。
- 响应 body 最大 4 MiB。
- 最多 64 个请求头。
- 单个请求头名称最大 128 UTF-8 字节。
- 单个请求头值最大 8192 UTF-8 字节。
- 请求头名称/值禁止 CR/LF，避免头注入。
- timeout 被限制在 1–30 秒。
- HTTP 等明确阻塞 Host API 会向父进程 watchdog 报告阻塞窗口，不会把正常网络等待误判为 JavaScript 死循环。

## 设计边界

首期不开放 PUT/PATCH/DELETE 等任意方法，也不返回全部响应头。需要新增能力时应通过新的 BiliPDJ 版本明确扩展 Host 契约，而不是让插件绕过 Host 权限边界。
