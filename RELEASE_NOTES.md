# 弹幕排队姬 v1.0.11

v1.0.11 是针对 v1.0.10 本地管理请求日志异常的热修复版本。

## 修复内容

### 本机客户端提前断开时不再打印错误堆栈

桌面控制台、浏览器配置页或透明窗口在刷新、关闭窗口、快速重复保存时，可能在后端写回 JSON 前主动取消本机 HTTP 请求。Windows 会将该情况报告为：

```text
ConnectionAbortedError: [WinError 10053]
```

该异常只表示客户端已经不再需要响应，不代表队列、配置保存或后端服务崩溃。旧版本未处理该网络状态，因此 `ThreadingHTTPServer` 会把完整 traceback 输出到 Log 框。

v1.0.11 现在会：

- 静默处理 `WinError 10053`、`WinError 10054`、BrokenPipe、ConnectionReset 和 ConnectionAborted；
- 将当前 HTTP 连接标记为关闭，不影响后续新请求；
- 保留其他未知 `OSError` 的正常抛出，避免掩盖真实程序错误；
- 增加回归测试，验证客户端断开被忽略，而非网络类异常仍会报告。

## 影响说明

- 已经完成的 POST 操作不会回滚；客户端断开只发生在服务器返回结果阶段。
- 队列、配置、黑名单和样式数据不会因为该异常丢失。
- 修复后 Log 框不再出现对应的 `Exception occurred during processing of request` 堆栈。

## 升级说明

Windows v1.0.10 用户可在程序“关于”页检查并自动安装 v1.0.11。

手动下载文件：

- `bilibili-danmuji-windows-x64-v1.0.11.zip`
- `bilibili-danmuji-windows-x64-v1.0.11.zip.sha256`

---

版本号：`v1.0.11`  
发布日期：`2026-07-30`
