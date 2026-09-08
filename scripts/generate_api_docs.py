from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

BASE_URL = "http://127.0.0.1:9816"


def ep(method: str, path: str, title: str, *, body: str = "", response: str = "", local: bool = True, notes: str = "") -> dict[str, Any]:
    return {"method": method, "path": path, "title": title, "body": body, "response": response, "local": local, "notes": notes}


HTTP_ENDPOINTS = [
    ep("GET", "/health", "健康检查", response='{"status":"ok","service":"bilipdj","port":9816}', local=False),
    ep("GET", "/model", "Bilibili 初始领域模型（调试）", response="JSON 模型结构"),
    ep("GET", "/api/config/basic", "读取基础配置（不含 Cookie）", response="platform、roomid、uid 等基础配置"),
    ep("GET", "/api/config/login", "读取 Bilibili 登录配置", response='{"uid":123,"cookie":"SESSDATA=..."}', notes="包含 Cookie，仅允许本机访问。"),
    ep("POST", "/api/config/login", "保存 Bilibili 登录配置", body='{"uid":123,"cookie":"SESSDATA=...; bili_jct=..."}', response='{"status":"ok","uid":123,...}'),
    ep("GET", "/api/config", "读取完整运行配置", response="完整配置 JSON，包含平台、权限、开关、样式等。"),
    ep("POST", "/api/config", "保存完整运行配置", body='{"platform":"bilibili","bilibili":{"roomid":3049445,"uid":123,"cookie":"..."}}', response='{"status":"ok",...}', notes="高级接口；建议先 GET 后按原结构修改。"),
    ep("GET", "/api/runtime-status", "读取后端/弹幕连接状态", response="平台、房间、连接状态、最近事件时间等。", local=False),
    ep("GET", "/api/platforms/active", "读取已激活弹幕平台", response='{"status":"ok","supported":["bilibili","douyin"],"active":["bilibili","douyin"],"one_room_per_platform":true,"runtime":{...}}', notes="当前每个平台最多监听一个直播间。"),
    ep("POST", "/api/platforms/active", "保存已激活弹幕平台并重连", body='{"active":["bilibili","douyin"]}', response='{"status":"ok","active":["bilibili","douyin"],"runtime":{...}}', notes="多个平台共享同一个后端队列；同一平台多个直播间暂不支持。"),
    ep("GET", "/api/danmu/identity/latest", "读取最近一条弹幕身份事件", response='{"status":"ok","event":{...}}'),
    ep("GET", "/api/gifts/state", "读取 B站礼物/插队状态", response='{"status":"ok",...}'),
    ep("GET", "/api/queue/state", "读取当前队列", response='{"queue":[...],"entries":[...],"size":1}', local=False),
    ep("GET", "/api/queue/archive", "读取队列存档槽状态", response="活动槽、存档快照与条目。"),
    ep("POST", "/api/queue/reload", "从当前存档槽重载队列", body="{}", response='{"status":"ok","queue":[...],"entries":[...]}'),
    ep("POST", "/api/queue/switch", "切换活动存档槽", body='{"slot":2}', response='{"status":"ok","slot":2,"queue":[...]}'),
    ep("POST", "/api/queue/delete", "删除队列项", body='{"index":1}', response='{"status":"ok","queue":[...]}', notes="index 为界面中的 1 起始序号。"),
    ep("POST", "/api/queue/move", "上移/下移队列项", body='{"index":2,"direction":"up"}', response='{"status":"ok","queue":[...]}', notes="direction: up / down。"),
    ep("POST", "/api/queue/insert", "插入队列项", body='{"after":0,"username":"用户名","content":"排队内容"}', response='{"status":"ok","queue":[...],"entries":[...]}', notes="username 必填，content 可留空；旧客户端仍可发送 entry 字段。after=0 表示队首。"),
    ep("POST", "/api/queue/update", "修改队列项内容", body='{"index":1,"content":"新内容"}', response='{"status":"ok","queue":[...]}'),
    ep("POST", "/api/queue/clear", "清空当前队列", body="{}", response='{"status":"ok","queue":[]}'),
    ep("POST", "/api/queue/log", "写入兼容队列操作日志", body='{"action":"...","content":"..."}', response='{"status":"ok"}', notes="兼容接口；新客户端通常不需要直接调用。"),
    ep("GET", "/api/blacklist/state", "读取黑名单", response='{"status":"ok","entries":[...],"size":1}'),
    ep("POST", "/api/blacklist/add", "添加黑名单", body='{"name":"用户名"}', response='{"status":"ok","entries":[...]}'),
    ep("POST", "/api/blacklist/delete", "删除黑名单项", body='{"index":1}', response='{"status":"ok","entries":[...]}'),
    ep("POST", "/api/blacklist/clear", "清空黑名单", body="{}", response='{"status":"ok","entries":[]}'),
    ep("GET", "/api/quanxian", "读取权限配置", response='{"status":"ok","super_admin":[],"admin":[],"jianzhang":[],"member":[]}'),
    ep("POST", "/api/quanxian", "保存权限配置", body='{"super_admin":["用户A"],"admin":["用户B"],"jianzhang":[],"member":[]}', response='{"status":"ok"}'),
    ep("GET", "/api/kaiguan", "读取功能开关", response='{"status":"ok","paidui":true,...}'),
    ep("POST", "/api/kaiguan", "保存功能开关", body='{"paidui":true,"guanfu_paidui":true}', response='{"status":"ok"}'),
    ep("GET", "/api/style", "读取队列显示样式", response='{"status":"ok","text_color":"#ffffff",...}'),
    ep("POST", "/api/style", "保存队列显示样式", body='{"text_color":"#ffffff","queue_font_size":50}', response='{"status":"ok"}'),
    ep("GET", "/api/appearance", "读取 Windows/Web 通用界面主题", response='{"status":"ok","exists":true,"appearance":{"schema":1,"design":"aurora","mode":"dark","dark":{...},"light":{...}}}', notes="由 Server 的 appearance.json 统一管理。Windows/Web 应读取同一份配置，不再各自维护独立主题文件。"),
    ep("POST", "/api/appearance", "保存 Windows/Web 通用界面主题", body='{"appearance":{"schema":1,"mode":"dark","font_family":"Microsoft YaHei UI","font_size":10,"radius":10,"dark":{...},"light":{...}}}', response='{"status":"ok","appearance":{...}}', notes="保存后 Windows/Web 可在下次刷新或轮询时同步。"),
    ep("GET", "/api/appearance/profile", "导出 Windows/Web/OBS 通用配置", response='{"schema":1,"kind":"bilipdj-appearance-profile","appearance":{...},"display_style":{...}}', notes="同一 JSON 文件可在 Windows 或 Web 端导出，再直接在另一端导入。display_style 对应现有 OBS/style.json。"),
    ep("POST", "/api/appearance/profile", "导入 Windows/Web/OBS 通用配置", body='{"schema":1,"kind":"bilipdj-appearance-profile","appearance":{...},"display_style":{...}}', response='{"status":"ok","kind":"bilipdj-appearance-profile","appearance":{...},"display_style":{...}}', notes="兼容完整 profile、直接 appearance.json、旧 Web localStorage 主题对象和旧 style.json。"),
    ep("GET", "/api/bili/qr/start", "创建 Bilibili 扫码二维码", response='{"data":{"qrcode_key":"...","qr_image_base64":"...","url":"..."}}'),
    ep("POST", "/api/bili/qr/poll", "轮询 Bilibili 扫码状态", body='{"qrcode_key":"..."}', response='{"data":{"code":0,"cookie":"...","uid":123}}', notes="code=86101 未扫码；86090 已扫码待确认；86038 失效；0 成功。"),
    ep("GET", "/api/backup/settings/config", "读取统一设置备份配置", response='{"status":"ok","config":{"backend":"webdav|local|smb",...}}'),
    ep("POST", "/api/backup/settings/config", "保存统一设置备份配置", body='{"config":{"backend":"local","local_dir":"D:\\\\Backup","keep_last":10}}', response='{"status":"ok","config":{...}}'),
    ep("GET", "/api/backup/settings/list", "列出设置备份历史", response='{"status":"ok","backups":[{"name":"BiliPDJ-settings-....zip",...}]}'),
    ep("POST", "/api/backup/settings/test", "测试备份目标", body="{}", response='{"status":"ok","message":"..."}'),
    ep("POST", "/api/backup/settings/run", "立即备份设置", body="{}", response='{"status":"ok","name":"BiliPDJ-settings-....zip",...}'),
    ep("POST", "/api/backup/settings/restore", "恢复指定设置备份", body='{"name":"BiliPDJ-settings-20260908-120000.zip"}', response='{"status":"ok","restored":[...]}'),
    ep("GET", "/api/backup/webdav/config", "读取旧版 WebDAV 备份配置（兼容）", response="与统一备份配置兼容。"),
    ep("POST", "/api/backup/webdav/config", "保存旧版 WebDAV 备份配置（兼容）", body='{"url":"https://dav.example.com/","username":"user","password":"..."}', response='{"status":"ok",...}'),
    ep("GET", "/api/backup/webdav/list", "列出旧版 WebDAV 备份（兼容）", response='{"status":"ok","backups":[...]}'),
    ep("POST", "/api/backup/webdav/test", "测试旧版 WebDAV（兼容）", body="{}", response='{"status":"ok"}'),
    ep("POST", "/api/backup/webdav/run", "执行旧版 WebDAV 设置备份（兼容）", body="{}", response='{"status":"ok",...}'),
    ep("POST", "/api/backup/webdav/restore", "恢复旧版 WebDAV 设置备份（兼容）", body='{"name":"BiliPDJ-settings-....zip"}', response='{"status":"ok",...}'),
    ep("GET", "/api/control/meta", "Web 控制台元信息", response='{"status":"ok","version":"2.0.4","platform":"bilibili","port":9816}'),
    ep("GET", "/api/control/logs?kind=all&limit=400", "Web 控制台日志读取", response='{"status":"ok","files":[...],"lines":[...]}', notes="kind: all/common/error/update；limit 20-2000。"),
    ep("GET", "/api/control/performance", "Web 控制台性能数据", response="系统 CPU/内存/磁盘、进程、队列、WebSocket 与 Relay 状态。"),
    ep("GET", "/api/control/update", "读取更新清单", response="当前版本、最新版本、Release URL，以及 Web 便携包 filename/url/sha256/size。", notes="客户端应先读取 manifest/本接口，再下载并校验 SHA-256，不要猜测资产文件名。"),
]

WS_ENDPOINTS = [
    {
        "path": "/ws",
        "title": "实时 WebSocket",
        "notes": "与 /danmu/sub 等价。浏览器从 LAN 接入时受 Server 只读保护，管理写操作仍限本机。",
    },
    {
        "path": "/danmu/sub",
        "title": "弹幕与队列实时订阅",
        "notes": "广播 QUEUE_UPDATE、PDJ_STATUS、DOUYIN_DANMU 等事件。",
    },
]

_ROUTE_RE = re.compile(r"[\"'](/(?:api/[^\"']*|health|model|ws|danmu/sub))[\"']")


def _canonical_path(path: str) -> str:
    return path.split("?", 1)[0]


def discover_server_paths(repo_root: Path) -> set[str]:
    paths: set[str] = set()
    for source in sorted((repo_root / "apps" / "server").glob("*.py")):
        text = source.read_text(encoding="utf-8")
        for match in _ROUTE_RE.finditer(text):
            paths.add(_canonical_path(match.group(1)))
    return paths


def documented_paths() -> set[str]:
    return {_canonical_path(item["path"]) for item in HTTP_ENDPOINTS} | {item["path"] for item in WS_ENDPOINTS}


def coverage_gap(repo_root: Path) -> set[str]:
    return discover_server_paths(repo_root) - documented_paths()


def _curl(item: dict[str, Any]) -> str:
    method, path = item["method"], item["path"]
    if method == "GET":
        return f"curl {BASE_URL}{path}"
    body = item.get("body") or "{}"
    return f"curl -X {method} {BASE_URL}{path} -H \"Content-Type: application/json\" -d '{body}'"


def render_http() -> str:
    chunks = [
        "# BiliPDJ HTTP API\n",
        f"默认地址：`{BASE_URL}`。管理接口默认只允许 `127.0.0.1 / ::1` 本机访问。\n",
        "所有 JSON POST 请求使用 `Content-Type: application/json`。对外提供 LAN 监听时，管理写接口仍由 Server 的 loopback / Origin / Host 防护限制。\n",
    ]
    for item in HTTP_ENDPOINTS:
        chunks.append(f"\n## `{item['method']} {item['path']}` — {item['title']}\n")
        chunks.append(f"访问范围：**{'仅本机' if item['local'] else '可读接口（可按 Server 监听配置访问）'}**。\n")
        chunks.append("\n示例：\n```bash\n" + _curl(item) + "\n```\n")
        if item.get("body"):
            chunks.append("\n请求 JSON 示例：\n```json\n" + item["body"] + "\n```\n")
        if item.get("response"):
            chunks.append("\n返回：\n```text\n" + item["response"] + "\n```\n")
        if item.get("notes"):
            chunks.append("\n注意：" + item["notes"] + "\n")
    return "".join(chunks)


def render_ws() -> str:
    chunks = ["# BiliPDJ WebSocket API\n", f"默认服务：`ws://127.0.0.1:9816`。\n"]
    for item in WS_ENDPOINTS:
        chunks.append(f"\n## `WS {item['path']}` — {item['title']}\n\n{item['notes']}\n")
    chunks.append(
        "\n常见消息：\n```json\n"
        '{"type":"QUEUE_UPDATE","queue":["..."],"entries":[...]}\n'
        '{"type":"PDJ_STATUS","status":"danmu_connected","platform":"bilibili","roomid":3049445}\n'
        '{"type":"DOUYIN_DANMU","nickname":"用户","content":"弹幕内容","platform":"douyin"}\n'
        "```\n"
    )
    return "".join(chunks)


def render_readme() -> str:
    return """# BiliPDJ 后端开放接口说明

本目录由 `python scripts/generate_api_docs.py` 自动生成，并被仓库根 `.gitignore` 的 `/api/` 规则排除。

- `http.md`：HTTP/JSON 接口、参数、curl 示例和返回说明。
- `websocket.md`：WebSocket 入口与常见消息。

安全规则：管理配置、登录 Cookie、备份、权限、开关、队列修改、主题/配置导入导出、控制台日志/性能等接口只允许本机访问。只读队列/运行状态/WebSocket 是否可从 LAN 访问取决于 Server 的监听配置和运行时安全保护。

接口发生变化后重新运行生成器；测试会检查 Server 源码中的开放路由是否存在未登记项。
"""


def generate(repo_root: Path, output: Path | None = None) -> Path:
    gap = coverage_gap(repo_root)
    if gap:
        raise RuntimeError("存在未登记的 Server 路由：" + ", ".join(sorted(gap)))
    target = output or (repo_root / "api")
    target.mkdir(parents=True, exist_ok=True)
    (target / "README.md").write_text(render_readme(), encoding="utf-8")
    (target / "http.md").write_text(render_http(), encoding="utf-8")
    (target / "websocket.md").write_text(render_ws(), encoding="utf-8")
    return target


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate ignored BiliPDJ API reference under api/.")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    target = generate(repo_root, args.output)
    print(f"Generated API reference: {target}")


if __name__ == "__main__":
    main()
