// myjs.js — 队列显示端
// 排队逻辑由 Python 后端处理；本文件只负责接收后端推送的队列并渲染到页面。

var zroomid = 0;
var zuid = 0;
var ws = null;
var pdjConnected = false;

// ---------- 工具函数 ----------

function escapeHtml(text) {
    return String(text || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}

function PDJ_EmitStatus(status, detail) {
    window.dispatchEvent(new CustomEvent("pdj:status", {
        detail: Object.assign({ status: status, roomid: zroomid, uid: zuid }, detail || {})
    }));
}

function PDJ_ReloadStylesheet() {
    var link = document.getElementById("pdj-style-link");
    if (!link) return;
    link.href = "moren.css?v=" + Date.now();
}

// ---------- 队列渲染 ----------

function PDJ_RenderQueue(queue) {
    if (!Array.isArray(queue)) return;
    var html = queue.map(function(item) {
        return "<span>" + escapeHtml(String(item || "")) + "<br></span>";
    }).join("");
    document.getElementById("danmu").innerHTML = html;
}

// ---------- 配置加载（仅读取 roomid / uid，供状态显示用） ----------

async function PDJ_LoadConfig() {
    try {
        var res = await fetch("/api/config");
        if (!res.ok) return;
        var cfg = await res.json();
        zroomid = Number(cfg.roomid || 0);
        zuid = Number(cfg.uid || 0);
        PDJ_EmitStatus("config_loaded");
    } catch (err) {
        console.error("[PDJ] 配置读取失败", err);
    }
}

// ---------- WebSocket 连接 ----------

function PDJ_GetWebSocketURL() {
    if (window.location && (window.location.protocol === "http:" || window.location.protocol === "https:")) {
        var protocol = window.location.protocol === "https:" ? "wss://" : "ws://";
        return protocol + window.location.host + "/danmu/sub";
    }
    return "ws://127.0.0.1:9816/danmu/sub";
}

async function PDJ_Connect() {
    await PDJ_LoadConfig();

    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
        return;
    }

    ws = new WebSocket(PDJ_GetWebSocketURL());

    ws.onopen = function() {
        pdjConnected = true;
        var statusEl = document.getElementById("status");
        if (statusEl) statusEl.textContent = "已连接";
        PDJ_EmitStatus("connected");
        console.log("[PDJ] WebSocket 已连接");
    };

    ws.onclose = function() {
        pdjConnected = false;
        PDJ_EmitStatus("disconnected");
        console.log("[PDJ] WebSocket 断开，2 秒后重连…");
        setTimeout(PDJ_Connect, 2000);
    };

    ws.onerror = function(err) {
        PDJ_EmitStatus("error", { error: String(err) });
    };

    ws.onmessage = function(msgEvent) {
        if (typeof msgEvent.data !== "string") return;

        var data;
        try {
            data = JSON.parse(msgEvent.data);
        } catch (_e) {
            return;
        }

        if (!data || typeof data !== "object") return;

        // 后端推送的队列更新
        if (data.type === "QUEUE_UPDATE" && Array.isArray(data.queue)) {
            PDJ_RenderQueue(data.queue);
            return;
        }

        if (data.type === "STYLE_UPDATE") {
            PDJ_ReloadStylesheet();
            return;
        }

        // 后端状态消息
        if (data.type === "PDJ_STATUS") {
            PDJ_EmitStatus(data.status || "server", data);
            return;
        }

        // 超级弹幕等其他事件
        if (data.cmd === "SUPER_CHAT_MESSAGE" && data.data) {
            console.log("[PDJ] 超级弹幕", data.data.price, data.data.user_info && data.data.user_info.uname);
        }
    };
}

PDJ_ReloadStylesheet();
PDJ_Connect();
