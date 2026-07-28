// myjs.js — 队列显示端
// 排队逻辑由 Python 后端处理；本文件只负责接收后端推送的队列并渲染到页面。

var zroomid = 0;
var zuid = 0;
var ws = null;
var pdjConnected = false;
var pdjReconnectTimer = null;
var pdjReconnectAttempt = 0;
var pdjCurrentQueue = [];
var pdjDisplayOptions = {
    auto_scroll: false,
    show_sequence: false
};
var pdjScrollFrame = null;
var pdjScrollLastTime = 0;
var pdjScrollPauseUntil = 0;
var pdjScrollPhase = "top-pause";
var PDJ_SCROLL_SPEED = 28;
var PDJ_SCROLL_TOP_PAUSE = 900;
var PDJ_SCROLL_BOTTOM_PAUSE = 1300;

// ---------- 工具函数 ----------

function escapeHtml(text) {
    return String(text || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}

function PDJ_AsBool(value, fallback) {
    if (typeof value === "boolean") return value;
    if (typeof value === "number") return value !== 0;
    var normalized = String(value == null ? "" : value).trim().toLowerCase();
    if (["1", "true", "yes", "on"].indexOf(normalized) >= 0) return true;
    if (["0", "false", "no", "off"].indexOf(normalized) >= 0) return false;
    return Boolean(fallback);
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

function PDJ_StopAutoScroll(resetPosition) {
    if (pdjScrollFrame !== null) {
        cancelAnimationFrame(pdjScrollFrame);
        pdjScrollFrame = null;
    }
    pdjScrollLastTime = 0;
    pdjScrollPauseUntil = 0;
    pdjScrollPhase = "top-pause";
    if (resetPosition) {
        var container = document.getElementById("danmu");
        if (container) container.scrollTop = 0;
    }
}

function PDJ_AutoScrollStep(timestamp) {
    pdjScrollFrame = null;
    var container = document.getElementById("danmu");
    if (!container || !pdjDisplayOptions.auto_scroll) {
        PDJ_StopAutoScroll(true);
        return;
    }

    var maxScroll = Math.max(0, container.scrollHeight - container.clientHeight);
    if (maxScroll <= 1) {
        container.scrollTop = 0;
        return;
    }

    if (!pdjScrollLastTime) pdjScrollLastTime = timestamp;
    var elapsedSeconds = Math.min(0.1, Math.max(0, timestamp - pdjScrollLastTime) / 1000);
    pdjScrollLastTime = timestamp;

    if (timestamp < pdjScrollPauseUntil) {
        pdjScrollFrame = requestAnimationFrame(PDJ_AutoScrollStep);
        return;
    }

    if (pdjScrollPhase === "top-pause") {
        pdjScrollPhase = "moving";
    } else if (pdjScrollPhase === "bottom-pause") {
        container.scrollTop = 0;
        pdjScrollPhase = "top-pause";
        pdjScrollPauseUntil = timestamp + PDJ_SCROLL_TOP_PAUSE;
        pdjScrollFrame = requestAnimationFrame(PDJ_AutoScrollStep);
        return;
    }

    container.scrollTop = Math.min(
        maxScroll,
        container.scrollTop + PDJ_SCROLL_SPEED * elapsedSeconds
    );
    if (container.scrollTop >= maxScroll - 0.5) {
        container.scrollTop = maxScroll;
        pdjScrollPhase = "bottom-pause";
        pdjScrollPauseUntil = timestamp + PDJ_SCROLL_BOTTOM_PAUSE;
    }
    pdjScrollFrame = requestAnimationFrame(PDJ_AutoScrollStep);
}

function PDJ_StartAutoScroll(resetPosition) {
    PDJ_StopAutoScroll(Boolean(resetPosition));
    var container = document.getElementById("danmu");
    if (!container) return;

    container.style.overflowY = pdjDisplayOptions.auto_scroll ? "hidden" : "auto";
    if (!pdjDisplayOptions.auto_scroll) return;

    requestAnimationFrame(function() {
        var maxScroll = Math.max(0, container.scrollHeight - container.clientHeight);
        if (maxScroll <= 1) {
            container.scrollTop = 0;
            return;
        }
        pdjScrollPhase = "top-pause";
        pdjScrollPauseUntil = performance.now() + PDJ_SCROLL_TOP_PAUSE;
        pdjScrollLastTime = 0;
        pdjScrollFrame = requestAnimationFrame(PDJ_AutoScrollStep);
    });
}

// ---------- 队列渲染 ----------

function PDJ_RenderQueue(queue) {
    if (!Array.isArray(queue)) return;
    pdjCurrentQueue = queue.slice();

    var container = document.getElementById("danmu");
    var empty = document.getElementById("emptyState");
    if (!container) return;

    var fragment = document.createDocumentFragment();
    queue.forEach(function(item, index) {
        var row = document.createElement("div");
        row.className = "queue-item";
        row.setAttribute("role", "listitem");
        row.style.display = "grid";
        row.style.gridTemplateColumns = pdjDisplayOptions.show_sequence ? "44px minmax(0, 1fr)" : "minmax(0, 1fr)";
        row.style.alignItems = "center";

        if (pdjDisplayOptions.show_sequence) {
            var number = document.createElement("span");
            number.className = "queue-number";
            number.textContent = String(index + 1);
            number.setAttribute("aria-label", "第 " + String(index + 1) + " 位");
            number.style.display = "inline-grid";
            number.style.placeItems = "center";
            number.style.width = "34px";
            number.style.height = "34px";
            number.style.flex = "0 0 auto";
            row.appendChild(number);
        }

        var content = document.createElement("span");
        content.className = "queue-content";
        content.textContent = String(item || "");
        content.style.minWidth = "0";
        row.appendChild(content);
        fragment.appendChild(row);
    });

    container.replaceChildren(fragment);
    if (empty) empty.hidden = queue.length > 0;
    requestAnimationFrame(function() { PDJ_StartAutoScroll(true); });
}

function PDJ_SetConnectionBadge(connected) {
    var badge = document.getElementById("connectionBadge");
    if (!badge) return;
    badge.textContent = connected ? "实时连接" : "正在重连";
    badge.classList.toggle("is-online", connected);
}

// ---------- 配置加载 ----------

async function PDJ_LoadConfig() {
    try {
        var res = await fetch("/api/config/basic", { cache: "no-store" });
        if (!res.ok) return;
        var cfg = await res.json();
        zroomid = Number(cfg.roomid || 0);
        zuid = Number(cfg.uid || 0);
        PDJ_EmitStatus("config_loaded");
    } catch (err) {
        console.error("[PDJ] 配置读取失败", err);
    }
}

async function PDJ_LoadDisplayOptions() {
    try {
        var res = await fetch("/api/style", { cache: "no-store" });
        if (!res.ok) throw new Error("HTTP " + res.status);
        var style = await res.json();
        pdjDisplayOptions.auto_scroll = PDJ_AsBool(style.auto_scroll, false);
        pdjDisplayOptions.show_sequence = PDJ_AsBool(style.show_sequence, false);
    } catch (err) {
        console.error("[PDJ] 展示选项读取失败", err);
        pdjDisplayOptions.auto_scroll = false;
        pdjDisplayOptions.show_sequence = false;
    }
    PDJ_RenderQueue(pdjCurrentQueue);
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
    await Promise.all([PDJ_LoadConfig(), PDJ_LoadDisplayOptions()]);

    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
        return;
    }

    ws = new WebSocket(PDJ_GetWebSocketURL());

    ws.onopen = function() {
        pdjConnected = true;
        pdjReconnectAttempt = 0;
        if (pdjReconnectTimer) clearTimeout(pdjReconnectTimer);
        PDJ_SetConnectionBadge(true);
        var statusEl = document.getElementById("status");
        if (statusEl) statusEl.textContent = "已连接";
        PDJ_EmitStatus("connected");
        console.log("[PDJ] WebSocket 已连接");
    };

    ws.onclose = function() {
        pdjConnected = false;
        PDJ_SetConnectionBadge(false);
        PDJ_EmitStatus("disconnected");
        console.log("[PDJ] WebSocket 断开，稍后重连…");
        var delay = Math.min(15000, 1000 * Math.pow(1.7, pdjReconnectAttempt++));
        pdjReconnectTimer = setTimeout(PDJ_Connect, delay);
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

        if (data.type === "QUEUE_UPDATE" && Array.isArray(data.queue)) {
            PDJ_RenderQueue(data.queue);
            return;
        }

        if (data.type === "STYLE_UPDATE") {
            PDJ_ReloadStylesheet();
            void PDJ_LoadDisplayOptions();
            return;
        }

        if (data.type === "PDJ_STATUS") {
            PDJ_EmitStatus(data.status || "server", data);
            return;
        }

        if (data.cmd === "SUPER_CHAT_MESSAGE" && data.data) {
            console.log("[PDJ] 超级弹幕", data.data.price, data.data.user_info && data.data.user_info.uname);
        }
    };
}

window.addEventListener("resize", function() {
    PDJ_StartAutoScroll(false);
});

document.addEventListener("visibilitychange", function() {
    if (document.hidden) {
        PDJ_StopAutoScroll(false);
    } else {
        PDJ_StartAutoScroll(false);
    }
});

PDJ_ReloadStylesheet();
PDJ_RenderQueue([]);
PDJ_Connect();
