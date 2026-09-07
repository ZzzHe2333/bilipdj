// performance.js — lightweight realtime display optimizations for OBS/browser sources.
(function () {
    "use strict";

    var PDJ_PERF_SCROLL_FRAME_MS = 1000 / 30;
    var pdjPerfLastScrollPaint = 0;
    var pdjPerfRestartFrame = null;
    var pdjPerfLastAutoScroll = null;
    var pdjPerfLastShowSequence = null;

    function queueEquals(left, right) {
        if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) {
            return false;
        }
        for (var index = 0; index < left.length; index += 1) {
            if (String(left[index] || "") !== String(right[index] || "")) {
                return false;
            }
        }
        return true;
    }

    function ensureQueueRow(container, index) {
        var row = container.children[index];
        if (row && row.classList && row.classList.contains("queue-item")) {
            return row;
        }
        row = document.createElement("div");
        row.className = "queue-item";
        row.setAttribute("role", "listitem");
        row.style.display = "grid";
        row.style.alignItems = "center";
        if (container.children[index]) {
            container.insertBefore(row, container.children[index]);
        } else {
            container.appendChild(row);
        }
        return row;
    }

    function updateQueueRow(row, text, index) {
        var showSequence = Boolean(pdjDisplayOptions.show_sequence);
        row.style.gridTemplateColumns = showSequence
            ? "44px minmax(0, 1fr)"
            : "minmax(0, 1fr)";

        var number = row.querySelector(".queue-number");
        if (showSequence) {
            if (!number) {
                number = document.createElement("span");
                number.className = "queue-number";
                number.style.display = "inline-grid";
                number.style.placeItems = "center";
                number.style.width = "34px";
                number.style.height = "34px";
                number.style.flex = "0 0 auto";
                row.insertBefore(number, row.firstChild);
            }
            var sequence = String(index + 1);
            if (number.textContent !== sequence) number.textContent = sequence;
            number.setAttribute("aria-label", "第 " + sequence + " 位");
        } else if (number) {
            number.remove();
        }

        var content = row.querySelector(".queue-content");
        if (!content) {
            content = document.createElement("span");
            content.className = "queue-content";
            content.style.minWidth = "0";
            row.appendChild(content);
        }
        if (content.textContent !== text) content.textContent = text;
    }

    function scheduleAutoScrollRestart(resetPosition) {
        if (pdjPerfRestartFrame !== null) {
            cancelAnimationFrame(pdjPerfRestartFrame);
        }
        pdjPerfRestartFrame = requestAnimationFrame(function () {
            pdjPerfRestartFrame = null;
            PDJ_StartAutoScroll(Boolean(resetPosition));
        });
    }

    var originalStopAutoScroll = PDJ_StopAutoScroll;
    PDJ_StopAutoScroll = function (resetPosition) {
        pdjPerfLastScrollPaint = 0;
        return originalStopAutoScroll(resetPosition);
    };

    PDJ_AutoScrollStep = function (timestamp) {
        pdjScrollFrame = null;
        var container = document.getElementById("danmu");
        if (!container || !pdjDisplayOptions.auto_scroll) {
            PDJ_StopAutoScroll(true);
            return;
        }

        if (
            pdjPerfLastScrollPaint &&
            timestamp - pdjPerfLastScrollPaint < PDJ_PERF_SCROLL_FRAME_MS
        ) {
            pdjScrollFrame = requestAnimationFrame(PDJ_AutoScrollStep);
            return;
        }
        pdjPerfLastScrollPaint = timestamp;

        var maxScroll = Math.max(0, container.scrollHeight - container.clientHeight);
        if (maxScroll <= 1) {
            container.scrollTop = 0;
            return;
        }

        if (!pdjScrollLastTime) pdjScrollLastTime = timestamp;
        var elapsedSeconds = Math.min(
            0.1,
            Math.max(0, timestamp - pdjScrollLastTime) / 1000
        );
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
    };

    PDJ_RenderQueue = function (queue) {
        if (!Array.isArray(queue)) return false;

        var normalized = queue.map(function (item) {
            return String(item || "");
        });
        var displayChanged =
            pdjPerfLastAutoScroll !== Boolean(pdjDisplayOptions.auto_scroll) ||
            pdjPerfLastShowSequence !== Boolean(pdjDisplayOptions.show_sequence);
        var queueChanged = !queueEquals(normalized, pdjCurrentQueue);

        if (!queueChanged && !displayChanged) {
            return false;
        }

        pdjCurrentQueue = normalized.slice();
        pdjPerfLastAutoScroll = Boolean(pdjDisplayOptions.auto_scroll);
        pdjPerfLastShowSequence = Boolean(pdjDisplayOptions.show_sequence);

        var container = document.getElementById("danmu");
        var empty = document.getElementById("emptyState");
        if (!container) return false;

        normalized.forEach(function (item, index) {
            var row = ensureQueueRow(container, index);
            updateQueueRow(row, item, index);
        });

        while (container.children.length > normalized.length) {
            container.removeChild(container.lastElementChild);
        }

        if (empty) empty.hidden = normalized.length > 0;
        scheduleAutoScrollRestart(true);
        return true;
    };

    window.PDJ_PerformanceProfile = {
        websocketQueueCoalescing: true,
        incrementalQueueRender: true,
        scrollFpsLimit: 30
    };
})();
