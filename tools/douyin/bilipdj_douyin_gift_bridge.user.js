// ==UserScript==
// @name         BiliPDJ 抖音礼物本机桥接（需配合 WSS 捕获脚本）
// @namespace    bilipdj.douyin.bridge
// @version      0.1.0
// @description  将当前直播网页解码出的礼物事件发给本机 BiliPDJ Python 后端
// @match        https://live.douyin.com/*
// @match        https://webcast.amemv.com/*
// @run-at       document-start
// @grant        GM_xmlhttpRequest
// @grant        GM_getValue
// @grant        GM_setValue
// @grant        GM_registerMenuCommand
// @connect      127.0.0.1
// @connect      localhost
// ==/UserScript==
(function () {
  'use strict';
  // Keep the secret only in the userscript manager's isolated storage.
  // This is a local browser aid, NOT evidence of a purchase.
  const PREFIX = '[bilipdj-douyin-bridge]';
  let inFlight = 0, failures = 0;
  const config = () => ({
    token: String(GM_getValue('token', '') || '').trim(),
    port: Number(GM_getValue('port', 9816)),
    enabled: Boolean(GM_getValue('enabled', false))
  });

  GM_registerMenuCommand('设置 BiliPDJ 礼物桥接', () => {
    const old = config();
    const rawPort = prompt('BiliPDJ Python 后端端口：', String(old.port));
    if (rawPort === null) return;
    const port = Number(rawPort);
    if (!Number.isInteger(port) || port < 1 || port > 65535) {
      alert('端口不合法'); return;
    }
    const token = prompt('输入本机 douyin-gift-bridge-token.txt 中的完整令牌：', old.token);
    if (token === null) return;
    if (token.trim().length < 32) {
      alert('令牌至少 32 个字符'); return;
    }
    GM_setValue('port', port);
    GM_setValue('token', token.trim());
    GM_setValue('enabled', true);
    alert('已保存。请刷新抖音直播间；桥接将在该浏览器的直播页面生效。');
  });
  GM_registerMenuCommand('切换礼物桥接开关', () => {
    const next = !config().enabled;
    GM_setValue('enabled', next);
    alert('礼物桥接已' + (next ? '启用' : '禁用') + '；刷新页面后生效');
  });

  window.addEventListener('message', (event) => {
    if (event.source !== window || event.origin !== window.location.origin) return;
    const obj = event.data;
    if (!obj || obj.source !== 'douyin-gift' || !obj.data ||
        obj.data.type !== 'WebcastGiftMessage') return;
    const settings = config();
    if (!settings.enabled || settings.token.length < 32 ||
        !Number.isInteger(settings.port) || settings.port < 1 || settings.port > 65535) return;
    if (inFlight >= 12) {
      if (++failures % 100 === 1) console.warn(PREFIX, 'too many events in flight; some dropped');
      return;
    }
    inFlight++;
    GM_xmlhttpRequest({
      method: 'POST',
      url: 'http://127.0.0.1:' + settings.port + '/api/douyin/gift-hook',
      headers: {
        'Authorization': 'Bearer ' + settings.token,
        'Content-Type': 'application/json'
      },
      data: JSON.stringify(obj.data),
      timeout: 4000,
      onload: (resp) => {
        inFlight--;
        if (resp.status !== 200 && ++failures % 15 === 1) {
          console.warn(PREFIX, 'BiliPDJ rejected gift event; HTTP', resp.status,
            (resp.responseText || '').slice(0, 250));
        }
      },
      onerror: () => {
        inFlight--;
        if (++failures % 15 === 1) console.warn(PREFIX, 'Cannot connect to local BiliPDJ backend');
      },
      ontimeout: () => {
        inFlight--;
        if (++failures % 15 === 1) console.warn(PREFIX, 'Local bridge timed out');
      }
    });
  });
})();
