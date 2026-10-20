// ==UserScript==
// @name         BiliPDJ 抖音直播礼物捕获（页面 WSS）
// @namespace    bilipdj.douyin.gift
// @version      0.3.0
// @description  在已登录直播间解码 WebcastGiftMessage；需配合 BiliPDJ 礼物桥接脚本
// @match        https://live.douyin.com/*
// @match        https://webcast.amemv.com/*
// @run-at       document-start
// @grant        none
// ==/UserScript==
(function () {
  'use strict';
  if (window.__douyinGiftHookInstalled) return;
  window.__douyinGiftHookInstalled = true;
  const TARGET = /\/webcast\/im\//;
  const KEEP = 200;
  const MAX_FRAME = 4 * 1024 * 1024;
  window.__douyinGifts = [];
  window.__douyinMsgCounts = Object.create(null);

  function varint(buf, start) {
    let value = 0n, shift = 0n, pos = start;
    for (let i = 0; i < 10 && pos < buf.length; i++) {
      const b = buf[pos++];
      value |= BigInt(b & 127) << shift;
      if (!(b & 128)) return [value, pos];
      shift += 7n;
    }
    throw new Error('truncated protobuf varint');
  }
  function numeric(v) {
    return v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : v.toString();
  }
  function text(bytes) {
    if (!bytes.length) return null;
    try {
      const result = new TextDecoder('utf-8', {fatal:true}).decode(bytes);
      for (const char of result) {
        const v = char.codePointAt(0);
        if (v < 32 && v !== 9 && v !== 10 && v !== 13) return null;
      }
      return result.replace(/\u0000+$/, '') || null;
    } catch (_) { return null; }
  }
  function parseFields(bytes, depth=0) {
    const fields = []; let p = 0;
    while (p < bytes.length) {
      const [tag, end] = varint(bytes, p); p = end;
      const f = Number(tag >> 3n), w = Number(tag & 7n);
      if (!f || f > 100000) throw new Error('invalid protobuf field');
      if (w === 0) {
        const [v, next] = varint(bytes, p); p = next;
        fields.push({f, w, v: numeric(v)});
      } else if (w === 2) {
        const [length, next] = varint(bytes, p); p = next;
        const n = Number(length);
        if (!Number.isSafeInteger(n) || p + n > bytes.length) throw new Error('invalid length');
        const part = bytes.subarray(p, p + n); p += n;
        const rec = {f, w, bytes: part, text: text(part)};
        if (rec.text === null && depth < 8 && n) {
          try {
            const inner = parseFields(part, depth + 1);
            if (inner.length) rec.nested = inner;
          } catch (_) {}
        }
        fields.push(rec);
      } else if (w === 5 && p + 4 <= bytes.length) p += 4;
      else if (w === 1 && p + 8 <= bytes.length) p += 8;
      else throw new Error('unsupported protobuf wire type');
    }
    return fields;
  }
  const field = (fields, n) => (fields || []).find(x => x.f === n);
  const fieldsOf = (fields, n) => (fields || []).filter(x => x.f === n);
  function scalar(fields, n) {
    const rec = field(fields, n);
    return rec && rec.v !== undefined ? rec.v : null;
  }
  function str(fields, n) {
    const rec = field(fields, n);
    return rec && rec.text !== undefined ? rec.text : null;
  }

  function decodeGift(payload) {
    const msg = parseFields(payload);
    const common = (field(msg, 1) || {}).nested || [];
    const user = (field(msg, 7) || {}).nested || [];
    const info = (field(msg, 15) || {}).nested || [];
    const description = str(common, 7);
    const m = description && description.match(/^(.*?)[:：]送给主播\s*(\d+)\s*个\s*(.+)$/);
    const parsed = m ? {nick:m[1], count:Number(m[2]), name:m[3]} : null;
    const urls = ((field(user, 9) || {}).nested || [])
      .map(x => x.text).filter(x => typeof x === 'string' && /^https?:\/\//.test(x));
    const count = scalar(msg, 5);
    const unit = scalar(info, 12);
    return {
      type: 'WebcastGiftMessage',
      receivedAt: Date.now(),
      msgId: scalar(common, 2) == null ? null : String(scalar(common, 2)),
      roomId: scalar(common, 3) == null ? null : String(scalar(common, 3)),
      createTime: scalar(common, 4),
      giftId: scalar(msg, 2),
      groupCount: scalar(msg, 4),
      repeatCount: count,
      comboCount: scalar(msg, 6),
      repeatEnd: scalar(msg, 9),
      groupId: scalar(msg, 10) == null ? null : String(scalar(msg, 10)),
      describe: description,
      sendCount: parsed ? parsed.count : null,
      user: {
        id: scalar(user, 1) == null ? null : String(scalar(user, 1)),
        nickname: str(user, 3) || (parsed ? parsed.nick : null),
        avatar: urls[0] || null
      },
      gift: {
        id: scalar(info, 5) || scalar(msg, 2),
        name: str(info, 16) || (parsed ? parsed.name : null),
        describe: str(info, 2),
        diamondCount: unit,
        duration: scalar(info, 4)
      },
      totalDiamond: unit != null && count != null ? Number(unit) * Number(count) : null
    };
  }
  async function uncompress(data) {
    if (data[0] !== 31 || data[1] !== 139) return data;
    if (!('DecompressionStream' in window)) throw new Error('DecompressionStream unavailable');
    const bytes = new Response(new Blob([data]).stream().pipeThrough(new DecompressionStream('gzip')));
    const result = new Uint8Array(await bytes.arrayBuffer());
    if (result.length > MAX_FRAME) throw new Error('large uncompressed frame');
    return result;
  }
  function emitGift(gift) {
    window.__douyinGifts.push(gift);
    if (window.__douyinGifts.length > KEEP) window.__douyinGifts.shift();
    window.dispatchEvent(new CustomEvent('douyin-gift', {detail:gift}));
    window.postMessage({source:'douyin-gift', data:gift}, window.location.origin);
  }
  async function decodeFrame(buffer) {
    const frame = parseFields(buffer);
    const item = field(frame, 8);
    if (!item || !item.bytes) return;
    const response = parseFields(await uncompress(item.bytes));
    for (const rec of fieldsOf(response, 1)) {
      const entry = rec.nested || [];
      const method = str(entry, 1);
      if (!method) continue;
      const stats = window.__douyinMsgCounts;
      stats[method] = (stats[method] || 0) + 1;
      if (method !== 'WebcastGiftMessage') continue;
      const giftBytes = field(entry, 2);
      if (giftBytes && giftBytes.bytes) emitGift(decodeGift(giftBytes.bytes));
    }
  }
  const NativeWS = window.WebSocket;
  if (!NativeWS) return;
  function hook(ws) {
    if (ws.__bilipdjGiftHooked || !TARGET.test(ws.url || '')) return;
    ws.__bilipdjGiftHooked = true;
    ws.addEventListener('message', ev => {
      if (typeof ev.data === 'string') return;
      Promise.resolve(ev.data instanceof Blob ? ev.data.arrayBuffer() : ev.data)
        .then(buffer => {
          if (!(buffer instanceof ArrayBuffer) || buffer.byteLength > MAX_FRAME) return;
          return decodeFrame(new Uint8Array(buffer));
        })
        .catch(error => console.debug('[bilipdj-gift] frame ignored', error));
    });
  }
  function HookedWebSocket(url, protocols) {
    const ws = protocols === undefined ? new NativeWS(url) : new NativeWS(url, protocols);
    hook(ws);
    return ws;
  }
  HookedWebSocket.prototype = NativeWS.prototype;
  for (const key of ['CONNECTING','OPEN','CLOSING','CLOSED']) {
    HookedWebSocket[key] = NativeWS[key];
  }
  window.WebSocket = HookedWebSocket;
  const nativeAdd = NativeWS.prototype.addEventListener;
  NativeWS.prototype.addEventListener = function (type, fn, opts) {
    if (type === 'message') hook(this);
    return nativeAdd.call(this, type, fn, opts);
  };
  console.info('[bilipdj-gift] WebSocket gift listener ready');
})();
