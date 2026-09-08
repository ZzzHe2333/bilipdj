(() => {
  'use strict';

  const $ = id => document.getElementById(id);
  const PLATFORM_KEY = 'twitch';
  const DISPLAY_NAME = '紫色老鼠';
  const TWITCH_PROBE_URL = 'wss://irc-ws.chat.twitch.tv:443';
  let accessState = { configured: false, unlocked: false };
  let deniedThisPage = false;
  let purpleWasActive = false;

  async function api(path, options = {}) {
    const response = await fetch(path, { cache: 'no-store', ...options });
    let payload = {};
    try { payload = await response.json(); } catch (_) { /* ignore */ }
    if (!response.ok || payload.status === 'error') {
      throw new Error(payload.message || `HTTP ${response.status}`);
    }
    return payload;
  }

  function post(path, payload = {}) {
    return api(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  }

  function extractChannel(value) {
    const text = String(value || '').trim();
    if (/^[A-Za-z0-9_]{1,25}$/.test(text)) return text.toLowerCase();
    try {
      const u = new URL(text);
      const host = u.hostname.toLowerCase();
      if (!(host === 'twitch.tv' || host.endsWith('.twitch.tv'))) return '';
      const parts = u.pathname.split('/').filter(Boolean);
      if (!parts.length) return '';
      const channel = parts[0].toLowerCase();
      const reserved = new Set([
        'directory', 'downloads', 'jobs', 'p', 'search', 'settings',
        'subscriptions', 'turbo', 'videos', 'wallet',
      ]);
      return !reserved.has(channel) && /^[A-Za-z0-9_]{1,25}$/.test(channel) ? channel : '';
    } catch (_) {
      return '';
    }
  }

  async function fullConfig() {
    const payload = await api('/api/config');
    delete payload.status;
    return payload;
  }

  function ensureOption() {
    const select = $('platform-select');
    if (!select) return;
    let option = [...select.options].find(item => item.value === PLATFORM_KEY);
    if (!option) {
      option = document.createElement('option');
      option.value = PLATFORM_KEY;
      select.appendChild(option);
    }
    option.textContent = DISPLAY_NAME;
  }

  function ensurePanel() {
    if ($('purple-mouse-host')) return;
    const select = $('platform-select');
    const card = select?.closest('.card');
    if (!select || !card) return;

    const host = document.createElement('div');
    host.id = 'purple-mouse-host';
    host.className = 'wide';
    host.style.display = 'none';
    host.style.gridColumn = '1 / -1';
    host.innerHTML = `
      <div id="purple-mouse-gate">
        <button id="purple-mouse-unlock" class="button mini" type="button">尝试解锁</button>
        <span id="purple-mouse-denied" style="display:none">您无权访问</span>
      </div>
      <div id="purple-mouse-config" style="display:none">
        <label class="wide" style="display:block;margin-top:8px">
          <span>直播链接 / 频道名</span>
          <input id="purple-mouse-room-url" placeholder="https://www.twitch.tv/gearbaby1010">
        </label>
        <div style="margin-top:8px">匿名只读，无需 OAuth；运行时默认自动使用系统代理，无代理时直连。</div>
        <div class="toolbar" style="margin-top:10px">
          <button id="purple-mouse-save" class="button" type="button">保存配置</button>
        </div>
        <div id="purple-mouse-status" class="status"></div>
      </div>`;
    card.appendChild(host);

    $('purple-mouse-unlock')?.addEventListener('click', tryUnlock);
    $('purple-mouse-save')?.addEventListener('click', saveConfig);
    select.addEventListener('change', refreshVisibility);
  }

  function hideBaseFields(active) {
    const select = $('platform-select');
    const card = select?.closest('.card');
    if (!select || !card) return;
    [...card.children].forEach(node => {
      if (node.id === 'purple-mouse-host' || node.id === 'redtv-host') return;
      if (node.contains?.(select)) {
        node.style.display = '';
        return;
      }
      if (node.tagName === 'LABEL') node.style.display = active ? 'none' : '';
    });

    const toolbar = $('platform-save')?.closest('.toolbar');
    if (toolbar) toolbar.style.display = active ? 'none' : '';
    const status = $('platform-status');
    if (status) status.style.display = active ? 'none' : '';
  }

  function renderGate() {
    const selectedValue = $('platform-select')?.value || '';
    const selected = selectedValue === PLATFORM_KEY;
    const host = $('purple-mouse-host');
    if (!host) return;
    host.style.display = selected ? '' : 'none';

    if (!selected) {
      // Do not undo the red-TV gate when switching directly between gated platforms.
      if (purpleWasActive && selectedValue !== 'youtube') hideBaseFields(false);
      purpleWasActive = false;
      return;
    }

    purpleWasActive = true;
    hideBaseFields(true);

    const unlock = $('purple-mouse-unlock');
    const denied = $('purple-mouse-denied');
    const config = $('purple-mouse-config');
    const allowed = accessState.configured || accessState.unlocked;

    if (unlock) unlock.style.display = allowed || deniedThisPage ? 'none' : '';
    if (denied) denied.style.display = deniedThisPage && !allowed ? '' : 'none';
    if (config) config.style.display = allowed ? '' : 'none';
  }

  async function loadSavedConfig() {
    const cfg = await fullConfig();
    const section = cfg.twitch || {};
    const savedTarget = String(section.room_url || section.room_id || '').trim();
    accessState.configured = Boolean(extractChannel(savedTarget));
    if (accessState.configured) {
      accessState.unlocked = true;
      deniedThisPage = false;
    }
    if ($('purple-mouse-room-url')) $('purple-mouse-room-url').value = savedTarget;
    if (accessState.configured) ensureActiveCheckbox();
    return cfg;
  }

  async function refreshAccess() {
    try {
      await loadSavedConfig();
    } catch (_) {
      accessState.configured = false;
    }
    renderGate();
  }

  function probeTwitch() {
    return new Promise(resolve => {
      let settled = false;
      let socket = null;
      const finish = allowed => {
        if (settled) return;
        settled = true;
        window.clearTimeout(timer);
        try { socket?.close(); } catch (_) { /* ignore */ }
        resolve(Boolean(allowed));
      };
      const timer = window.setTimeout(() => finish(false), 4000);
      try {
        socket = new WebSocket(TWITCH_PROBE_URL);
        socket.binaryType = 'arraybuffer';
        socket.addEventListener('open', () => finish(true), { once: true });
        socket.addEventListener('error', () => finish(false), { once: true });
        socket.addEventListener('close', () => finish(false), { once: true });
      } catch (_) {
        finish(false);
      }
    });
  }

  async function tryUnlock() {
    const button = $('purple-mouse-unlock');
    if (button) {
      button.disabled = true;
      button.textContent = '检测中…';
    }
    const allowed = await probeTwitch();
    if (allowed) {
      accessState.unlocked = true;
      deniedThisPage = false;
      try { await loadSavedConfig(); } catch (_) { /* keep session unlock */ }
    } else {
      accessState.unlocked = false;
      deniedThisPage = true;
    }
    if (button) {
      button.disabled = false;
      button.textContent = '尝试解锁';
    }
    renderGate();
  }

  async function saveConfig() {
    const status = $('purple-mouse-status');
    if (!accessState.configured && !accessState.unlocked) {
      if (status) status.textContent = '请先点击“尝试解锁”。';
      return;
    }
    const value = String($('purple-mouse-room-url')?.value || '').trim();
    const channel = extractChannel(value);
    if (!channel) {
      if (status) status.textContent = '直播链接 / 频道名无效。';
      return;
    }
    try {
      if (status) status.textContent = '正在保存…';
      const cfg = await fullConfig();
      cfg.platform = PLATFORM_KEY;
      cfg.twitch = {
        ...(cfg.twitch || {}),
        enabled: true,
        room_id: channel,
        room_url: `https://www.twitch.tv/${channel}`,
      };
      await post('/api/config', cfg);
      accessState = { configured: true, unlocked: true };
      deniedThisPage = false;
      if ($('purple-mouse-room-url')) $('purple-mouse-room-url').value = cfg.twitch.room_url;
      if (status) status.textContent = '配置已保存。';
      ensureActiveCheckbox();
      renderGate();
    } catch (error) {
      if (status) status.textContent = `保存失败：${error.message}`;
    }
  }

  function ensureActiveCheckbox() {
    if (!accessState.configured || $('active-twitch')) return;
    const grid = document.querySelector('.issue79-platform-grid');
    if (!grid) return;
    const label = document.createElement('label');
    label.className = 'issue79-platform-option';
    label.innerHTML = `<input id="active-twitch" type="checkbox"><div><strong>${DISPLAY_NAME}</strong><span>使用已保存的唯一直播配置。</span></div>`;
    grid.appendChild(label);
  }

  function aliasStatusText() {
    const node = $('active-platform-status');
    if (!node) return;
    const replace = () => {
      const raw = node.textContent || '';
      const next = raw.replace(/\btwitch\b/ig, DISPLAY_NAME);
      if (next !== raw) node.textContent = next;
    };
    replace();
    const observer = new MutationObserver(replace);
    observer.observe(node, { childList: true, characterData: true, subtree: true });
  }

  function installActiveFetchBridge() {
    const nativeFetch = window.fetch.bind(window);
    window.fetch = async function(input, init = {}) {
      const url = typeof input === 'string' ? input : String(input?.url || '');
      const path = (() => {
        try { return new URL(url, window.location.href).pathname; }
        catch (_) { return url; }
      })();

      let options = init;
      if (path === '/api/platforms/active' && String(init.method || 'GET').toUpperCase() === 'POST') {
        try {
          const body = JSON.parse(String(init.body || '{}'));
          if (Array.isArray(body.active) && $('active-twitch')) {
            const active = body.active.filter(value => value !== PLATFORM_KEY);
            if ($('active-twitch').checked && accessState.configured) active.push(PLATFORM_KEY);
            options = { ...init, body: JSON.stringify({ ...body, active }) };
          }
        } catch (_) { /* leave the original request untouched */ }
      }

      const response = await nativeFetch(input, options);
      if (path === '/api/platforms/active' && String(options.method || 'GET').toUpperCase() === 'GET') {
        try {
          const clone = response.clone();
          const payload = await clone.json();
          const active = Array.isArray(payload.active) ? payload.active : [];
          if (accessState.configured) ensureActiveCheckbox();
          if ($('active-twitch')) $('active-twitch').checked = active.includes(PLATFORM_KEY);
        } catch (_) { /* ignore */ }
      }
      return response;
    };
  }

  function refreshVisibility() {
    if ($('platform-select')?.value === PLATFORM_KEY) refreshAccess();
    else renderGate();
  }

  function boot() {
    ensureOption();
    ensurePanel();
    installActiveFetchBridge();
    aliasStatusText();
    refreshAccess();
    document.querySelector('[data-settings="platform"]')?.addEventListener('click', () => {
      window.setTimeout(refreshVisibility, 30);
    });
    document.querySelector('[data-view="settings"]')?.addEventListener('click', () => {
      window.setTimeout(refreshVisibility, 80);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot, { once: true });
  } else {
    boot();
  }
})();
