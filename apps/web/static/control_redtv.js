(() => {
  'use strict';

  const $ = id => document.getElementById(id);
  const PLATFORM_KEY = 'youtube';
  const DISPLAY_NAME = '红色小电视';
  const GOOGLE_PROBE_URL = 'https://www.google.com/generate_204';
  let accessState = { configured: false, unlocked: false };
  let deniedThisPage = false;

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

  function extractVideoId(value) {
    let text = String(value || '').trim();
    if (/^[A-Za-z0-9_-]{11}$/.test(text)) return text;
    if (!text.includes('://') && (text.includes('youtube.com') || text.includes('youtu.be'))) {
      text = `https://${text.replace(/^\/+/, '')}`;
    }
    try {
      const u = new URL(text);
      if (u.hostname.endsWith('youtu.be')) {
        const id = u.pathname.split('/').filter(Boolean)[0] || '';
        if (/^[A-Za-z0-9_-]{11}$/.test(id)) return id;
      }
      if (u.hostname.includes('youtube.com')) {
        const q = u.searchParams.get('v') || '';
        if (/^[A-Za-z0-9_-]{11}$/.test(q)) return q;
        const parts = u.pathname.split('/').filter(Boolean);
        if (parts.length >= 2 && ['live', 'shorts', 'embed'].includes(parts[0]) &&
            /^[A-Za-z0-9_-]{11}$/.test(parts[1])) {
          return parts[1];
        }
      }
    } catch (_) { /* invalid URL */ }
    return '';
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
    if ($('redtv-host')) return;
    const select = $('platform-select');
    const card = select?.closest('.card');
    if (!select || !card) return;

    const host = document.createElement('div');
    host.id = 'redtv-host';
    host.className = 'wide';
    host.style.display = 'none';
    host.style.gridColumn = '1 / -1';
    host.innerHTML = `
      <div id="redtv-gate">
        <button id="redtv-unlock" class="button mini" type="button">尝试解锁</button>
        <span id="redtv-denied" style="display:none">您无权访问</span>
      </div>
      <div id="redtv-config" style="display:none">
        <label class="wide" style="display:block;margin-top:8px">
          <span>直播链接</span>
          <input id="redtv-room-url" placeholder="填写直播链接">
        </label>
        <label class="wide" style="display:block;margin-top:8px">
          <span>Cookie（可选）</span>
          <textarea id="redtv-cookie" rows="3"></textarea>
        </label>
        <div class="toolbar" style="margin-top:10px">
          <button id="redtv-save" class="button" type="button">保存配置</button>
        </div>
        <div id="redtv-status" class="status"></div>
      </div>`;
    card.appendChild(host);

    $('redtv-unlock')?.addEventListener('click', tryUnlock);
    $('redtv-save')?.addEventListener('click', saveConfig);
    select.addEventListener('change', refreshVisibility);
  }

  function hideBaseFields(active) {
    const select = $('platform-select');
    const card = select?.closest('.card');
    if (!select || !card) return;
    [...card.children].forEach(node => {
      if (node.id === 'redtv-host') return;
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
    const selected = $('platform-select')?.value === PLATFORM_KEY;
    const host = $('redtv-host');
    if (!host) return;
    host.style.display = selected ? '' : 'none';
    hideBaseFields(selected);
    if (!selected) return;

    const unlock = $('redtv-unlock');
    const denied = $('redtv-denied');
    const config = $('redtv-config');
    const allowed = accessState.configured || accessState.unlocked;

    if (unlock) unlock.style.display = allowed || deniedThisPage ? 'none' : '';
    if (denied) denied.style.display = deniedThisPage && !allowed ? '' : 'none';
    if (config) config.style.display = allowed ? '' : 'none';
  }

  async function loadSavedConfig() {
    const cfg = await fullConfig();
    const section = cfg.youtube || {};
    const savedTarget = String(section.room_url || section.room_id || '').trim();
    accessState.configured = Boolean(extractVideoId(savedTarget));
    if (accessState.configured) {
      accessState.unlocked = true;
      deniedThisPage = false;
    }
    if ($('redtv-room-url')) $('redtv-room-url').value = savedTarget;
    if ($('redtv-cookie')) $('redtv-cookie').value = String(section.cookie || '');
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

  async function probeGoogle() {
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), 4000);
    try {
      await fetch(`${GOOGLE_PROBE_URL}?_=${Date.now()}`, {
        method: 'GET',
        mode: 'no-cors',
        cache: 'no-store',
        signal: controller.signal,
      });
      return true;
    } catch (_) {
      return false;
    } finally {
      window.clearTimeout(timer);
    }
  }

  async function tryUnlock() {
    const button = $('redtv-unlock');
    if (button) {
      button.disabled = true;
      button.textContent = '检测中…';
    }
    const allowed = await probeGoogle();
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
    const status = $('redtv-status');
    if (!accessState.configured && !accessState.unlocked) {
      if (status) status.textContent = '请先点击“尝试解锁”。';
      return;
    }
    const value = String($('redtv-room-url')?.value || '').trim();
    const videoId = extractVideoId(value);
    if (!videoId) {
      if (status) status.textContent = '直播链接无效。';
      return;
    }
    try {
      if (status) status.textContent = '正在保存…';
      const cfg = await fullConfig();
      cfg.platform = PLATFORM_KEY;
      cfg.youtube = {
        ...(cfg.youtube || {}),
        enabled: true,
        room_id: videoId,
        room_url: `https://www.youtube.com/watch?v=${videoId}`,
        cookie: String($('redtv-cookie')?.value || '').trim(),
      };
      await post('/api/config', cfg);
      accessState = { configured: true, unlocked: true };
      deniedThisPage = false;
      if ($('redtv-room-url')) $('redtv-room-url').value = cfg.youtube.room_url;
      if (status) status.textContent = '配置已保存。';
      ensureActiveCheckbox();
      renderGate();
    } catch (error) {
      if (status) status.textContent = `保存失败：${error.message}`;
    }
  }

  function ensureActiveCheckbox() {
    if (!accessState.configured || $('active-youtube')) return;
    const grid = document.querySelector('.issue79-platform-grid');
    if (!grid) return;
    const label = document.createElement('label');
    label.className = 'issue79-platform-option';
    label.innerHTML = `<input id="active-youtube" type="checkbox"><div><strong>${DISPLAY_NAME}</strong><span>使用已保存的唯一直播配置。</span></div>`;
    grid.appendChild(label);
  }

  function aliasStatusText() {
    const node = $('active-platform-status');
    if (!node) return;
    const replace = () => {
      const raw = node.textContent || '';
      const next = raw.replace(/\byoutube\b/ig, DISPLAY_NAME);
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
          if (Array.isArray(body.active) && $('active-youtube')) {
            const active = body.active.filter(value => value !== PLATFORM_KEY);
            if ($('active-youtube').checked && accessState.configured) active.push(PLATFORM_KEY);
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
          if ($('active-youtube')) $('active-youtube').checked = active.includes(PLATFORM_KEY);
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
