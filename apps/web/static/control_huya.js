(() => {
  'use strict';

  const $ = id => document.getElementById(id);
  const originalFetch = window.fetch.bind(window);

  function requestPath(input) {
    try {
      const raw = typeof input === 'string' ? input : (input?.url || '');
      return new URL(raw, location.href).pathname;
    } catch (_) {
      return '';
    }
  }

  // Issue79 owns the active-platform save closure. Add Huya at the fetch
  // boundary so the existing Bilibili/Douyin UI keeps working unchanged.
  window.fetch = async function huyaAwareFetch(input, init = {}) {
    const path = requestPath(input);
    let options = init || {};
    const method = String(options.method || (typeof input !== 'string' ? input?.method : '') || 'GET').toUpperCase();
    if (path === '/api/platforms/active' && method === 'POST') {
      try {
        const payload = JSON.parse(String(options.body || '{}'));
        const active = Array.isArray(payload.active) ? [...payload.active] : [];
        const next = active.filter(name => String(name) !== 'huya');
        if (Boolean($('active-huya')?.checked)) next.push('huya');
        payload.active = [...new Set(next)];
        options = { ...options, body: JSON.stringify(payload) };
      } catch (_) { /* keep original request */ }
    }

    const response = await originalFetch(input, options);
    if (path === '/api/platforms/active' && method === 'GET') {
      response.clone().json().then(payload => {
        const active = new Set(Array.isArray(payload?.active) ? payload.active.map(String) : []);
        const node = $('active-huya');
        if (node) node.checked = active.has('huya');
      }).catch(() => {});
    }
    return response;
  };

  async function api(path, options = {}) {
    const response = await originalFetch(path, { cache: 'no-store', ...options });
    let payload = {};
    try { payload = await response.json(); } catch (_) { /* ignore */ }
    if (!response.ok || payload.status === 'error') throw new Error(payload.message || `HTTP ${response.status}`);
    return payload;
  }

  function setStatus(text, ok = true) {
    const node = $('platform-status');
    if (!node) return;
    node.textContent = text || '';
    node.style.color = ok ? '' : '#d84f63';
  }

  function promoteHuyaOption() {
    const option = document.querySelector('#platform-select option[value="huya"]');
    if (option) option.textContent = '虎牙';
  }

  function ensureHuyaFields() {
    const pane = $('settings-platform');
    const grid = pane?.querySelector('.form-grid');
    if (!pane || !grid || $('platform-huya-url')) return;

    const fields = document.createElement('div');
    fields.style.display = 'contents';
    fields.innerHTML = `
      <label class="wide"><span>虎牙直播间链接</span><input id="platform-huya-url" placeholder="https://www.huya.com/lpl"></label>
      <label><span>虎牙房间号</span><input id="platform-huya-room" inputmode="numeric" placeholder="可留空，启动时自动解析"></label>
      <label><span>虎牙主播 UID</span><input id="platform-huya-anchor" inputmode="numeric" placeholder="可留空，启动时自动解析"></label>
    `;
    grid.appendChild(fields);

    const toolbar = pane.querySelector('.toolbar');
    if (toolbar) {
      const saveButton = document.createElement('button');
      saveButton.id = 'platform-huya-save';
      saveButton.className = 'button ghost';
      saveButton.textContent = '保存虎牙参数';
      saveButton.addEventListener('click', saveHuyaConfig);
      toolbar.appendChild(saveButton);
    }
    loadHuyaConfig();
  }

  async function loadHuyaConfig() {
    if (!$('platform-huya-url')) return;
    try {
      const cfg = await api('/api/config');
      const huya = cfg.huya || {};
      $('platform-huya-url').value = String(huya.room_url || '');
      $('platform-huya-room').value = String(huya.room_id || '');
      $('platform-huya-anchor').value = String(huya.anchor_id || '');
    } catch (_) { /* regular control.js displays config failures */ }
  }

  async function saveHuyaConfig() {
    setStatus('正在保存虎牙参数……');
    try {
      const cfg = await api('/api/config');
      delete cfg.status;
      cfg.huya = {
        ...(cfg.huya || {}),
        room_url: String($('platform-huya-url')?.value || '').trim(),
        room_id: String($('platform-huya-room')?.value || '').trim(),
        anchor_id: String($('platform-huya-anchor')?.value || '').trim(),
      };
      await api('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cfg),
      });
      setStatus('虎牙参数已保存；激活虎牙后会自动解析房间信息并连接弹幕。');
    } catch (error) {
      setStatus(`虎牙参数保存失败：${error.message}`, false);
    }
  }

  function ensureActiveHuya() {
    const grid = document.querySelector('#settings-active-platforms .issue79-platform-grid');
    if (!grid || $('active-huya')) return;
    const label = document.createElement('label');
    label.className = 'issue79-platform-option';
    label.innerHTML = '<input id="active-huya" type="checkbox"><div><strong>虎牙</strong><span>使用“平台参数”中配置的唯一虎牙直播间。</span></div>';
    grid.appendChild(label);

    const pane = $('settings-active-platforms');
    const hints = pane?.querySelectorAll('p.hint') || [];
    hints.forEach(node => {
      if (node.textContent.includes('虎牙') && node.textContent.includes('预留')) {
        node.textContent = '快手、斗鱼、微信视频号当前仍为预留配置，暂不能激活弹幕流。';
      }
    });

    originalFetch('/api/platforms/active', { cache: 'no-store' })
      .then(response => response.json())
      .then(payload => { $('active-huya').checked = Array.isArray(payload.active) && payload.active.includes('huya'); })
      .catch(() => {});
  }

  promoteHuyaOption();
  ensureHuyaFields();
  ensureActiveHuya();
})();
