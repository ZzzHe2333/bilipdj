(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const state = { view: 'logs', logTimer: 0, perfTimer: 0, meta: null, queue: null };

  async function json(path, options = {}) {
    const response = await fetch(path, { cache: 'no-store', ...options });
    let payload = {};
    try { payload = await response.json(); } catch (_) { /* ignore */ }
    if (!response.ok || payload.status === 'error') throw new Error(payload.message || `HTTP ${response.status}`);
    return payload;
  }
  function post(path, payload = {}) {
    return json(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
  }
  async function fullConfig() {
    const payload = await json('/api/config');
    delete payload.status;
    return payload;
  }
  function message(id, text, ok = true) {
    const node = $(id); if (!node) return; node.textContent = text || ''; node.style.color = ok ? '' : '#ff7a8b';
  }
  function fmtBytes(value) {
    const n = Number(value || 0); if (!Number.isFinite(n) || n <= 0) return '0 B';
    const units = ['B','KB','MB','GB','TB']; let i = 0, v = n;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
    return `${v.toFixed(i ? 1 : 0)} ${units[i]}`;
  }
  function esc(text) { return String(text ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
  function intValue(id, fallback, min = Number.MIN_SAFE_INTEGER) {
    const value = Number.parseInt($(id).value, 10);
    return Math.max(min, Number.isFinite(value) ? value : fallback);
  }

  function switchView(name) {
    state.view = name;
    document.querySelectorAll('.nav').forEach(n => n.classList.toggle('active', n.dataset.view === name));
    document.querySelectorAll('.view').forEach(v => v.classList.toggle('active', v.id === `view-${name}`));
    stopTimers();
    if (name === 'logs') { refreshLogs(); state.logTimer = window.setInterval(refreshLogs, 2500); }
    if (name === 'queue') refreshQueueAll();
    if (name === 'settings') loadSettingsBundle();
    if (name === 'permissions') loadPermissions();
    if (name === 'performance') { refreshPerformance(); state.perfTimer = window.setInterval(refreshPerformance, 2500); }
    if (name === 'update') refreshUpdate(false);
  }
  function stopTimers() {
    if (state.logTimer) clearInterval(state.logTimer); state.logTimer = 0;
    if (state.perfTimer) clearInterval(state.perfTimer); state.perfTimer = 0;
  }

  async function refreshHeader() {
    try {
      const [health, meta, runtime] = await Promise.all([json('/health'), json('/api/control/meta'), json('/api/runtime-status')]);
      state.meta = meta;
      $('version').textContent = `v${meta.version || 'unknown'} · Web 控制台`;
      $('health-dot').className = 'dot ok';
      const connected = Boolean(runtime.danmu_connected || runtime.connected || runtime.relay_connected);
      const room = runtime.roomid || runtime.room_id || '';
      $('health-text').textContent = `${health.service || 'bilipdj'} 已连接${room ? ` · 房间 ${room}` : ''}${connected ? ' · 弹幕在线' : ''}`;
    } catch (error) {
      $('health-dot').className = 'dot bad'; $('health-text').textContent = `后端连接异常：${error.message}`;
    }
  }

  async function refreshLogs() {
    if (state.view !== 'logs' && !$('log-auto').checked) return;
    try {
      const payload = await json(`/api/control/logs?kind=${encodeURIComponent($('log-kind').value)}&limit=800`);
      const term = $('log-search').value.trim().toLowerCase();
      const lines = (payload.lines || []).filter(line => !term || String(line).toLowerCase().includes(term));
      const out = $('log-output'); const atBottom = out.scrollTop + out.clientHeight >= out.scrollHeight - 30;
      out.textContent = lines.join('\n') || '暂无日志。';
      if (atBottom || $('log-auto').checked) out.scrollTop = out.scrollHeight;
    } catch (error) { $('log-output').textContent = `读取日志失败：${error.message}`; }
  }

  async function refreshQueueAll() { await Promise.all([refreshQueue(), refreshArchive()]); }
  async function refreshArchive() {
    try {
      const payload = await json('/api/queue/archive');
      const select = $('queue-slot');
      let count = Number(payload.slots || 10);
      if (Array.isArray(payload.slots)) count = payload.slots.length || 10;
      count = Math.max(1, Math.min(50, count || 10));
      const active = Number(payload.active_slot || payload.slot || 1);
      select.innerHTML = Array.from({ length: count }, (_, i) => `<option value="${i + 1}">存档 ${i + 1}</option>`).join('');
      select.value = String(Math.max(1, Math.min(count, active)));
    } catch (error) { message('queue-status', `读取存档失败：${error.message}`, false); }
  }
  async function refreshQueue() {
    try {
      const payload = await json('/api/queue/state'); state.queue = payload;
      const entries = Array.isArray(payload.entries) && payload.entries.length ? payload.entries : (payload.queue || []).map(content => ({ content }));
      $('queue-body').innerHTML = entries.map((entry, i) => {
        const content = entry.content ?? entry.entry ?? entry.text ?? payload.queue?.[i] ?? '';
        const last = entry.last_operation_at || entry.updated_at || '';
        return `<tr><td>${i + 1}</td><td>${esc(content)}</td><td class="muted">${esc(last)}</td><td class="actions"><button class="button mini ghost" data-q="up" data-i="${i + 1}">↑</button> <button class="button mini ghost" data-q="down" data-i="${i + 1}">↓</button> <button class="button mini ghost" data-q="edit" data-i="${i + 1}" data-content="${esc(content)}">编辑</button> <button class="button mini danger" data-q="delete" data-i="${i + 1}">删除</button></td></tr>`;
      }).join('') || '<tr><td colspan="4" class="muted">当前队列为空</td></tr>';
      message('queue-status', `当前 ${Number(payload.size ?? entries.length)} 人`);
    } catch (error) { message('queue-status', `读取队列失败：${error.message}`, false); }
  }
  async function queueAction(path, payload) {
    try { await post(path, payload); await refreshQueue(); }
    catch (error) { message('queue-status', `操作失败：${error.message}`, false); }
  }

  async function loadPlatform() {
    try {
      const cfg = await fullConfig();
      const bili = cfg.bilibili || cfg.api || {};
      const douyin = cfg.douyin || {};
      $('platform-select').value = cfg.platform || 'bilibili';
      $('platform-bili-room').value = String(bili.roomid || 0);
      $('platform-bili-uid').value = String(bili.uid || 0);
      $('platform-douyin-live').value = String(douyin.live_id || '');
      $('platform-douyin-enabled').checked = Boolean(douyin.enabled);
      $('platform-douyin-cookie').value = String(douyin.cookie || '');
      message('platform-status', '平台参数已加载。');
    } catch (error) { message('platform-status', `加载失败：${error.message}`, false); }
  }
  async function savePlatform() {
    try {
      const cfg = await fullConfig();
      cfg.platform = $('platform-select').value;
      cfg.bilibili = { ...(cfg.bilibili || cfg.api || {}), roomid: intValue('platform-bili-room', 0, 0), uid: intValue('platform-bili-uid', 0, 0) };
      cfg.douyin = { ...(cfg.douyin || {}), enabled: $('platform-douyin-enabled').checked, live_id: $('platform-douyin-live').value.trim(), cookie: $('platform-douyin-cookie').value.trim() };
      await post('/api/config', cfg);
      message('platform-status', '平台参数已保存。');
      await Promise.all([refreshHeader(), loadRawConfig()]);
    } catch (error) { message('platform-status', `保存失败：${error.message}`, false); }
  }

  async function loadGifts() {
    try {
      const [cfg, giftState] = await Promise.all([fullConfig(), json('/api/gifts/state')]);
      const myjs = cfg.myjs || {};
      const names = Array.isArray(myjs.gift_queue_names) ? myjs.gift_queue_names : (giftState.gift_names || []);
      $('gift-enabled').checked = Boolean(myjs.gift_queue_enabled ?? giftState.enabled);
      $('gift-names').value = names.join('\n');
      $('gift-min-batteries').value = String(myjs.gift_queue_min_batteries ?? giftState.min_batteries ?? 0);
      $('gift-multiple').checked = Boolean(myjs.gift_queue_allow_multiple ?? giftState.allow_multiple);
      $('gift-slots').value = String(myjs.gift_queue_slots_per_gift ?? giftState.slots_per_gift ?? 1);
      $('gift-rank').value = String(myjs.gift_queue_insert_rank ?? giftState.insert_rank ?? 1);
      $('gift-saved-rank').value = String(myjs.gift_queue_saved_insert_rank ?? 1);
      $('gift-only').checked = Boolean(myjs.gift_queue_only ?? giftState.gift_only);
      const display = { ...giftState }; delete display.status;
      $('gift-state').textContent = JSON.stringify(display, null, 2);
      message('gift-status', `礼物规则已加载；当前观测到 ${Array.isArray(giftState.observed_catalog) ? giftState.observed_catalog.length : 0} 种直播间礼物。`);
    } catch (error) { message('gift-status', `加载失败：${error.message}`, false); }
  }
  async function saveGifts() {
    try {
      const cfg = await fullConfig();
      const names = $('gift-names').value.split(/[，,\r\n]+/).map(value => value.trim()).filter(Boolean);
      const myjs = { ...(cfg.myjs || {}) };
      const giftOnly = $('gift-only').checked;
      const savedRank = intValue('gift-saved-rank', 1, 1);
      myjs.gift_queue_enabled = $('gift-enabled').checked;
      myjs.gift_queue_names = [...new Set(names)];
      myjs.gift_queue_rule = myjs.gift_queue_rule || 'gift_or_battery';
      myjs.gift_queue_min_batteries = intValue('gift-min-batteries', 0, 0);
      myjs.gift_queue_allow_multiple = $('gift-multiple').checked;
      myjs.gift_queue_slots_per_gift = intValue('gift-slots', 1, 1);
      myjs.gift_queue_saved_insert_rank = savedRank;
      myjs.gift_queue_insert_rank = giftOnly ? 0 : intValue('gift-rank', savedRank, 0);
      myjs.gift_queue_only = giftOnly;
      cfg.myjs = myjs;
      await post('/api/config', cfg);
      message('gift-status', 'B站礼物插队规则已保存。');
      await Promise.all([loadGifts(), loadRawConfig()]);
    } catch (error) { message('gift-status', `保存失败：${error.message}`, false); }
  }

  async function loadSwitches() {
    try {
      const payload = await json('/api/kaiguan');
      const data = Object.fromEntries(Object.entries(payload).filter(([k]) => k !== 'status'));
      $('switch-grid').innerHTML = Object.entries(data).map(([key, value]) => `<label class="switch-item"><span>${esc(key)}</span><input type="checkbox" data-switch="${esc(key)}" ${value ? 'checked' : ''}></label>`).join('');
      message('switch-status', '开关已加载。');
    } catch (error) { message('switch-status', `加载失败：${error.message}`, false); }
  }
  async function saveSwitches() {
    const payload = {}; document.querySelectorAll('[data-switch]').forEach(node => { payload[node.dataset.switch] = node.checked; });
    try { await post('/api/kaiguan', payload); message('switch-status', '功能开关已保存。'); }
    catch (error) { message('switch-status', `保存失败：${error.message}`, false); }
  }
  async function loadBlacklist() {
    try {
      const payload = await json('/api/blacklist/state'); const entries = payload.entries || payload.blacklist || [];
      $('blacklist-list').innerHTML = entries.map((name, i) => `<li><span>${esc(typeof name === 'string' ? name : (name.name || name.id || JSON.stringify(name)))}</span><button class="button mini danger" data-blacklist-delete="${i + 1}">删除</button></li>`).join('') || '<li class="muted">黑名单为空</li>';
      message('blacklist-status', `共 ${entries.length} 项。`);
    } catch (error) { message('blacklist-status', `加载失败：${error.message}`, false); }
  }
  async function loadStyle() {
    try { const payload = await json('/api/style'); delete payload.status; $('style-json').value = JSON.stringify(payload, null, 2); message('style-status', '样式已加载。'); }
    catch (error) { message('style-status', `加载失败：${error.message}`, false); }
  }
  async function loadRawConfig() {
    try { const payload = await fullConfig(); $('config-json').value = JSON.stringify(payload, null, 2); message('config-status', '完整配置已加载。'); }
    catch (error) { message('config-status', `加载失败：${error.message}`, false); }
  }
  function loadSettingsBundle() { loadPlatform(); loadGifts(); loadSwitches(); loadBlacklist(); loadStyle(); loadRawConfig(); }

  async function loadPermissions() {
    try {
      const payload = await json('/api/quanxian');
      ['super_admin','admin','jianzhang','member'].forEach(key => { const v = payload[key]; $(`perm-${key}`).value = Array.isArray(v) ? v.join('\n') : ''; });
      message('perm-status', '权限已加载。');
    } catch (error) { message('perm-status', `加载失败：${error.message}`, false); }
  }
  async function savePermissions() {
    const payload = {}; ['super_admin','admin','jianzhang','member'].forEach(key => { payload[key] = $(`perm-${key}`).value.split(/\r?\n/).map(v => v.trim()).filter(Boolean); });
    try { await post('/api/quanxian', payload); message('perm-status', '权限已保存并重新加载。'); }
    catch (error) { message('perm-status', `保存失败：${error.message}`, false); }
  }

  async function refreshPerformance() {
    try {
      const payload = await json('/api/control/performance'); const sys = payload.system || {}, proc = payload.process || {};
      const metrics = [
        ['系统 CPU', `${Number(sys.cpu_percent || 0).toFixed(1)}%`],
        ['系统内存', `${Number(sys.memory_percent || 0).toFixed(1)}%`],
        ['进程内存', fmtBytes(proc.memory_rss)],
        ['磁盘', `${Number(sys.disk_percent || 0).toFixed(1)}%`],
        ['队列人数', String(payload.queue_size ?? 0)],
        ['WebSocket', String(payload.websocket_clients ?? 0)],
      ];
      $('perf-cards').innerHTML = metrics.map(([label, value]) => `<div class="metric"><span>${label}</span><strong>${value}</strong></div>`).join('');
      $('perf-detail').textContent = JSON.stringify(payload, null, 2);
    } catch (error) { $('perf-detail').textContent = `读取性能失败：${error.message}`; }
  }

  async function refreshUpdate(showStatus = true) {
    if (showStatus) message('update-status', '正在检查 GitHub Release…');
    try {
      const payload = await json('/api/control/update');
      $('update-current').textContent = payload.current_version || '-'; $('update-latest').textContent = payload.latest_version || '-';
      $('update-notes').textContent = payload.body || '暂无发行说明。';
      const link = $('update-open'); link.href = payload.html_url || '#'; link.classList.toggle('disabled', !payload.html_url);
      message('update-status', payload.current_version === payload.latest_version ? '当前已是最新版本。' : `发现版本 ${payload.tag_name || payload.latest_version}。Web Portable 请下载新包后完整解压替换。`);
    } catch (error) { message('update-status', error.message, false); }
  }

  document.querySelectorAll('.nav').forEach(btn => btn.addEventListener('click', () => switchView(btn.dataset.view)));
  document.querySelectorAll('.subtab').forEach(btn => btn.addEventListener('click', () => {
    document.querySelectorAll('.subtab').forEach(n => n.classList.toggle('active', n === btn));
    document.querySelectorAll('.settings-pane').forEach(n => n.classList.toggle('active', n.id === `settings-${btn.dataset.settings}`));
  }));
  $('refresh-all').addEventListener('click', async () => { await refreshHeader(); switchView(state.view); });
  $('log-refresh').addEventListener('click', refreshLogs); $('log-kind').addEventListener('change', refreshLogs); $('log-search').addEventListener('input', refreshLogs); $('log-clear').addEventListener('click', () => { $('log-output').textContent = ''; });
  $('queue-body').addEventListener('click', event => {
    const btn = event.target.closest('[data-q]'); if (!btn) return; const index = Number(btn.dataset.i); const action = btn.dataset.q;
    if (action === 'delete') return queueAction('/api/queue/delete', { index });
    if (action === 'up' || action === 'down') return queueAction('/api/queue/move', { index, direction: action });
    if (action === 'edit') { const content = prompt('修改排队内容', btn.dataset.content || ''); if (content !== null) queueAction('/api/queue/update', { index, content }); }
  });
  $('queue-insert').addEventListener('click', () => queueAction('/api/queue/insert', { after: Number($('queue-after').value || 0), entry: $('queue-entry').value.trim() }));
  $('queue-clear').addEventListener('click', () => { if (confirm('确认清空当前队列？')) queueAction('/api/queue/clear', {}); });
  $('queue-reload').addEventListener('click', () => queueAction('/api/queue/reload', {}));
  $('queue-switch').addEventListener('click', () => queueAction('/api/queue/switch', { slot: Number($('queue-slot').value || 1) }).then(refreshArchive));
  $('platform-save').addEventListener('click', savePlatform); $('platform-refresh').addEventListener('click', loadPlatform);
  $('gift-save').addEventListener('click', saveGifts); $('gift-refresh').addEventListener('click', loadGifts);
  $('switch-save').addEventListener('click', saveSwitches);
  $('blacklist-add').addEventListener('click', async () => { const name = $('blacklist-name').value.trim(); if (!name) return; try { await post('/api/blacklist/add', { name }); $('blacklist-name').value = ''; await loadBlacklist(); } catch (e) { message('blacklist-status', e.message, false); } });
  $('blacklist-clear').addEventListener('click', async () => { if (!confirm('确认清空黑名单？')) return; try { await post('/api/blacklist/clear', {}); await loadBlacklist(); } catch (e) { message('blacklist-status', e.message, false); } });
  $('blacklist-list').addEventListener('click', async event => { const btn = event.target.closest('[data-blacklist-delete]'); if (!btn) return; try { await post('/api/blacklist/delete', { index: Number(btn.dataset.blacklistDelete) }); await loadBlacklist(); } catch (e) { message('blacklist-status', e.message, false); } });
  $('style-save').addEventListener('click', async () => { try { const payload = JSON.parse($('style-json').value); await post('/api/style', payload); message('style-status', '样式保存成功。'); } catch (e) { message('style-status', `保存失败：${e.message}`, false); } });
  $('config-save').addEventListener('click', async () => { try { const payload = JSON.parse($('config-json').value); await post('/api/config', payload); message('config-status', '完整配置保存成功。'); await Promise.all([refreshHeader(), loadPlatform(), loadGifts()]); } catch (e) { message('config-status', `保存失败：${e.message}`, false); } });
  $('perm-save').addEventListener('click', savePermissions); $('perf-refresh').addEventListener('click', refreshPerformance); $('update-check').addEventListener('click', () => refreshUpdate(true));
  $('copy-overlay').addEventListener('click', async () => { try { await navigator.clipboard.writeText(`${location.origin}/index`); $('copy-overlay').textContent = '已复制'; setTimeout(() => $('copy-overlay').textContent = '复制 OBS 地址', 1200); } catch (_) { prompt('复制此地址', `${location.origin}/index`); } });
  $('overlay-url').textContent = `${location.origin}/index`;

  window.addEventListener('beforeunload', stopTimers);
  refreshHeader(); switchView('logs');
})();
