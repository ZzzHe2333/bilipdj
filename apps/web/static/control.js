(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const state = { view: 'queue', logTimer: 0, queueTimer: 0, perfTimer: 0, headerTimer: 0, meta: null, queue: null, moving: false, undo: null, undoTimer: 0 };

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

  async function checkStorageConflict() {
    try {
      const state = await json('/api/storage/status');
      if (!state.conflict) return;
      const dialog = $('storage-conflict-dialog');
      $('storage-conflict-paths').textContent = '项目目录：' + state.legacy + ' ｜ 用户目录：' + state.user;
      if (dialog && !dialog.open) dialog.showModal();
    } catch (_) { /* Older servers may not yet implement this API. */ }
  }

  async function selectStorage(choice) {
    try {
      await post('/api/storage/choice', {choice});
      $('storage-conflict-status').textContent = '选择已保存；必须重启 BiliPDJ 才会切换数据来源，两份原数据均未删除。';
      $('storage-use-user').disabled = true;
      $('storage-use-legacy').disabled = true;
    } catch (error) {
      $('storage-conflict-status').textContent = '保存选择失败：' + error.message;
    }
  }

  function switchView(name) {
    state.view = name;
    document.querySelectorAll('.nav').forEach(n => n.classList.toggle('active', n.dataset.view === name));
    document.querySelectorAll('.view').forEach(v => v.classList.toggle('active', v.id === `view-${name}`));
    stopTimers();
    if (name === 'logs') { refreshLogs(); state.logTimer = window.setInterval(refreshLogs, 2500); }
    if (name === 'queue') { refreshQueueAll(); state.queueTimer = window.setInterval(refreshQueue, 2600); }
    if (name === 'settings') loadSettingsBundle();
    if (name === 'permissions') loadPermissions();
    if (name === 'performance') { refreshPerformance(); state.perfTimer = window.setInterval(refreshPerformance, 2500); }
    if (name === 'update') refreshUpdate(false);
  }
  function stopTimers() {
    if (state.logTimer) clearInterval(state.logTimer); state.logTimer = 0;
    if (state.queueTimer) clearInterval(state.queueTimer); state.queueTimer = 0;
    if (state.perfTimer) clearInterval(state.perfTimer); state.perfTimer = 0;
  }

  async function refreshHeader() {
    try {
      const [health, meta, runtime] = await Promise.all([json('/health'), json('/api/control/meta'), json('/api/runtime-status')]);
      state.meta = meta;
      $('version').textContent = `v${meta.version || 'unknown'} · ${meta.deployment === 'docker' ? 'Docker Web 控制台' : 'Web 控制台'}`;
      $('health-dot').className = 'dot ok';
      const connected = Boolean(runtime.danmu_connected || runtime.connected || runtime.relay_connected);
      const room = runtime.roomid || runtime.room_id || '';
      $('health-text').textContent = `${health.service || 'bilipdj'} 已连接${room ? ` · 房间 ${room}` : ''}${connected ? ' · 弹幕在线' : ''}`;
      await refreshPlatformConnections();
    } catch (error) {
      $('health-dot').className = 'dot bad'; $('health-text').textContent = `后端连接异常：${error.message}`;
      for (const platform of ['bilibili', 'douyin']) setPlatformStatus(platform, '断开', 'disconnected');
    }
  }

  function setPlatformStatus(platform, label, status) {
    const node = $('platform-status-' + platform);
    if (!node) return;
    node.className = 'platform-chip ' + status;
    node.textContent = (platform === 'bilibili' ? 'B站' : '抖音') + ' · ' + label;
  }
  async function refreshPlatformConnections() {
    try {
      const result = await json('/api/platforms/active');
      const active = Array.isArray(result.active) ? result.active : [];
      const platforms = result.runtime?.platforms || {};
      for (const platform of ['bilibili','douyin']) {
        const detail = platforms[platform] || {};
        if (!active.includes(platform)) { setPlatformStatus(platform, '未启用', 'disabled'); continue; }
        if (detail.connected) { setPlatformStatus(platform, '已连接', 'connected'); continue; }
        const reason = detail.last_disconnect_reason || detail.error || '';
        setPlatformStatus(platform, reason ? '断开' : '重连中', reason ? 'disconnected' : 'reconnecting');
      }
    } catch (_) {
      for (const platform of ['bilibili','douyin']) setPlatformStatus(platform, '状态未知', 'checking');
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
      refreshCurrentQueue(entries);
      $('queue-body').innerHTML = entries.map((entry, i) => {
        const content = entry.content ?? entry.entry ?? entry.text ?? payload.queue?.[i] ?? '';
        const last = entry.last_operation_at || entry.updated_at || '';
        return `<tr><td>${i + 1}</td><td>${esc(content)}</td><td class="muted">${esc(last)}</td><td class="actions"><button class="button mini ghost" data-q="up" data-i="${i + 1}">↑</button> <button class="button mini ghost" data-q="down" data-i="${i + 1}">↓</button> <button class="button mini ghost" data-q="edit" data-i="${i + 1}" data-content="${esc(content)}">编辑</button> <button class="button mini danger" data-q="delete" data-i="${i + 1}">删除</button></td></tr>`;
      }).join('') || '<tr><td colspan="4" class="muted">当前队列为空</td></tr>';
      message('queue-status', `当前 ${Number(payload.size ?? entries.length)} 人`);
    } catch (error) { message('queue-status', `读取队列失败：${error.message}`, false); }
  }
  function refreshCurrentQueue(entries) {
    const current = Array.isArray(entries) ? entries[0] : null;
    const name = current?.id ?? current?.username ?? current?.content ?? current?.entry ?? '队列为空';
    const detail = current?.content && current?.id ? current.content : '';
    if ($('queue-current-name')) $('queue-current-name').textContent = String(name);
    if ($('queue-current-detail')) $('queue-current-detail').textContent = String(detail || (current ? '下一个操作：完成当前项并进入下一位' : '等待观众加入队列'));
    if ($('queue-next')) $('queue-next').disabled = !current || state.moving;
  }

  function queueSignature(snapshot) {
    const entries = Array.isArray(snapshot?.entries) ? snapshot.entries : (snapshot?.queue || []);
    return JSON.stringify(entries.map(e => typeof e === 'string' ? e : [e.id ?? e.username ?? '', e.content ?? '']));
  }
  function clearQueueUndo() {
    state.undo = null;
    if (state.undoTimer) clearTimeout(state.undoTimer);
    state.undoTimer = 0;
    if ($('queue-undo')) $('queue-undo').hidden = true;
  }
  function offerQueueUndo(record, signature) {
    clearQueueUndo();
    state.undo = { record, signature };
    if ($('queue-undo')) $('queue-undo').hidden = false;
    state.undoTimer = setTimeout(clearQueueUndo, 9000);
  }
  async function undoCompleted() {
    if (state.moving || !state.undo) return;
    const item = state.undo;
    clearQueueUndo();
    state.moving = true;
    try {
      const current = await json('/api/queue/state');
      if (queueSignature(current) !== item.signature) {
        message('queue-status', '队列已发生变化，为避免错误恢复，撤销已取消。', false);
        return;
      }
      await post('/api/queue/insert', { after: 0, ...item.record });
      await refreshQueue();
      message('queue-status', '已撤销上一次“下一位”。');
    } catch (error) { message('queue-status', '撤销失败：' + error.message, false); }
    finally { state.moving = false; }
  }

  async function completeCurrent() {
    if (state.moving) return;
    state.moving = true;
    if ($('queue-next')) $('queue-next').disabled = true;
    try {
      // Read fresh state immediately before a destructive operation.
      const snapshot = await json('/api/queue/state');
      if (!Number(snapshot.size ?? (snapshot.entries || snapshot.queue || []).length)) {
        message('queue-status', '当前队列为空。'); return;
      }
      const first = Array.isArray(snapshot.entries) ? snapshot.entries[0] : null;
      const fallback = String(snapshot.queue?.[0] || '').trim();
      const name = String(first?.id ?? first?.username ?? fallback.split(' ')[0] ?? '').trim();
      const content = String(first?.content ?? fallback.slice(name.length).trim() ?? '').trim();
      if (!name) { message('queue-status','无法取得可靠的队首用户名，已取消操作。',false); return; }
      clearQueueUndo();
      await post('/api/queue/delete', { index: 1 });
      const after = await json('/api/queue/state');
      state.queue = after;
      await refreshQueue();
      offerQueueUndo({ username:name, content, entry:content ? name+' '+content : name, platform: String(first?.platform || 'manual') }, queueSignature(after));
      message('queue-status', '已进入下一位；9 秒内可以撤销。');
    } catch (error) { message('queue-status', '下一位失败：' + error.message, false); }
    finally { state.moving = false; if ($('queue-next')) $('queue-next').disabled = !Number(state.queue?.size ?? (state.queue?.entries || state.queue?.queue || []).length); }
  }
  async function moveQueueItem(from, to) {
    if (state.moving || from === to) return;
    const snap = await json('/api/queue/state');
    const size = Number(snap.size ?? (snap.entries || snap.queue || []).length);
    if (!Number.isInteger(from) || !Number.isInteger(to) || from < 1 || to < 1 || from > size || to > size) return;
    state.moving = true;
    try {
      const direction = from > to ? 'up' : 'down';
      let current = from;
      while (current !== to) {
        await post('/api/queue/move', { index: current, direction });
        current += direction === 'up' ? -1 : 1;
      }
      await refreshQueue();
    } catch (error) { message('queue-status', '排序失败：' + error.message, false); await refreshQueue(); }
    finally { state.moving = false; }
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

  const permissionRoles = {super_admin: '最高管理员', admin: '管理员'};
  const permissionPlatforms = {bilibili: 'Bilibili', douyin: '抖音', huya: '虎牙', youtube: 'YouTube', twitch: 'Twitch'};
  let permissionRows = [], permissionBlacklist = [], editingPermission = -1;

  function renderPermissionRows() {
    const list = $('perm-list');
    list.innerHTML = permissionRows.map((entry, index) => `<button type="button" class="permission-row" data-perm-edit="${index}" aria-label="编辑 ${esc(entry.id)} 的权限"><span class="permission-identity"><strong>${esc(entry.id)}</strong><small>${entry.kind === 'name' ? '历史用户名' : '平台用户 ID'}</small></span><span>${esc(permissionRoles[entry.role] || entry.role)}</span><span>${esc(entry.platforms.length ? entry.platforms.map(p => permissionPlatforms[p] || p).join(' / ') : '全平台')}</span><span>编辑 ›</span></button>`).join('') || '<div class="permission-empty">暂无管理员，点击右上角「＋ 新增管理员」。</div>';
  }

  async function loadPermissions() {
    try {
      const payload = await json('/api/quanxian');
      permissionRows = Array.isArray(payload.entries) ? payload.entries.map(item => ({
        id: String(item.id || ''), kind: item.kind === 'name' ? 'name' : 'id',
        role: item.role, platforms: Array.isArray(item.platforms) ? item.platforms.slice() : []
      })).filter(item => Object.hasOwn(permissionRoles, item.role)) : ['super_admin','admin'].flatMap(role =>
        (payload[role] || []).map(id => ({id, kind: 'name', role, platforms: []})));
      permissionBlacklist = Array.isArray(payload.blacklist) ? payload.blacklist.slice() : [];
      // Bilibili-only guards and ordinary members keep their existing name lists.
      // They are NEVER interpreted as multi-platform administrator entries.
      for (const role of ['jianzhang', 'member']) {
        const entries = Array.isArray(payload[role]) ? payload[role] : [];
        $('perm-legacy-' + role).value = entries.join('\n');
      }
      renderPermissionRows();
      message('perm-status', `已加载 ${permissionRows.length} 条权限记录。`);
    } catch (error) { message('perm-status', `加载失败：${error.message}`, false); }
  }

  async function savePermissions() {
    const readLegacyNames = role => [...new Set($('perm-legacy-' + role).value.split(/\r?\n/).map(v => v.trim()).filter(Boolean))];
    const payload = {super_admin: [], admin: [], jianzhang: readLegacyNames('jianzhang'), member: readLegacyNames('member'), blacklist: permissionBlacklist.slice(), scoped_entries: []};
    permissionRows.filter(entry => Object.hasOwn(permissionRoles, entry.role)).forEach(entry => {
      if (entry.kind === 'name' && entry.platforms.length === 0) payload[entry.role].push(entry.id);
      else payload.scoped_entries.push({ id: entry.id, kind: entry.kind, role: entry.role, platforms: entry.platforms });
    });
    try {
      await post('/api/quanxian', payload);
      message('perm-status', '权限已保存并应用到全部监听平台。');
      await loadPermissions();
      return true;
    } catch (error) { message('perm-status', `保存失败：${error.message}`, false); return false; }
  }

  function editPermission(index = -1) {
    editingPermission = index;
    const entry = index >= 0 ? permissionRows[index] : {id: '', kind: 'id', role: 'admin', platforms: []};
    $('perm-dialog-title').textContent = index < 0 ? '新增权限' : '编辑权限';
    $('perm-kind').value = entry.kind;
    $('perm-identity').value = entry.id;
    $('perm-role').value = entry.role;
    document.querySelectorAll('[name="perm-platform"]').forEach(cb => { cb.checked = entry.platforms.includes(cb.value); });
    $('perm-delete').hidden = index < 0;
    $('perm-dialog').showModal();
    $('perm-identity').focus();
  }

  async function applyPermission(event) {
    event.preventDefault();
    const id = $('perm-identity').value.trim();
    const kind = $('perm-kind').value;
    const role = $('perm-role').value;
    const platforms = [...document.querySelectorAll('[name="perm-platform"]:checked')].map(node => node.value);
    if (!id || /[\r\n\0]/.test(id)) { $('perm-identity').reportValidity(); return; }
    if (!Object.hasOwn(permissionRoles, role)) { message('perm-status', '仅最高管理员和管理员可以配置来源平台。', false); return; }
    if (permissionRows.some((r, i) => i !== editingPermission && r.id === id && r.kind === kind && r.role === role && JSON.stringify(r.platforms.slice().sort()) === JSON.stringify(platforms.slice().sort()))) {
      message('perm-status', '该用户、角色和平台组合已存在。', false); return;
    }
    if (editingPermission >= 0) permissionRows[editingPermission] = {id, kind, role, platforms};
    else permissionRows.push({id, kind, role, platforms});
    renderPermissionRows();
    $('perm-dialog').close();
    await savePermissions();
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
      const docker = state.meta?.deployment === 'docker';
      message('update-status', docker
        ? (payload.current_version === payload.latest_version
          ? 'Docker 容器已是当前版本；需要重新部署时，请在宿主机执行 docker compose up -d --build，持久化 ./data 不受影响。'
          : `发现版本 ${payload.tag_name || payload.latest_version}。Docker 版不能使用 Web Portable 安装包覆盖容器；请在宿主机更新代码并执行 docker compose up -d --build。`)
        : (payload.current_version === payload.latest_version ? '当前已是最新版本。' : `发现版本 ${payload.tag_name || payload.latest_version}。Web Portable 请下载新包后完整解压替换。`));
    } catch (error) { message('update-status', error.message, false); }
  }

  document.querySelectorAll('.nav').forEach(btn => btn.addEventListener('click', () => switchView(btn.dataset.view)));
  document.querySelectorAll('.subtab').forEach(btn => btn.addEventListener('click', () => {
    document.querySelectorAll('.subtab').forEach(n => n.classList.toggle('active', n === btn));
    document.querySelectorAll('.settings-pane').forEach(n => n.classList.toggle('active', n.id === `settings-${btn.dataset.settings}`));
  }));
  const compactKey = 'bilipdj.console.compact';
  const applyCompact = enabled => {
    document.body.classList.toggle('compact-mode', enabled);
    const button = $('compact-console');
    button.setAttribute('aria-pressed', String(enabled));
    button.textContent = enabled ? '标准模式' : '紧凑模式';
  };
  try { applyCompact(localStorage.getItem(compactKey) === '1'); } catch (_) { applyCompact(false); }
  $('compact-console').addEventListener('click', () => {
    const enabled = !document.body.classList.contains('compact-mode');
    applyCompact(enabled);
    try { localStorage.setItem(compactKey, enabled ? '1' : '0'); } catch (_) {}
  });
  $('refresh-all').addEventListener('click', async () => { await refreshHeader(); switchView(state.view); });
  $('log-refresh').addEventListener('click', refreshLogs); $('log-kind').addEventListener('change', refreshLogs); $('log-search').addEventListener('input', refreshLogs); $('log-clear').addEventListener('click', () => { $('log-output').textContent = ''; });
  $('queue-body').addEventListener('click', event => {
    const btn = event.target.closest('[data-q]'); if (!btn) return; const index = Number(btn.dataset.i); const action = btn.dataset.q;
    if (action === 'done' && index === 1) return completeCurrent();
    if (action === 'top') return moveQueueItem(index, 1);
    if (action === 'delete') return queueAction('/api/queue/delete', { index });
    if (action === 'up' || action === 'down') return queueAction('/api/queue/move', { index, direction: action });
    if (action === 'edit') { const content = prompt('修改排队内容', btn.dataset.content || ''); if (content !== null) queueAction('/api/queue/update', { index, content }); }
  });
  $('queue-next').addEventListener('click', completeCurrent);
  $('queue-undo-button').addEventListener('click', undoCompleted);
  document.addEventListener('keydown', event => {
    if (state.view !== 'queue' || event.repeat || event.key.toLowerCase() !== 'n' || !event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
    if (event.target?.closest?.('input,select,textarea,[contenteditable="true"]')) return;
    event.preventDefault();
    void completeCurrent();
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
  $('storage-use-user').addEventListener('click', () => selectStorage('user'));
  $('storage-use-legacy').addEventListener('click', () => selectStorage('legacy'));
  $('perm-add').addEventListener('click', () => editPermission());
  $('perm-list').addEventListener('click', event => { const row = event.target.closest('[data-perm-edit]'); if (row) editPermission(Number(row.dataset.permEdit)); });
  $('perm-form').addEventListener('submit', applyPermission);
  $('perm-cancel').addEventListener('click', () => $('perm-dialog').close());
  $('perm-delete').addEventListener('click', async () => {
    if (editingPermission < 0 || !confirm('确认移除此账号的权限记录？')) return;
    permissionRows.splice(editingPermission, 1);
    $('perm-dialog').close(); renderPermissionRows(); await savePermissions();
  });
  $('perm-refresh').addEventListener('click', loadPermissions);
  $('perm-save').addEventListener('click', savePermissions); $('perf-refresh').addEventListener('click', refreshPerformance); $('update-check').addEventListener('click', () => refreshUpdate(true));
  $('copy-overlay').addEventListener('click', async () => { try { await navigator.clipboard.writeText(`${location.origin}/index`); $('copy-overlay').textContent = '已复制'; setTimeout(() => $('copy-overlay').textContent = '复制 OBS 地址', 1200); } catch (_) { prompt('复制此地址', `${location.origin}/index`); } });
  $('overlay-url').textContent = `${location.origin}/index`;

  window.addEventListener('beforeunload', () => { stopTimers(); if (state.headerTimer) clearInterval(state.headerTimer); });
  refreshHeader(); state.headerTimer = window.setInterval(refreshHeader, 4500); switchView('queue'); void checkStorageConflict();
})();
