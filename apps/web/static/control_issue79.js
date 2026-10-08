(() => {
  'use strict';

  const $ = id => document.getElementById(id);
  const REPO_URL = 'https://github.com/ZzzHe2333/bilipdj';
  const RELEASES_URL = 'https://github.com/ZzzHe2333/bilipdj/releases';
  const THEME_KEY = 'bilipdj.control.theme.v1';
  let queueObserver = null;
  let queueRenderPending = false;

  async function api(path, options = {}) {
    const response = await fetch(path, { cache: 'no-store', ...options });
    let payload = {};
    try { payload = await response.json(); } catch (_) { /* ignore */ }
    if (!response.ok || payload.status === 'error') throw new Error(payload.message || `HTTP ${response.status}`);
    return payload;
  }
  function post(path, payload) {
    return api(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload || {}) });
  }
  function esc(value) {
    return String(value ?? '').replace(/[&<>"']/g, char => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[char]));
  }
  function setStatus(id, text, ok = true) {
    const node = $(id); if (!node) return;
    node.textContent = text || '';
    node.style.color = ok ? '' : '#d84f63';
  }

  // ---------- Navigation / project pages ----------
  function normalizeProjectNavigation() {
    const sidebar = document.querySelector('.sidebar');
    const about = sidebar?.querySelector('[data-view="about"]');
    const support = sidebar?.querySelector('[data-view="support"]');
    if (about) about.textContent = '关于项目';
    if (support) support.textContent = '支持我们';
    if (sidebar && about && support && about.nextElementSibling !== support) sidebar.insertBefore(about, support);

    const aboutView = $('view-about');
    const card = aboutView?.querySelector('.card.about');
    if (aboutView) {
      const heading = aboutView.querySelector('.page-head h1');
      const subtitle = aboutView.querySelector('.page-head p');
      if (heading) heading.textContent = '关于项目';
      if (subtitle) subtitle.textContent = '项目源码、版本与发行包信息';
    }
    if (card && !card.querySelector('.issue79-project-links')) {
      const links = document.createElement('div');
      links.className = 'issue79-project-links';
      links.innerHTML = `<a class="button" href="${REPO_URL}" target="_blank" rel="noopener">GitHub 仓库</a><a class="button ghost" href="${RELEASES_URL}" target="_blank" rel="noopener">Releases 发行包</a>`;
      card.appendChild(links);
      const note = document.createElement('p');
      note.className = 'hint';
      note.textContent = `发行包地址：${RELEASES_URL}`;
      card.appendChild(note);
    }
  }

  // ---------- Detailed queue: username(required) + content(optional) ----------
  function ensureQueueInputs() {
    const row = document.querySelector('#view-queue .queue-add');
    const content = $('queue-entry');
    if (!row || !content || $('queue-username')) return;
    row.classList.add('issue79-two-fields');
    const username = document.createElement('input');
    username.id = 'queue-username';
    username.className = 'issue79-required';
    username.placeholder = '填写用户名（必填）';
    username.autocomplete = 'off';
    row.insertBefore(username, content);
    content.placeholder = '填写排队内容（可选）';

    const insert = $('queue-insert');
    insert?.addEventListener('click', async event => {
      event.preventDefault();
      event.stopImmediatePropagation();
      const name = username.value.trim();
      const detail = content.value.trim();
      if (!name) {
        setStatus('queue-status', '用户名为必填项。', false);
        username.focus();
        return;
      }
      try {
        await post('/api/queue/insert', {
          after: Number($('queue-after')?.value || 0),
          username: name,
          content: detail,
          entry: detail ? `${name} ${detail}` : name,
        });
        username.value = '';
        content.value = '';
        await renderQueueDetailed();
        username.focus();
      } catch (error) {
        setStatus('queue-status', `新增失败：${error.message}`, false);
      }
    }, true);
    for (const field of [username, content, $('queue-after')]) {
      field?.addEventListener('keydown', event => {
        if (event.key !== 'Enter' || event.isComposing || event.shiftKey || event.ctrlKey || event.metaKey) return;
        event.preventDefault();
        insert?.click();
      });
    }
  }

  async function renderQueueDetailed() {
    const body = $('queue-body');
    if (!body) return;
    try {
      const payload = await api('/api/queue/state');
      const entries = Array.isArray(payload.entries) && payload.entries.length
        ? payload.entries
        : (payload.queue || []).map(value => {
            const text = String(value || '').trim();
            const split = text.indexOf(' ');
            return split < 0 ? { id: text, content: '' } : { id: text.slice(0, split), content: text.slice(split + 1) };
          });
      const header = body.closest('table')?.querySelector('thead tr');
      if (header) header.innerHTML = '<th>#</th><th>用户名</th><th>内容</th><th>最近操作</th><th>操作</th>';
      if (queueObserver) queueObserver.disconnect();
      body.innerHTML = entries.map((entry, i) => {
        const username = String(entry.id ?? entry.username ?? '').trim();
        const content = String(entry.content ?? '').trim();
        const last = String(entry.last_operation_at ?? entry.updated_at ?? '');
        return `<tr draggable="true" data-queue-row="${i + 1}" class="${i === 0 ? 'queue-row-active' : 'queue-row-waiting'}"><td><span class="queue-drag-handle" title="拖动排序">⇅</span> ${i + 1}</td><td><strong class="queue-name">${esc(username)}</strong><span class="queue-state-label ${i === 0 ? 'in-progress' : 'waiting'}">${i === 0 ? '进行中' : '等待中'}</span></td><td>${esc(content || '—')}</td><td class="muted">${esc(last)}</td><td class="actions">${i === 0 ? '<button class="button mini" data-q="done" data-i="1">完成</button>' : ''} <button class="button mini ghost" data-q="top" data-i="${i + 1}">置顶</button> <button class="button mini ghost" data-q="up" data-i="${i + 1}">↑</button> <button class="button mini ghost" data-q="down" data-i="${i + 1}">↓</button> <button class="button mini ghost" data-issue79-edit="${i + 1}" data-username="${esc(username)}" data-content="${esc(content)}">编辑</button> <button class="button mini danger" data-q="delete" data-i="${i + 1}">删除</button></td></tr>`;
      }).join('') || '<tr><td colspan="5" class="muted">当前队列为空</td></tr>';
      setStatus('queue-status', `当前 ${Number(payload.size ?? entries.length)} 人`);
    } catch (error) {
      setStatus('queue-status', `读取队列失败：${error.message}`, false);
    } finally {
      if (queueObserver && body) queueObserver.observe(body, { childList: true, subtree: true });
    }
  }

  function installQueueObserver() {
    const body = $('queue-body'); if (!body) return;
    let dragFrom = 0;
    body.addEventListener('dragstart', event => {
      const row = event.target.closest?.('[data-queue-row]');
      if (!row) return;
      dragFrom = Number(row.dataset.queueRow);
      event.dataTransfer.effectAllowed = 'move';
      event.dataTransfer.setData('text/plain', String(dragFrom));
    });
    body.addEventListener('dragover', event => {
      if (!dragFrom || !event.target.closest?.('[data-queue-row]')) return;
      event.preventDefault();
      event.dataTransfer.dropEffect = 'move';
    });
    body.addEventListener('drop', async event => {
      const row = event.target.closest?.('[data-queue-row]');
      if (!row || !dragFrom) return;
      event.preventDefault();
      const from = dragFrom, to = Number(row.dataset.queueRow);
      dragFrom = 0;
      if (from === to) return;
      try {
        const snapshot = await api('/api/queue/state');
        const size = Number(snapshot.size ?? (snapshot.entries || snapshot.queue || []).length);
        if (from < 1 || to < 1 || from > size || to > size) return;
        const direction = to < from ? 'up' : 'down';
        let at = from;
        while (at !== to) { await post('/api/queue/move', { index: at, direction }); at += direction === 'up' ? -1 : 1; }
        await renderQueueDetailed();
      } catch (error) {
        setStatus('queue-status', '拖动排序失败：' + error.message, false);
        await renderQueueDetailed();
      }
    });
    body.addEventListener('dragend', () => { dragFrom = 0; });
    body.addEventListener('click', async event => {
      const button = event.target.closest('[data-issue79-edit]');
      if (!button) return;
      event.preventDefault();
      event.stopImmediatePropagation();
      const username = String(button.dataset.username || '').trim();
      const oldContent = String(button.dataset.content || '');
      const next = prompt(`修改 ${username} 的排队内容（可留空）`, oldContent);
      if (next === null) return;
      const content = next.trim();
      try {
        await post('/api/queue/update', {
          index: Number(button.dataset.issue79Edit),
          content: content ? `${username} ${content}` : username,
        });
        await renderQueueDetailed();
      } catch (error) {
        setStatus('queue-status', `编辑失败：${error.message}`, false);
      }
    }, true);

    queueObserver = new MutationObserver(() => {
      if (queueRenderPending) return;
      queueRenderPending = true;
      window.setTimeout(async () => {
        queueRenderPending = false;
        await renderQueueDetailed();
      }, 40);
    });
    queueObserver.observe(body, { childList: true, subtree: true });
    document.querySelector('[data-view="queue"]')?.addEventListener('click', () => window.setTimeout(renderQueueDetailed, 60));
    renderQueueDetailed();
  }

  // ---------- Active platforms ----------
  function activateSettingsPane(button, pane) {
    document.querySelectorAll('.subtab').forEach(node => node.classList.toggle('active', node === button));
    document.querySelectorAll('.settings-pane').forEach(node => node.classList.toggle('active', node === pane));
  }

  function ensureActivePlatformPane() {
    const subtabs = document.querySelector('#view-settings .subtabs');
    const settingsView = $('view-settings');
    if (!subtabs || !settingsView || $('settings-active-platforms')) return;
    const button = document.createElement('button');
    button.className = 'subtab';
    button.dataset.settings = 'active-platforms';
    button.textContent = '激活平台';
    const platformButton = subtabs.querySelector('[data-settings="platform"]');
    if (platformButton?.nextSibling) subtabs.insertBefore(button, platformButton.nextSibling); else subtabs.appendChild(button);

    const pane = document.createElement('div');
    pane.className = 'settings-pane';
    pane.id = 'settings-active-platforms';
    pane.innerHTML = `<div class="card"><div class="issue79-section-title"><div><h3>激活平台</h3><p class="hint">多个平台可同时接收弹幕并进入同一个排队系统；当前每个平台只支持一个直播间。</p></div></div><div class="issue79-platform-grid"><label class="issue79-platform-option"><input id="active-bilibili" type="checkbox"><div><strong>Bilibili</strong><span>使用“平台参数”中配置的唯一 B站直播间。</span></div></label><label class="issue79-platform-option"><input id="active-douyin" type="checkbox"><div><strong>抖音</strong><span>使用“平台参数”中配置的唯一抖音 Live ID。</span></div></label></div><p class="hint">虎牙、快手、斗鱼、微信视频号当前仍为预留配置，暂不能激活弹幕流。</p><div class="toolbar" style="margin-top:14px"><button id="active-platform-save" class="button">保存并重连</button><button id="active-platform-refresh" class="button ghost">刷新状态</button></div><div id="active-platform-status" class="status"></div></div>`;
    const firstPane = settingsView.querySelector('.settings-pane');
    if (firstPane) firstPane.parentNode.insertBefore(pane, firstPane.nextSibling); else settingsView.appendChild(pane);
    button.addEventListener('click', () => { activateSettingsPane(button, pane); loadActivePlatforms(); });
    $('active-platform-refresh')?.addEventListener('click', loadActivePlatforms);
    $('active-platform-save')?.addEventListener('click', saveActivePlatforms);
  }

  async function loadActivePlatforms() {
    try {
      const payload = await api('/api/platforms/active');
      const active = new Set(Array.isArray(payload.active) ? payload.active : []);
      if ($('active-bilibili')) $('active-bilibili').checked = active.has('bilibili');
      if ($('active-douyin')) $('active-douyin').checked = active.has('douyin');
      const states = payload.runtime?.platforms || {};
      const online = Object.entries(states).filter(([, value]) => Boolean(value?.connected)).map(([name]) => name);
      setStatus('active-platform-status', `已激活：${[...active].join(' + ') || '无'}；在线：${online.join(' + ') || '暂无'}。每个平台仅监听一个直播间。`);
    } catch (error) {
      setStatus('active-platform-status', `读取失败：${error.message}`, false);
    }
  }
  async function saveActivePlatforms() {
    const active = [];
    if ($('active-bilibili')?.checked) active.push('bilibili');
    if ($('active-douyin')?.checked) active.push('douyin');
    setStatus('active-platform-status', '正在保存并重连弹幕流……');
    try {
      await post('/api/platforms/active', { active });
      await loadActivePlatforms();
    } catch (error) {
      setStatus('active-platform-status', `保存失败：${error.message}`, false);
    }
  }

  // ---------- Visual style editor with realtime preview ----------
  const STYLE_FIELDS = [
    ['bg1','背景色 1','color'], ['bg2','背景色 2','color'], ['bg3','背景色 3','color'],
    ['text_color','文字颜色','color'], ['text_stroke_color','描边颜色','color'],
    ['queue_font_size','字号','number'], ['queue_font_weight','字重','select'], ['queue_font_style','字体样式','select'],
    ['queue_font_family','字体','text'], ['queue_line_height','行高','number'], ['queue_item_gap','条目间距','number'],
    ['queue_letter_spacing','字间距','number'], ['queue_word_spacing','词间距','number'],
    ['queue_text_opacity','文字透明度','number'], ['queue_item_padding_x','左右内边距','number'], ['queue_item_padding_y','上下内边距','number'],
    ['queue_text_align','文字对齐','select'],
  ];
  const STYLE_DEFAULTS = { bg1:'#0e2036',bg2:'#060b14',bg3:'#020409',text_color:'#eaf6ff',text_stroke_color:'#000000',text_stroke_enabled:true,queue_font_size:50,queue_font_weight:'700',queue_font_style:'normal',queue_font_family:'Microsoft YaHei, Noto Sans SC, PingFang SC, sans-serif',queue_line_height:'1.20',queue_item_gap:10,queue_letter_spacing:0,queue_word_spacing:0,queue_text_opacity:100,queue_item_padding_x:14,queue_item_padding_y:8,queue_text_align:'left',auto_scroll:false,show_sequence:false };

  function styleFieldMarkup([key, label, kind]) {
    if (kind === 'select' && key === 'queue_font_weight') return `<label>${label}<select id="issue-style-${key}"><option value="400">常规 400</option><option value="500">中等 500</option><option value="600">半粗 600</option><option value="700">粗体 700</option><option value="800">特粗 800</option></select></label>`;
    if (kind === 'select' && key === 'queue_font_style') return `<label>${label}<select id="issue-style-${key}"><option value="normal">正常</option><option value="italic">斜体</option></select></label>`;
    if (kind === 'select' && key === 'queue_text_align') return `<label>${label}<select id="issue-style-${key}"><option value="left">左对齐</option><option value="center">居中</option><option value="right">右对齐</option></select></label>`;
    const attrs = kind === 'number' ? ' type="number" step="1"' : kind === 'color' ? ' type="color"' : ' type="text"';
    return `<label class="${key === 'queue_font_family' ? 'issue79-wide' : ''}">${label}<input id="issue-style-${key}"${attrs}></label>`;
  }

  function ensureVisualStyleEditor() {
    const pane = $('settings-style');
    const textarea = $('style-json');
    if (!pane || !textarea || $('issue79-style-editor')) return;
    const editor = document.createElement('div');
    editor.id = 'issue79-style-editor';
    editor.innerHTML = `<div class="card"><div class="issue79-section-title"><div><h3>可视化样式设置</h3><p class="hint">调整后下方实时预览；点击原“保存样式”按钮后写入后端并刷新队列看板。</p></div><button id="issue-style-reload" class="button ghost">重新读取</button></div><div class="issue79-editor-grid">${STYLE_FIELDS.map(styleFieldMarkup).join('')}<label>开启文字描边<input id="issue-style-text_stroke_enabled" type="checkbox"></label><label>自动滚动<input id="issue-style-auto_scroll" type="checkbox"></label><label>显示序号<input id="issue-style-show_sequence" type="checkbox"></label></div><div class="issue79-preview-tools">
       <label>预览背景 <select id="issue79-preview-bg"><option value="checker">透明棋盘格</option><option value="green">纯绿幕</option><option value="game">游戏画面（本地截图）</option></select></label>
       <label class="issue79-preview-upload">选择游戏截图 <input type="file" accept="image/png,image/jpeg,image/webp" id="issue79-preview-file"></label>
       <label>模拟数据 <select id="issue79-preview-case"><option value="long">超长用户名</option><option value="gift">礼物插队</option><option value="empty">空队列</option><option value="normal">正常队列</option></select></label>
       <button class="button ghost" id="issue79-copy-obs" type="button">复制 OBS 地址</button>
       </div><p id="issue79-contrast-warning" class="issue79-contrast-warning" role="status" aria-live="polite"></p><div class="issue79-preview"><div id="issue79-preview-stage" class="issue79-preview-stage preview-checker" class="issue79-preview-stage"><div id="issue79-preview-queue" class="issue79-preview-queue"><div class="issue79-preview-row"><span class="issue79-preview-number">01</span><span>雪梦茉莉 牵丝霖</span></div><div class="issue79-preview-row"><span class="issue79-preview-number">02</span><span>示例用户 排队内容</span></div><div class="issue79-preview-row"><span class="issue79-preview-number">03</span><span>第三位玩家</span></div></div></div></div><details class="issue79-advanced"><summary>高级 JSON（保留原编辑方式）</summary><p class="hint">可直接修改下方 JSON；重新读取会以服务器数据覆盖。</p></details></div>`;
    pane.insertBefore(editor, pane.firstChild);
    const details = editor.querySelector('.issue79-advanced');
    details?.appendChild(textarea);
    textarea.classList.add('tall');
    editor.querySelectorAll('input,select').forEach(node => node.addEventListener('input', applyStylePreview));
    $('issue79-preview-bg')?.addEventListener('change', updatePreviewBackground);
    $('issue79-preview-case')?.addEventListener('change', renderPreviewCase);
    $('issue79-preview-file')?.addEventListener('change', loadPreviewScreenshot);
    $('issue79-copy-obs')?.addEventListener('click', async () => {
      const value = location.origin + '/index';
      try { await navigator.clipboard.writeText(value); setStatus('style-status', 'OBS 地址已复制：' + value); }
      catch (_) { window.prompt('复制 OBS 地址', value); }
    });
    renderPreviewCase();
    updatePreviewBackground();
    $('issue-style-reload')?.addEventListener('click', loadVisualStyle);

    const styleButton = document.querySelector('[data-settings="style"]');
    styleButton?.addEventListener('click', () => window.setTimeout(loadVisualStyle, 20));
    $('style-save')?.addEventListener('click', () => {
      const payload = collectVisualStyle();
      textarea.value = JSON.stringify(payload, null, 2);
    }, true);
    loadVisualStyle();
  }

  function normalizeColor(value, fallback) {
    const text = String(value || '').trim();
    return /^#[0-9a-f]{6}$/i.test(text) ? text : fallback;
  }
  function fieldValue(key, fallback) {
    const node = $(`issue-style-${key}`); if (!node) return fallback;
    if (node.type === 'checkbox') return Boolean(node.checked);
    if (node.type === 'number') {
      const value = Number(node.value); return Number.isFinite(value) ? value : fallback;
    }
    return node.value || fallback;
  }
  function collectVisualStyle() {
    let base = {};
    try { base = JSON.parse($('style-json')?.value || '{}'); } catch (_) { base = {}; }
    const payload = { ...STYLE_DEFAULTS, ...base };
    STYLE_FIELDS.forEach(([key]) => { payload[key] = fieldValue(key, payload[key]); });
    payload.text_stroke_enabled = fieldValue('text_stroke_enabled', true);
    payload.auto_scroll = fieldValue('auto_scroll', false);
    payload.show_sequence = fieldValue('show_sequence', false);
    ['bg1','bg2','bg3','text_color','text_stroke_color'].forEach(key => { payload[key] = normalizeColor(payload[key], STYLE_DEFAULTS[key]); });
    payload.queue_font_size = Math.max(8, Math.min(300, Number(payload.queue_font_size || 50)));
    payload.queue_text_opacity = Math.max(0, Math.min(100, Number(payload.queue_text_opacity ?? 100)));
    payload.queue_line_height = String(Math.max(.6, Math.min(5, Number(payload.queue_line_height || 1.2))).toFixed(2));
    return payload;
  }
  function fillVisualStyle(style) {
    const payload = { ...STYLE_DEFAULTS, ...(style || {}) };
    STYLE_FIELDS.forEach(([key]) => {
      const node = $(`issue-style-${key}`); if (!node) return;
      const value = payload[key];
      node.value = String(value ?? STYLE_DEFAULTS[key] ?? '');
    });
    ['text_stroke_enabled','auto_scroll','show_sequence'].forEach(key => { const node = $(`issue-style-${key}`); if (node) node.checked = Boolean(payload[key]); });
    applyStylePreview();
  }
  async function loadVisualStyle() {
    try {
      const payload = await api('/api/style');
      delete payload.status;
      if ($('style-json')) $('style-json').value = JSON.stringify(payload, null, 2);
      fillVisualStyle(payload);
      setStatus('style-status', '样式已加载，可实时预览。');
    } catch (error) {
      setStatus('style-status', `样式加载失败：${error.message}`, false);
    }
  }
  let previewImageUrl = '';
  function renderPreviewCase() {
    const target = $('issue79-preview-queue');
    if (!target) return;
    const scenario = $('issue79-preview-case')?.value || 'long';
    const samples = {
      long: ['01', '某位名字特别特别特别长的直播观众_测试昵称ABCDEFGHIJKLMNOPQRSTUVWXYZ'],
      gift: ['01', '礼物插队 · 感谢观众支持', '02', '普通观众排队等待'],
      normal: ['01', '第一位玩家', '02', '第二位玩家'],
      empty: [],
    };
    const data = samples[scenario] || samples.normal;
    target.replaceChildren();
    if (data.length === 0) {
      const blank = document.createElement('div');
      blank.className = 'issue79-preview-empty';
      blank.textContent = '当前无人排队（空队列模拟）';
      target.appendChild(blank);
    } else {
      for (let i = 0; i < data.length; i += 2) {
        const row = document.createElement('div');
        row.className = 'issue79-preview-row';
        const number = document.createElement('span');
        number.className = 'issue79-preview-number';
        number.textContent = data[i];
        const text = document.createElement('span');
        text.className = 'issue79-preview-text';
        text.textContent = data[i + 1];
        row.append(number, text);
        target.appendChild(row);
      }
    }
    applyStylePreview();
  }
  function updatePreviewBackground() {
    const stage = $('issue79-preview-stage');
    if (!stage) return;
    const mode = $('issue79-preview-bg')?.value || 'checker';
    stage.classList.remove('preview-checker', 'preview-green', 'preview-game');
    stage.classList.add('preview-' + mode);
    stage.style.setProperty('--preview-game-image', mode === 'game' && previewImageUrl ? 'url("' + previewImageUrl + '")' : 'none');
    applyStylePreview();
  }
  function loadPreviewScreenshot() {
    const file = $('issue79-preview-file')?.files?.[0];
    if (previewImageUrl) { URL.revokeObjectURL(previewImageUrl); previewImageUrl = ''; }
    if (file && /^image\/(png|jpeg|webp)$/.test(file.type) && file.size <= 12 * 1024 * 1024) {
      previewImageUrl = URL.createObjectURL(file);
      if ($('issue79-preview-bg')) $('issue79-preview-bg').value = 'game';
    } else if (file) {
      setStatus('style-status', '只支持不超过 12 MB 的 PNG、JPEG 或 WebP 图片。', false);
    }
    updatePreviewBackground();
  }
  function luminance(hex) {
    const value = String(hex || '').replace('#','');
    if (!/^[0-9a-f]{6}$/i.test(value)) return 0;
    const linear = [0,2,4].map(i => {
      const c = parseInt(value.slice(i,i+2),16) / 255;
      return c <= 0.04045 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2];
  }
  function contrastRatio(a,b) {
    const l1 = luminance(a), l2 = luminance(b);
    return (Math.max(l1,l2)+0.05) / (Math.min(l1,l2)+0.05);
  }
  function applyStylePreview() {
    const payload = collectVisualStyle();
    const stage = $('issue79-preview-stage');
    const queue = $('issue79-preview-queue');
    if (!stage || !queue) return;
    stage.style.setProperty('--preview-bg1', payload.bg1);
    stage.style.setProperty('--preview-bg2', payload.bg2);
    stage.style.setProperty('--preview-bg3', payload.bg3);
    queue.style.setProperty('--preview-text', payload.text_color);
    queue.style.setProperty('--preview-stroke', payload.text_stroke_color);
    queue.style.setProperty('--preview-size', `${payload.queue_font_size}px`);
    queue.style.setProperty('--preview-weight', payload.queue_font_weight);
    queue.style.setProperty('--preview-style', payload.queue_font_style);
    queue.style.setProperty('--preview-family', payload.queue_font_family);
    queue.style.setProperty('--preview-line-height', payload.queue_line_height);
    queue.style.setProperty('--preview-gap', `${Number(payload.queue_item_gap || 0)}px`);
    queue.style.setProperty('--preview-letter-spacing', `${Number(payload.queue_letter_spacing || 0)}px`);
    queue.style.setProperty('--preview-word-spacing', `${Number(payload.queue_word_spacing || 0)}px`);
    queue.style.setProperty('--preview-align', payload.queue_text_align);
    queue.style.setProperty('--preview-opacity', String(Number(payload.queue_text_opacity ?? 100) / 100));
    queue.style.setProperty('--preview-px', `${Number(payload.queue_item_padding_x || 0)}px`);
    queue.style.setProperty('--preview-py', `${Number(payload.queue_item_padding_y || 0)}px`);
    queue.querySelectorAll('.issue79-preview-row').forEach(row => row.classList.toggle('issue79-stroke', Boolean(payload.text_stroke_enabled)));
    queue.querySelectorAll('.issue79-preview-number').forEach(node => { node.style.display = payload.show_sequence ? 'inline-block' : 'none'; });
    const mode = $('issue79-preview-bg')?.value || 'checker';
    const backgrounds = mode === 'green' ? ['#00ff00'] : mode === 'checker'
      ? ['#f3f3f3','#444444'] : [payload.bg1, payload.bg2, payload.bg3];
    const ratio = Math.min(...backgrounds.map(bg => contrastRatio(payload.text_color, bg)));
    const warning = $('issue79-contrast-warning');
    if (warning) {
      warning.classList.toggle('low-contrast', ratio < 4.5);
      warning.textContent = (ratio < 4.5 ? '对比度不足' : '对比度达标') + '：最低 ' + ratio.toFixed(2) +
        ':1（建议至少 4.5:1）。' + (mode === 'game' ? '游戏图片背景只能作为视觉预览，真实截图请人工确认可读性。' : '');
    }
  }

  // ---------- Control panel theme ----------
  function systemTheme() { return window.matchMedia?.('(prefers-color-scheme: light)').matches ? 'light' : 'dark'; }
  function loadThemeState() {
    try { return { mode:'system', accent:'', bg:'', panel:'', text:'', ...JSON.parse(localStorage.getItem(THEME_KEY) || '{}') }; }
    catch (_) { return { mode:'system', accent:'', bg:'', panel:'', text:'' }; }
  }
  function applyThemeState(state) {
    const root = document.documentElement;
    const mode = ['system','light','dark'].includes(state.mode) ? state.mode : 'system';
    root.dataset.theme = mode === 'system' ? systemTheme() : mode;
    const vars = { accent: state.accent, bg: state.bg, panel: state.panel, text: state.text };
    Object.entries(vars).forEach(([key, value]) => {
      if (value && /^#[0-9a-f]{6}$/i.test(value)) root.style.setProperty(`--${key}`, value);
      else root.style.removeProperty(`--${key}`);
    });
  }
  function saveThemeState(state) { localStorage.setItem(THEME_KEY, JSON.stringify(state)); applyThemeState(state); }

  function ensureThemePane() {
    const subtabs = document.querySelector('#view-settings .subtabs');
    const settingsView = $('view-settings');
    if (!subtabs || !settingsView || $('settings-theme')) return;
    const button = document.createElement('button');
    button.className = 'subtab'; button.dataset.settings = 'theme'; button.textContent = '界面主题';
    subtabs.appendChild(button);
    const pane = document.createElement('div');
    pane.className = 'settings-pane'; pane.id = 'settings-theme';
    pane.innerHTML = `<div class="card issue79-theme-card"><h3>Web 界面主题</h3><p class="hint">主题设置只影响当前浏览器的 Web 控制台，不改变 OBS 队列样式。</p><div class="issue79-editor-grid"><label>模式<select id="issue-theme-mode"><option value="system">跟随系统</option><option value="light">白天模式</option><option value="dark">夜晚模式</option></select></label></div><div class="issue79-theme-swatches"><label>主色 / 强调色<input id="issue-theme-accent" type="color"></label><label>页面背景<input id="issue-theme-bg" type="color"></label><label>卡片背景<input id="issue-theme-panel" type="color"></label><label>正文颜色<input id="issue-theme-text" type="color"></label></div><div class="toolbar" style="margin-top:16px"><button id="issue-theme-reset" class="button ghost">恢复主题默认颜色</button></div><p class="issue79-theme-note">选择颜色后立即生效并保存在浏览器本地。若恢复默认颜色，浅色/深色模式会重新使用内置配色。</p></div>`;
    settingsView.appendChild(pane);
    button.addEventListener('click', () => activateSettingsPane(button, pane));

    const state = loadThemeState();
    const defaults = { accent:'#6c5ce7', bg: state.mode === 'light' ? '#f5f6fb' : '#07101d', panel: state.mode === 'light' ? '#ffffff' : '#0c1827', text: state.mode === 'light' ? '#24263a' : '#d8e8f7' };
    $('issue-theme-mode').value = state.mode;
    $('issue-theme-accent').value = state.accent || defaults.accent;
    $('issue-theme-bg').value = state.bg || defaults.bg;
    $('issue-theme-panel').value = state.panel || defaults.panel;
    $('issue-theme-text').value = state.text || defaults.text;

    const persist = () => saveThemeState({ mode:$('issue-theme-mode').value, accent:$('issue-theme-accent').value, bg:$('issue-theme-bg').value, panel:$('issue-theme-panel').value, text:$('issue-theme-text').value });
    ['issue-theme-mode','issue-theme-accent','issue-theme-bg','issue-theme-panel','issue-theme-text'].forEach(id => $(id)?.addEventListener('input', persist));
    $('issue-theme-reset')?.addEventListener('click', () => {
      const clean = { mode:$('issue-theme-mode').value, accent:'', bg:'', panel:'', text:'' };
      localStorage.setItem(THEME_KEY, JSON.stringify(clean));
      applyThemeState(clean);
      const resolved = clean.mode === 'system' ? systemTheme() : clean.mode;
      const light = resolved === 'light';
      $('issue-theme-accent').value = '#6c5ce7'; $('issue-theme-bg').value = light ? '#f5f6fb' : '#07101d'; $('issue-theme-panel').value = light ? '#ffffff' : '#0c1827'; $('issue-theme-text').value = light ? '#24263a' : '#d8e8f7';
    });
    applyThemeState(state);
  }

  // ---------- Update manifest details ----------
  function ensureUpdateDetails() {
    const view = $('view-update');
    const card = view?.querySelector('.card');
    if (!card || $('issue79-update-detail')) return;
    const detail = document.createElement('div');
    detail.id = 'issue79-update-detail'; detail.className = 'issue79-update-detail'; detail.textContent = '尚未读取更新清单。';
    const toolbar = card.querySelector('.toolbar');
    if (toolbar) toolbar.insertAdjacentElement('afterend', detail); else card.appendChild(detail);
    const download = document.createElement('a');
    download.id = 'issue79-update-download'; download.className = 'button ghost disabled'; download.textContent = '下载 Web 更新包'; download.target = '_blank'; download.rel = 'noopener';
    toolbar?.appendChild(download);
    document.querySelector('[data-view="update"]')?.addEventListener('click', () => window.setTimeout(refreshUpdateDetails, 30));
    $('update-check')?.addEventListener('click', () => window.setTimeout(refreshUpdateDetails, 250));
  }
  async function refreshUpdateDetails() {
    const detail = $('issue79-update-detail'); if (!detail) return;
    detail.textContent = '正在读取 update-manifest.json……';
    try {
      const payload = await api('/api/control/update');
      const pkg = payload.package || {};
      detail.innerHTML = `<strong>更新清单</strong><br>最新版本：v${esc(payload.latest_version || '-')}<br>文件名：<code>${esc(pkg.filename || '未提供')}</code><br>SHA-256：<code>${esc(pkg.sha256 || '未提供')}</code><br>大小：${pkg.size ? `${(Number(pkg.size)/1024/1024).toFixed(1)} MB` : '未知'}`;
      const download = $('issue79-update-download');
      if (download && pkg.url) { download.href = pkg.url; download.classList.remove('disabled'); }
      else if (download) { download.removeAttribute('href'); download.classList.add('disabled'); }
    } catch (error) {
      detail.textContent = `读取更新清单失败：${error.message}`;
    }
  }

  // System theme changes only matter while following system.
  window.matchMedia?.('(prefers-color-scheme: light)').addEventListener?.('change', () => {
    const state = loadThemeState(); if (state.mode === 'system') applyThemeState(state);
  });

  normalizeProjectNavigation();
  ensureQueueInputs();
  installQueueObserver();
  ensureActivePlatformPane();
  ensureVisualStyleEditor();
  ensureThemePane();
  ensureUpdateDetails();
})();
