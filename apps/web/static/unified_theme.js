(() => {
  'use strict';

  const LEGACY_THEME_KEY = 'bilipdj.control.theme.v1';
  const PROFILE_NAME = 'BiliPDJ-appearance-profile.json';
  const COLOR_FIELDS = [
    ['background', '页面背景'], ['sidebar', '侧栏 / 顶栏'], ['surface', '卡片背景'],
    ['surface_alt', '次级卡片 / Hover'], ['input', '输入框背景'], ['border', '边框'],
    ['text', '正文'], ['muted', '次要文字'], ['accent', '品牌主色'],
    ['accent_hover', '主色 Hover'], ['selection', '选中颜色'], ['success', '成功'],
    ['warning', '警告'], ['danger', '错误 / 危险'],
  ];
  const DEFAULT_APPEARANCE = {
    schema: 1, design: 'aurora', mode: 'dark', font_family: 'Microsoft YaHei UI', font_size: 10, radius: 10,
    dark: {background:'#090E1A',sidebar:'#0D1424',surface:'#111A2C',surface_alt:'#18233A',input:'#0D1424',border:'#26334D',text:'#E6EDF7',muted:'#8A9AB3',accent:'#7C6CF2',accent_hover:'#9184FF',selection:'#7C6CF2',success:'#32D583',warning:'#F5B942',danger:'#F97066'},
    light:{background:'#F4F6FB',sidebar:'#EAEDF5',surface:'#FFFFFF',surface_alt:'#F0EFFF',input:'#FBFBFE',border:'#D5D9E7',text:'#20263A',muted:'#687089',accent:'#6757D9',accent_hover:'#5142BC',selection:'#6757D9',success:'#007A40',warning:'#B57600',danger:'#D92D20'},
  };

  let appearance = structuredClone(DEFAULT_APPEARANCE);
  let dirty = false;
  let polling = 0;

  const $ = id => document.getElementById(id);
  const clone = value => JSON.parse(JSON.stringify(value));
  const hex = value => /^#[0-9a-f]{6}$/i.test(String(value || ''));

  async function api(path, options = {}) {
    const response = await fetch(path, {cache:'no-store', ...options});
    let payload = {};
    try { payload = await response.json(); } catch (_) { /* ignore */ }
    if (!response.ok || payload.status === 'error') throw new Error(payload.message || `HTTP ${response.status}`);
    return payload;
  }
  function post(path, payload) {
    return api(path, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
  }
  function systemMode() {
    return window.matchMedia?.('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
  }
  function normalize(raw) {
    const incoming = raw && typeof raw === 'object' ? raw : {};
    const result = clone(DEFAULT_APPEARANCE);
    result.mode = ['system','light','dark'].includes(incoming.mode) ? incoming.mode : result.mode;
    result.font_family = String(incoming.font_family || result.font_family).slice(0, 80);
    result.font_size = Math.max(8, Math.min(20, Number(incoming.font_size || result.font_size)));
    result.radius = Math.max(0, Math.min(24, Number(incoming.radius ?? result.radius)));
    for (const scheme of ['dark','light']) {
      const source = incoming[scheme] && typeof incoming[scheme] === 'object' ? incoming[scheme] : {};
      for (const [key] of COLOR_FIELDS) if (hex(source[key])) result[scheme][key] = String(source[key]).toUpperCase();
    }
    return result;
  }
  function resolvedMode(value = appearance) {
    return value.mode === 'system' ? systemMode() : value.mode;
  }
  function apply(value) {
    appearance = normalize(value);
    const mode = resolvedMode(appearance);
    const p = appearance[mode];
    const root = document.documentElement;
    root.dataset.theme = mode;
    root.style.colorScheme = mode;
    const vars = {
      '--bg':p.background,'--sidebar':p.sidebar,'--panel':p.surface,'--panel2':p.surface_alt,
      '--input':p.input,'--line':p.border,'--text':p.text,'--muted':p.muted,'--accent':p.accent,
      '--accent-hover':p.accent_hover,'--ok':p.success,'--warning':p.warning,'--danger':p.danger,
      '--radius':`${appearance.radius}px`,'--font-family':`"${appearance.font_family.replaceAll('"','')}" , "Microsoft YaHei", "PingFang SC", system-ui, sans-serif`,
    };
    Object.entries(vars).forEach(([key,val]) => root.style.setProperty(key, val));
    document.body.style.fontSize = `${appearance.font_size}px`;

    // Keep a compatibility snapshot for users who temporarily run an older Web client.
    try {
      localStorage.setItem(LEGACY_THEME_KEY, JSON.stringify({mode:appearance.mode,accent:p.accent,bg:p.background,panel:p.surface,text:p.text}));
    } catch (_) { /* ignore */ }
  }

  function migrateLegacy() {
    try {
      const old = JSON.parse(localStorage.getItem(LEGACY_THEME_KEY) || '{}');
      if (!old || typeof old !== 'object' || !Object.keys(old).some(k => ['accent','bg','panel','text'].includes(k) && old[k])) return null;
      const next = clone(DEFAULT_APPEARANCE);
      next.mode = ['system','light','dark'].includes(old.mode) ? old.mode : 'dark';
      const scheme = next.mode === 'light' ? 'light' : 'dark';
      if (hex(old.accent)) next[scheme].accent = String(old.accent).toUpperCase();
      if (hex(old.bg)) next[scheme].background = String(old.bg).toUpperCase();
      if (hex(old.panel)) next[scheme].surface = String(old.panel).toUpperCase();
      if (hex(old.text)) next[scheme].text = String(old.text).toUpperCase();
      return next;
    } catch (_) { return null; }
  }

  function setStatus(text, ok = true) {
    const node = $('unified-theme-status');
    if (!node) return;
    node.textContent = text || '';
    node.style.color = ok ? '' : 'var(--danger)';
  }

  function paletteMarkup() {
    return COLOR_FIELDS.map(([key,label]) => `<label class="unified-theme-color">${label}<input id="unified-theme-${key}" data-theme-color="${key}" type="color"></label>`).join('');
  }

  function ensureEditor() {
    const pane = $('settings-theme');
    if (!pane || pane.dataset.unifiedTheme === '1') return;
    pane.dataset.unifiedTheme = '1';
    pane.innerHTML = `
      <div class="card unified-theme-card">
        <div class="unified-theme-head"><div><h3>BiliPDJ Aurora · 通用界面主题</h3><p class="hint">Windows、Web 共用 Server 的 appearance.json；导出的 JSON 可在任一端直接导入。OBS/队列显示样式会一并放入通用配置文件。</p></div><span class="unified-theme-chip">跨端同步</span></div>
        <div class="unified-theme-grid">
          <label>当前模式<select id="unified-theme-mode"><option value="system">跟随系统</option><option value="light">白天模式</option><option value="dark">夜晚模式</option></select></label>
          <label>正在编辑的配色<select id="unified-theme-scheme"><option value="dark">夜晚配色</option><option value="light">白天配色</option></select></label>
          <label class="wide">字体<input id="unified-theme-font" type="text" maxlength="80" placeholder="Microsoft YaHei UI"></label>
          <label>界面字号<input id="unified-theme-font-size" type="number" min="8" max="20"></label>
          <label>圆角<input id="unified-theme-radius" type="number" min="0" max="24"></label>
        </div>
        <div id="unified-theme-colors" class="unified-theme-colors">${paletteMarkup()}</div>
        <div class="unified-theme-preview"><strong>实时预览</strong><div class="unified-theme-preview-row"><button class="button" type="button">主按钮</button><button class="button ghost" type="button">次按钮</button><span class="unified-theme-chip">● 后端已连接</span><input value="统一输入框样式" readonly></div></div>
        <div class="unified-theme-actions">
          <button id="unified-theme-save" class="button" type="button">保存到后端</button>
          <button id="unified-theme-refresh" class="button ghost" type="button">从后端刷新</button>
          <button id="unified-theme-export" class="button ghost" type="button">导出通用配置</button>
          <button id="unified-theme-import" class="button ghost" type="button">导入通用配置</button>
          <button id="unified-theme-reset" class="button ghost" type="button">恢复 Aurora 默认</button>
          <input id="unified-theme-file" type="file" accept="application/json,.json" hidden>
        </div>
        <p class="unified-theme-note">通用配置格式同时包含 <code>appearance</code>（Windows/Web 界面）和 <code>display_style</code>（OBS/队列展示）。因此从 Windows 导出的文件可以直接在 Web 导入，反向也一样。</p>
        <div id="unified-theme-status" class="unified-theme-status"></div>
      </div>`;

    $('unified-theme-mode')?.addEventListener('change', () => { appearance.mode = $('unified-theme-mode').value; dirty = true; apply(appearance); });
    $('unified-theme-scheme')?.addEventListener('change', fillEditor);
    $('unified-theme-font')?.addEventListener('input', onEditorInput);
    $('unified-theme-font-size')?.addEventListener('input', onEditorInput);
    $('unified-theme-radius')?.addEventListener('input', onEditorInput);
    pane.querySelectorAll('[data-theme-color]').forEach(node => node.addEventListener('input', onEditorInput));
    $('unified-theme-save')?.addEventListener('click', saveToServer);
    $('unified-theme-refresh')?.addEventListener('click', () => refreshFromServer(true));
    $('unified-theme-export')?.addEventListener('click', exportProfile);
    $('unified-theme-import')?.addEventListener('click', () => $('unified-theme-file')?.click());
    $('unified-theme-file')?.addEventListener('change', importProfile);
    $('unified-theme-reset')?.addEventListener('click', () => {
      appearance = clone(DEFAULT_APPEARANCE);
      dirty = true;
      apply(appearance);
      fillEditor();
      setStatus('已恢复 Aurora 默认预览；点击“保存到后端”后同步到 Windows/Web。');
    });
    fillEditor();
  }

  function editorScheme() { return $('unified-theme-scheme')?.value === 'light' ? 'light' : 'dark'; }
  function fillEditor() {
    if (!$('unified-theme-mode')) return;
    $('unified-theme-mode').value = appearance.mode;
    $('unified-theme-font').value = appearance.font_family;
    $('unified-theme-font-size').value = String(appearance.font_size);
    $('unified-theme-radius').value = String(appearance.radius);
    const p = appearance[editorScheme()];
    for (const [key] of COLOR_FIELDS) { const node = $(`unified-theme-${key}`); if (node) node.value = p[key]; }
  }
  function onEditorInput() {
    appearance.font_family = String($('unified-theme-font')?.value || appearance.font_family).trim() || DEFAULT_APPEARANCE.font_family;
    appearance.font_size = Math.max(8, Math.min(20, Number($('unified-theme-font-size')?.value || appearance.font_size)));
    appearance.radius = Math.max(0, Math.min(24, Number($('unified-theme-radius')?.value ?? appearance.radius)));
    const p = appearance[editorScheme()];
    for (const [key] of COLOR_FIELDS) { const value = $(`unified-theme-${key}`)?.value; if (hex(value)) p[key] = value.toUpperCase(); }
    dirty = true;
    apply(appearance);
    setStatus('正在本地实时预览，尚未保存到后端。');
  }

  async function refreshFromServer(force = false) {
    if (dirty && !force) return;
    try {
      const payload = await api('/api/appearance');
      let next = normalize(payload.appearance);
      if (!payload.exists) {
        const legacy = migrateLegacy();
        if (legacy) {
          const saved = await post('/api/appearance', {appearance:legacy});
          next = normalize(saved.appearance);
          setStatus('已把旧 Web 浏览器主题自动迁移到通用 Server 配置。');
        }
      }
      appearance = next;
      dirty = false;
      apply(appearance);
      fillEditor();
      if (force) setStatus('已从后端读取通用主题。');
    } catch (error) {
      setStatus(`读取通用主题失败：${error.message}`, false);
    }
  }

  async function saveToServer() {
    try {
      const payload = await post('/api/appearance', {appearance});
      appearance = normalize(payload.appearance);
      dirty = false;
      apply(appearance); fillEditor();
      setStatus('已保存到 Server。Windows 和 Web 将使用同一份 appearance.json。');
    } catch (error) { setStatus(`保存失败：${error.message}`, false); }
  }

  async function exportProfile() {
    try {
      const profile = await api('/api/appearance/profile');
      const blob = new Blob([JSON.stringify(profile, null, 2) + '\n'], {type:'application/json'});
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a'); a.href = url; a.download = PROFILE_NAME; document.body.appendChild(a); a.click(); a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      setStatus('已导出 Windows/Web/OBS 通用配置文件。');
    } catch (error) { setStatus(`导出失败：${error.message}`, false); }
  }

  async function importProfile(event) {
    const file = event.target.files?.[0];
    event.target.value = '';
    if (!file) return;
    try {
      const raw = JSON.parse(await file.text());
      const payload = await post('/api/appearance/profile', raw);
      appearance = normalize(payload.appearance);
      dirty = false;
      apply(appearance); fillEditor();
      setStatus(`已导入 ${file.name}；界面主题与 OBS 样式均已同步到后端。`);
    } catch (error) { setStatus(`导入失败：${error.message}`, false); }
  }

  function init() {
    ensureEditor();
    refreshFromServer(true);
    polling = window.setInterval(() => refreshFromServer(false), 10000);
    window.matchMedia?.('(prefers-color-scheme: light)').addEventListener?.('change', () => {
      if (appearance.mode === 'system') apply(appearance);
    });
  }

  window.addEventListener('load', () => window.setTimeout(init, 0), {once:true});
  window.addEventListener('beforeunload', () => polling && window.clearInterval(polling), {once:true});
})();
