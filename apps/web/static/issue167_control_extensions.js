(() => {
  'use strict';

  const $ = id => document.getElementById(id);
  const SWITCH_LABELS = Object.freeze({
    paidui: '排队总开关',
    guanfu_paidui: '官服排队',
    bfu_paidui: 'B服排队',
    chaoji_paidui: '超级排队',
    mifu_paidui: '米服排队',
    quxiao_paidui: '取消排队',
    xiugai_paidui: '修改排队内容',
    jianzhang_chadui: '舰长插队',
    fangguan_op: '允许房管执行管理命令',
  });
  const PLATFORM_FIELD_GROUPS = Object.freeze({
    'platform-bili-room': 'bilibili',
    'platform-bili-uid': 'bilibili',
    'platform-douyin-live': 'douyin',
    'platform-douyin-enabled': 'douyin',
    'platform-douyin-cookie': 'douyin',
    'platform-huya-url': 'huya',
    'platform-huya-room': 'huya',
    'platform-huya-anchor': 'huya',
  });

  function fmtMiB(value) {
    const bytes = Number(value || 0);
    if (!Number.isFinite(bytes) || bytes < 0) return '未知';
    return `${(bytes / 1024 / 1024).toFixed(1)} MiB`;
  }

  function ensureCommandConsole() {
    const output = $('log-output');
    if (!output || $('backend-command-form')) return;

    const form = document.createElement('form');
    form.id = 'backend-command-form';
    form.className = 'backend-command-row';
    form.style.margin = '0 0 12px';
    form.setAttribute('aria-label', '后端指令输入');
    form.innerHTML = `
      <div class="backend-command-copy">
        <strong class="backend-command-label">后端指令</strong>
        <span>与直播弹幕共用同一套后端命令处理链</span>
      </div>
      <div class="backend-command-controls">
        <input id="backend-command-input" maxlength="500" autocomplete="off" aria-describedby="backend-command-status" placeholder="例如：暂停排队功能">
        <button id="backend-command-send" class="button" type="submit">发送</button>
      </div>
      <p id="backend-command-status" class="backend-command-status" role="status" aria-live="polite">仅发送 BiliPDJ 后端指令，不会执行系统 Shell。</p>`;
    output.insertAdjacentElement('beforebegin', form);

    form.addEventListener('submit', async event => {
      event.preventDefault();
      const input = $('backend-command-input');
      const button = $('backend-command-send');
      const status = $('backend-command-status');
      const command = String(input?.value || '').trim();
      if (!command) {
        status.textContent = '请输入后端指令。';
        input?.focus();
        return;
      }

      button.disabled = true;
      form.setAttribute('aria-busy', 'true');
      status.textContent = '正在送入后端弹幕流…';
      try {
        const response = await fetch('/api/control/command', {
          method: 'POST',
          cache: 'no-store',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ command }),
        });
        let payload = {};
        try { payload = await response.json(); } catch (_) { /* ignore */ }
        if (!response.ok || payload.status !== 'ok') throw new Error(payload.message || `HTTP ${response.status}`);
        status.textContent = payload.message || '指令已发送。';
        input.value = '';
      } catch (error) {
        status.textContent = `发送失败：${error.message}`;
      } finally {
        button.disabled = false;
        form.removeAttribute('aria-busy');
        input?.focus();
      }
    });
  }

  function translateSwitchLabels() {
    const grid = $('switch-grid');
    if (!grid) return;
    grid.querySelectorAll('[data-switch]').forEach(input => {
      const key = String(input.dataset.switch || '');
      const translated = SWITCH_LABELS[key];
      const label = input.closest('label');
      const text = label?.querySelector('span');
      if (text && translated && text.textContent !== translated) text.textContent = translated;
      if (label && translated) {
        const title = `配置键：${key}`;
        if (label.title !== title) label.title = title;
      }
    });
  }

  function observeSwitchLabels() {
    const grid = $('switch-grid');
    if (!grid) return;
    translateSwitchLabels();
    let queued = false;
    const observer = new MutationObserver(() => {
      if (queued) return;
      queued = true;
      queueMicrotask(() => {
        queued = false;
        translateSwitchLabels();
      });
    });
    observer.observe(grid, { childList: true, subtree: true });
  }

  function syncPlatformFields() {
    const select = $('platform-select');
    const pane = $('settings-platform');
    if (!select || !pane) return;
    const platform = String(select.value || 'bilibili');
    Object.entries(PLATFORM_FIELD_GROUPS).forEach(([id, group]) => {
      const input = $(id);
      const label = input?.closest('label');
      if (label) {
        label.hidden = group !== platform;
        label.dataset.platformGroup = group;
      }
    });

    const huyaSave = $('platform-huya-save');
    if (huyaSave) huyaSave.hidden = platform !== 'huya';
    const status = $('platform-status');
    if (status && ['kuaishou', 'douyu', 'weixin_videohao'].includes(platform)) {
      status.textContent = '当前平台暂未内置参数表单；后续接入时可由平台插件提供动态配置。';
    }
  }

  function enablePlatformDropdownLayout() {
    const select = $('platform-select');
    const pane = $('settings-platform');
    if (!select || !pane) return;
    let lastPlatform = String(select.value || 'bilibili');
    select.addEventListener('change', () => {
      lastPlatform = String(select.value || 'bilibili');
      window.setTimeout(syncPlatformFields, 0);
    });
    new MutationObserver(syncPlatformFields).observe(pane, { childList: true, subtree: true });
    window.setInterval(() => {
      const current = String(select.value || 'bilibili');
      if (current === lastPlatform) return;
      lastPlatform = current;
      syncPlatformFields();
    }, 250);
    syncPlatformFields();
  }

  async function switchThemeMode(button) {
    button.disabled = true;
    const oldText = button.textContent;
    button.textContent = '切换中…';
    try {
      const response = await fetch('/api/appearance', { cache: 'no-store' });
      const payload = await response.json();
      if (!response.ok || payload.status === 'error') throw new Error(payload.message || `HTTP ${response.status}`);
      const appearance = payload.appearance && typeof payload.appearance === 'object' ? payload.appearance : {};
      const current = document.documentElement.dataset.theme === 'light' ? 'light' : 'dark';
      appearance.mode = current === 'dark' ? 'light' : 'dark';
      const saved = await fetch('/api/appearance', {
        method: 'POST',
        cache: 'no-store',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ appearance }),
      });
      const savedPayload = await saved.json();
      if (!saved.ok || savedPayload.status === 'error') throw new Error(savedPayload.message || `HTTP ${saved.status}`);
      location.reload();
    } catch (error) {
      button.disabled = false;
      button.textContent = oldText;
      button.title = `主题切换失败：${error.message}`;
    }
  }

  function refreshThemeButtonLabel() {
    const button = $('theme-toggle');
    if (!button) return;
    const current = document.documentElement.dataset.theme === 'light' ? 'light' : 'dark';
    button.textContent = current === 'dark' ? '切换白天' : '切换黑夜';
    button.title = '主题会保存到后端 appearance.json，下次打开自动恢复';
  }

  async function shutdownServer(button) {
    button.disabled = true;
    button.textContent = '正在关闭…';
    try {
      const response = await fetch('/api/control/shutdown', {
        method: 'POST',
        cache: 'no-store',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ confirm: 'shutdown' }),
      });
      let payload = {};
      try { payload = await response.json(); } catch (_) { /* server may already be stopping */ }
      if (!response.ok || payload.status === 'error') throw new Error(payload.message || `HTTP ${response.status}`);
      button.textContent = '服务器已关闭';
      button.title = payload.message || '后端服务器已关闭';
    } catch (error) {
      button.disabled = false;
      button.textContent = '关闭服务器';
      button.title = `关闭失败：${error.message}`;
    }
  }

  function ensureControlActions() {
    const actions = document.querySelector('.top-actions');
    if (!actions) return;

    if (!$('theme-toggle')) {
      const theme = document.createElement('button');
      theme.id = 'theme-toggle';
      theme.type = 'button';
      theme.className = 'button ghost';
      theme.addEventListener('click', () => switchThemeMode(theme));
      actions.prepend(theme);
    }

    if (!$('server-shutdown')) {
      const shutdown = document.createElement('button');
      shutdown.id = 'server-shutdown';
      shutdown.type = 'button';
      shutdown.className = 'button danger';
      shutdown.textContent = '关闭服务器';
      shutdown.title = '双击后关闭 Web 后端服务器';
      shutdown.setAttribute('aria-label', '双击关闭服务器');
      shutdown.addEventListener('dblclick', event => {
        event.preventDefault();
        shutdownServer(shutdown);
      });
      actions.appendChild(shutdown);
    }

    refreshThemeButtonLabel();
    new MutationObserver(refreshThemeButtonLabel).observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
  }

  function ensureUpdateEstimates() {
    const grid = $('view-update')?.querySelector('.metric-grid');
    if (!grid || $('update-full-estimate')) return;

    const full = document.createElement('div');
    full.className = 'metric update-estimate-card update-estimate-full';
    full.innerHTML = '<span>全量更新预估</span><strong id="update-full-estimate" aria-live="polite">等待检测</strong><small>完整便携包</small>';

    const incremental = document.createElement('div');
    incremental.className = 'metric update-estimate-card update-estimate-incremental';
    incremental.innerHTML = '<span>增量更新预估</span><strong id="update-incremental-estimate" aria-live="polite">等待检测</strong><small>按本地文件差异计算</small>';
    grid.append(full, incremental);
  }

  async function refreshUpdateEstimates() {
    ensureUpdateEstimates();
    const full = $('update-full-estimate');
    const incremental = $('update-incremental-estimate');
    const updateView = $('view-update');
    if (!full || !incremental) return;

    updateView?.setAttribute('aria-busy', 'true');
    full.textContent = '检测中…';
    incremental.textContent = '检测中…';
    incremental.removeAttribute('title');
    try {
      const response = await fetch('/api/control/update-estimate', { cache: 'no-store' });
      let payload = {};
      try { payload = await response.json(); } catch (_) { /* ignore */ }
      if (!response.ok || payload.status !== 'ok') throw new Error(payload.message || `HTTP ${response.status}`);
      const web = payload.web || {};
      full.textContent = fmtMiB(web.full_download_bytes);
      incremental.textContent = web.incremental_available
        ? fmtMiB(web.incremental_download_bytes)
        : '当前 Release 未提供增量资源';
      if (web.note) incremental.title = web.note;
    } catch (error) {
      full.textContent = '检测失败';
      incremental.textContent = '检测失败';
      incremental.title = String(error.message || error);
    } finally {
      updateView?.removeAttribute('aria-busy');
    }
  }

  ensureCommandConsole();
  observeSwitchLabels();
  enablePlatformDropdownLayout();
  ensureControlActions();
  ensureUpdateEstimates();
  $('update-check')?.addEventListener('click', refreshUpdateEstimates);
})();