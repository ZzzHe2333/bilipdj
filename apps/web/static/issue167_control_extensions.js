(() => {
  'use strict';

  const $ = id => document.getElementById(id);

  function fmtMiB(value) {
    const bytes = Number(value || 0);
    if (!Number.isFinite(bytes) || bytes < 0) return '未知';
    return `${(bytes / 1024 / 1024).toFixed(1)} MiB`;
  }

  function ensureStyles() {
    if ($('issue167-control-styles')) return;
    const style = document.createElement('style');
    style.id = 'issue167-control-styles';
    style.textContent = `
      .backend-command-row{display:grid;grid-template-columns:auto minmax(0,1fr) auto;gap:8px;align-items:center;margin-top:10px}
      .backend-command-row input{min-width:0;width:100%;box-sizing:border-box}
      .backend-command-status{grid-column:2/4;margin:0;font-size:12px;opacity:.75}
      .update-estimate-card strong{display:block;margin-top:5px;font-size:16px}
      @media(max-width:720px){.backend-command-row{grid-template-columns:1fr auto}.backend-command-row .backend-command-label{grid-column:1/3}.backend-command-status{grid-column:1/3}}
    `;
    document.head.appendChild(style);
  }

  function ensureCommandConsole() {
    const output = $('log-output');
    if (!output || $('backend-command-form')) return;
    const form = document.createElement('form');
    form.id = 'backend-command-form';
    form.className = 'backend-command-row';
    form.innerHTML = `
      <span class="backend-command-label">后端指令 &gt;</span>
      <input id="backend-command-input" class="input" maxlength="500" autocomplete="off" placeholder="输入排队 / 权限等后端指令，例如：暂停排队功能">
      <button id="backend-command-send" class="button" type="submit">发送</button>
      <p id="backend-command-status" class="backend-command-status">指令会进入与直播弹幕相同的后端命令处理链；不会执行系统 Shell。</p>`;
    output.insertAdjacentElement('afterend', form);

    form.addEventListener('submit', async event => {
      event.preventDefault();
      const input = $('backend-command-input');
      const button = $('backend-command-send');
      const status = $('backend-command-status');
      const command = String(input?.value || '').trim();
      if (!command) {
        status.textContent = '请输入后端指令。';
        return;
      }
      button.disabled = true;
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
        input?.focus();
      }
    });
  }

  function ensureUpdateEstimates() {
    const grid = $('view-update')?.querySelector('.metric-grid');
    if (!grid || $('update-full-estimate')) return;
    const full = document.createElement('div');
    full.className = 'metric update-estimate-card';
    full.innerHTML = '<span>全量更新预估</span><strong id="update-full-estimate">等待检测</strong>';
    const incremental = document.createElement('div');
    incremental.className = 'metric update-estimate-card';
    incremental.innerHTML = '<span>增量更新预估</span><strong id="update-incremental-estimate">等待检测</strong>';
    grid.append(full, incremental);
  }

  async function refreshUpdateEstimates() {
    ensureUpdateEstimates();
    const full = $('update-full-estimate');
    const incremental = $('update-incremental-estimate');
    if (!full || !incremental) return;
    full.textContent = '检测中…';
    incremental.textContent = '检测中…';
    try {
      const response = await fetch('/api/control/update-estimate', { cache: 'no-store' });
      let payload = {};
      try { payload = await response.json(); } catch (_) { /* ignore */ }
      if (!response.ok || payload.status !== 'ok') throw new Error(payload.message || `HTTP ${response.status}`);
      const web = payload.web || {};
      full.textContent = fmtMiB(web.full_download_bytes);
      incremental.textContent = web.incremental_available
        ? fmtMiB(web.incremental_download_bytes)
        : 'Web 便携版暂不支持';
      if (!web.incremental_available && web.note) incremental.title = web.note;
    } catch (error) {
      full.textContent = '检测失败';
      incremental.textContent = '检测失败';
      incremental.title = String(error.message || error);
    }
  }

  ensureStyles();
  ensureCommandConsole();
  ensureUpdateEstimates();
  $('update-check')?.addEventListener('click', () => { refreshUpdateEstimates(); });
})();
