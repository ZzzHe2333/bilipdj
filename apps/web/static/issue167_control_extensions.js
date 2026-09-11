(() => {
  'use strict';

  const $ = id => document.getElementById(id);

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
    output.insertAdjacentElement('afterend', form);

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
        : 'Web 便携版暂不支持';
      if (!web.incremental_available && web.note) incremental.title = web.note;
    } catch (error) {
      full.textContent = '检测失败';
      incremental.textContent = '检测失败';
      incremental.title = String(error.message || error);
    } finally {
      updateView?.removeAttribute('aria-busy');
    }
  }

  ensureCommandConsole();
  ensureUpdateEstimates();
  $('update-check')?.addEventListener('click', refreshUpdateEstimates);
})();
