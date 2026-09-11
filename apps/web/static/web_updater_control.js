(() => {
  'use strict';

  const $ = id => document.getElementById(id);
  const api = async (path, options = {}) => {
    const response = await fetch(path, { cache: 'no-store', ...options });
    let payload = {};
    try { payload = await response.json(); } catch (_) { /* ignore */ }
    if (!response.ok || payload.status === 'error') throw new Error(payload.message || `HTTP ${response.status}`);
    return payload;
  };
  const post = (path, payload) => api(path, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload || {}),
  });
  const fmtBytes = value => {
    let n = Number(value || 0); if (!Number.isFinite(n) || n <= 0) return '0 B';
    const units = ['B','KiB','MiB','GiB']; let i = 0;
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i += 1; }
    return `${n.toFixed(i ? 1 : 0)} ${units[i]}`;
  };
  let state = null;
  let candidates = new Map();

  function setStatus(text, ok = true) {
    const node = $('web-update-action-status');
    if (!node) return;
    node.textContent = text || '';
    node.style.color = ok ? '' : 'var(--danger)';
  }

  function ensureUi() {
    const view = $('view-update');
    const card = view?.querySelector('.card');
    if (!view || !card || $('web-update-controls')) return;
    const section = document.createElement('section');
    section.id = 'web-update-controls';
    section.className = 'web-update-controls';
    section.innerHTML = `
      <div class="web-update-head">
        <div><strong>Web Portable 自动更新</strong><p>同时读取云端正式版和测试版；默认选择最新正式版。支持全量更新、逐文件增量更新和本地版本恢复。</p></div>
        <span id="web-update-capability" class="web-update-chip">检测中</span>
      </div>
      <div class="web-update-picker-row">
        <label>选择版本<select id="web-update-version"><option>正在读取…</option></select></label>
        <div class="web-update-actions">
          <button id="web-update-full" class="button" type="button" disabled>全量更新</button>
          <button id="web-update-incremental" class="button ghost" type="button" disabled>增量更新</button>
          <button id="web-update-refresh" class="button ghost" type="button">刷新版本</button>
        </div>
      </div>
      <div id="web-update-detail" class="web-update-detail">正在读取 Web Portable 更新能力…</div>
      <div id="web-update-action-status" class="status" aria-live="polite"></div>`;
    const notes = $('update-notes')?.closest('.notes') || $('update-notes');
    if (notes?.parentElement === card) card.insertBefore(section, notes);
    else card.appendChild(section);

    $('web-update-version').addEventListener('change', renderSelected);
    $('web-update-refresh').addEventListener('click', refreshState);
    $('web-update-full').addEventListener('click', () => startSelected('full'));
    $('web-update-incremental').addEventListener('click', () => startSelected('incremental'));
  }

  function buildCandidates() {
    candidates = new Map();
    const releases = Array.isArray(state?.releases) && state.releases.length
      ? state.releases
      : (state?.cloud?.version ? [state.cloud] : []);
    for (const cloud of releases) {
      if (!cloud?.version) continue;
      const kind = cloud.prerelease ? '云端测试版' : '云端正式版';
      const label = `${kind} · v${cloud.version}`;
      candidates.set(label, { source: 'cloud', version: cloud.version, cloud });
    }
    for (const backup of (state?.backups || [])) {
      const label = `本地备份 · v${backup.version} · ${backup.created_at}`;
      candidates.set(label, { source: 'local', version: backup.version, backup });
    }

    const select = $('web-update-version');
    if (!select) return;
    select.innerHTML = '';
    let defaultLabel = '';
    for (const [label, candidate] of candidates.entries()) {
      const option = document.createElement('option');
      option.value = label;
      option.textContent = label;
      select.appendChild(option);
      if (candidate.source === 'cloud' && candidate.cloud?.tag_name === state?.default_tag) defaultLabel = label;
    }
    if (!candidates.size) {
      const option = document.createElement('option'); option.textContent = '暂无可用版本'; select.appendChild(option);
    }
    select.disabled = !candidates.size;
    if (defaultLabel) select.value = defaultLabel;
  }

  function selected() {
    return candidates.get(String($('web-update-version')?.value || '')) || null;
  }

  function renderSelected() {
    const candidate = selected();
    const full = $('web-update-full'), incremental = $('web-update-incremental'), detail = $('web-update-detail');
    if (!candidate || !full || !incremental || !detail) return;
    const canRun = Boolean(state?.frozen && state?.updater_available);
    if (candidate.source === 'local') {
      full.textContent = '恢复旧版'; full.disabled = !canRun; incremental.disabled = true;
      detail.innerHTML = `<strong>本地离线恢复</strong><span>目标 v${candidate.version} · ${fmtBytes(candidate.backup.size)} · 恢复前会再次备份当前版本，并保留当前配置、插件、日志和备份历史。</span>`;
    } else {
      full.textContent = '全量更新'; full.disabled = !canRun;
      incremental.disabled = !canRun || !candidate.cloud.incremental_available;
      const releaseKind = candidate.cloud.prerelease ? '测试版' : '正式版';
      const incrementalText = candidate.cloud.incremental_available
        ? `增量资源上限 ${fmtBytes(candidate.cloud.incremental_max_bytes)}；实际仅按本机 SHA-256 差异 Range 下载。`
        : '当前 Release 没有 Web 增量资源。';
      detail.innerHTML = `<strong>云端${releaseKind} v${candidate.version}</strong><span>全量包 ${fmtBytes(candidate.cloud.full_download_bytes)}。${incrementalText}</span>`;
      const fullEstimate = $('update-full-estimate');
      const incrementalEstimate = $('update-incremental-estimate');
      if (fullEstimate) fullEstimate.textContent = fmtBytes(candidate.cloud.full_download_bytes);
      if (incrementalEstimate) {
        incrementalEstimate.textContent = candidate.cloud.incremental_available ? `≤ ${fmtBytes(candidate.cloud.incremental_max_bytes)}` : '当前版本不可用';
        incrementalEstimate.title = candidate.cloud.incremental_available ? '这是增量资源上限；执行时会扫描本地 SHA-256，只下载实际变化文件。' : '当前 Release 未提供 Web Portable 增量清单。';
      }
    }
  }

  function renderState() {
    buildCandidates();
    const chip = $('web-update-capability');
    if (chip) {
      chip.classList.remove('warning', 'danger', 'ok');
      if (!state?.frozen) { chip.textContent = '源码模式'; chip.classList.add('warning'); }
      else if (!state?.updater_available) { chip.textContent = '缺少更新器'; chip.classList.add('danger'); }
      else { chip.textContent = '自动更新可用'; chip.classList.add('ok'); }
    }
    renderSelected();
    if (state?.cloud_error) setStatus(`云端版本读取失败：${state.cloud_error}`, false);
    else {
      const cloudCount = Array.isArray(state?.releases) ? state.releases.length : (state?.cloud ? 1 : 0);
      setStatus(`当前 v${state?.current_version || '?'}；已读取 ${cloudCount} 个云端通道版本和 ${(state?.backups || []).length} 个本地可恢复版本。默认选择正式版。`);
    }
  }

  async function refreshState() {
    ensureUi();
    const refresh = $('web-update-refresh'); if (refresh) refresh.disabled = true;
    setStatus('正在读取云端正式版、测试版与本地备份…');
    try {
      state = await api('/api/control/web-update/state');
      renderState();
    } catch (error) {
      setStatus(`读取 Web 更新状态失败：${error.message}`, false);
    } finally {
      if (refresh) refresh.disabled = false;
    }
  }

  async function startSelected(preferredMode) {
    const candidate = selected(); if (!candidate) return;
    let mode = preferredMode;
    if (candidate.source === 'local') mode = 'restore';
    if (mode === 'incremental' && !candidate.cloud?.incremental_available) {
      setStatus('当前 Release 没有 Web 增量资源，请使用全量更新。', false); return;
    }
    const label = mode === 'restore' ? `恢复到本地备份 v${candidate.version}` : `${mode === 'incremental' ? '增量' : '全量'}更新到 v${candidate.version}`;
    if (!confirm(`${label}？\n\n更新开始后会打开独立更新页面，主 Web 服务随后会停止并替换程序文件。`)) return;

    let popup = null;
    try { popup = window.open('about:blank', 'bilipdj-web-update'); } catch (_) { popup = null; }
    if (popup) {
      try { popup.document.write('<meta charset="utf-8"><title>BiliPDJ 更新器</title><body style="background:#090e1a;color:#e6edf7;font-family:sans-serif;padding:32px">正在启动独立更新器…</body>'); } catch (_) { /* ignore */ }
    }
    document.querySelectorAll('#web-update-controls button,#web-update-version').forEach(node => { node.disabled = true; });
    setStatus(`正在启动独立更新器：${label}…`);
    try {
      const payload = { mode };
      if (mode === 'restore') payload.backup_id = candidate.backup.id;
      else payload.target_tag = candidate.cloud?.tag_name || '';
      const result = await post('/api/control/web-update/start', payload);
      setStatus(result.message || '独立更新器已启动。');
      if (popup && !popup.closed) popup.location.replace(result.update_url);
      else location.href = result.update_url;
    } catch (error) {
      try { popup?.close(); } catch (_) { /* ignore */ }
      setStatus(`启动更新失败：${error.message}`, false);
      renderState();
    }
  }

  function init() {
    ensureUi();
    document.querySelector('[data-view="update"]')?.addEventListener('click', () => window.setTimeout(refreshState, 50));
    $('update-check')?.addEventListener('click', () => window.setTimeout(refreshState, 150));
    refreshState();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, { once: true }); else init();
})();
