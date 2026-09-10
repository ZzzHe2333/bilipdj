(() => {
  'use strict';

  const $ = id => document.getElementById(id);
  let pluginByPlatform = new Map();
  let saveInstalled = false;
  let refreshInstalled = false;

  async function api(path, options = {}) {
    const response = await fetch(path, { cache: 'no-store', ...options });
    let payload = {};
    try { payload = await response.json(); } catch (_) { /* ignore */ }
    if (!response.ok || payload.status === 'error') {
      throw new Error(payload.message || `HTTP ${response.status}`);
    }
    return payload;
  }

  function post(path, payload) {
    return api(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload || {}),
    });
  }

  function pluginLabel(plugin) {
    const source = plugin.source === 'builtin' ? '内置' : '外部';
    return `${source} · Plugin API v${plugin.plugin_api || 1}`;
  }

  function renamePluginSettings() {
    const activeTab = document.querySelector('[data-settings="active-platforms"]');
    if (activeTab) activeTab.textContent = '获取弹幕插件';

    const platformTab = document.querySelector('[data-settings="platform"]');
    if (platformTab) platformTab.textContent = '弹幕插件配置';

    const pane = $('settings-active-platforms');
    const heading = pane?.querySelector('h3');
    if (heading) heading.textContent = '获取弹幕插件';
    const hint = pane?.querySelector('.issue79-section-title .hint');
    if (hint) {
      hint.textContent = '选择需要启用的获取弹幕插件；多个插件可同时接收弹幕并进入同一个排队系统。';
    }

    const select = $('platform-select');
    const label = select?.closest('label');
    const labelText = label?.querySelector('span');
    if (labelText) labelText.textContent = '获取弹幕插件';
  }

  function renderPluginCards(payload) {
    const pane = $('settings-active-platforms');
    const grid = pane?.querySelector('.issue79-platform-grid');
    if (!grid) return false;

    const plugins = Array.isArray(payload.plugins) ? payload.plugins : [];
    const active = new Set(Array.isArray(payload.active) ? payload.active.map(String) : []);
    pluginByPlatform = new Map(plugins.map(plugin => [String(plugin.platform || ''), plugin]));

    grid.innerHTML = '';
    for (const plugin of plugins) {
      const platform = String(plugin.platform || '').trim();
      if (!platform) continue;
      const label = document.createElement('label');
      label.className = 'issue79-platform-option';
      const checkbox = document.createElement('input');
      checkbox.id = `active-${platform}`;
      checkbox.type = 'checkbox';
      checkbox.dataset.danmuPluginPlatform = platform;
      checkbox.checked = active.has(platform);
      checkbox.disabled = plugin.available === false;

      const body = document.createElement('div');
      const strong = document.createElement('strong');
      strong.textContent = String(plugin.name || `${platform} 获取弹幕插件`);
      const meta = document.createElement('span');
      meta.textContent = `${pluginLabel(plugin)} · 平台 ${platform}`;
      body.append(strong, meta);
      label.append(checkbox, body);
      grid.appendChild(label);
    }

    const legacyHint = [...(pane?.querySelectorAll('p.hint') || [])].find(node =>
      node.textContent.includes('预留配置') || node.textContent.includes('暂不能激活弹幕流')
    );
    if (legacyHint) {
      legacyHint.textContent = '未出现在列表中的平台尚未安装可用的获取弹幕插件；后续可通过插件机制扩展。';
    }
    return true;
  }

  function statusText(payload) {
    const active = Array.isArray(payload.active) ? payload.active.map(String) : [];
    const states = payload.runtime?.platforms || {};
    const online = Object.entries(states)
      .filter(([, value]) => Boolean(value?.connected))
      .map(([platform]) => pluginByPlatform.get(platform)?.name || platform);
    const activeNames = active.map(platform => pluginByPlatform.get(platform)?.name || platform);
    return `已启用：${activeNames.join(' + ') || '无'}；在线：${online.join(' + ') || '暂无'}。`;
  }

  function setStatus(text, ok = true) {
    const node = $('active-platform-status');
    if (!node) return;
    node.textContent = text || '';
    node.style.color = ok ? '' : '#d84f63';
  }

  async function refreshPlugins() {
    renamePluginSettings();
    try {
      const payload = await api('/api/platforms/active');
      if (!renderPluginCards(payload)) return;
      setStatus(statusText(payload));
    } catch (error) {
      setStatus(`读取获取弹幕插件失败：${error.message}`, false);
    }
  }

  async function savePlugins(event) {
    event?.preventDefault();
    event?.stopImmediatePropagation();
    const active = [...document.querySelectorAll('[data-danmu-plugin-platform]')]
      .filter(node => node.checked && !node.disabled)
      .map(node => String(node.dataset.danmuPluginPlatform || '').trim())
      .filter(Boolean);
    setStatus('正在保存获取弹幕插件并重连……');
    try {
      await post('/api/platforms/active', { active });
      await refreshPlugins();
    } catch (error) {
      setStatus(`保存获取弹幕插件失败：${error.message}`, false);
    }
  }

  function bindToolbar() {
    const save = $('active-platform-save');
    if (save && !saveInstalled) {
      saveInstalled = true;
      save.textContent = '保存插件并重连';
      save.addEventListener('click', savePlugins, true);
    }
    const refresh = $('active-platform-refresh');
    if (refresh && !refreshInstalled) {
      refreshInstalled = true;
      refresh.textContent = '刷新插件状态';
      refresh.addEventListener('click', event => {
        event.preventDefault();
        event.stopImmediatePropagation();
        refreshPlugins();
      }, true);
    }
  }

  function boot() {
    renamePluginSettings();
    bindToolbar();
    refreshPlugins();
    window.setTimeout(() => {
      renamePluginSettings();
      bindToolbar();
      refreshPlugins();
    }, 80);

    document.querySelector('[data-settings="active-platforms"]')?.addEventListener('click', () => {
      window.setTimeout(() => {
        renamePluginSettings();
        bindToolbar();
        refreshPlugins();
      }, 30);
    });
    document.querySelector('[data-view="settings"]')?.addEventListener('click', () => {
      window.setTimeout(() => {
        renamePluginSettings();
        bindToolbar();
      }, 60);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot, { once: true });
  } else {
    boot();
  }
})();
