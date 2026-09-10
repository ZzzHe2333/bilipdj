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
    if (hint) hint.textContent = '选择需要启用的获取弹幕插件；多个插件可同时接收弹幕并进入同一个排队系统。';
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
    if (legacyHint) legacyHint.textContent = '未出现在列表中的平台尚未安装并启用可用的获取弹幕插件。';
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

  function setManagerStatus(text, ok = true) {
    const node = $('plugin-manager-status');
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

  function activateSettingsPane(button, pane) {
    document.querySelectorAll('.subtab').forEach(node => node.classList.toggle('active', node === button));
    document.querySelectorAll('.settings-pane').forEach(node => node.classList.toggle('active', node === pane));
  }

  function ensureManagerPane() {
    const subtabs = document.querySelector('#view-settings .subtabs');
    const settingsView = $('view-settings');
    if (!subtabs || !settingsView || $('settings-plugin-manager')) return;
    const button = document.createElement('button');
    button.className = 'subtab';
    button.dataset.settings = 'plugin-manager';
    button.textContent = '插件管理';
    const activeButton = subtabs.querySelector('[data-settings="active-platforms"]');
    if (activeButton?.nextSibling) subtabs.insertBefore(button, activeButton.nextSibling);
    else subtabs.appendChild(button);

    const pane = document.createElement('div');
    pane.className = 'settings-pane';
    pane.id = 'settings-plugin-manager';
    pane.innerHTML = `
      <div class="card">
        <div class="issue79-section-title"><div><h3>插件管理器</h3><p class="hint">安装 .bilipdj-plugin 本地包。安装阶段不会执行插件代码；插件只有启用后才会加载。</p></div></div>
        <div class="form-grid">
          <label class="wide"><span>插件包</span><input id="plugin-package-file" type="file" accept=".bilipdj-plugin,application/zip"></label>
          <label class="wide"><input id="plugin-allow-unsigned" type="checkbox"> 允许安装未签名的本地插件（需要你明确确认来源可信）</label>
        </div>
        <div class="toolbar"><button id="plugin-install" class="button">安装插件</button><button id="plugin-manager-refresh" class="button ghost">刷新</button></div>
        <p class="hint">第三方 Python 插件在当前版本中与 BiliPDJ 同进程运行。权限清单用于安装审批和 Host API 能力控制，不等同于操作系统沙箱；未知来源插件不要启用。</p>
        <div id="plugin-manager-status" class="status"></div>
        <div id="plugin-manager-list" style="display:grid;gap:10px;margin-top:14px"></div>
      </div>
      <div class="card">
        <div class="issue79-section-title"><div><h3>可信签名公钥</h3><p class="hint">签名插件使用 Ed25519；只有这里登记的 key_id 才被信任。</p></div></div>
        <div class="form-grid">
          <label><span>Key ID</span><input id="plugin-key-id" placeholder="publisher.example"></label>
          <label class="wide"><span>Ed25519 公钥（Base64，32 字节）</span><input id="plugin-key-value" placeholder="Base64 public key"></label>
        </div>
        <div class="toolbar"><button id="plugin-key-add" class="button ghost">添加/更新公钥</button></div>
        <div id="plugin-key-list" style="display:grid;gap:8px;margin-top:10px"></div>
      </div>`;
    const firstPane = settingsView.querySelector('.settings-pane');
    if (firstPane) firstPane.parentNode.insertBefore(pane, firstPane.nextSibling);
    else settingsView.appendChild(pane);
    button.addEventListener('click', () => { activateSettingsPane(button, pane); refreshManager(); });
    $('plugin-manager-refresh')?.addEventListener('click', refreshManager);
    $('plugin-install')?.addEventListener('click', installPackage);
    $('plugin-key-add')?.addEventListener('click', addTrustedKey);
    $('plugin-manager-list')?.addEventListener('click', handlePluginAction);
    $('plugin-key-list')?.addEventListener('click', handleKeyAction);
  }

  function actionButton(text, action, id, danger = false) {
    const button = document.createElement('button');
    button.className = `button mini ${danger ? 'danger' : 'ghost'}`;
    button.textContent = text;
    button.dataset.pluginAction = action;
    button.dataset.pluginId = id;
    return button;
  }

  function renderManagedPlugins(payload) {
    const host = $('plugin-manager-list');
    if (!host) return;
    host.innerHTML = '';
    const plugins = Array.isArray(payload.plugins) ? payload.plugins : [];
    for (const plugin of plugins) {
      const card = document.createElement('div');
      card.className = 'issue79-platform-option';
      card.style.display = 'block';
      const title = document.createElement('strong');
      title.textContent = String(plugin.name || plugin.id || '插件');
      const details = document.createElement('div');
      details.className = 'hint';
      const permissionText = Array.isArray(plugin.permissions) && plugin.permissions.length ? plugin.permissions.join(', ') : '无';
      const integrity = plugin.verified === false ? '校验失败' : '已校验';
      details.textContent = `${plugin.source === 'builtin' ? '内置' : '外部'} · ${plugin.id || ''} · 平台 ${plugin.platform || ''} · v${plugin.version || 'builtin'} · ${integrity} · 权限：${permissionText}`;
      card.append(title, details);
      if (plugin.signature_status) {
        const sig = document.createElement('div');
        sig.className = 'hint';
        sig.textContent = `签名：${plugin.signature_status}${plugin.package_sha256 ? ` · SHA-256 ${plugin.package_sha256}` : ''}`;
        card.appendChild(sig);
      }
      if (plugin.error) {
        const error = document.createElement('div');
        error.className = 'status';
        error.style.color = '#d84f63';
        error.textContent = `错误：${plugin.error}`;
        card.appendChild(error);
      }
      if (plugin.source === 'external') {
        const toolbar = document.createElement('div');
        toolbar.className = 'toolbar';
        toolbar.style.marginTop = '8px';
        toolbar.appendChild(actionButton('校验完整性', 'verify', plugin.id));
        toolbar.appendChild(actionButton(plugin.enabled ? '禁用' : '启用', plugin.enabled ? 'disable' : 'enable', plugin.id));
        toolbar.appendChild(actionButton('卸载', 'uninstall', plugin.id, true));
        card.appendChild(toolbar);
      }
      host.appendChild(card);
    }
    if (!plugins.length) host.textContent = '暂无插件。';
  }

  function renderTrustedKeys(payload) {
    const host = $('plugin-key-list');
    if (!host) return;
    host.innerHTML = '';
    const keys = Array.isArray(payload.keys) ? payload.keys : [];
    for (const item of keys) {
      const row = document.createElement('div');
      row.className = 'toolbar';
      const text = document.createElement('span');
      text.textContent = `${item.key_id} · ${item.algorithm || 'ed25519'}`;
      const button = document.createElement('button');
      button.className = 'button mini danger';
      button.textContent = '删除';
      button.dataset.keyAction = 'delete';
      button.dataset.keyId = item.key_id;
      row.append(text, button);
      host.appendChild(row);
    }
    if (!keys.length) host.textContent = '暂无可信公钥。';
  }

  async function refreshManager() {
    if (!$('settings-plugin-manager')) return;
    try {
      const [plugins, keys] = await Promise.all([api('/api/plugins/manage'), api('/api/plugins/trusted-keys')]);
      renderManagedPlugins(plugins);
      renderTrustedKeys(keys);
      setManagerStatus(`BiliPDJ ${plugins.bilipdj_version || ''} · Plugin API v${plugins.plugin_api || 1} · 支持权限：${(plugins.supported_permissions || []).join(', ')}`);
      await refreshPlugins();
    } catch (error) {
      setManagerStatus(`读取插件管理器失败：${error.message}`, false);
    }
  }

  function fileToBase64(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onerror = () => reject(reader.error || new Error('读取文件失败'));
      reader.onload = () => {
        const text = String(reader.result || '');
        const index = text.indexOf(',');
        resolve(index >= 0 ? text.slice(index + 1) : text);
      };
      reader.readAsDataURL(file);
    });
  }

  async function installPackage() {
    const file = $('plugin-package-file')?.files?.[0];
    if (!file) { setManagerStatus('请选择 .bilipdj-plugin 文件。', false); return; }
    if (!file.name.toLowerCase().endsWith('.bilipdj-plugin')) { setManagerStatus('文件扩展名必须为 .bilipdj-plugin。', false); return; }
    if (file.size > 1024 * 1024) { setManagerStatus('插件包不能超过 1 MiB。', false); return; }
    setManagerStatus('正在校验并安装插件……');
    try {
      const dataBase64 = await fileToBase64(file);
      const response = await post('/api/plugins/install', {
        filename: file.name,
        data_base64: dataBase64,
        allow_unsigned: Boolean($('plugin-allow-unsigned')?.checked),
      });
      setManagerStatus(`已安装 ${response.plugin?.name || response.plugin?.id || file.name}；默认保持禁用，请检查权限后再启用。`);
      if ($('plugin-package-file')) $('plugin-package-file').value = '';
      await refreshManager();
    } catch (error) {
      setManagerStatus(`安装失败：${error.message}`, false);
    }
  }

  async function handlePluginAction(event) {
    const button = event.target.closest('[data-plugin-action]');
    if (!button) return;
    const id = String(button.dataset.pluginId || '');
    const action = String(button.dataset.pluginAction || '');
    if (!id || !action) return;
    if (action === 'uninstall' && !confirm(`确定卸载插件 ${id}？插件数据目录会保留。`)) return;
    setManagerStatus(`正在执行 ${action}：${id}……`);
    try {
      await post(`/api/plugins/${action}`, { id });
      setManagerStatus(`${id}：${action} 完成。`);
      await refreshManager();
    } catch (error) {
      setManagerStatus(`${id} 操作失败：${error.message}`, false);
    }
  }

  async function addTrustedKey() {
    const keyId = String($('plugin-key-id')?.value || '').trim();
    const publicKey = String($('plugin-key-value')?.value || '').trim();
    if (!keyId || !publicKey) { setManagerStatus('Key ID 和公钥都不能为空。', false); return; }
    try {
      await post('/api/plugins/trusted-keys', { key_id: keyId, public_key: publicKey });
      if ($('plugin-key-id')) $('plugin-key-id').value = '';
      if ($('plugin-key-value')) $('plugin-key-value').value = '';
      setManagerStatus(`可信公钥 ${keyId} 已保存。`);
      await refreshManager();
    } catch (error) {
      setManagerStatus(`保存公钥失败：${error.message}`, false);
    }
  }

  async function handleKeyAction(event) {
    const button = event.target.closest('[data-key-action="delete"]');
    if (!button) return;
    const keyId = String(button.dataset.keyId || '');
    if (!keyId || !confirm(`删除可信公钥 ${keyId}？使用该密钥签名的插件下次校验会失败。`)) return;
    try {
      await post('/api/plugins/trusted-keys/delete', { key_id: keyId });
      setManagerStatus(`可信公钥 ${keyId} 已删除。`);
      await refreshManager();
    } catch (error) {
      setManagerStatus(`删除公钥失败：${error.message}`, false);
    }
  }

  function boot() {
    renamePluginSettings();
    bindToolbar();
    ensureManagerPane();
    refreshPlugins();
    window.setTimeout(() => {
      renamePluginSettings();
      bindToolbar();
      ensureManagerPane();
      refreshPlugins();
    }, 80);
    document.querySelector('[data-settings="active-platforms"]')?.addEventListener('click', () => {
      window.setTimeout(() => { renamePluginSettings(); bindToolbar(); refreshPlugins(); }, 30);
    });
    document.querySelector('[data-view="settings"]')?.addEventListener('click', () => {
      window.setTimeout(() => { renamePluginSettings(); bindToolbar(); ensureManagerPane(); }, 60);
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot, { once: true });
  else boot();
})();
