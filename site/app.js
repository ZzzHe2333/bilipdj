(() => {
  'use strict';
  const releasesUrl = 'https://github.com/ZzzHe2333/bilipdj/releases/latest';
  const year = document.querySelector('#year');
  if (year) year.textContent = new Date().getFullYear();
  const setText = (selector, value) => { const el = document.querySelector(selector); if (el && value) el.textContent = value; };
  const themeButton = document.querySelector('#theme-toggle');
  const stored = (() => { try { return localStorage.getItem('bilipdj.site.theme'); } catch (_) { return null; } })();
  const prefersLight = window.matchMedia?.('(prefers-color-scheme: light)').matches;
  const setTheme = mode => {
    const light = mode === 'light';
    document.documentElement.dataset.theme = light ? 'light' : 'dark';
    if (themeButton) { themeButton.textContent = light ? '深色模式' : '浅色模式'; themeButton.setAttribute('aria-pressed', String(light)); }
  };
  setTheme(stored === 'light' || stored === 'dark' ? stored : (prefersLight ? 'light' : 'dark'));
  themeButton?.addEventListener('click', () => {
    const mode = document.documentElement.dataset.theme === 'light' ? 'dark' : 'light';
    setTheme(mode);
    try { localStorage.setItem('bilipdj.site.theme', mode); } catch (_) {}
  });
  document.querySelector('#copy-obs')?.addEventListener('click', async event => {
    const url = document.querySelector('#obs-url')?.textContent || 'http://127.0.0.1:9816/index';
    try { await navigator.clipboard.writeText(url); event.currentTarget.textContent = '已复制'; }
    catch (_) { window.prompt('复制 OBS 地址', url); }
  });
  const prettyBytes = bytes => (Number(bytes) / 1000000).toFixed(1) + ' MB';
  // A release API response is authoritative; no hard-coded asset versions.
  // If GitHub rate limits or the network fails, both links stay on latest.
  const assets = [
    { suffix: '-Windows-Tk-Portable-x64.zip', meta: '#windows-meta', link: '#windows-download', label: '下载 Windows 客户端' },
    { suffix: '-Web-Portable-x64.zip', meta: '#web-meta', link: '#web-download', label: '下载 Web 便携版' },
  ];
  fetch('https://api.github.com/repos/ZzzHe2333/bilipdj/releases/latest', { headers: { Accept: 'application/vnd.github+json' } })
    .then(resp => { if (!resp.ok) throw new Error('release fetch failed'); return resp.json(); })
    .then(data => {
      if (!/^v\d+\.\d+\.\d+$/.test(data?.tag_name || '') || data.prerelease || data.draft) throw new Error('no stable release');
      setText('#releaseValue', data.tag_name);
      for (const entry of assets) {
        const asset = Array.isArray(data.assets) ? data.assets.find(item => typeof item.name === 'string' && item.name.endsWith(entry.suffix)) : null;
        const expectedUrl = releasesUrl.replace('/latest', '/download/' + encodeURIComponent(data.tag_name) + '/' + encodeURIComponent(asset?.name || ''));
        const valid = asset && asset.browser_download_url === expectedUrl && Number.isFinite(Number(asset.size)) && Number(asset.size) > 0;
        setText(entry.meta, valid ? data.tag_name + ' · ' + prettyBytes(asset.size) + ' · ZIP 完整包' : data.tag_name + ' · 请在发行页选择完整包');
        const link = document.querySelector(entry.link);
        if (link) { link.href = valid ? asset.browser_download_url : releasesUrl; link.textContent = entry.label + ' · ' + data.tag_name; }
      }
    })
    .catch(() => { for (const entry of assets) setText(entry.meta, '最新正式版 · 请在 GitHub Releases 查看大小'); });
  fetch('https://api.github.com/repos/ZzzHe2333/bilipdj', { headers: { Accept: 'application/vnd.github+json' } })
    .then(response => response.ok ? response.json() : null)
    .then(data => { if (typeof data?.stargazers_count === 'number') setText('#starsValue', String(data.stargazers_count)); })
    .catch(() => {});
})();
