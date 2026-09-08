(() => {
  'use strict';

  if (window.__bilipdjIssue123FetchGuardInstalled) return;
  window.__bilipdjIssue123FetchGuardInstalled = true;

  const nativeFetch = window.fetch.bind(window);
  window.fetch = async function issue123Fetch(input, init = {}) {
    const rawUrl = typeof input === 'string' ? input : String(input?.url || '');
    let target = null;
    try {
      target = new URL(rawUrl, window.location.href);
    } catch (_) {
      return nativeFetch(input, init);
    }

    const isGoogleProbe = target.hostname.toLowerCase() === 'www.google.com' &&
      target.pathname === '/generate_204';
    if (!isGoogleProbe) return nativeFetch(input, init);

    const response = await nativeFetch('/api/platforms/youtube/probe', {
      method: 'GET',
      cache: 'no-store',
      signal: init?.signal,
    });
    let payload = {};
    try { payload = await response.json(); } catch (_) { /* ignore */ }
    if (!response.ok || payload.status === 'error' || payload.allowed !== true) {
      throw new TypeError(payload.message || 'Google access probe failed');
    }
    return new Response(null, { status: 204, statusText: 'No Content' });
  };
})();
