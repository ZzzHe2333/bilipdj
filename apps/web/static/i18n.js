(() => {
  'use strict';

  const API = '/api/language';
  const SOURCE_TEXT = Symbol('bilipdjI18nSourceText');
  const LAST_TEXT = Symbol('bilipdjI18nLastText');
  const SOURCE_ATTR = Symbol('bilipdjI18nSourceAttr');
  const LAST_ATTR = Symbol('bilipdjI18nLastAttr');
  const SKIP_SELECTOR = 'script,style,pre,code,textarea';
  const ATTRS = ['placeholder', 'title', 'aria-label'];
  let state = { active: 'zh-CN', languages: [], translations: {} };
  let applying = false;

  function translated(source) {
    const mapping = state.translations || {};
    return Object.prototype.hasOwnProperty.call(mapping, source) ? mapping[source] : source;
  }

  function shouldSkip(node) {
    const parent = node.nodeType === Node.TEXT_NODE ? node.parentElement : node;
    return Boolean(parent && parent.closest && parent.closest(SKIP_SELECTOR));
  }

  function applyTextNode(node) {
    if (!node || node.nodeType !== Node.TEXT_NODE || shouldSkip(node)) return;
    const current = node.nodeValue || '';
    if (!current.trim()) return;
    if (node[SOURCE_TEXT] === undefined || (node[LAST_TEXT] !== undefined && current !== node[LAST_TEXT])) {
      node[SOURCE_TEXT] = current;
    }
    const next = translated(String(node[SOURCE_TEXT]));
    if (current !== next) node.nodeValue = next;
    node[LAST_TEXT] = next;
  }

  function applyAttributes(element) {
    if (!(element instanceof Element) || shouldSkip(element)) return;
    if (!element[SOURCE_ATTR]) element[SOURCE_ATTR] = Object.create(null);
    if (!element[LAST_ATTR]) element[LAST_ATTR] = Object.create(null);
    for (const attr of ATTRS) {
      if (!element.hasAttribute(attr)) continue;
      const current = element.getAttribute(attr) || '';
      if (!current) continue;
      if (!(attr in element[SOURCE_ATTR]) || (attr in element[LAST_ATTR] && current !== element[LAST_ATTR][attr])) {
        element[SOURCE_ATTR][attr] = current;
      }
      const next = translated(String(element[SOURCE_ATTR][attr]));
      if (current !== next) element.setAttribute(attr, next);
      element[LAST_ATTR][attr] = next;
    }
  }

  function apply(root = document.body) {
    if (!root || applying) return;
    applying = true;
    try {
      if (root.nodeType === Node.TEXT_NODE) applyTextNode(root);
      if (root instanceof Element) applyAttributes(root);
      const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT);
      let node = walker.currentNode;
      while (node) {
        if (node.nodeType === Node.TEXT_NODE) applyTextNode(node);
        else if (node instanceof Element) applyAttributes(node);
        node = walker.nextNode();
      }
      document.documentElement.lang = state.active || 'zh-CN';
    } finally {
      applying = false;
    }
  }

  function ensureSelector() {
    const host = document.querySelector('.top-actions');
    if (!host) return;
    let select = document.getElementById('bilipdj-language-select');
    if (!select) {
      select = document.createElement('select');
      select.id = 'bilipdj-language-select';
      select.className = 'button ghost';
      select.title = '界面语言';
      select.setAttribute('aria-label', '界面语言');
      select.addEventListener('change', async () => {
        try {
          const response = await fetch(API, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ language: select.value }),
          });
          const payload = await response.json();
          if (!response.ok || payload.status === 'error') throw new Error(payload.message || `HTTP ${response.status}`);
          updateState(payload);
        } catch (error) {
          console.error('[BiliPDJ i18n] failed to switch language', error);
          await refresh();
        }
      });
      host.prepend(select);
    }
    const old = select.value;
    select.replaceChildren();
    for (const item of state.languages || []) {
      const option = document.createElement('option');
      option.value = item.code;
      option.textContent = `${item.name} (${item.code})`;
      select.append(option);
    }
    select.value = state.active || old || 'zh-CN';
  }

  function updateState(payload) {
    if (!payload || payload.status === 'error') return;
    state = {
      active: String(payload.active || 'zh-CN'),
      languages: Array.isArray(payload.languages) ? payload.languages : [],
      translations: payload.translations && typeof payload.translations === 'object' ? payload.translations : {},
    };
    ensureSelector();
    apply(document.body);
    window.dispatchEvent(new CustomEvent('bilipdj-language-changed', { detail: state }));
  }

  async function refresh() {
    try {
      const response = await fetch(API, { cache: 'no-store' });
      const payload = await response.json();
      if (!response.ok || payload.status === 'error') throw new Error(payload.message || `HTTP ${response.status}`);
      updateState(payload);
      return state;
    } catch (error) {
      console.warn('[BiliPDJ i18n] unavailable, using source language', error);
      state = { active: 'zh-CN', languages: [{ code: 'zh-CN', name: '简体中文' }], translations: {} };
      apply(document.body);
      return state;
    }
  }

  const observer = new MutationObserver((mutations) => {
    if (applying) return;
    for (const mutation of mutations) {
      if (mutation.type === 'characterData') applyTextNode(mutation.target);
      for (const node of mutation.addedNodes || []) {
        if (node.nodeType === Node.TEXT_NODE) applyTextNode(node);
        else if (node instanceof Element) apply(node);
      }
      if (mutation.type === 'attributes' && mutation.target instanceof Element) applyAttributes(mutation.target);
    }
  });

  function start() {
    observer.observe(document.documentElement, {
      subtree: true,
      childList: true,
      characterData: true,
      attributes: true,
      attributeFilter: ATTRS,
    });
    void refresh();
    window.setInterval(() => void refresh(), 15000);
  }

  window.BiliPDJI18n = {
    refresh,
    apply,
    translate: translated,
    getState: () => ({ ...state, translations: { ...(state.translations || {}) } }),
  };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start, { once: true });
  else start();
})();
