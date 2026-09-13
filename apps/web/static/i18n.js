(() => {
  'use strict';

  const API = '/api/language';
  const SOURCE_TEXT = Symbol('bilipdjI18nSourceText');
  const LAST_TEXT = Symbol('bilipdjI18nLastText');
  const SOURCE_ATTR = Symbol('bilipdjI18nSourceAttr');
  const LAST_ATTR = Symbol('bilipdjI18nLastAttr');
  const SKIP_SELECTOR = 'script,style,pre,code,textarea';
  const DYNAMIC_UI_SELECTOR = 'button,label,legend,summary,option,th,[data-i18n],[data-i18n-ui]';
  const ATTRS = ['placeholder', 'title', 'aria-label'];
  const INITIAL_TEXT_NODES = new WeakSet();
  const INITIAL_ELEMENTS = new WeakSet();
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

  function markInitialUi(root = document.body) {
    if (!root) return;
    if (root instanceof Element) INITIAL_ELEMENTS.add(root);
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT);
    let node = walker.currentNode;
    while (node) {
      if (node.nodeType === Node.TEXT_NODE) INITIAL_TEXT_NODES.add(node);
      else if (node instanceof Element) INITIAL_ELEMENTS.add(node);
      node = walker.nextNode();
    }
  }

  function isTextTranslatable(node) {
    if (INITIAL_TEXT_NODES.has(node)) return true;
    const parent = node && node.parentElement;
    return Boolean(parent && parent.closest && parent.closest(DYNAMIC_UI_SELECTOR));
  }

  function isElementTranslatable(element) {
    if (INITIAL_ELEMENTS.has(element)) return true;
    if (!(element instanceof Element)) return false;
    return Boolean(
      element.matches(DYNAMIC_UI_SELECTOR)
      || (element.closest && element.closest('[data-i18n-ui]'))
    );
  }

  function applyTextNode(node) {
    if (!node || node.nodeType !== Node.TEXT_NODE || shouldSkip(node) || !isTextTranslatable(node)) return;
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
    if (!(element instanceof Element) || shouldSkip(element) || !isElementTranslatable(element)) return;
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
      select.dataset.i18nUi = 'true';
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
    // Only text/attributes that belong to the initial UI are implicitly
    // translatable. Later DOM data (queue names, blacklist entries, plugin/user
    // content, etc.) stays verbatim unless its element is an explicit UI control
    // or carries data-i18n / data-i18n-ui.
    markInitialUi(document.body);
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
