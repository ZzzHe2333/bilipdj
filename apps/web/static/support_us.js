(() => {
  'use strict';

  const DONATION_COPY = '如果这个项目帮到了你，欢迎自愿扫码赞赏，支持后续维护与更新。';
  const DONATION_IMAGE = '/WxZSM.png';

  const ABOUT_TOOL_NAME = 'Bilibili 直播弹幕排队管理工具';
  const ABOUT_ARCHITECTURE = '排队逻辑由 Python 后端统一处理，前端仅负责显示。';
  const ABOUT_FREE_NOTICE = '本软件完全免费，源码公开，Github Action自动打包，无后台无病毒，不损害电脑。若有人向你收费获取此软件（亲手帮安装调试除外），请立刻退款并举报！';
  const ABOUT_CIVIL = '• 民事责任：侵权方须停止侵权、赔偿损失（含维权合理费用）。';
  const ABOUT_CRIMINAL = '• 刑事责任：以营利为目的的侵权行为，情节严重时可能被追究刑事责任。';

  let aboutVersionObserver = null;

  function ensureUnifiedAboutPage() {
    const view = document.getElementById('view-about');
    const card = view?.querySelector('.card.about');
    if (!view || !card) return;

    const subtitle = view.querySelector('.page-head p');
    if (subtitle) subtitle.textContent = '项目与版本信息';

    card.innerHTML = `
      <h2>弹幕排队姬</h2>
      <p><strong>当前版本号：</strong><span id="about-version">读取中…</span></p>
      <p><strong>您的版本是：</strong>网页版</p>
      <p>${ABOUT_TOOL_NAME}</p>
      <p>${ABOUT_ARCHITECTURE}</p>
      <p>${ABOUT_FREE_NOTICE}</p>
      <p><strong>【侵权/倒卖责任】</strong></p>
      <p>${ABOUT_CIVIL}</p>
      <p>${ABOUT_CRIMINAL}</p>
      <div class="toolbar"><a class="button" href="https://github.com/ZzzHe2333/bilipdj" target="_blank" rel="noopener">GitHub 仓库</a></div>`;

    const headerVersion = document.getElementById('version');
    const syncVersion = () => {
      const node = document.getElementById('about-version');
      if (!node) return;
      const text = String(headerVersion?.textContent || '');
      const match = text.match(/v([^\s·]+)/i);
      node.textContent = match ? match[1] : '读取中…';
    };
    syncVersion();
    if (headerVersion) {
      aboutVersionObserver?.disconnect();
      aboutVersionObserver = new MutationObserver(syncVersion);
      aboutVersionObserver.observe(headerVersion, { childList: true, characterData: true, subtree: true });
    }
  }

  function ensureSupportStyles() {
    if (document.getElementById('support-us-styles')) return;
    const style = document.createElement('style');
    style.id = 'support-us-styles';
    style.textContent = `
      .support-grid{display:grid;grid-template-columns:minmax(0,1fr);max-width:540px;margin:0 auto}
      .support-card{min-width:0;text-align:center;padding:26px 22px;display:flex;flex-direction:column;align-items:center}
      .support-card h2{margin:0 0 10px}
      .support-card p{line-height:1.75}
      .support-donation{width:220px;max-width:min(78vw,100%);height:auto;background:#fff;padding:8px;border-radius:10px;box-sizing:border-box;margin:4px 0 18px}
      .support-donation{object-fit:contain;aspect-ratio:1/1}
      @media (max-width:760px){.support-card{padding:22px 18px}}
    `;
    document.head.appendChild(style);
  }

  function ensureSupportPage() {
    const sidebar = document.querySelector('.sidebar');
    const content = document.querySelector('.content');
    if (!sidebar || !content) return;

    ensureSupportStyles();

    if (!sidebar.querySelector('[data-view="support"]')) {
      const button = document.createElement('button');
      button.className = 'nav';
      button.dataset.view = 'support';
      button.textContent = '支持我们';
      const about = sidebar.querySelector('[data-view="about"]');
      sidebar.insertBefore(button, about || null);
    }

    let section = document.getElementById('view-support');
    if (!section) {
      section = document.createElement('section');
      section.className = 'view';
      section.id = 'view-support';
      const about = document.getElementById('view-about');
      content.insertBefore(section, about || null);
    }

    section.innerHTML = `
      <div class="page-head"><div><h1>支持我们</h1><p>项目本体永久免费开源，赞赏完全自愿。</p></div></div>
      <div class="support-grid">
        <article class="card support-card">
          <h2>赞赏项目</h2>
          <p>${DONATION_COPY}</p>
          <img id="support-donation" class="support-donation" src="${DONATION_IMAGE}" alt="微信赞赏码">
          <p style="margin:0 0 6px;"><strong>微信赞赏码</strong></p>
          <p class="hint" style="margin:0;">赞赏完全自愿，不影响任何功能使用。</p>
        </article>
      </div>`;
  }

  ensureUnifiedAboutPage();
  ensureSupportPage();
})();
