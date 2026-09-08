(() => {
  'use strict';

  const SUPPORT_URL = 'https://m.sdyuntuo.cn/ProductEn/Index/01929ef4362a8858';
  const SUPPORT_COPY = '项目免费开源使用，申请流量卡给开发者回回血。';
  const GUANGGAO_LEVEL = 'GUANGGAO';
  const GUANGGAO_TEXT = '【打扰一下】如果有需要正规大流量电话卡的，可以点击 支持我们-申请流量卡，自助申请哦。你的每张正常申请使用，都能给本项目带来持续的支持！';
  const INITIAL_DELAY_RANGE_MS = [45000, 150000];
  const REPEAT_DELAY_RANGE_MS = [480000, 1080000];

  const ABOUT_TOOL_NAME = 'Bilibili 直播弹幕排队管理工具';
  const ABOUT_ARCHITECTURE = '排队逻辑由 Python 后端统一处理，前端仅负责显示。';
  const ABOUT_FREE_NOTICE = '本软件完全免费，源码公开，Github Action自动打包，无后台无病毒，不损害电脑。若有人向你收费获取此软件（亲手帮安装调试除外），请立刻退款并举报！';
  const ABOUT_CIVIL = '• 民事责任：侵权方须停止侵权、赔偿损失（含维权合理费用）。';
  const ABOUT_CRIMINAL = '• 刑事责任：以营利为目的的侵权行为，情节严重时可能被追究刑事责任。';

  const QR_MATRIX = [
    "00000000000000000000000000000000000000000",
    "00000000000000000000000000000000000000000",
    "00000000000000000000000000000000000000000",
    "00000000000000000000000000000000000000000",
    "00001111111011111101101101110011111110000",
    "00001000001011110100011011111010000010000",
    "00001011101000111000001010100010111010000",
    "00001011101011010011111110011010111010000",
    "00001011101000111010001110101010111010000",
    "00001000001000011110011100001010000010000",
    "00001111111010101010101010101011111110000",
    "00000000000010100000011010111000000000000",
    "00001011011101011010001001001010010110000",
    "00000101100000100000110111010011011010000",
    "00000011011110110010110000110011110110000",
    "00000000010100011111100000101101010000000",
    "00001001111011011001001010001000110010000",
    "00001011100001101100110111101100001100000",
    "00000111001010101011100101001011001000000",
    "00001110100100101010101001001110111000000",
    "00001001101011101111001111100110111000000",
    "00001011010111111101011010011110110100000",
    "00000000101101000111110000000001101100000",
    "00000001010000100100100100100110100100000",
    "00001011111101111000001011101100011010000",
    "00001010010101011011001101111111011010000",
    "00000001001010110100100000110100010110000",
    "00000111110100111110100100110000010110000",
    "00001000001101101101110000101111100100000",
    "00000000000010111101110110101000110000000",
    "00001111111010101010100111011010100100000",
    "00001000001011100111100111011000111010000",
    "00001011101001100110010111011111101100000",
    "00001011101010001100011010001101010010000",
    "00001011101010110010010001110011011000000",
    "00001000001001100111111110101001100010000",
    "00001111111010000011111101000000111000000",
    "00000000000000000000000000000000000000000",
    "00000000000000000000000000000000000000000",
    "00000000000000000000000000000000000000000",
    "00000000000000000000000000000000000000000"
  ];

  const promotions = [];
  let promotionTimer = 0;
  let applyingLogOverlay = false;

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

    fetch('/api/control/meta', { cache: 'no-store' })
      .then(response => response.ok ? response.json() : Promise.reject(new Error(`HTTP ${response.status}`)))
      .then(payload => {
        const node = document.getElementById('about-version');
        if (node) node.textContent = String(payload.version || '未知');
      })
      .catch(() => {
        const node = document.getElementById('about-version');
        if (node) node.textContent = '未知';
      });
  }

  function ensureSupportPage() {
    const sidebar = document.querySelector('.sidebar');
    const content = document.querySelector('.content');
    if (!sidebar || !content) return;

    if (!sidebar.querySelector('[data-view="support"]')) {
      const button = document.createElement('button');
      button.className = 'nav';
      button.dataset.view = 'support';
      button.textContent = '支持我们';
      const about = sidebar.querySelector('[data-view="about"]');
      sidebar.insertBefore(button, about || null);
    }

    if (!document.getElementById('view-support')) {
      const section = document.createElement('section');
      section.className = 'view';
      section.id = 'view-support';
      section.innerHTML = `
        <div class="page-head"><div><h1>支持我们</h1><p>项目本体免费开源，流量卡申请会给项目带来持续支持。</p></div></div>
        <div class="card" style="max-width:760px;text-align:center;margin:0 auto;padding:32px 24px;">
          <h2 style="margin:0 0 12px;">流量卡推广</h2>
          <p style="margin:0 0 22px;line-height:1.8;">${SUPPORT_COPY}</p>
          <canvas id="support-qr" width="246" height="246" aria-label="申请流量卡二维码" style="width:246px;max-width:80vw;height:auto;background:#fff;padding:0;border-radius:8px;margin-bottom:20px;"></canvas>
          <div style="margin-bottom:12px;"><a id="support-apply" class="button" href="${SUPPORT_URL}" target="_blank" rel="noopener noreferrer">申请流量卡</a></div>
          <p class="hint" style="margin-top:12px;">可点击按钮跳转，也可以使用手机扫描二维码申请。</p>
          <p class="hint" style="margin-top:8px;word-break:break-all;">${SUPPORT_URL}</p>
        </div>`;
      const about = document.getElementById('view-about');
      content.insertBefore(section, about || null);
    }
  }

  function randomDelay([low, high]) {
    return Math.floor(low + Math.random() * (high - low + 1));
  }

  function drawQr() {
    const canvas = document.getElementById('support-qr');
    if (!canvas) return;
    const size = QR_MATRIX.length;
    const cell = Math.max(1, Math.floor(canvas.width / size));
    canvas.width = size * cell;
    canvas.height = size * cell;
    const ctx = canvas.getContext('2d');
    ctx.imageSmoothingEnabled = false;
    ctx.fillStyle = '#ffffff';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#000000';
    QR_MATRIX.forEach((row, y) => {
      for (let x = 0; x < row.length; x += 1) {
        if (row[x] === '1') ctx.fillRect(x * cell, y * cell, cell, cell);
      }
    });
  }

  function promotionLine() {
    const localTime = new Date().toLocaleTimeString('zh-CN', { hour12: false });
    return `${localTime}  ${GUANGGAO_LEVEL.padEnd(7, ' ')}  ${GUANGGAO_TEXT}`;
  }

  function applyPromotionOverlay() {
    const out = document.getElementById('log-output');
    if (!out || applyingLogOverlay) return;
    const kind = document.getElementById('log-kind')?.value || 'all';
    const term = (document.getElementById('log-search')?.value || '').trim().toLowerCase();
    const current = out.textContent || '';
    const base = current
      .split('\n')
      .filter(line => line && !line.includes(` ${GUANGGAO_LEVEL} `) && !line.includes(` ${GUANGGAO_LEVEL.padEnd(7, ' ')} `));
    const visiblePromotions = promotions.filter(line => !term || line.toLowerCase().includes(term));

    let lines;
    if (kind === 'guanggao') {
      lines = visiblePromotions.length ? visiblePromotions : ['暂无 GUANGGAO 提示，等待随机展示。'];
    } else if (kind === 'all' || kind === 'common') {
      lines = [...base, ...visiblePromotions];
    } else {
      lines = base;
    }
    const next = lines.join('\n') || '暂无日志。';
    if (next === current) return;
    applyingLogOverlay = true;
    out.textContent = next;
    applyingLogOverlay = false;
  }

  function emitPromotion() {
    promotions.push(promotionLine());
    if (promotions.length > 3) promotions.shift();
    applyPromotionOverlay();
  }

  function schedulePromotion(range) {
    if (promotionTimer) window.clearTimeout(promotionTimer);
    promotionTimer = window.setTimeout(() => {
      emitPromotion();
      schedulePromotion(REPEAT_DELAY_RANGE_MS);
    }, randomDelay(range));
  }

  ensureUnifiedAboutPage();
  ensureSupportPage();

  const logOutput = document.getElementById('log-output');
  if (logOutput) {
    const observer = new MutationObserver(() => applyPromotionOverlay());
    observer.observe(logOutput, { childList: true, characterData: true, subtree: true });
  }

  const logKind = document.getElementById('log-kind');
  if (logKind && !logKind.querySelector('option[value="guanggao"]')) {
    const option = document.createElement('option');
    option.value = 'guanggao';
    option.textContent = '推广';
    logKind.appendChild(option);
  }
  logKind?.addEventListener('change', () => queueMicrotask(applyPromotionOverlay));
  document.getElementById('log-search')?.addEventListener('input', () => queueMicrotask(applyPromotionOverlay));
  document.getElementById('log-clear')?.addEventListener('click', () => {
    promotions.length = 0;
    queueMicrotask(applyPromotionOverlay);
  });

  const applyButton = document.getElementById('support-apply');
  if (applyButton && applyButton.getAttribute('href') !== SUPPORT_URL) {
    applyButton.setAttribute('href', SUPPORT_URL);
  }

  drawQr();
  schedulePromotion(INITIAL_DELAY_RANGE_MS);
})();
