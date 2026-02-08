/* =====================================================================
   inline.js
   Собрание инлайновых <script> из исходного index.html.
   Скрипты объединены в порядке появления в документе.
   Комментарии подсказывают, где что стояло.
   ===================================================================== */


/* ----- Инлайновый скрипт #2: отвечает за логику из соответствующего блока страницы. ----- */

(function bootOnce(){
    if (window.__stilvaBoot) return; window.__stilvaBoot = true;
// ====== FULLSIZE IMAGE VIEWER (микро) ======
function __ensureImgViewer(){
  let box = document.getElementById('img-viewer');
  if (box) return box;

  box = document.createElement('div');
  box.id = 'img-viewer';
  box.innerHTML = `
    <button class="iv__close" aria-label="Закрыть">×</button>
    <img class="iv__img" alt="">
  `;
  document.body.appendChild(box);

  // компактные стили без внешних CSS
  const css = document.createElement('style');
  css.textContent = `
    #img-viewer{position:fixed;inset:0;display:none;align-items:center;justify-content:center;
      background:rgba(0,0,0,.8);z-index:9999}
    #img-viewer.open{display:flex}
    #img-viewer .iv__img{max-width:96vw;max-height:96vh;border-radius:12px;box-shadow:0 10px 40px rgba(0,0,0,.6)}
    #img-viewer .iv__close{position:absolute;top:14px;right:14px;width:42px;height:42px;border:0;border-radius:10px;
      background:rgba(255,255,255,.92);font:700 26px/1 system-ui;cursor:pointer}
  `;
  document.head.appendChild(css);

  function close(){
    box.classList.remove('open');
    document.body.style.overflow = '';
    box.querySelector('.iv__img').src = '';
  }
  box.addEventListener('click', e=>{
    if (e.target === box || e.target.classList.contains('iv__close')) close();
  }, {passive:true});
  document.addEventListener('keydown', e=>{ if (e.key === 'Escape') close(); });

  box.open = src=>{
    const img = box.querySelector('.iv__img');
    img.src = src;
    box.classList.add('open');
    document.body.style.overflow = 'hidden';
  };
  return box;
}

// делегирование клика по фото карточки каталога
document.addEventListener('click', (e)=>{
  const img = e.target.closest('#catalog .product__img img');
  if (!img) return;
  const full = img.getAttribute('data-full') || img.currentSrc || img.src;
  const viewer = __ensureImgViewer();
  viewer.open(full);
}, {passive:true});
// ====== /FULLSIZE IMAGE VIEWER ======

    function focusProductFromQuery(grid){
      if (!grid) return;
      const params = new URLSearchParams(window.location.search);
      const id = parseInt(params.get('product') || '', 10);
      if (!id) return;
      const card = grid.querySelector(`.product[data-id="${id}"]`);
      if (!card) return;
      card.classList.add('product--focus');
      try { card.scrollIntoView({ behavior:'smooth', block:'center' }); } catch(_) {}
    }

    async function renderCatalog(){
      const sect = document.getElementById('catalog');
      const grid = sect && sect.querySelector('.catalog__grid');
      if (!grid) return;

      sect.setAttribute('aria-busy','true');
      try{
        const res = await fetch('/api/products?published=true', {
          credentials: 'include',
          headers: { 'x-dev-auth': '1' }
        });
        if (!res.ok) throw new Error('api');
        const list = await res.json();
        try{ window.__stilvaProducts = list; renderHeroMini(list); }catch(_){}

        grid.innerHTML = list.map(p=>{
          const title   = p.name || 'Товар';
          const price   = +p.price || 0;
          const shelves = +p.shelves || 0;
          const desc    = (p.description || '').trim();
const imgHtml = p.image_url
  ? `<img src="${p.image_url}"
          data-full="${p.image_full_url || p.image_url}"
          alt="${title}"
          style="width:100%;height:100%;object-fit:cover;border-radius:12px;cursor:pointer">`
  : `Фото изделия`;

          const available = Math.max(0, Number(p.available_qty ?? p.stock_qty ?? 0));
          const onOrder = Math.max(0, Number(p.on_order_qty ?? 0));
          const mode = (p.supply_mode || 'stock');
          let statusHtml = '';
          if (mode === 'mixed') {
            if (available > 0) statusHtml += `<span class="pill pill--stock">В наличии: ${available} шт.</span>`;
            if (onOrder > 0) statusHtml += `<span class="pill pill--order">Под заказ: ${onOrder} шт.</span>`;
            if (!statusHtml) statusHtml = `<span class="pill pill--order">Под заказ</span>`;
          } else {
            if (available > 0) statusHtml = `<span class="pill pill--stock">В наличии: ${available} шт.</span>`;
            else statusHtml = `<span class="pill pill--order">Под заказ</span>`;
          }

          return `
            <article class="product" data-id="${p.id}">
              <div class="product__body">
                <div class="product__title">${title}</div>
                <div class="product__status">${statusHtml}</div>
                <div class="product__price">${price.toLocaleString('ru-RU')}&nbsp;₽</div>
                <div class="product__tags">
  ${[
      p.material ? `Материал: ${p.material}` : '',
      p.construction ? `Конструкция: ${p.construction}` : '',
      p.perforation ? `Перфорация: ${p.perforation}` : '',
      p.shelf_thickness ? `Толщ.: ${p.shelf_thickness} мм` : '',
      (p.shelves ? `Полок: ${p.shelves}` : '')
    ]
    .filter(Boolean)
    .map(x => `<span class="tag">${x}</span>`)
    .join('')}
</div>
                <div class="product__actions">
                  <button class="btn btn--lite"
                          data-order
                          data-id="${p.id}"
                          data-name="${title}"
                          data-price="${price}">
                    Заказать
                  </button>
                </div>
              </div>
              <div class="product__img">${imgHtml}</div>
            </article>`;
        }).join('');
        focusProductFromQuery(grid);
      } catch(_) {
        /* тишина — не драматизируем */
      } finally {
        sect.removeAttribute('aria-busy');
      }
    }

    /* мини-карточки в херо: плавающие, с мини-фото и короткими тегами */
    async function renderHeroMini(list){
      try{
        const wrap = document.getElementById('hero-mini');
        if (!wrap) return;
        const src = (Array.isArray(list) ? list : []).filter(p=>p && p.published!==false);
        const pick = src.sort(()=>Math.random()-0.5).slice(0,4);

        // фиксированные слоты, чтобы не наезжали друг на друга
const slots = [
  { left: 14, top: 10, z: 2 },
  { left: 56, top: 12, z: 3 },
  { left: 18, top: 58, z: 1 },
  { left: 60, top: 60, z: 4 }
];
const jitter = () => (Math.random()*2 - 1) | 0;   // ±1 px
const rot    = () => 0;                           // без наклона

        wrap.innerHTML = pick.map((p, i)=>{
          const title = p.name || 'Товар';
          const price = +p.price || 0;
          const img   = p.image_url
            ? `<img class="hero-mini__img" src="${p.image_url}" alt="${title}">`
            : `<div class="hero-mini__img" aria-hidden="true"></div>`;

          const tags = [p.material && `Материал: ${p.material}`,
                        p.construction && `Конструкция: ${p.construction}`]
                        .filter(Boolean).slice(0,2);

          const pos = slots[i] || {left: 10 + i*20, top: 10 + i*20, z: i+1};
          const style = `left:${pos.left}%; top:${pos.top}%; --dx:${jitter()}px; --dy:${jitter()}px; --rot:${rot()}deg; --z:${pos.z}`;

return `
  <div class="hero-mini__card" style="${style}" data-goto="#catalog" data-id="${p.id}">
    ${img}
    <div class="hero-mini__right">
      <div class="hero-mini__title">${title}</div>
      <div class="hero-mini__price">${price.toLocaleString('ru-RU')}&nbsp;₽</div>
      <div class="hero-mini__tags">
        ${tags.map(t=>`<span class="hero-mini__tag">${t}</span>`).join('')}
      </div>
    </div>
  </div>`;
        }).join('');
      }catch(_){}
    }

    function normalizePrice(text){
      if (!text) return 0;
      const n = String(text).replace(/\u00A0/g,' ').replace(/[^\d.,]/g,'').replace(/\s+/g,'').replace(',', '.');
      const v = parseFloat(n); return isNaN(v) ? 0 : v;
    }

    function openCartPanel(){
      try{
        if (window.cart && typeof window.cart.open === 'function') { window.cart.open(); return; }
        const badge = document.querySelector('#cart-badge, [data-cart-open], .js-open-cart, .cart-toggle, .order-open');
        if (badge) { badge.click(); return; }
        const panel = document.querySelector('#order-panel, .order, .cart, [aria-label="Ваш заказ"]');
        if (panel) { panel.style.display='block'; panel.classList.add('open','active','show'); document.body.classList.add('cart-open','order-open'); }
      }catch(_){}
    }

    // совместимость: cart.add → cart.upsert
    if (window.cart && typeof window.cart.add !== 'function' && typeof window.cart.upsert === 'function'){
      window.cart.add = function(item){ return window.cart.upsert(item); };
    }

    // Переход к каталогу по клику по мини-карточке
    document.addEventListener('click', (e)=>{
      const card = e.target.closest('.hero-mini__card');
      if (!card) return;
      const tgt = document.querySelector(card.dataset.goto || '#catalog');
      if (tgt) { tgt.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
    }, {passive:true});

    document.addEventListener('click', function(e){
      const btn = e.target.closest('button, a');
      if (!btn) return;

      const isOrderBtn = btn.hasAttribute('data-order') || /заказать|купить/i.test(btn.textContent||'');
      if (!isOrderBtn) return;

      const card  = btn.closest('article.product, .product, [data-id]');
      if (!card) return;

      const id    = +(btn.dataset.id || card.getAttribute('data-id') || 0);
      const name  = (btn.dataset.name || (card.querySelector('.product__title, [data-name], .title, h3, h4')||{}).textContent || 'Товар').trim();
      const price = btn.dataset.price ? +btn.dataset.price
                                      : normalizePrice((card.querySelector('.product__price, [data-price]')||{}).textContent);

      if (window.cart && (typeof window.cart.add === 'function' || typeof window.cart.upsert === 'function')){
        (window.cart.add || window.cart.upsert).call(window.cart, { id, name, price, qty: 1 });
        openCartPanel();
        e.preventDefault();
      }
    }, { capture:false, passive:false });

    renderCatalog();
  })();



/* ----- Инлайновый скрипт #3: отвечает за логику из соответствующего блока страницы. ----- */

// === interactive tweaks (safe, isolated) ===
(function(){
  if (window.__interactiveTweaks) return;
  window.__interactiveTweaks = true;

  function openCartPanel(){
    try{
      if (window.cart && typeof window.cart.open === 'function') { window.cart.open(); return; }
      var panel = document.querySelector('#order-panel');
      if (!panel) return;
      panel.style.display = 'block';
      panel.getBoundingClientRect();        // force reflow for transition
      panel.classList.add('open');
      document.body.classList.add('order-open');
    }catch(_){}
  }
  window.openCartPanel = openCartPanel;

  // Open on cart badge
  document.addEventListener('click', function(e){
    var badge = e.target.closest && e.target.closest('#cart-badge');
    if (!badge) return;
    e.preventDefault();
    openCartPanel();
  }, { passive: false });

  // Close on "X"
  document.addEventListener('click', function(e){
    var closeBtn = e.target.closest && e.target.closest('#order-close');
    if (!closeBtn) return;
    e.preventDefault();
    var panel = document.getElementById('order-panel');
    if (!panel) return;
    panel.classList.remove('open');
    setTimeout(function(){ panel.style.display = 'none'; }, 360);
  }, { passive: false });
})();



/* ----- Инлайновый скрипт #4: отвечает за логику из соответствующего блока страницы. ----- */

(function(){
  if (window.__miniCardsMotion) return;
  window.__miniCardsMotion = { cards: [], raf: null, t0: 0 };
  var state = window.__miniCardsMotion;

  function loop(ts){
    if (!state.cards.length) { state.raf = null; return; }
    if (!state.t0) state.t0 = ts;
    var t = (ts - state.t0) / 1000;
    for (var i=0;i<state.cards.length;i++){
      var it = state.cards[i];
      if (!it || !it.el) continue;
      if (it.el.dataset.pause === "1"){
        it.el.style.setProperty('--fx','0px');
        it.el.style.setProperty('--fy','0px');
        continue;
      }
      var x = Math.sin(t * it.sx + it.phiX) * it.ax;
      var y = Math.cos(t * it.sy + it.phiY) * it.ay;
      it.el.style.setProperty('--fx', x.toFixed(2) + 'px');
      it.el.style.setProperty('--fy', y.toFixed(2) + 'px');
    }
    state.raf = requestAnimationFrame(loop);
  }

  window.__startMiniFloat = function(root){
    try{
      var mq = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)');
      if (mq && mq.matches) return;
      if (state.raf) cancelAnimationFrame(state.raf);
      state.cards = [];
      state.t0 = 0;
      var scope = root || document;
      var cards = Array.prototype.slice.call(scope.querySelectorAll('.hero-mini__card'));
      for (var i=0;i<cards.length;i++){
        var el = cards[i];
        var ax = 10 + Math.random()*2;       // 2–4 px
        var ay = 10 + Math.random()*3;       // 2–5 px
        var sx = 0.30 + Math.random()*0.25; // 0.30–0.55
        var sy = 0.25 + Math.random()*0.25; // 0.25–0.50
        var phiX = Math.random()*Math.PI*2;
        var phiY = Math.random()*Math.PI*2;
        state.cards.push({ el: el, ax: ax, ay: ay, sx: sx, sy: sy, phiX: phiX, phiY: phiY });
        el.addEventListener('mouseenter', function(e){ e.currentTarget.dataset.pause = "1"; }, {passive:true});
        el.addEventListener('mouseleave', function(e){ e.currentTarget.dataset.pause = "0"; }, {passive:true});
        el.addEventListener('focusin',   function(e){ e.currentTarget.dataset.pause = "1"; });
        el.addEventListener('focusout',  function(e){ e.currentTarget.dataset.pause = "0"; });
      }
      state.raf = requestAnimationFrame(loop);
    }catch(_){}
  };
})();



/* ----- Инлайновый скрипт #5: отвечает за логику из соответствующего блока страницы. ----- */

(function(){
  if (window.__miniFloatBoot) return; window.__miniFloatBoot = 1;
  function start(){
    var wrap = document.getElementById('hero-mini');
    if (!wrap){ setTimeout(start, 200); return; }
    function waitCards(){
      if (wrap.querySelector('.hero-mini__card')){
        if (window.__startMiniFloat) window.__startMiniFloat(wrap);
      } else {
        setTimeout(waitCards, 120);
      }
    }
    waitCards();
    var mo = new MutationObserver(function(){
      if (window.__startMiniFloat) window.__startMiniFloat(wrap);
    });
    mo.observe(wrap, { childList: true, subtree: true });
  }
  if (document.readyState !== 'loading') start();
  else document.addEventListener('DOMContentLoaded', start);
})();



/* ----- Инлайновый скрипт #6: отвечает за логику из соответствующего блока страницы. ----- */

(function(){
  const root = document.documentElement;

  // base anchors (TL and BR) with small random offset
  const base = [
    { x: 18 + Math.random()*6, y: 12 + Math.random()*6 },  // top-left
    { x: 82 + Math.random()*6, y: 78 + Math.random()*6 }   // bottom-right
  ];

  // state with velocities
  let c = [
    { x: base[0].x, y: base[0].y, vx: 0, vy: 0, t: Math.random()*1000 },
    { x: base[1].x, y: base[1].y, vx: 0, vy: 0, t: Math.random()*1000 }
  ];

  function setVars(){
    root.style.setProperty('--cloud1-x', c[0].x.toFixed(2) + '%');
    root.style.setProperty('--cloud1-y', c[0].y.toFixed(2) + '%');
    root.style.setProperty('--cloud2-x', c[1].x.toFixed(2) + '%');
    root.style.setProperty('--cloud2-y', c[1].y.toFixed(2) + '%');
  }
  setVars();

  let mouse = {x: innerWidth/2, y: innerHeight/2, seen:false};

  function step(){
    const w = innerWidth, h = innerHeight;

    for (let i=0;i<2;i++){
      const s = c[i];
      s.t += 0.004 + i*0.001;                       // чуть быстрее базовое время

      // target drifts around its anchor (larger amplitude)
      const ax = base[i].x + Math.sin(s.t*5.9 + i)*5.6 + Math.cos(s.t*5.35 + i)*5.2;
      const ay = base[i].y + Math.cos(s.t*5.8 + i)*5.4 + Math.sin(s.t*5.3 + i)*5.0;

      // spring toward drifting target
      const k = 0.05;                                // пружина (↑ — быстрее возврат)
      s.vx += (ax - s.x) * k;
      s.vy += (ay - s.y) * k;

      // mouse repulsion: stronger and wider
      if(mouse.seen){
        const cx = s.x/100*w, cy = s.y/100*h;
        const dx = cx - mouse.x, dy = cy - mouse.y;
        const dist = Math.hypot(dx, dy) || 1;
        const R = Math.max(w, h) * 0.7;            // радиус влияния
        const infl = Math.max(0, 1 - dist / R);
        const push = 15 * infl*infl;                // сила (квадр. затухание)
        s.vx += (dx/dist) * push * 0.18;
        s.vy += (dy/dist) * push * 0.18;
      }

      // damping (friction)
      s.vx *= 0.90;
      s.vy *= 0.90;

      s.x += s.vx;
      s.y += s.vy;

      // soft bounds
      s.x = Math.max(3, Math.min(97, s.x));
      s.y = Math.max(3, Math.min(97, s.y));
    }

    setVars();
    requestAnimationFrame(step);
  }

  addEventListener('mousemove', e=>{
    mouse = {x:e.clientX, y:e.clientY, seen:true};
  }, {passive:true});

  // start
  if (document.readyState !== 'loading') requestAnimationFrame(step);
  else document.addEventListener('DOMContentLoaded', ()=>requestAnimationFrame(step));
})();
