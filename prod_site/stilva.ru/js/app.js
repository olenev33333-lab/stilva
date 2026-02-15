/* ================================
   STILVA — Корзина и заказ (app.js)
   Ванильный JS. Теперь со спиннером при отправке.
   ================================ */

(function(){
  const LS_KEY = 'cart';

  function $id(id){ return document.getElementById(id); }
  function $(sel, root){ return (root||document).querySelector(sel); }
  function $all(sel, root){ return Array.from((root||document).querySelectorAll(sel)); }

  const cart = {
    _items: [],
    _loaded: false,

    load(){
      if (this._loaded) return;
      try {
        const raw = localStorage.getItem(LS_KEY);
        this._items = Array.isArray(JSON.parse(raw)) ? JSON.parse(raw) : [];
      } catch(_) { this._items = []; }
      this._loaded = true;
    },

    save(){
      try { localStorage.setItem(LS_KEY, JSON.stringify(this._items)); } catch(_){}
      this.render();
    },

    list(){ this.load(); return this._items.map(x => ({...x})); },
    findIndex(id){ this.load(); return this._items.findIndex(it => String(it.id) === String(id)); },

    upsert(item){
      this.load();
      const id    = item.id ?? '';
      const name  = (item.name ?? 'Товар') + '';
      const price = Number(item.price) || 0;
      const qty   = Number(item.qty) || 1;

      const i = this.findIndex(id);
      if (i >= 0) this._items[i].qty = Number(this._items[i].qty || 0) + qty;
      else       this._items.push({ id: String(id), name, price, qty });

      this.save();
    },
    add(item){ return this.upsert(item); },

    dec(id){ const i=this.findIndex(id); if(i>=0){ this._items[i].qty=(+this._items[i].qty||1)-1; if(this._items[i].qty<=0)this._items.splice(i,1); this.save(); } },
    inc(id){ const i=this.findIndex(id); if(i>=0){ this._items[i].qty=(+this._items[i].qty||0)+1; this.save(); } },
    remove(id){ const i=this.findIndex(id); if(i>=0){ this._items.splice(i,1); this.save(); } },
    clear(){ this._items=[]; this._loaded=true; this.save(); },

    total(){ this.load(); return this._items.reduce((s,it)=>s+(+it.price||0)*(+it.qty||1),0); },
    count(){ this.load(); return this._items.reduce((s,it)=>s+(+it.qty||0),0); },

    open(){
      const panel = $('#order-panel, .order, .cart, [aria-label="Ваш заказ"]');
      if (panel){
        panel.style.display='block';
        panel.classList.add('open','active','show');
        document.body.classList.add('cart-open','order-open');
      }
      this.render();
    },
    close(){
      const panel = $('#order-panel, .order, .cart, [aria-label="Ваш заказ"]');
      if (panel){
        panel.classList.remove('open','active','show');
        panel.style.display='none';
        document.body.classList.remove('cart-open','order-open');
      }
    },

    render(){
      const badge = $id('cart-badge');
      const countEl = $id('cart-count');
      if (badge && countEl) countEl.textContent = this.count().toLocaleString('ru-RU');

      const linesWrap = $id('cart-lines');
      const totalEl   = $id('cart-total');
      if (!linesWrap || !totalEl) return;

      const rows = this.list().map(it => `
        <div class="cart-line" data-id="${it.id}"
             style="display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center;
                    border:1px solid #eee;padding:10px;border-radius:12px">
          <div class="cart-line__info" style="min-width:0">
            <div class="cart-line__name" style="font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">
              ${escapeHtml(it.name)}
            </div>
            <div class="cart-line__unit" style="opacity:.75;font-size:13px;white-space:nowrap">
              ${(Number(it.price)||0).toLocaleString('ru-RU')}&nbsp;₽ за&nbsp;шт.
            </div>
          </div>
          <div class="cart-line__actions" style="display:flex;gap:8px;align-items:center;justify-content:flex-end;flex-wrap:nowrap">
            <div class="cart-line__qty" style="display:flex;gap:6px;align-items:center">
              <button class="qty-dec" aria-label="Минус"
                      style="border:1px solid #ddd;border-radius:8px;background:#fff;padding:4px 8px;cursor:pointer">-</button>
              <span class="qty-val" style="min-width:18px;text-align:center">${Number(it.qty)}</span>
              <button class="qty-inc" aria-label="Плюс"
                      style="border:1px solid #ddd;border-radius:8px;background:#fff;padding:4px 8px;cursor:pointer">+</button>
            </div>
            <button class="line-del"
                    style="background:#fee2e2;border:1px solid #fca5a5;color:#991b1b;border-radius:8px;padding:6px 10px;cursor:pointer">
              Удалить
            </button>
          </div>
        </div>`).join('');

      linesWrap.innerHTML = rows || `<div style="opacity:.6">Корзина пуста</div>`;
      totalEl.textContent = this.total().toLocaleString('ru-RU');
    }
  };

  function escapeHtml(s){
    return String(s).replace(/[&<>"']/g, ch =>
      ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[ch]
    );
  }

  function track(eventName, params){
    try{
      if (typeof window.stilvaTrack === 'function') {
        window.stilvaTrack(eventName, params || {});
      }
    }catch(_){}
  }

  // Экспорт
  window.cart = cart;

  // Бейдж → открыть
  document.addEventListener('click', (e)=>{
    const btn = e.target.closest('#cart-badge, [data-cart-open], .js-open-cart, .cart-toggle');
    if (!btn) return;
    e.preventDefault();
    track('begin_checkout', { items_count: cart.count(), value: cart.total() });
    cart.open();
  });

  // Закрыть
  document.addEventListener('click', (e)=>{
    const btn = e.target.closest('#order-close, .cart-close, .order__close');
    if (!btn) return;
    e.preventDefault();
    cart.close();
  });

  // Делегирование на qty/delete
  document.addEventListener('click', (e)=>{
    const line = e.target.closest('#cart-lines .cart-line');
    if (!line) return;
    const id = line.getAttribute('data-id');

    if (e.target.closest('.qty-dec')) { e.preventDefault(); cart.dec(id); }
    else if (e.target.closest('.qty-inc')) { e.preventDefault(); cart.inc(id); }
    else if (e.target.closest('.line-del')){ e.preventDefault(); cart.remove(id); }
  });

  // sync по storage
  window.addEventListener('storage', (ev)=>{
    if (ev.key === LS_KEY) {
      cart._loaded = false;
      cart.load();
      cart.render();
    }
  });

  // Первый рендер
  cart.load();
  cart.render();

  document.addEventListener('click', (e)=>{
    const link = e.target.closest('a[href]');
    if (!link) return;
    const href = String(link.getAttribute('href') || '');
    if (!href) return;
    if (href.startsWith('tel:')) {
      track('phone_click', { href });
    } else if (href.startsWith('mailto:')) {
      track('email_click', { href });
    } else if (href.includes('t.me/') || href.includes('wa.me/')) {
      track('messenger_click', { href });
    }
  });

  /* ====== Отправка заказа с индикатором ====== */

  function setFormBusy(form, busy){
    const submit = $id('order-submit');
    const progress = $id('order-progress');

    // лочим/разлочим поля
    $all('input, textarea, button', form).forEach(el => { el.disabled = !!busy; });

    // визуал
    if (progress) progress.style.display = busy ? 'flex' : 'none';
    if (submit)  submit.style.opacity   = busy ? '.7'  : '1';
  }

  document.addEventListener('submit', async (e) => {
    const form = e.target.closest('#checkout-form');
    if (!form) return;
    e.preventDefault();

    const name  = form.elements.name?.value?.trim()  || '';
    const phone = form.elements.phone?.value?.trim() || '';
    const email = form.elements.email?.value?.trim() || '';
    const note  = form.elements.note?.value?.trim()  || '';

    const items = cart.list();
    const total = items.reduce((s, it) => s + (Number(it.price)||0)*(Number(it.qty)||1), 0);

    const payload = {
      customer_name: name,
      phone,
      email,
      note,
      items: items.map(it => ({
        id: String(it.id ?? ''),
        name: String(it.name ?? 'Товар'),
        price: Number(it.price) || 0,
        qty: Number(it.qty) || 1
      })),
      total
    };
    track('submit_order', { items_count: items.length, value: total });

    const okEl  = $id('order-success');
    const errEl = $id('order-error');
    if (okEl)  { okEl.style.display = 'none'; okEl.textContent = ''; }
    if (errEl) { errEl.style.display = 'none'; errEl.textContent = ''; }

    // показываем прогресс и лочим
    setFormBusy(form, true);

    try {
      const res = await fetch(`/api/orders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-dev-auth': '1' },
        credentials: 'include',
        body: JSON.stringify(payload)
      });

      if (!res.ok) throw new Error('HTTP '+res.status);

      // успех
      cart.clear();
      form.reset();
      track('order_success', { items_count: items.length, value: total });
      if (okEl)  { okEl.textContent = 'Заявка отправлена! Мы свяжемся с Вами сегодня, в ближайшее время'; okEl.style.display = 'block'; }
    } catch (err) {
      track('order_error', { items_count: items.length, value: total });
      if (errEl) {
        errEl.textContent = 'Не удалось отправить заказ. Сервер недоступен или отклонил запрос.';
        errEl.style.display = 'block';
      }
    } finally {
      setFormBusy(form, false);
      cart.render(); // обновить бейдж/панель
    }
  });

})();

