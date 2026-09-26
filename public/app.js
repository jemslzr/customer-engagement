(() => {
  'use strict';

  // ── State ──────────────────────────────────────────────────────────────────
  let totalEventsCount = 38;
  let epsWindow = [];
  let currentSessions = [];
  let currentForecasts = [];
  let selectedModalSession = null;

  // ── DOM References ─────────────────────────────────────────────────────────
  const $ = id => document.getElementById(id);

  const $statActive    = $('stat-active-visitors');
  const $statAtRisk    = $('stat-at-risk-count');
  const $statSaved     = $('stat-saved-revenue');
  const $statEps       = $('stat-events-sec');
  const $statTotal     = $('stat-total-events');

  const $custTbody     = $('customers-tbody');
  const $regionsTbody  = $('regions-tbody');
  const $activityFeed  = $('activity-feed');
  const $toastBox      = $('toast-container');
  const $custSearch    = $('cust-search');
  const $btnExportCsv  = $('btn-export-csv');

  // Modal elements
  const $modal         = $('rescue-modal');
  const $modalName     = $('modal-cust-name');
  const $modalMeta     = $('modal-cust-meta');
  const $modalRisk     = $('modal-risk-score');
  const $modalMrr      = $('modal-mrr');
  const $modalLastEvt  = $('modal-last-event');
  const $modalTrail    = $('modal-trail');
  const $btnConfirm    = $('btn-modal-confirm-rescue');

  // Test Buttons
  const $btnAbandon    = $('btn-test-abandon');
  const $btnSupport    = $('btn-test-support');
  const $btnPurchase   = $('btn-test-purchase');

  // ── Simple Tab Switching ───────────────────────────────────────────────────
  const navButtons = document.querySelectorAll('.nav-btn[data-view]');
  const viewPanes  = document.querySelectorAll('.view-content');

  function switchView(viewName) {
    navButtons.forEach(btn => {
      btn.classList.toggle('active', btn.dataset.view === viewName);
    });

    viewPanes.forEach(pane => {
      pane.style.display = (pane.id === `view-${viewName}`) ? 'block' : 'none';
    });

    if (viewName === 'dashboard') {
      loadSessions();
      loadAnalytics();
    }
  }

  navButtons.forEach(btn => {
    btn.addEventListener('click', () => switchView(btn.dataset.view));
  });

  // ── WebSocket Connection ───────────────────────────────────────────────────
  let ws;
  function connect() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(`${proto}://${location.host}`);

    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.type === 'init') {
          handleInit(msg);
        } else if (msg.type === 'live_event') {
          handleLiveEvent(msg);
        } else if (msg.type === 'window') {
          handleWindow(msg);
        } else if (msg.type === 'playbook_result') {
          handlePlaybookResult(msg);
        }
      } catch (err) {
        console.error('WebSocket parse error', err);
      }
    };

    ws.onclose = () => setTimeout(connect, 2000);
  }
  connect();

  function handleInit(msg) {
    if (msg.savedMrrTotal) {
      $statSaved.textContent = `$${msg.savedMrrTotal.toLocaleString()}`;
    }
    if (msg.forecasts) {
      currentForecasts = msg.forecasts;
      renderRegions(currentForecasts);
    }
    if (msg.recentEvents) {
      msg.recentEvents.forEach(evt => appendActivity(evt));
    }
    loadSessions();
    loadAnalytics();
  }

  function handleLiveEvent(msg) {
    totalEventsCount++;
    epsWindow.push(Date.now());
    appendActivity(msg.event);

    if (['checkout_abandon', 'support_ticket_created'].includes(msg.event.event)) {
      showToast(`⚠️ Customer friction: ${msg.event.event} on ${msg.event.path}`);
    }

    loadSessions();
  }

  function handleWindow(msg) {
    if (msg.savedMrrTotal) {
      $statSaved.textContent = `$${msg.savedMrrTotal.toLocaleString()}`;
    }
    if (msg.forecasts) {
      currentForecasts = msg.forecasts;
      renderRegions(currentForecasts);
    }
    $statActive.textContent = msg.activeUsers || 6;
    $statAtRisk.textContent = msg.atRisk || 2;
  }

  function handlePlaybookResult(msg) {
    if (msg.savedMrrTotal) {
      $statSaved.textContent = `$${msg.savedMrrTotal.toLocaleString()}`;
    }
    showToast(`✅ Rescue Offer Sent! Saved $${msg.preservedAmount.toLocaleString()} revenue.`);
    loadSessions();
  }

  // ── EPS Calculator ─────────────────────────────────────────────────────────
  setInterval(() => {
    const now = Date.now();
    epsWindow = epsWindow.filter(t => now - t < 3000);
    const eps = Math.round((epsWindow.length / 3) * 10) / 10;
    $statEps.innerHTML = `${eps.toFixed(1)} <span class="text-xs text-muted" style="font-size:0.8rem">events/s</span>`;
    $statTotal.textContent = totalEventsCount;
  }, 400);

  // ── Activity Feed Item ─────────────────────────────────────────────────────
  function appendActivity(evt) {
    if (!$activityFeed) return;
    const item = document.createElement('div');
    item.className = 'activity-item';

    const isFriction = ['checkout_abandon', 'support_ticket_created', 'rage_click'].includes(evt.event);
    const dotClass = isFriction ? 'friction' : 'normal';
    const time = new Date(evt.ts).toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });

    let actionLabel = `viewed ${evt.path || '/'}`;
    if (evt.event === 'checkout_abandon') actionLabel = `abandoned checkout on ${evt.path || '/checkout'}`;
    else if (evt.event === 'support_ticket_created') actionLabel = `filed frustrated support ticket`;
    else if (evt.event === 'add_to_cart') actionLabel = `added product to cart`;
    else if (evt.event === 'purchase_success') actionLabel = `completed purchase!`;
    else if (evt.event === 'plan_selected') actionLabel = `selected subscription plan`;

    item.innerHTML = `
      <div class="activity-dot ${dotClass}"></div>
      <div class="activity-content">
        <div class="activity-user">${evt.userId || 'Guest Visitor'}</div>
        <div class="activity-event">${actionLabel} <span class="text-xs text-muted">&bull; ${evt.regionLabel || evt.regionid}</span></div>
      </div>
      <div class="activity-time">${time}</div>
    `;

    $activityFeed.prepend(item);
    while ($activityFeed.children.length > 50) {
      $activityFeed.removeChild($activityFeed.lastChild);
    }
  }

  // ── Sessions & At-Risk Customers ───────────────────────────────────────────
  async function loadSessions() {
    try {
      const res = await fetch('/api/v1/sessions');
      currentSessions = await res.json();
      renderCustomers(currentSessions);

      const atRiskCount = currentSessions.filter(s => s.riskScore >= 50 && !s.playbookApplied).length;
      $statAtRisk.textContent = atRiskCount;
      $statActive.textContent = currentSessions.length;
    } catch (e) {
      console.warn('Failed to load sessions', e);
    }
  }

  function renderCustomers(sessions) {
    if (!$custTbody) return;
    $custTbody.innerHTML = '';

    const query = ($custSearch.value || '').toLowerCase().trim();
    const filtered = sessions.filter(s => {
      if (!query) return true;
      const haystack = `${s.accountName} ${s.userId} ${s.sessionId} ${s.regionLabel}`.toLowerCase();
      return haystack.includes(query);
    });

    if (!filtered.length) {
      $custTbody.innerHTML = `<tr><td colspan="6" class="text-muted" style="text-align:center;padding:24px">No active visitors matching "${query}".</td></tr>`;
      return;
    }

    filtered.forEach(s => {
      const tr = document.createElement('tr');
      tr.className = 'row-clickable';
      const isAtRisk = s.riskScore >= 50 && !s.playbookApplied;

      let reason = 'Healthy browsing flow';
      if (s.playbookApplied) reason = '✅ Rescue offer applied';
      else if (s.lastEvent === 'checkout_abandon') reason = '⚠️ Left items on checkout page';
      else if (s.lastEvent === 'support_ticket_created') reason = '⚠️ Filed urgent support ticket';
      else if (s.idleSeconds > 40) reason = `⚠️ Idle for ${s.idleSeconds}s without activity`;

      let statusBadge = `<span class="badge-status healthy">✅ Active</span>`;
      if (s.playbookApplied) {
        statusBadge = `<span class="badge-status rescued">✨ Rescued</span>`;
      } else if (isAtRisk) {
        statusBadge = `<span class="badge-status at-risk">⚠️ At-Risk</span>`;
      }

      const actionBtn = isAtRisk
        ? `<button class="btn btn-primary btn-sm" onclick="event.stopPropagation(); window.CustomerPulse.openRescueModal('${s.sessionId}')">Send Discount Offer</button>`
        : `<button class="btn btn-secondary btn-sm" onclick="event.stopPropagation(); window.CustomerPulse.openRescueModal('${s.sessionId}')">Engage</button>`;

      const currentPage = (s.pages && s.pages.length) ? s.pages[s.pages.length - 1] : '/';

      tr.innerHTML = `
        <td>
          <strong>${s.accountName || s.userId}</strong>
          <div class="text-xs text-muted mono">${s.regionLabel || s.regionid}</div>
        </td>
        <td><code>${currentPage}</code></td>
        <td class="r">$${s.mrr || 499}/mo</td>
        <td>${statusBadge}</td>
        <td class="text-sm">${reason}</td>
        <td class="r">${actionBtn}</td>
      `;

      tr.addEventListener('click', () => {
        window.CustomerPulse.openRescueModal(s.sessionId);
      });

      $custTbody.appendChild(tr);
    });
  }

  $custSearch.addEventListener('input', () => renderCustomers(currentSessions));

  // CSV Export
  $btnExportCsv.addEventListener('click', () => {
    window.location.href = '/api/export?format=csv';
  });

  // ── Regional Trends ────────────────────────────────────────────────────────
  function renderRegions(forecasts) {
    if (!$regionsTbody) return;
    $regionsTbody.innerHTML = '';

    forecasts.forEach(f => {
      const tr = document.createElement('tr');
      const cur = f.current != null ? f.current : 18;
      const pred = f.predicted != null ? f.predicted : 19;
      const chg = f.changePercent || 0;

      let trendBadge = `<span class="text-muted mono">→ Steady</span>`;
      if (chg > 5) trendBadge = `<span class="text-emerald mono font-bold">↑ +${chg}% Growing</span>`;
      else if (chg < -5) trendBadge = `<span class="text-rose mono font-bold">↓ ${chg}% Declining</span>`;

      tr.innerHTML = `
        <td><strong>${f.regionLabel || f.regionid}</strong></td>
        <td class="r">${cur} visits</td>
        <td class="r">${pred} predicted</td>
        <td class="r">${trendBadge}</td>
        <td class="r">
          <button class="btn btn-secondary btn-sm" onclick="window.CustomerPulse.sendRescue(null, '${f.regionid}')">
            Send Region Offer
          </button>
        </td>
      `;
      $regionsTbody.appendChild(tr);
    });
  }

  // ── Analytics ──────────────────────────────────────────────────────────────
  async function loadAnalytics() {
    try {
      const res = await fetch('/api/v1/analytics/realtime');
      const data = await res.json();
      if (data.savedMrrTotal) $statSaved.textContent = `$${data.savedMrrTotal.toLocaleString()}`;
      if (data.forecasts) {
        currentForecasts = data.forecasts;
        renderRegions(currentForecasts);
      }
    } catch (e) {}
  }

  // ── Interactive Quick Test Buttons ─────────────────────────────────────────
  $btnAbandon.addEventListener('click', async () => {
    try {
      await fetch('/api/v1/track', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          event: 'checkout_abandon',
          userId: 'Sarah Connor (Acme Corp)',
          path: '/checkout/payment',
          regionid: 'Region_1',
          durationMs: 45000,
          properties: { cartValue: 1999, tier: 'Enterprise ($1,999/mo)' }
        })
      });
      showToast('🚨 Simulated: Customer abandoned $1,999 cart!');
    } catch (e) {}
  });

  $btnSupport.addEventListener('click', async () => {
    try {
      await fetch('/api/v1/track', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          event: 'support_ticket_created',
          userId: 'David Miller',
          path: '/support/ticket',
          regionid: 'Region_2',
          durationMs: 22000,
          properties: { issue: 'Payment checkout failed' }
        })
      });
      showToast('⚠️ Simulated: Customer filed support issue!');
    } catch (e) {}
  });

  $btnPurchase.addEventListener('click', async () => {
    try {
      await fetch('/api/v1/track', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          event: 'purchase_success',
          userId: 'Elena Rostova',
          path: '/checkout/success',
          regionid: 'Region_3',
          durationMs: 15000,
          properties: { orderTotal: 3499 }
        })
      });
      showToast('✅ Simulated: Customer made purchase ($3,499)!');
    } catch (e) {}
  });

  // Modal interaction logic
  document.querySelectorAll('.offer-option').forEach(opt => {
    opt.addEventListener('click', () => {
      document.querySelectorAll('.offer-option').forEach(o => o.classList.remove('selected'));
      opt.classList.add('selected');
      const radio = opt.querySelector('input[type="radio"]');
      if (radio) radio.checked = true;
    });
  });

  $btnConfirm.addEventListener('click', async () => {
    if (!selectedModalSession) return;
    const selectedOffer = document.querySelector('input[name="rescue_offer"]:checked')?.value || '15_percent';
    window.CustomerPulse.closeModal();

    try {
      const res = await fetch('/api/v1/playbooks/dispatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          playbookId: selectedOffer,
          sessionId: selectedModalSession.sessionId
        })
      });
      const data = await res.json();
      showToast(`✅ Dispatched offer to ${selectedModalSession.accountName}! Preserved $${data.preservedAmount.toLocaleString()} revenue.`);
      loadSessions();
    } catch (err) {
      showToast(`Failed: ${err.message}`);
    }
  });

  // ── Toast Utility ──────────────────────────────────────────────────────────
  function showToast(msg) {
    if (!$toastBox) return;
    const t = document.createElement('div');
    t.className = 'toast';
    t.textContent = msg;
    $toastBox.appendChild(t);
    setTimeout(() => t.remove(), 4000);
  }

  // ── Public API ─────────────────────────────────────────────────────────────
  const CustomerPulseAPI = {
    refreshData: () => {
      loadSessions();
      loadAnalytics();
      showToast('Data refreshed.');
    },
    openRescueModal: (sessionId) => {
      const s = currentSessions.find(x => x.sessionId === sessionId);
      if (!s) return;
      selectedModalSession = s;

      $modalName.textContent = s.accountName || s.userId;
      $modalMeta.textContent = `${s.tier || 'Starter ($99/mo)'} • ${s.regionLabel || s.regionid}`;
      $modalRisk.textContent = `${s.riskScore}% Risk`;
      $modalMrr.textContent = `$${s.mrr || 499}/mo`;
      $modalLastEvt.textContent = s.lastEvent || 'pageview';

      $modalTrail.innerHTML = '';
      (s.pages || ['/']).forEach(p => {
        const span = document.createElement('span');
        span.className = 'trail-step';
        span.textContent = p;
        $modalTrail.appendChild(span);
      });

      $modal.style.display = 'flex';
    },
    closeModal: () => {
      $modal.style.display = 'none';
      selectedModalSession = null;
    },
    sendRescue: async (sessionId, regionid) => {
      try {
        const res = await fetch('/api/v1/playbooks/dispatch', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            playbookId: 'rescue_offer',
            sessionId: sessionId || undefined,
            targetRegion: regionid || undefined
          })
        });
        const data = await res.json();
        showToast(`✅ Rescue Offer Sent! Preserved $${data.preservedAmount.toLocaleString()} revenue.`);
        loadSessions();
      } catch (err) {
        showToast(`Failed to send rescue: ${err.message}`);
      }
    }
  };

  window.CustomerPulse = CustomerPulseAPI;
  window.StreamPulse = CustomerPulseAPI;

  // Initial load
  loadSessions();
  loadAnalytics();

})();
