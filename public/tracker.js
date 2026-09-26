/**
 * CustomerPulse Analytics & Churn Telemetry Tracker SDK
 * Embeddable script for websites and web apps.
 * Usage: <script src="http://localhost:3000/tracker.js" data-site-id="cp_store_01"></script>
 */
(function(window, document) {
  'use strict';

  // Config & Session
  const scriptTag = document.currentScript || document.querySelector('script[src*="tracker.js"]');
  const siteId = scriptTag ? scriptTag.getAttribute('data-site-id') || 'cp_default' : 'cp_default';
  const endpoint = (scriptTag ? scriptTag.getAttribute('data-endpoint') : null) || '/api/v1/track';

  let sessionId = sessionStorage.getItem('cp_sid');
  if (!sessionId) {
    sessionId = 'sess_' + Math.random().toString(36).substring(2, 10) + '_' + Date.now().toString(36);
    sessionStorage.setItem('cp_sid', sessionId);
  }

  let identifiedUserId = localStorage.getItem('cp_uid') || null;
  let userTraits = {};
  try {
    userTraits = JSON.parse(localStorage.getItem('cp_traits') || '{}');
  } catch(e) {}

  // Region detection from timezone / language
  function detectRegion() {
    const tz = (Intl.DateTimeFormat().resolvedOptions().timeZone || '').toLowerCase();
    if (tz.includes('america') || tz.includes('new_york') || tz.includes('chicago') || tz.includes('los_angeles')) return 'Region_1'; // North America
    if (tz.includes('europe/london') || tz.includes('europe/paris') || tz.includes('europe/berlin')) return 'Region_2'; // Europe West
    if (tz.includes('europe')) return 'Region_3'; // Europe North/East
    if (tz.includes('tokyo') || tz.includes('asia/seoul') || tz.includes('singapore') || tz.includes('hong_kong') || tz.includes('shanghai')) return 'Region_4'; // Asia Pacific
    if (tz.includes('sao_paulo') || tz.includes('buenos_aires') || tz.includes('santiago')) return 'Region_5'; // Latin America
    if (tz.includes('dubai') || tz.includes('riyadh')) return 'Region_6'; // Middle East
    if (tz.includes('calcutta') || tz.includes('kolkata')) return 'Region_7'; // South Asia
    if (tz.includes('africa') || tz.includes('cairo') || tz.includes('johannesburg')) return 'Region_8'; // Africa
    if (tz.includes('sydney') || tz.includes('melbourne') || tz.includes('auckland')) return 'Region_9'; // Oceania
    return 'Region_1';
  }

  const detectedRegion = detectRegion();
  const startTime = Date.now();
  let lastActive = Date.now();

  function send(eventName, properties = {}) {
    const payload = {
      siteId,
      sessionId,
      userId: identifiedUserId || ('anon_' + sessionId.substring(5, 11)),
      event: eventName,
      path: window.location.pathname || '/',
      url: window.location.href,
      referrer: document.referrer || '',
      title: document.title,
      regionid: properties.regionid || detectedRegion,
      durationMs: Math.max(0, Date.now() - startTime),
      idleMs: Math.max(0, Date.now() - lastActive),
      timestamp: new Date().toISOString(),
      properties: Object.assign({}, userTraits, properties)
    };

    const bodyStr = JSON.stringify(payload);

    if (navigator.sendBeacon) {
      try {
        const blob = new Blob([bodyStr], { type: 'application/json' });
        if (navigator.sendBeacon(endpoint, blob)) return;
      } catch (err) {}
    }

    fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: bodyStr,
      keepalive: true
    }).catch(function(err) {
      console.warn('[CustomerPulse Tracker] Ingestion failed', err);
    });
  }

  // Update activity timestamp on user interactions
  ['click', 'scroll', 'keydown', 'mousemove', 'touchstart'].forEach(function(evt) {
    window.addEventListener(evt, function() {
      lastActive = Date.now();
    }, { passive: true });
  });

  // Track initial pageview
  send('pageview', {
    screen: `${window.screen.width}x${window.screen.height}`,
    viewport: `${window.innerWidth}x${window.innerHeight}`
  });

  // Public SDK instance
  const CustomerPulse = {
    track: function(eventName, properties) {
      send(eventName, properties || {});
    },
    identify: function(userId, traits) {
      identifiedUserId = userId;
      userTraits = traits || {};
      localStorage.setItem('cp_uid', userId);
      localStorage.setItem('cp_traits', JSON.stringify(userTraits));
      send('identify', { traits: userTraits });
    },
    page: function(path, properties) {
      send('pageview', Object.assign({ path: path || window.location.pathname }, properties));
    },
    getSessionId: function() {
      return sessionId;
    },
    getRegion: function() {
      return detectedRegion;
    }
  };

  window.CustomerPulse = CustomerPulse;
  window.StreamPulse = CustomerPulse; // Backward compatibility alias
})(window, document);
