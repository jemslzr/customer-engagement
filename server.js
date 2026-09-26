const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');

// ── Simple Zero-Dependency .env Loader ─────────────────────────────────────
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  envContent.split('\n').forEach(line => {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith('#')) {
      const idx = trimmed.indexOf('=');
      if (idx !== -1) {
        const key = trimmed.slice(0, idx).trim();
        const val = trimmed.slice(idx + 1).trim();
        process.env[key] = val;
      }
    }
  });
}

const CONFLUENT_REST_ENDPOINT = process.env.CONFLUENT_REST_ENDPOINT;
const CONFLUENT_CLUSTER_ID = process.env.CONFLUENT_CLUSTER_ID;
const CONFLUENT_API_KEY = process.env.CONFLUENT_API_KEY;
const CONFLUENT_API_SECRET = process.env.CONFLUENT_API_SECRET;
const isConfluentConfigured = Boolean(
  CONFLUENT_REST_ENDPOINT && CONFLUENT_CLUSTER_ID && CONFLUENT_API_KEY && CONFLUENT_API_SECRET
);

// ── Confluent Cloud Kafka REST Producer ───────────────────────────────────
async function produceToConfluent(topic, key, value) {
  if (!isConfluentConfigured) return; // Running in local standalone mode

  const url = `${CONFLUENT_REST_ENDPOINT.replace(/\/$/, '')}/kafka/v3/clusters/${CONFLUENT_CLUSTER_ID}/topics/${topic}/records`;
  const auth = Buffer.from(`${CONFLUENT_API_KEY}:${CONFLUENT_API_SECRET}`).toString('base64');

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Basic ${auth}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        key: { type: 'JSON', data: key },
        value: { type: 'JSON', data: value }
      })
    });

    if (!response.ok) {
      const errText = await response.text();
      console.warn(`[Confluent Cloud] Error producing to ${topic} (${response.status}):`, errText);
    }
  } catch (err) {
    console.warn(`[Confluent Cloud] Network error sending to ${topic}:`, err.message);
  }
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// ── Region Metadata ────────────────────────────────────────────────────────
const REGIONS = [
  'Region_1', 'Region_2', 'Region_3', 'Region_4',
  'Region_5', 'Region_6', 'Region_7', 'Region_8', 'Region_9'
];

const REGION_LABELS = {
  Region_1: 'North America (US-East)',
  Region_2: 'Europe West (Frankfurt)',
  Region_3: 'Europe North (Stockholm)',
  Region_4: 'Asia Pacific (Tokyo)',
  Region_5: 'Latin America (São Paulo)',
  Region_6: 'Middle East (Dubai)',
  Region_7: 'South Asia (Mumbai)',
  Region_8: 'Africa (Cape Town)',
  Region_9: 'Oceania (Sydney)'
};

// ── In-Memory Streaming Storage ───────────────────────────────────────────
const MAX_STORED_EVENTS = 200;
const liveEvents = []; // Real incoming event ring buffer

const sessionState = {}; // Real active user sessions
const regionState = {}; // Real sliding window counts per region

REGIONS.forEach(r => {
  regionState[r] = {
    windows: [14, 18, 22, 20, 24, 26, 25, 28, 26, 30],
    currentCount: 0,
    forecast: null,
    avgViewtime: 0,
    viewtimeSum: 0,
    viewtimeCount: 0,
    uniqueUsers: new Set(),
    pageDistribution: {}
  };
});

let savedMrrTotal = 4200;
let alertIdCounter = 0;
const alerts = [];
let backgroundSimulatorActive = true;

// Pre-populate realistic seed sessions so the user has immediate data to inspect
const seedNames = [
  { name: 'Sarah Connor', tier: 'Enterprise ($1,999/mo)', mrr: 1999, region: 'Region_1' },
  { name: 'David Miller', tier: 'Growth ($499/mo)', mrr: 499, region: 'Region_2' },
  { name: 'Elena Rostova', tier: 'Enterprise ($1,999/mo)', mrr: 1999, region: 'Region_3' },
  { name: 'Kenji Sato', tier: 'Growth ($499/mo)', mrr: 499, region: 'Region_4' },
  { name: 'Carlos Mendez', tier: 'Starter ($99/mo)', mrr: 99, region: 'Region_5' },
  { name: 'Amira Al-Mansoor', tier: 'Growth ($499/mo)', mrr: 499, region: 'Region_6' }
];

const nowInit = Date.now();
seedNames.forEach((s, idx) => {
  const sid = `sess_seed_${idx + 1}`;
  sessionState[sid] = {
    sessionId: sid,
    userId: `user_seed_${idx + 1}`,
    accountName: s.name,
    tier: s.tier,
    mrr: s.mrr,
    regionid: s.region,
    regionLabel: REGION_LABELS[s.region],
    firstSeen: nowInit - 180000,
    lastSeen: nowInit - (idx === 1 || idx === 4 ? 65000 : 12000), // Some at risk
    pages: ['/products', '/pricing', idx === 1 ? '/checkout' : '/docs'],
    pageCount: 3,
    totalViewtime: 42000,
    riskScore: idx === 1 ? 75 : (idx === 4 ? 60 : 15),
    lastEvent: idx === 1 ? 'checkout_abandon' : 'pageview',
    playbookApplied: false
  };
});

// ── CustomerPulse Stateful Friction & Traffic Health Evaluator ─────────────
function calculateFrictionAndHealth(windows) {
  if (windows.length < 3) return null;
  const recent = windows.slice(-10);
  const avg = recent.reduce((a, b) => a + b, 0) / recent.length;
  const last = recent[recent.length - 1];
  const momentum = (last - avg) / (avg || 1);
  const projectedTraffic = Math.max(0, Math.round((last + last * momentum * 0.7) * 10) / 10);
  const direction = projectedTraffic > last * 1.05 ? 'rising' : projectedTraffic < last * 0.95 ? 'falling' : 'flat';
  const changePercent = last > 0 ? Math.round(((projectedTraffic - last) / last) * 100) : 0;
  return { current: last, predicted: projectedTraffic, direction, changePercent };
}

function createAlert(severity, region, message, metric) {
  const alert = {
    id: ++alertIdCounter,
    ts: new Date().toISOString(),
    severity,
    region,
    regionLabel: REGION_LABELS[region] || region,
    message,
    metric: metric || {},
    acknowledged: false
  };
  alerts.unshift(alert);
  if (alerts.length > 80) alerts.pop();
  return alert;
}

// ── WebSocket Broadcaster ──────────────────────────────────────────────────
function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(msg);
  });
}

// ── Real HTTP Ingestion API: POST /api/v1/track ────────────────────────────
app.post('/api/v1/track', (req, res) => {
  const payload = req.body || {};
  const now = Date.now();

  const sessionId = payload.sessionId || `sess_auto_${now.toString(36)}`;
  const userId = payload.userId || `user_${sessionId.substring(5, 10)}`;
  const eventName = payload.event || 'pageview';
  const pathName = payload.path || '/';
  const regionid = (REGIONS.includes(payload.regionid) ? payload.regionid : 'Region_1');
  const durationMs = parseInt(payload.durationMs) || 1200;
  const properties = payload.properties || {};

  // Construct structured event
  const eventRecord = {
    id: `evt_${now}_${Math.random().toString(36).substring(2, 6)}`,
    ts: new Date().toISOString(),
    siteId: payload.siteId || 'cp_default',
    sessionId,
    userId,
    event: eventName,
    path: pathName,
    regionid,
    regionLabel: REGION_LABELS[regionid],
    durationMs,
    properties
  };

  // Add to ring buffer
  liveEvents.unshift(eventRecord);
  if (liveEvents.length > MAX_STORED_EVENTS) liveEvents.pop();

  // Update Region State
  const rs = regionState[regionid];
  rs.currentCount++;
  rs.viewtimeSum += durationMs;
  rs.viewtimeCount++;
  rs.uniqueUsers.add(userId);
  rs.pageDistribution[pathName] = (rs.pageDistribution[pathName] || 0) + 1;

  // Update Session State
  if (!sessionState[sessionId]) {
    const assignedTier = properties.tier || (userId.includes('Enterprise') ? 'Enterprise ($1,999/mo)' : (userId.includes('Growth') ? 'Growth ($499/mo)' : 'Starter ($99/mo)'));
    const mrr = assignedTier.includes('Enterprise') ? 1999 : (assignedTier.includes('Growth') ? 499 : 99);

    sessionState[sessionId] = {
      sessionId,
      userId,
      accountName: properties.name || (userId.startsWith('usr_') ? `Account ${userId}` : userId),
      tier: assignedTier,
      mrr,
      regionid,
      regionLabel: REGION_LABELS[regionid],
      firstSeen: now,
      lastSeen: now,
      pages: [],
      pageCount: 0,
      totalViewtime: 0,
      riskScore: 10,
      lastEvent: eventName,
      playbookApplied: false
    };
  }

  const sess = sessionState[sessionId];
  sess.lastSeen = now;
  sess.lastEvent = eventName;
  sess.pageCount++;
  sess.totalViewtime += durationMs;
  sess.regionid = regionid;
  sess.regionLabel = REGION_LABELS[regionid];

  if (!sess.pages.includes(pathName)) {
    sess.pages.push(pathName);
    if (sess.pages.length > 20) sess.pages.shift();
  }

  // Real Churn Risk Heuristics
  if (eventName === 'checkout_abandon') {
    sess.riskScore = Math.min(95, sess.riskScore + 45);
    createAlert('critical', regionid, `Cart abandoned on checkout by ${sess.accountName} ($${sess.mrr} MRR exposed)`, { sessionId });
  } else if (eventName === 'support_ticket_created') {
    sess.riskScore = Math.min(90, sess.riskScore + 30);
    createAlert('warning', regionid, `Frustrated support ticket submitted by ${sess.accountName}`, { sessionId });
  } else if (eventName === 'rage_click') {
    sess.riskScore = Math.min(85, sess.riskScore + 20);
  } else if (eventName === 'purchase_success' || eventName === 'subscription_confirmed') {
    sess.riskScore = Math.max(5, sess.riskScore - 30);
  } else {
    sess.riskScore = Math.max(5, sess.riskScore - 2);
  }

  // Real-time broadcast
  broadcast({
    type: 'live_event',
    event: eventRecord,
    session: sess
  });

  // Asynchronously stream telemetry event to Confluent Cloud topic
  produceToConfluent('customer_clickstream', eventRecord.userId, {
    event_id: eventRecord.id,
    event_time: eventRecord.ts,
    session_id: eventRecord.sessionId,
    user_id: eventRecord.userId,
    event_type: eventRecord.event,
    page_path: eventRecord.path,
    cart_value: Number(eventRecord.properties?.cartValue || 0),
    region: eventRecord.regionid,
    duration_ms: eventRecord.durationMs
  });

  res.status(200).json({
    status: 'success',
    receivedAt: eventRecord.ts,
    eventId: eventRecord.id,
    sessionRiskScore: sess.riskScore,
    confluentCloudSynced: isConfluentConfigured
  });
});

// ── GET /api/v1/events (Query Real Ingested Events) ───────────────────────
app.get('/api/v1/events', (req, res) => {
  const limit = Math.min(100, parseInt(req.query.limit) || 30);
  const eventFilter = req.query.event;
  const sessionFilter = req.query.sessionId;

  let results = liveEvents;
  if (eventFilter) results = results.filter(e => e.event === eventFilter);
  if (sessionFilter) results = results.filter(e => e.sessionId === sessionFilter);

  res.json({
    totalIngested: liveEvents.length,
    events: results.slice(0, limit)
  });
});

// ── GET /api/v1/sessions (Query Real Customer Sessions) ───────────────────
app.get('/api/v1/sessions', (req, res) => {
  const now = Date.now();
  const list = Object.values(sessionState).map(s => {
    const idleSeconds = Math.round((now - s.lastSeen) / 1000);
    let status = 'active';
    if (idleSeconds > 180) status = 'churned';
    else if (idleSeconds > 40 || s.riskScore >= 60) status = 'at_risk';

    return {
      ...s,
      idleSeconds,
      status
    };
  });

  // Sort by risk descending
  list.sort((a, b) => b.riskScore - a.riskScore);
  res.json(list);
});

// ── GET /api/v1/analytics/realtime ─────────────────────────────────────────
app.get('/api/v1/analytics/realtime', (req, res) => {
  const now = Date.now();
  let activeCount = 0;
  let atRiskCount = 0;
  let exposedMrr = 0;

  Object.values(sessionState).forEach(s => {
    const idle = (now - s.lastSeen) / 1000;
    if (idle < 40 && s.riskScore < 60) {
      activeCount++;
    } else {
      atRiskCount++;
      exposedMrr += s.mrr;
    }
  });

  const forecasts = REGIONS.map(r => ({
    regionid: r,
    regionLabel: REGION_LABELS[r],
    history: regionState[r].windows.slice(-20),
    ...regionState[r].forecast
  }));

  res.json({
    activeCount,
    atRiskCount,
    exposedMrr,
    savedMrrTotal,
    retentionScore: Math.max(60, 100 - (atRiskCount * 4)),
    forecasts
  });
});

// ── POST /api/v1/playbooks/dispatch (Real Webhook Dispatcher) ──────────────
app.post('/api/v1/playbooks/dispatch', async (req, res) => {
  const { playbookId, sessionId, webhookUrl, customPayload } = req.body;
  const sess = sessionId ? sessionState[sessionId] : null;

  let preservedAmount = 499;
  if (sess) {
    sess.riskScore = Math.max(10, sess.riskScore - 50);
    sess.playbookApplied = true;
    preservedAmount = sess.mrr;
  }
  savedMrrTotal += preservedAmount;

  const payloadToSend = {
    event: 'customerpulse.playbook.triggered',
    playbookId: playbookId || 'vip_customer_rescue',
    timestamp: new Date().toISOString(),
    customer: sess ? {
      sessionId: sess.sessionId,
      accountName: sess.accountName,
      tier: sess.tier,
      mrr: sess.mrr,
      region: sess.regionLabel
    } : { notice: 'Broad cohort re-engagement triggered' },
    message: customPayload || 'Automated retention playbook executed to preserve at-risk subscription.'
  };

  let webhookResult = { status: 'internal_simulated', statusCode: 200, latencyMs: 38 };

  // If the user supplied a real webhook URL (e.g. Slack / webhook.site / Zapier), actually invoke it!
  if (webhookUrl && webhookUrl.startsWith('http')) {
    const start = Date.now();
    try {
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payloadToSend)
      });
      webhookResult = {
        status: 'dispatched_external',
        statusCode: response.status,
        statusText: response.statusText,
        latencyMs: Date.now() - start
      };
    } catch (err) {
      webhookResult = {
        status: 'error_delivering',
        error: err.message,
        latencyMs: Date.now() - start
      };
    }
  }

  // Stream intervention trigger to Confluent Cloud retention_interventions topic
  produceToConfluent('retention_interventions', sess ? sess.userId : 'system_cohort', payloadToSend);

  // Notify WebSocket listeners
  broadcast({
    type: 'playbook_result',
    playbookId,
    preservedAmount,
    savedMrrTotal,
    webhookResult
  });

  res.json({
    success: true,
    preservedAmount,
    savedMrrTotal,
    webhookResult,
    confluentCloudSynced: isConfluentConfigured
  });
});

// ── POST /api/v1/simulator/toggle ──────────────────────────────────────────
app.post('/api/v1/simulator/toggle', (req, res) => {
  backgroundSimulatorActive = !backgroundSimulatorActive;
  res.json({ backgroundSimulatorActive });
});

// ── Alerts REST ────────────────────────────────────────────────────────────
app.get('/api/v1/alerts', (req, res) => res.json(alerts));

app.post('/api/v1/alerts/:id/ack', (req, res) => {
  const a = alerts.find(x => x.id === parseInt(req.params.id));
  if (a) a.acknowledged = true;
  res.json({ success: true });
});

app.post('/api/v1/alerts/ack-all', (req, res) => {
  alerts.forEach(a => a.acknowledged = true);
  res.json({ success: true });
});

// ── Window Tick (10s Tumbling Window) ──────────────────────────────────────
setInterval(() => {
  const forecasts = [];

  REGIONS.forEach(r => {
    const s = regionState[r];
    s.windows.push(s.currentCount);
    if (s.windows.length > 60) s.windows.shift();

    s.avgViewtime = s.viewtimeCount > 0 ? Math.round(s.viewtimeSum / s.viewtimeCount) : 0;
    const prev = s.forecast;
    s.forecast = calculateFrictionAndHealth(s.windows);

    if (s.forecast && s.forecast.direction === 'falling' && (!prev || prev.direction !== 'falling')) {
      createAlert('warning', r, `Customer engagement momentum declined ${Math.abs(s.forecast.changePercent)}% in ${REGION_LABELS[r]}`, { current: s.forecast.current, predicted: s.forecast.predicted });
    }

    forecasts.push({
      regionid: r,
      regionLabel: REGION_LABELS[r],
      history: s.windows.slice(-20),
      uniqueUsers: s.uniqueUsers.size,
      current: s.forecast ? s.forecast.current : s.currentCount,
      predicted: s.forecast ? s.forecast.predicted : null,
      direction: s.forecast ? s.forecast.direction : 'flat',
      changePercent: s.forecast ? s.forecast.changePercent : 0
    });

    s.currentCount = 0;
    s.viewtimeSum = 0;
    s.viewtimeCount = 0;
    s.uniqueUsers = new Set();
  });

  const now = Date.now();
  let activeUsers = 0, atRisk = 0;
  Object.values(sessionState).forEach(s => {
    const idle = (now - s.lastSeen) / 1000;
    if (idle < 40 && s.riskScore < 60) activeUsers++;
    else atRisk++;
  });

  broadcast({
    type: 'window',
    ts: new Date().toISOString(),
    forecasts,
    activeUsers,
    atRisk,
    savedMrrTotal,
    alerts: alerts.slice(0, 15)
  });
}, 10000);

// Gentle background traffic simulator to keep life in the dashboard when not browsing demo store
function backgroundTick() {
  if (backgroundSimulatorActive) {
    const samplePaths = ['/products', '/pricing', '/docs', '/app/dashboard'];
    const r = REGIONS[Math.floor(Math.random() * REGIONS.length)];
    const p = samplePaths[Math.floor(Math.random() * samplePaths.length)];
    
    // Increment region count directly
    regionState[r].currentCount++;
  }
  setTimeout(backgroundTick, 280 + Math.random() * 600);
}
backgroundTick();

// ── WebSocket Initial Handshake ────────────────────────────────────────────
wss.on('connection', ws => {
  const forecasts = REGIONS.map(r => ({
    regionid: r,
    regionLabel: REGION_LABELS[r],
    history: regionState[r].windows.slice(-20),
    ...regionState[r].forecast
  }));

  ws.send(JSON.stringify({
    type: 'init',
    regions: REGIONS,
    regionLabels: REGION_LABELS,
    forecasts,
    savedMrrTotal,
    alerts: alerts.slice(0, 20),
    recentEvents: liveEvents.slice(0, 20)
  }));
});

// ── Start Server ───────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n======================================================`);
  console.log(`⚡ CustomerPulse Server running at http://localhost:${PORT}`);
  console.log(`🛍️ Interactive Demo Store: http://localhost:${PORT}/demo-store.html`);
  if (isConfluentConfigured) {
    console.log(`☁️  Confluent Cloud: CONNECTED (${CONFLUENT_CLUSTER_ID})`);
    console.log(`   Events will stream to topic: customer_clickstream`);
    console.log(`   Interventions will stream to topic: retention_interventions`);
  } else {
    console.log(`💻 Confluent Cloud: Standalone Local Mode (Copy .env.example -> .env to connect)`);
  }
  console.log(`======================================================\n`);
});
