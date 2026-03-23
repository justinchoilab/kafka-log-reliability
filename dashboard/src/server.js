'use strict';

const express             = require('express');
const { WebSocketServer } = require('ws');
const Redis               = require('ioredis');
const http                = require('http');
const path                = require('path');

const PORT         = 3000;
const REDIS_HOST   = process.env.REDIS_HOST || 'localhost';
const TOTAL_TARGET = 1000;
const FAULT_CHANNEL = 'fault:events';

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

const redis = new Redis({
  host: REDIS_HOST, port: 6379,
  lazyConnect: true, retryStrategy: () => 2000,
  enableOfflineQueue: false, commandTimeout: 500,
});

const stats = { dirS:0, dirF:0, kafS:0, kafD:0, kafP:0, kafF:0, total:0 };
let pending         = 0;
let dbFaulted       = false;
let redisReady      = false;
let engineRunning   = false;
let completionSent  = false;
let resetBeforeMs   = Date.now();
let brokerUp        = null;
let producersReady  = false;
let consumerKafkaOk = null;
let consumerEverConnected = false;
const recentLogs    = { direct: [], kafka: [] };
const systemEvents  = [];

const producerStatus = {};
const trackedKafkaPending = new Set();
const pendingFirstSeenAt = new Map();
const settledKafkaEvents = new Set();
const knownKafkaEvents = new Set();
const emitterIds = ['1', '2', '3'];
let emissionIntervalMs = 100;
let emissionSlotKey = -1;
let emissionSlotWinner = null;
let lastSystemEventKey = '';
let lastSystemEventAt  = 0;
let pendingReconcileTimer = null;
const ORPHAN_PENDING_TIMEOUT_MS = 12000;

function rememberSettledKafkaEvent(key) {
  if (!key) return;
  settledKafkaEvents.add(key);
  // Bound memory: keep only the most recent settled event keys.
  if (settledKafkaEvents.size > 50000) {
    const oldest = settledKafkaEvents.values().next().value;
    settledKafkaEvents.delete(oldest);
  }
}

function isKafkaPipelineReady() {
  const ids = ['1', '2', '3'];
  return ids.every(id => producerStatus[id]?.kafkaOk === true) && consumerKafkaOk === true;
}

function getKafkaEventKey(producerId, ts) {
  if (producerId === undefined || !ts) return null;
  return `${producerId}:${ts}`;
}

function sanitizeIntervalMs(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return 100;
  return Math.max(1, Math.floor(n));
}

function tryAcquireEmissionPermit(producerId) {
  const pid = String(producerId || '');
  if (!emitterIds.includes(pid)) return false;

  const slotMs = sanitizeIntervalMs(emissionIntervalMs);
  const nowSlotKey = Math.floor(Date.now() / slotMs);
  if (nowSlotKey !== emissionSlotKey || !emissionSlotWinner) {
    emissionSlotKey = nowSlotKey;
    emissionSlotWinner = emitterIds[Math.floor(Math.random() * emitterIds.length)];
  }
  return emissionSlotWinner === pid;
}

function syncPendingCount() {
  pending = trackedKafkaPending.size;
  stats.kafP = pending;
}

async function reconcilePendingFromRedis() {
  if (!redisReady || trackedKafkaPending.size === 0) return;

  const keys = [...trackedKafkaPending];
  const fields = keys.map((key) => {
    const idx = key.indexOf(':');
    if (idx < 0) return null;
    const producerId = key.slice(0, idx);
    const ts = key.slice(idx + 1);
    if (!producerId || !ts) return null;
    return `c:producer-${producerId}:${ts}`;
  });

  const pipeline = redis.pipeline();
  for (const field of fields) {
    if (field) pipeline.hexists('logs:kafka', field);
    else pipeline.echo('0');
  }

  const results = await pipeline.exec().catch(() => null);
  if (!results) return;

  let removed = 0;
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    const exists = Number(results[i]?.[1] || 0) === 1;
    if (exists) {
      if (trackedKafkaPending.delete(key)) {
        pendingFirstSeenAt.delete(key);
        knownKafkaEvents.delete(key);
        rememberSettledKafkaEvent(key);
        removed++;
      }
      continue;
    }

    // Recovery window fallback: if a pending key still has no consumer write for long enough,
    // mark it failed to prevent permanent residual pending entries.
    const firstSeenAt = pendingFirstSeenAt.get(key) || Date.now();
    pendingFirstSeenAt.set(key, firstSeenAt);
    if (!dbFaulted && brokerUp === true && Date.now() - firstSeenAt >= ORPHAN_PENDING_TIMEOUT_MS) {
      if (trackedKafkaPending.delete(key)) {
        pendingFirstSeenAt.delete(key);
        knownKafkaEvents.delete(key);
        rememberSettledKafkaEvent(key);
        stats.kafF++;
        addEvent(`복구 지연으로 orphan pending 정리: ${key} -> FAIL 확정`, 'warn');
        removed++;
      }
    }
  }

  if (removed > 0) {
    syncPendingCount();
    broadcast('stats', { stats });
  }
}

function schedulePendingReconcile(rounds = 6, delayMs = 800) {
  if (pendingReconcileTimer) {
    clearTimeout(pendingReconcileTimer);
    pendingReconcileTimer = null;
  }

  let left = Math.max(1, rounds);
  const run = () => {
    reconcilePendingFromRedis().catch(() => {});
    left -= 1;
    if (left > 0) {
      pendingReconcileTimer = setTimeout(run, delayMs);
    } else {
      pendingReconcileTimer = null;
    }
  };

  pendingReconcileTimer = setTimeout(run, 200);
}

function nowStr() {
  const d = new Date();
  return d.toLocaleTimeString('en-GB', { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

function addEvent(text, type = 'info') {
  const key = `${type}:${text}`;
  const now = Date.now();
  if (key === lastSystemEventKey && now - lastSystemEventAt < 1500) return;
  lastSystemEventKey = key;
  lastSystemEventAt = now;

  const entry = { ts: nowStr(), text, type };
  systemEvents.unshift(entry);
  if (systemEvents.length > 50) systemEvents.pop();
  broadcast('system_event', entry);
}

function pushLog(side, entry) {
  recentLogs[side] = [...recentLogs[side].slice(-99), entry];
}

function emitLog(side, entry) {
  const list = recentLogs[side];
  const last = list[list.length - 1];
  if (last && last.type === entry.type && last.text === entry.text) return;

  pushLog(side, entry);
  broadcast(`${side}_log`, entry);
}

function getEventMs(ts) {
  if (!ts) return null;
  const ms = new Date(ts).getTime();
  return Number.isFinite(ms) ? ms : null;
}

function broadcast(type, data) {
  const msg = JSON.stringify({ type, data });
  wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}

async function callProducers(route, body = {}, ms = 2000) {
  await Promise.all([1, 2, 3].map(i =>
    fetch(`http://producer-${i}:3001${route}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(ms),
    }).catch(() => {}),
  ));
}

function checkReady() {
  if (!redisReady || producersReady) return;
  const ids = Object.keys(producerStatus);
  if (ids.length < 3) return;
  if (!isKafkaPipelineReady()) return;

  producersReady = true;
  brokerUp       = true;
  broadcast('system_ready', {});
  addEvent('시스템 준비 완료 - 시작 대기 중', 'ok');
}

let _brokerStateTimer = null;
function updateBrokerState() {
  if (_brokerStateTimer) clearTimeout(_brokerStateTimer);
  _brokerStateTimer = setTimeout(() => {
    _brokerStateTimer = null;
    const ready = isKafkaPipelineReady();
    if (ready === brokerUp) return;

    brokerUp = ready;
    if (!ready) {
      broadcast('broker_down', {});
      addEvent('Kafka 파이프라인 미준비 - Direct DB 모드', 'warn');
    } else {
      broadcast('broker_up', {});
      addEvent('Kafka 파이프라인 복구 완료 - 정상 운영 재개', 'ok');
      if (!dbFaulted) schedulePendingReconcile();
    }
  }, 300);
}

// 상태 이벤트 누락이 있어도 준비 상태를 주기적으로 재평가한다.
setInterval(() => {
  if (!producersReady && redisReady && isKafkaPipelineReady()) {
    checkReady();
  }
}, 1000);

async function processEvent(event) {
  if (completionSent) return;
  const eventMs = getEventMs(event.ts);
  if (eventMs !== null && eventMs < resetBeforeMs) return;
  stats.total++;
  const ts  = event.timeStr || nowStr();
  const pid = `producer-${event.producerId}`;

  if (event.directOk) stats.dirS++; else stats.dirF++;
  const dEntry = event.directOk
    ? { type: 'ok',  text: `[${ts}]  DB insert success  (${pid})` }
    : { type: 'err', text: `[${ts}]  DB insert failed   (${pid})` };
  emitLog('direct', dEntry);

  if (event.kafkaSent === true) {
    const key = getKafkaEventKey(event.producerId, event.ts);
    const alreadySettled = key ? settledKafkaEvents.has(key) : false;
    if (key && !alreadySettled) knownKafkaEvents.add(key);
    const queuedFromFault = event.dbFaultAtProduce === true;
    if (queuedFromFault && key && !alreadySettled && !trackedKafkaPending.has(key)) {
      trackedKafkaPending.add(key);
      pendingFirstSeenAt.set(key, Date.now());
      syncPendingCount();
      const kEntry = { type: 'warn', text: `[${ts}]  Kafka ACK OK -> pending (DB fault)  (${pid})` };
      emitLog('kafka', kEntry);
    }
  } else {
    if (event.fallbackOk) {
      stats.kafD++;
      const kEntry = { type: 'info', text: `[${ts}]  Broker down -> Producer fallback OK  (${pid})` };
      emitLog('kafka', kEntry);
    } else {
      stats.kafF++;
      const kEntry = { type: 'err', text: `[${ts}]  Broker down + Fallback failed  (${pid})` };
      emitLog('kafka', kEntry);
    }
  }

  broadcast('stats', { stats });

  if (stats.total >= TOTAL_TARGET && !completionSent) {
    completionSent = true;
    engineRunning  = false;
    await callProducers('/stop');
    await redis.set('engine:completedAt', String(Date.now())).catch(() => {});

    broadcast('stats', { stats });
    broadcast('engine_completed', {});
    addEvent(`${TOTAL_TARGET}개 수집 완료 - RESET으로 초기화하세요`, 'ok');
  }
}

function getUnitSignals() {
  return {
    p1:       producerStatus['1']?.kafkaOk || false,
    p2:       producerStatus['2']?.kafkaOk || false,
    p3:       producerStatus['3']?.kafkaOk || false,
    consumer: consumerKafkaOk === true,
    kafka:    brokerUp === true,
    redis:    !dbFaulted,
  };
}

wss.on('connection', (ws) => {
  const readyNow = producersReady || isKafkaPipelineReady();
  ws.send(JSON.stringify({
    type: 'init',
    data: { stats, redisOk: !dbFaulted, brokerUp, recentLogs: { direct: [], kafka: [] }, systemEvents: [], redisReady, engineRunning, producersReady: readyNow },
  }));
});

app.post('/api/event', async (req, res) => {
  try { await processEvent(req.body); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/emit-permit', (req, res) => {
  const allow = tryAcquireEmissionPermit(req.body?.producerId);
  res.json({ ok: true, allow });
});

app.post('/api/event/kafka', (req, res) => {
  try {
    const { producerId, ts, timeStr, dbOk, pending: isPending, recovered, queuedFromFault } = req.body;
    const eventMs = getEventMs(ts);
    if (eventMs !== null && eventMs < resetBeforeMs) {
      res.json({ ok: true, ignored: true });
      return;
    }
    const key = getKafkaEventKey(producerId, ts);
    const wasPending = key ? trackedKafkaPending.has(key) : false;
    const isKnownEvent = key ? knownKafkaEvents.has(key) : false;
    // 완료 이후에는 새 이벤트를 받지 않되, 이미 집계된 이벤트의 최종 Kafka ACK는 반영한다.
    if (completionSent && !isKnownEvent && !wasPending) {
      res.json({ ok: true, ignored: true });
      return;
    }
    if (key && settledKafkaEvents.has(key)) {
      // 드물게 settle 중복 이벤트가 먼저 도착한 경우 pending 카운트가 남지 않도록 정리한다.
      if (wasPending) {
        trackedKafkaPending.delete(key);
        syncPendingCount();
        broadcast('stats', { stats });
      }
      res.json({ ok: true, ignored: true });
      return;
    }
    const pid = `producer-${producerId}`;
    const t   = timeStr || nowStr();
    const fromPending = recovered === true || queuedFromFault === true || wasPending;
    let kEntry;

    if (isPending) {
      let newlyTracked = false;
      if (key && !settledKafkaEvents.has(key)) {
        knownKafkaEvents.add(key);
        if (!trackedKafkaPending.has(key)) {
          trackedKafkaPending.add(key);
          pendingFirstSeenAt.set(key, Date.now());
          syncPendingCount();
          newlyTracked = true;
        }
      }
      if (newlyTracked) {
        kEntry = { type: 'warn', text: `[${t}]  Kafka -> DB fault - pending  (${pid})` };
      }
    } else {
      if (wasPending && key) {
        trackedKafkaPending.delete(key);
        pendingFirstSeenAt.delete(key);
        syncPendingCount();
      }
      if (key) knownKafkaEvents.delete(key);
      rememberSettledKafkaEvent(key);
      if (dbOk) {
        stats.kafS++;
        if (fromPending) {
          kEntry = { type: 'info', text: `[${t}]  Kafka pending -> DB insert success  (${pid})` };
        } else {
          kEntry = { type: 'ok', text: `[${t}]  Kafka -> DB insert success  (${pid})` };
        }
      } else {
        stats.kafF++;
        kEntry = { type: 'err', text: `[${t}]  Kafka -> DB insert failed  (${pid})` };
      }
    }

    if (!completionSent && kEntry) {
      emitLog('kafka', kEntry);
    }
    broadcast('stats', { stats });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/broker-status', (req, res) => {
  const { kafkaOk, producerId } = req.body;
  const pid   = String(producerId);
  const isNew = !(pid in producerStatus);
  const wasOk = producerStatus[pid]?.kafkaOk;

  producerStatus[pid] = { kafkaOk, lastSeen: Date.now() };

  if (isNew)             console.log(`[${nowStr()}] [INIT] Producer-${pid} 감지됨`);
  if (!wasOk && kafkaOk) console.log(`[${nowStr()}] [INIT] Producer-${pid} Kafka 연결 완료`);

  if (!producersReady) checkReady();
  else updateBrokerState();

  res.json({ ok: true });
});

app.post('/api/consumer-status', (req, res) => {
  const { kafkaOk } = req.body;
  const prev = consumerKafkaOk;
  const isFirstSignal = prev === null;

  if (isFirstSignal) console.log(`[${nowStr()}] [INIT] Consumer 감지됨`);

  if (kafkaOk !== prev) {
    consumerKafkaOk = kafkaOk;

    if (kafkaOk) {
      if (!consumerEverConnected) {
        consumerEverConnected = true;
        console.log(`[${nowStr()}] [INIT] Consumer Kafka 연결 완료`);
      } else {
        console.log(`[${nowStr()}] [INIT] Consumer Kafka 재연결 완료`);
        if (producersReady) addEvent('Consumer Kafka 재연결 완료', 'ok');
      }
    } else {
      if (consumerEverConnected) {
        console.log(`[${nowStr()}] [INIT] Consumer Kafka 연결 끊김`);
        if (producersReady) addEvent('Consumer Kafka 연결 끊김', 'warn');
      }
    }
  }

  if (!producersReady) checkReady();
  else updateBrokerState();

  res.json({ ok: true });
});

app.post('/api/engine/start', async (_req, res) => {
  engineRunning = true;
  await callProducers('/start');
  addEvent('Producer 시작됨', 'ok');
  res.json({ ok: true });
});

app.post('/api/engine/stop', async (_req, res) => {
  engineRunning = false;
  await callProducers('/stop');
  addEvent('Producer 정지됨', 'warn');
  res.json({ ok: true });
});

app.post('/api/interval', async (req, res) => {
  emissionIntervalMs = sanitizeIntervalMs(req.body.ms);
  emissionSlotKey = -1;
  emissionSlotWinner = null;
  await callProducers('/interval', { ms: emissionIntervalMs });
  res.json({ ok: true });
});

app.post('/api/fault/db', async (_req, res) => {
  dbFaulted = !dbFaulted;
  try {
    if (dbFaulted) {
      await redis.set('fault:db', '1');
      await redis.publish(FAULT_CHANNEL, 'db:on');
    } else {
      await redis.del('fault:db');
      await redis.publish(FAULT_CHANNEL, 'db:off');
    }
  } catch {}
  broadcast('redis_status', { ok: !dbFaulted });
  addEvent(dbFaulted ? 'Redis DB 장애 발생' : 'Redis DB 복구', dbFaulted ? 'err' : 'ok');
  if (!dbFaulted) schedulePendingReconcile();
  res.json({ ok: true });
});

app.post('/api/reset', async (_req, res) => {
  try {
    resetBeforeMs = Date.now();
    engineRunning = false;
    await callProducers('/stop', {}, 300);
    await callProducers('/interval', { ms: 100 }, 300);
    redis.set('consumer:skipUntil', String(Date.now())).catch(() => {});
    Object.assign(stats, { dirS:0, dirF:0, kafS:0, kafD:0, kafP:0, kafF:0, total:0 });
    trackedKafkaPending.clear();
    pendingFirstSeenAt.clear();
    settledKafkaEvents.clear();
    knownKafkaEvents.clear();
    emissionSlotKey = -1;
    emissionSlotWinner = null;
    syncPendingCount();
    completionSent      = false;
    dbFaulted           = false;
    lastSystemEventKey  = '';
    lastSystemEventAt   = 0;
    systemEvents.length = 0;
    recentLogs.direct   = [];
    recentLogs.kafka    = [];
    if (pendingReconcileTimer) {
      clearTimeout(pendingReconcileTimer);
      pendingReconcileTimer = null;
    }
    broadcast('redis_status', { ok: true });
    broadcast('reset', {});
    if (redisReady && producersReady) addEvent('시스템 준비 완료 - 시작 대기 중', 'ok');
    res.json({ ok: true });
    redis.publish(FAULT_CHANNEL, 'db:off').catch(() => {});
    redis.del('logs:direct', 'logs:kafka', 'fault:db', 'pending:kafka', 'engine:completedAt').catch(() => {});
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/status', (_req, res) => {
  res.json({ stats, brokerUp, redisOk: !dbFaulted, redisReady, producersReady: (producersReady || isKafkaPipelineReady()) });
});

app.get('/api/kafka-ready', (_req, res) => {
  res.json({ ready: isKafkaPipelineReady() });
});

async function main() {
  addEvent('Redis 연결 대기 중...', 'info');
  while (true) {
    try {
      await redis.connect();
      await redis.ping();
      resetBeforeMs = Date.now();
      await redis.del('logs:direct', 'logs:kafka', 'fault:db', 'pending:kafka', 'engine:completedAt');
      await redis.set('consumer:skipUntil', String(Date.now()));
      redisReady = true;
      addEvent('Redis 연결됨 - 데이터 초기화 완료', 'ok');
      break;
    } catch (e) {
      console.log(`[${nowStr()}] [WAIT] Redis: ${e.message}`);
      try { redis.disconnect(); } catch {}
      await new Promise(r => setTimeout(r, 2000));
    }
  }

  server.listen(PORT, () => console.log(`[${nowStr()}] Dashboard listening on http://localhost:${PORT}`));
  checkReady();
}

main().catch(console.error);
process.on('SIGTERM', () => { redis.disconnect(); process.exit(0); });
