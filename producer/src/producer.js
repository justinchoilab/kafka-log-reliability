'use strict';

const express           = require('express');
const { Kafka, logLevel } = require('kafkajs');
const Redis             = require('ioredis');
const http              = require('http');

const PRODUCER_ID   = process.env.PRODUCER_ID   || '1';
const BROKERS       = (process.env.KAFKA_BROKERS || 'kafka:9092').split(',');
const REDIS_HOST    = process.env.REDIS_HOST     || 'redis';
const DASHBOARD_URL = process.env.DASHBOARD_URL  || 'http://dashboard:3000';
const PORT          = 3001;
const TOPIC         = 'logs';

function nowStr() {
  const d = new Date();
  return d.toLocaleTimeString('en-GB', { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

// ── Kafka ──────────────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: `producer-${PRODUCER_ID}`,
  brokers: BROKERS,
  logLevel: logLevel.WARN,
  connectionTimeout: 3000,
  requestTimeout:    1000,
  retry: { retries: 0 },
});

let producer     = null;
let kafkaOk      = false;
let reconnecting = false;

// Dashboard로 브로커 상태 즉시 push
function pushStatus(ok) {
  httpPost(`${DASHBOARD_URL}/api/broker-status`, { kafkaOk: ok, producerId: PRODUCER_ID }).catch(() => {});
}

// DISCONNECT 이벤트로 Kafka 다운 즉시 감지
function attachListeners(p) {
  p.on(p.events.DISCONNECT, () => {
    if (!reconnecting) reconnectKafka();
  });
}

async function reconnectKafka() {
  if (reconnecting) return;
  reconnecting = true;
  kafkaOk = false;
  pushStatus(false);  // 즉시 dashboard에 알림
  while (true) {
    try {
      if (producer) await producer.disconnect().catch(() => {});
      producer = kafka.producer();
      await producer.connect();
      attachListeners(producer);
      kafkaOk      = true;
      reconnecting = false;
      pushStatus(true);  // 복구 즉시 dashboard에 알림
      console.log(`[${nowStr()}] [Producer-${PRODUCER_ID}] Kafka 재연결 완료`);
      return;
    } catch (e) {
      console.log(`[${nowStr()}] [Producer-${PRODUCER_ID}] Kafka 재연결 실패: ${e.message}`);
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

// ── Redis ──────────────────────────────────────────────────────────────
const redis = new Redis({
  host: REDIS_HOST, port: 6379,
  lazyConnect: true, retryStrategy: () => 2000,
  enableOfflineQueue: false, commandTimeout: 300,
});

// ── State ──────────────────────────────────────────────────────────────
let running   = false;
let interval  = 100;
let timer     = null;
let insertSeq = 0;
let pipelineReady = false;
let pipelineReadyExpiry = 0;
let lastStatusPushAt = 0;

// ── HTTP helper ────────────────────────────────────────────────────────
function httpPost(url, body) {
  return new Promise((resolve) => {
    const parsed = new URL(url);
    const data   = JSON.stringify(body);
    const req = http.request({
      hostname: parsed.hostname,
      port:     parsed.port || 80,
      path:     parsed.pathname,
      method:   'POST',
      headers:  { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) },
      timeout:  1000,
    }, (res) => { res.resume(); resolve(); });
    req.on('timeout', () => { req.destroy(); resolve(); });
    req.on('error',   () => resolve());
    req.write(data);
    req.end();
  });
}

function httpGetJson(url) {
  return new Promise((resolve) => {
    const parsed = new URL(url);
    const req = http.request({
      hostname: parsed.hostname,
      port:     parsed.port || 80,
      path:     parsed.pathname,
      method:   'GET',
      timeout:  800,
    }, (res) => {
      let raw = '';
      res.on('data', chunk => { raw += chunk.toString(); });
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); }
        catch { resolve(null); }
      });
    });
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.on('error',   () => resolve(null));
    req.end();
  });
}

function httpPostJson(url, body) {
  return new Promise((resolve) => {
    const parsed = new URL(url);
    const data   = JSON.stringify(body);
    const req = http.request({
      hostname: parsed.hostname,
      port:     parsed.port || 80,
      path:     parsed.pathname,
      method:   'POST',
      headers:  { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) },
      timeout:  800,
    }, (res) => {
      let raw = '';
      res.on('data', chunk => { raw += chunk.toString(); });
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); }
        catch { resolve(null); }
      });
    });
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.on('error',   () => resolve(null));
    req.write(data);
    req.end();
  });
}

async function refreshPipelineReady(force = false) {
  if (!force && Date.now() < pipelineReadyExpiry) return pipelineReady;
  const result = await httpGetJson(`${DASHBOARD_URL}/api/kafka-ready`);
  pipelineReady = result?.ready === true;
  pipelineReadyExpiry = Date.now() + 300;
  return pipelineReady;
}

async function canEmitThisTick() {
  const result = await httpPostJson(`${DASHBOARD_URL}/api/emit-permit`, { producerId: PRODUCER_ID });
  return result?.allow === true;
}

// ── Tick ──────────────────────────────────────────────────────────────
async function tick() {
  if (!running) return;
  const permit = await canEmitThisTick();
  if (!permit) return;

  const now     = new Date();
  const ts      = now.toISOString();
  const timeStr = now.toLocaleTimeString('en-GB', { hour12: false }) + '.' + String(now.getMilliseconds()).padStart(3, '0');

  const dbFault = await redis.get('fault:db').catch(() => null);
  const dbFaultAtProduce = Boolean(dbFault);

  // 01. 직접 DB insert
  let directOk = false;
  if (!dbFault) {
    try {
      await redis.hset(
        'logs:direct',
        `producer-${PRODUCER_ID}:${ts}:${++insertSeq}`,
        JSON.stringify({ producerId: PRODUCER_ID, ts }),
      );
      directOk = true;
    } catch {}
  }

  const payload = { producerId: PRODUCER_ID, ts, timeStr, directOk, dbFaultAtProduce };

  // 02. Kafka pipeline: 전체(Producer 3 + Consumer) 준비 전에는 Direct fallback 유지
  const clusterReady = await refreshPipelineReady();
  if (!kafkaOk || !clusterReady) {
    if (kafkaOk && Date.now() - lastStatusPushAt > 500) {
      lastStatusPushAt = Date.now();
      pushStatus(true);
    }
    let fallbackOk = false;
    if (!dbFaultAtProduce) {
      try {
        await redis.hset('logs:kafka', `f:${PRODUCER_ID}:${ts}:${++insertSeq}`, JSON.stringify({ ...payload, source: 'fallback' }));
        fallbackOk = true;
      } catch {}
    }
    await httpPost(`${DASHBOARD_URL}/api/event`, { ...payload, kafkaSent: false, fallbackOk });
    return; // 전송 종료
  }

  try {
    await producer.send({ topic: TOPIC, messages: [{ key: PRODUCER_ID, value: JSON.stringify(payload) }] });
    await httpPost(`${DASHBOARD_URL}/api/event`, { ...payload, kafkaSent: true });
  } catch {
    // Kafka 전송 실패: Fallback 시도
    pipelineReady = false;
    pipelineReadyExpiry = 0;
    let fallbackOk = false;
    if (!dbFaultAtProduce) {
      try {
        await redis.hset('logs:kafka', `f:${PRODUCER_ID}:${ts}:${++insertSeq}`, JSON.stringify({ ...payload, source: 'fallback' }));
        fallbackOk = true;
      } catch {}
    }
    
    // DISCONNECT 이벤트가 이미 처리했거나 send가 먼저 실패한 경우
    if (!reconnecting) reconnectKafka();
    await httpPost(`${DASHBOARD_URL}/api/event`, { ...payload, kafkaSent: false, fallbackOk });
  }
}

// ── Loop ──────────────────────────────────────────────────────────────
function scheduleNext() {
  if (!running) return;
  timer = setTimeout(async () => {
    try { await tick(); } catch {}
    scheduleNext();
  }, interval);
}

// ── Heartbeat — dashboard 재시작 후 상태 재동기화용 ──────────────────
setInterval(() => pushStatus(kafkaOk), 1000);

// ── HTTP Server ────────────────────────────────────────────────────────
const app = express();
app.use(express.json());

app.post('/start',    (_req, res) => { if (!running) { running = true; scheduleNext(); } res.json({ ok: true }); });
app.post('/stop',     (_req, res) => { running = false; clearTimeout(timer); res.json({ ok: true }); });
app.post('/interval', (req,  res) => { interval = Number(req.body.ms) || 100; res.json({ ok: true }); });
app.get( '/health',   (_req, res) => res.json({ ok: true, producer: PRODUCER_ID, running, interval, kafkaOk }));

// ── Boot ──────────────────────────────────────────────────────────────
async function main() {
  // Redis 대기
  while (true) {
    try {
      await redis.connect();
      await redis.ping();
      console.log(`[${nowStr()}] [Producer-${PRODUCER_ID}] Redis 연결됨`);
      break;
    } catch (e) {
      console.log(`[${nowStr()}] [WAIT] Redis: ${e.message}`);
      await redis.disconnect().catch(() => {});
      await new Promise(r => setTimeout(r, 2000));
    }
  }

  // Kafka 대기
  while (true) {
    try {
      producer = kafka.producer();
      await producer.connect();
      attachListeners(producer);
      kafkaOk = true;
      pushStatus(true);  // dashboard에 연결 완료 알림
      console.log(`[${nowStr()}] [Producer-${PRODUCER_ID}] Kafka 연결됨`);
      break;
    } catch (e) {
      console.log(`[${nowStr()}] [WAIT] Kafka: ${e.message}`);
      await producer.disconnect().catch(() => {});
      await new Promise(r => setTimeout(r, 3000));
    }
  }

  app.listen(PORT, () => console.log(`[${nowStr()}] Producer-${PRODUCER_ID} listening on :${PORT}`));
}

main().catch(console.error);

process.on('SIGTERM', async () => {
  running  = false;
  kafkaOk  = false;  // DISCONNECT 리스너 재진입 방지
  clearTimeout(timer);
  if (producer) await producer.disconnect().catch(() => {});
  redis.disconnect();
  process.exit(0);
});
