'use strict';

const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const Redis = require('ioredis');
const http = require('http');

const BROKERS = (process.env.KAFKA_BROKERS || 'kafka:9092').split(',');
const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const DASHBOARD_URL = process.env.DASHBOARD_URL || 'http://dashboard:3000';
const PORT = 3002;
const TOPIC = 'logs';
const GROUP_ID = 'log-consumer';
const FAULT_CHANNEL = 'fault:events';

const TRANSIENT_GROUP_ERROR_RE = /coordinator is not aware of this member|group is rebalancing|not coordinator|unknown member/i;
const TRANSIENT_CONN_ERROR_RE = /econnrefused|connection error|etimedout|enotfound|request timed out|dashboard post failed/i;

function nowStr() {
  const d = new Date();
  return d.toLocaleTimeString('en-GB', { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

function getErrMessage(err) {
  if (!err) return '';
  return String(err.message || err);
}

function isTransientGroupError(err) {
  return TRANSIENT_GROUP_ERROR_RE.test(getErrMessage(err));
}

function isTransientConnError(err) {
  return TRANSIENT_CONN_ERROR_RE.test(getErrMessage(err));
}

const transientLogAt = new Map();
function logTransientOnce(key, text, windowMs = 3000) {
  const now = Date.now();
  const last = transientLogAt.get(key) || 0;
  if (now - last < windowMs) return;
  transientLogAt.set(key, now);
  console.log(text);
}

const kafka = new Kafka({
  clientId: 'log-consumer',
  brokers: BROKERS,
  // Suppress noisy internal retry logs; we log concise state transitions ourselves.
  logLevel: logLevel.NOTHING,
  connectionTimeout: 5000,
  requestTimeout: 5000,
  retry: {
    initialRetryTime: 300,
    retries: 20,
    maxRetryTime: 3000,
  },
});

const redis = new Redis({
  host: REDIS_HOST,
  port: 6379,
  lazyConnect: true,
  retryStrategy: () => 2000,
  enableOfflineQueue: false,
  commandTimeout: 500,
});
const redisSub = redis.duplicate();

let insertSeq = 0;
let kafkaOk = false;
let consumerRef = null;
let skipUntilCache = null;
let skipUntilExpiry = 0;
let completedAtCache = null;
let completedAtExpiry = 0;
const faultClearWaiters = new Set();

function notifyFaultClear() {
  for (const resolve of [...faultClearWaiters]) resolve();
}

function waitForFaultClearSignal(timeoutMs = 1000) {
  return new Promise((resolve) => {
    const finish = () => {
      clearTimeout(timer);
      faultClearWaiters.delete(finish);
      resolve();
    };
    const timer = setTimeout(finish, timeoutMs);
    faultClearWaiters.add(finish);
  });
}

redisSub.on('message', (channel, message) => {
  if (channel === FAULT_CHANNEL && message === 'db:off') notifyFaultClear();
});

async function getSkipUntil() {
  if (Date.now() < skipUntilExpiry) return skipUntilCache;
  skipUntilCache = await redis.get('consumer:skipUntil').catch(() => null);
  skipUntilExpiry = Date.now() + 500;
  return skipUntilCache;
}

async function getCompletedAt() {
  if (Date.now() < completedAtExpiry) return completedAtCache;
  completedAtCache = await redis.get('engine:completedAt').catch(() => null);
  completedAtExpiry = Date.now() + 100;
  return completedAtCache;
}

function httpPost(url, body) {
  return new Promise((resolve) => {
    const parsed = new URL(url);
    const data = JSON.stringify(body);
    const req = http.request({
      hostname: parsed.hostname,
      port: parsed.port || 80,
      path: parsed.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
      },
      timeout: 1000,
    }, (res) => {
      res.resume();
      resolve();
    });

    req.on('timeout', () => {
      req.destroy();
      resolve();
    });
    req.on('error', () => resolve());
    req.write(data);
    req.end();
  });
}

function httpPostStrict(url, body, timeoutMs = 1000) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const data = JSON.stringify(body);
    const req = http.request({
      hostname: parsed.hostname,
      port: parsed.port || 80,
      path: parsed.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
      },
      timeout: timeoutMs,
    }, (res) => {
      res.resume();
      const code = res.statusCode || 0;
      if (code >= 200 && code < 300) {
        resolve();
      } else {
        reject(new Error(`dashboard post failed: ${code}`));
      }
    });

    req.on('timeout', () => {
      req.destroy(new Error('dashboard post failed: timeout'));
    });
    req.on('error', (err) => reject(err));
    req.write(data);
    req.end();
  });
}

async function postKafkaEventWithRetry(payload, maxAttempts = 5) {
  let lastErr;
  for (let i = 0; i < maxAttempts; i++) {
    try {
      await httpPostStrict(`${DASHBOARD_URL}/api/event/kafka`, payload, 1200);
      return;
    } catch (err) {
      lastErr = err;
      await new Promise((r) => setTimeout(r, 120 * (i + 1)));
    }
  }
  throw new Error(`dashboard post failed: ${getErrMessage(lastErr)}`);
}

function pushStatus(ok) {
  httpPost(`${DASHBOARD_URL}/api/consumer-status`, { kafkaOk: ok }).catch(() => {});
}

// Keep dashboard status in sync even after dashboard restarts.
setInterval(() => pushStatus(kafkaOk), 1000);

async function processMessage(event, recovered = false) {
  const { producerId, ts, timeStr } = event;

  // Freeze post-completion updates: discard remaining Kafka backlog.
  const completedAt = await getCompletedAt();
  if (completedAt) return;

  // Idempotent Redis field for redelivery cases.
  const redisField = ts
    ? `c:producer-${producerId}:${ts}`
    : `c:producer-${producerId}:seq-${++insertSeq}`;

  await redis.hset(
    'logs:kafka',
    redisField,
    JSON.stringify({ ...event, source: 'kafka' }),
  );

  await postKafkaEventWithRetry({
    producerId,
    ts,
    timeStr,
    dbOk: true,
    recovered,
    queuedFromFault: recovered,
  });
}

// ── In-memory processing queue ─────────────────────────────────────────
// eachMessage는 즉시 commit 후 여기에 적재, worker가 순차 처리
const processingQueue = [];
let workerActive = false;

async function handleQueuedEvent(event, messageTimestamp) {
  const completedAt = await getCompletedAt();
  if (completedAt) return;

  const skipUntil = await getSkipUntil();
  if (skipUntil && Number(messageTimestamp) < Number(skipUntil)) return;

  let waitedForDbFaultClear = false;
  let pendingNotified = false;

  while (true) {
    const completedAtDuringWait = await getCompletedAt();
    if (completedAtDuringWait) return;

    const dbFault = await redis.get('fault:db').catch(() => null);
    if (!dbFault) break;

    if (!waitedForDbFaultClear) {
      waitedForDbFaultClear = true;
      console.log(`[${nowStr()}] [Consumer] waiting for DB fault clear (pending)...`);
    }

    if (!pendingNotified) {
      await postKafkaEventWithRetry({
        producerId: event.producerId,
        ts: event.ts,
        timeStr: event.timeStr,
        pending: true,
        dbOk: false,
        queuedFromFault: true,
      });
      pendingNotified = true;
    }

    // heartbeat 불필요: eachMessage가 이미 리턴했으므로 KafkaJS가 자체적으로 처리
    await waitForFaultClearSignal();
  }

  const completedAtAfterWait = await getCompletedAt();
  if (completedAtAfterWait) return;

  if (waitedForDbFaultClear) {
    const skipAfter = await getSkipUntil();
    if (skipAfter && Number(messageTimestamp) < Number(skipAfter)) return;
  }

  const recovered = event.dbFaultAtProduce === true || event.directOk === false || waitedForDbFaultClear || pendingNotified;
  await processMessage(event, recovered);
}

async function runWorker() {
  if (workerActive) return;
  workerActive = true;
  while (processingQueue.length > 0) {
    const item = processingQueue.shift();
    try {
      await handleQueuedEvent(item.event, item.messageTimestamp);
    } catch (e) {
      if (isTransientConnError(e)) {
        logTransientOnce('worker-transient', `[${nowStr()}] [Consumer] worker transient error: ${getErrMessage(e)}`, 3000);
      } else {
        console.error(`[${nowStr()}] [Consumer] worker error: ${getErrMessage(e)}`);
      }
    }
  }
  workerActive = false;
}

const app = express();
app.get('/health', (_req, res) => res.json({ ok: true }));
app.listen(PORT, () => console.log(`[${nowStr()}] Consumer listening on :${PORT}`));

async function main() {
  while (true) {
    try {
      await redis.connect();
      await redis.ping();
      await redisSub.connect();
      await redisSub.subscribe(FAULT_CHANNEL);
      console.log(`[${nowStr()}] [Consumer] Redis connected`);
      break;
    } catch (e) {
      console.log(`[${nowStr()}] [WAIT] Redis: ${e.message}`);
      await redis.disconnect().catch(() => {});
      await redisSub.disconnect().catch(() => {});
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  const admin = kafka.admin();
  while (true) {
    try {
      await admin.connect();
      await admin.createTopics({
        topics: [{ topic: TOPIC, numPartitions: 1, replicationFactor: 1 }],
        waitForLeaders: true,
      });
      await admin.disconnect();
      console.log(`[${nowStr()}] [Consumer] Topic '${TOPIC}' ready`);
      break;
    } catch (e) {
      console.log(`[${nowStr()}] [WAIT] Topic: ${e.message}`);
      await admin.disconnect().catch(() => {});
      await new Promise((r) => setTimeout(r, 3000));
    }
  }

  const consumer = kafka.consumer({
    groupId: GROUP_ID,
    sessionTimeout: 6000,
    heartbeatInterval: 2000,
    rebalanceTimeout: 10000,
  });
  consumerRef = consumer;

  while (true) {
    try {
      await consumer.connect();
      console.log(`[${nowStr()}] [Consumer] Kafka connected`);
      break;
    } catch (e) {
      console.log(`[${nowStr()}] [WAIT] Kafka: ${e.message}`);
      await new Promise((r) => setTimeout(r, 3000));
    }
  }

  consumer.on(consumer.events.GROUP_JOIN, () => {
    if (!kafkaOk) {
      kafkaOk = true;
      pushStatus(true);
    }
  });

  consumer.on(consumer.events.DISCONNECT, () => {
    if (kafkaOk) {
      kafkaOk = false;
      pushStatus(false);
    }
  });

  consumer.on(consumer.events.CRASH, (event) => {
    if (kafkaOk) {
      kafkaOk = false;
      pushStatus(false);
    }

    const errMsg = getErrMessage(event?.payload?.error);
    const willRestart = event?.payload?.restart !== false;

    if ((isTransientGroupError(errMsg) || isTransientConnError(errMsg)) && willRestart) {
      logTransientOnce(
        'consumer-crash-transient',
        `[${nowStr()}] [Consumer] transient rejoin/reconnect: ${errMsg || 'transient error'}`,
        4000,
      );
      return;
    }

    if (event?.payload?.restart === false) {
      console.error(`[${nowStr()}] [Consumer] fatal crash -> restart: ${errMsg || 'unknown error'}`);
      setTimeout(() => process.exit(1), 50);
    }
  });

  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const completedAt = await getCompletedAt();
        if (completedAt) {
          await consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);
          return;
        }

        const skipUntil = await getSkipUntil();
        if (skipUntil && Number(message.timestamp) < Number(skipUntil)) {
          await consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);
          return;
        }

        let event;
        try {
          event = JSON.parse(message.value.toString());
        } catch (parseErr) {
          console.error(`[${nowStr()}] [Consumer] invalid message skipped: ${parseErr.message}`);
          await consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);
          return;
        }

        // 즉시 commit 후 worker에 이관 — eachMessage는 블로킹 없이 리턴
        await consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);
        processingQueue.push({ event, messageTimestamp: message.timestamp });
        runWorker();
      } catch (e) {
        if (isTransientGroupError(e) || isTransientConnError(e)) {
          logTransientOnce(
            'consumer-message-transient',
            `[${nowStr()}] [Consumer] transient commit error: ${getErrMessage(e)}`,
            3000,
          );
          return;
        }

        console.error(`[${nowStr()}] [Consumer] message error: ${getErrMessage(e)}`);
        throw e;
      }
    },
  });
}

main().catch((e) => {
  console.error(`[${nowStr()}] [Consumer] main fatal: ${getErrMessage(e)}`);
  process.exit(1);
});

process.on('SIGTERM', async () => {
  kafkaOk = false;
  if (consumerRef) await consumerRef.disconnect().catch(() => {});
  redisSub.disconnect();
  redis.disconnect();
  process.exit(0);
});
