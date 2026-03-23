'use strict';

const TOTAL_TARGET = 1000;

// ── Client State ───────────────────────────────────────────────────────
let ws             = null;
let running        = false;
let completed      = false;
let dbFault        = false;
let brokerDown     = false;
let producersReady = false;
let redisReady     = false;
let lastStats      = { dirS:0, dirF:0, kafS:0, kafD:0, kafP:0, kafF:0, total:0 };
let chartDirect    = null;
let chartKafka     = null;

// ── DOM refs ───────────────────────────────────────────────────────────
const $      = id => document.getElementById(id);
const conDir = $('con-direct');
const conKaf = $('con-kafka');
const evLog  = $('event-log');
const btnEng = $('btn-engine');
const btnDb  = $('btn-dbfault');
const btnRep = $('btn-report');
const badge  = $('conn-badge');

// ── Stats rendering ────────────────────────────────────────────────────
function renderStats(s) {
  lastStats = { ...s };
  $('m-dirs').textContent   = s.dirS;
  $('m-dirf').textContent   = s.dirF;
  $('m-total').textContent  = `${s.total} / ${TOTAL_TARGET}`;
  $('m-kafs').textContent   = s.kafS;
  $('m-kafd').textContent   = s.kafD;
  $('m-kafp').textContent   = s.kafP;
  $('m-kaff').textContent   = s.kafF;
  $('m-total2').textContent = `${s.total} / ${TOTAL_TARGET}`;
}

// ── Log append ─────────────────────────────────────────────────────────
const LOG_CLS = { ok: 'log-ok', err: 'log-err', warn: 'log-warn', info: 'log-info' };
const EV_CLS  = { ok: 'ev-ok',  err: 'ev-err',  warn: 'ev-warn',  info: 'ev-info'  };

function addLog(el, entry) {
  const div = document.createElement('div');
  div.className   = LOG_CLS[entry.type] || '';
  div.textContent = entry.text;
  el.appendChild(div);
  while (el.children.length > 200) el.removeChild(el.firstChild);
  el.scrollTop = el.scrollHeight;
}

function addEvent(entry) {
  const div = document.createElement('div');
  div.className   = EV_CLS[entry.type] || '';
  div.textContent = `[${entry.ts}]  ${entry.text}`;
  evLog.appendChild(div);
  while (evLog.children.length > 50) evLog.removeChild(evLog.firstChild);
  evLog.scrollTop = evLog.scrollHeight;
}

// ── Status display ─────────────────────────────────────────────────────
const sysStat = $('sys-status');

function setStatus(text, cls) {
  sysStat.textContent = text;
  sysStat.className   = `sys-status ${cls}`;
}

function refreshStatus() {
  if (!redisReady || !producersReady) return setStatus('INITIALIZING', 'status-init');
  if (brokerDown)                     return setStatus('BROKER DOWN',  'status-warn');
  if (completed)                      return setStatus('COMPLETED',    'status-ok');
  if (running)                        return setStatus('RUNNING',      'status-ok');
  setStatus('READY', 'status-ok');
}

// ── Button states ──────────────────────────────────────────────────────
function refreshButtons() {
  const ready = redisReady && producersReady;
  btnDb.disabled  = !ready;
  btnEng.disabled = !ready;

  if (completed) {
    btnEng.textContent = 'RESET';
    btnEng.className   = 'btn btn-reset';
  } else if (running) {
    btnEng.textContent = 'STOP ENGINE';
    btnEng.className   = 'btn btn-running';
  } else {
    btnEng.textContent = 'START ENGINE';
    btnEng.className   = 'btn';
  }

  btnDb.textContent = dbFault ? 'RECOVER DB' : 'DB_FAULT';
  btnDb.className   = dbFault ? 'btn btn-fault-active' : 'btn';
}

// ── Modal ──────────────────────────────────────────────────────────────
function openModal(name)  { $(`modal-${name}`).classList.add('show');    $('overlay').classList.add('show');    }
function closeModal(name) { $(`modal-${name}`).classList.remove('show'); $('overlay').classList.remove('show'); }

$('overlay').addEventListener('click', () => ['result', 'help'].forEach(closeModal));

// ── Result Chart ───────────────────────────────────────────────────────
function makeBarChart(canvasId, labels, data, colors) {
  return new Chart($(canvasId).getContext('2d'), {
    type: 'bar',
    data: {
      labels,
      datasets: [{ data, backgroundColor: colors, borderWidth: 0, borderRadius: 4, barThickness: 40 }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#666', font: { family: 'monospace', size: 10 } }, grid: { color: '#1e1e1e' } },
        y: { ticks: { color: '#666', font: { family: 'monospace', size: 10 } }, grid: { color: '#1e1e1e' } },
      },
    },
  });
}

function showChart(s) {
  if (chartDirect) { chartDirect.destroy(); chartDirect = null; }
  if (chartKafka)  { chartKafka.destroy();  chartKafka  = null; }
  btnRep.style.display = 'inline-flex';
  openModal('result');
  chartDirect = makeBarChart('chart-direct', ['SUCCESS', 'FAIL'], [s.dirS, s.dirF], ['#059669', '#dc2626']);
  chartKafka  = makeBarChart('chart-kafka',  ['SUCCESS', 'DIRECT DB', 'PENDING', 'FAIL'],
    [s.kafS, s.kafD, s.kafP, s.kafF], ['#059669', '#3b82f6', '#f59e0b', '#dc2626']);
}

// ── REST helper ────────────────────────────────────────────────────────
async function api(path, body) {
  await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body ?? {}),
  }).catch(() => {});
}

// ── WebSocket ──────────────────────────────────────────────────────────
function connect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${proto}//${location.host}`);

  ws.onopen  = () => { badge.textContent = 'CONNECTED'; badge.className = 'ok'; };
  ws.onclose = () => { badge.textContent = 'DISCONNECTED'; badge.className = 'err'; ws = null; setTimeout(connect, 3000); };
  ws.onerror = () => ws.close();

  ws.onmessage = ({ data }) => {
    const { type, data: d } = JSON.parse(data);
    switch (type) {

      case 'init':
        redisReady     = d.redisReady;
        producersReady = d.producersReady ?? false;
        brokerDown     = d.brokerUp === false;
        running        = d.engineRunning;
        dbFault        = !d.redisOk;
        completed      = (d.stats?.total ?? 0) >= TOTAL_TARGET;
        renderStats(d.stats || lastStats);
        conDir.innerHTML = '';
        conKaf.innerHTML = '';
        evLog.innerHTML  = '';
        (d.recentLogs?.direct || []).forEach(e => addLog(conDir, e));
        (d.recentLogs?.kafka  || []).forEach(e => addLog(conKaf, e));
        (d.systemEvents || []).slice().reverse().forEach(e => addEvent(e));
        if (completed) btnRep.style.display = 'inline-flex';
        refreshButtons();
        refreshStatus();
        break;

      case 'direct_log':   addLog(conDir, d); break;
      case 'kafka_log':    addLog(conKaf, d); break;
      case 'system_event': addEvent(d);       break;
      case 'stats':
        renderStats(d.stats);
        if (completed && chartDirect && chartKafka) {
          chartDirect.data.datasets[0].data = [lastStats.dirS, lastStats.dirF];
          chartDirect.update();
          chartKafka.data.datasets[0].data = [lastStats.kafS, lastStats.kafD, lastStats.kafP, lastStats.kafF];
          chartKafka.update();
        }
        break;

      case 'system_ready':
        producersReady = true;
        redisReady     = true;
        brokerDown     = false;
        refreshButtons();
        refreshStatus();
        break;

      case 'broker_down':
        brokerDown = true;
        refreshStatus();
        break;

      case 'broker_up':
        brokerDown = false;
        refreshStatus();
        break;

      case 'redis_status':
        dbFault = !d.ok;
        refreshButtons();
        break;

      case 'engine_completed':
        running   = false;
        completed = true;
        refreshButtons();
        refreshStatus();
        showChart(lastStats);
        break;

      case 'reset':
        running    = false;
        completed  = false;
        dbFault    = false;
        brokerDown = false;
        renderStats({ dirS:0, dirF:0, kafS:0, kafD:0, kafP:0, kafF:0, total:0 });
        conDir.innerHTML = '';
        conKaf.innerHTML = '';
        evLog.innerHTML  = '';
        btnRep.style.display = 'none';
        $('speed-slider').value    = 100;
        $('speed-val').textContent = '100ms';
        if (chartDirect) { chartDirect.destroy(); chartDirect = null; }
        if (chartKafka)  { chartKafka.destroy();  chartKafka  = null; }
        refreshButtons();
        refreshStatus();
        break;
    }
  };
}

async function boot() {
  await api('/api/reset');
  connect();
}

// ── Button handlers ────────────────────────────────────────────────────
async function handleEngine() {
  if (completed) {
    await api('/api/reset');
    return;
  }
  if (running) {
    running = false;
    refreshButtons();
    setStatus('STOPPING...', 'status-warn');
    await api('/api/engine/stop');
    refreshStatus();
  } else {
    running = true;
    refreshButtons();
    refreshStatus();
    await api('/api/engine/start');
  }
}

async function toggleDbFault() {
  await api('/api/fault/db');
}

let speedTimer = null;
function handleSpeedChange(ms) {
  $('speed-val').textContent = ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(1)}s`;
  clearTimeout(speedTimer);
  speedTimer = setTimeout(() => api('/api/interval', { ms }), 300);
}

// ── Boot ──────────────────────────────────────────────────────────────
boot();

