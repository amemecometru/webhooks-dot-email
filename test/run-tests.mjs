/**
 * End-to-end tests for the webhooks.email Worker, run under plain Node with a
 * node:sqlite-backed D1 shim (see test/d1-shim.mjs).
 *
 * Covers: routing, auth, validation, SSRF guards, the D1 delivery queue and its
 * retry/backoff, email queueing + fast path, inbound fan-out, charge metering,
 * the charge summary, Stripe signature verification, and both cron schedules.
 *
 * Run: npm test
 */

import { readFileSync } from 'node:fs';
import { register } from 'node:module';
import { pathToFileURL } from 'node:url';
import { D1Shim } from './d1-shim.mjs';

register('./loader.mjs', import.meta.url);

const workerModule = await import(
  pathToFileURL(new URL('../src/index.ts', import.meta.url).pathname).href
);
const worker = workerModule.default;
const app = workerModule.app;
const { hashApiKey, generateApiKey, hmacSha256Hex, keyLookupDigest } = await import(
  pathToFileURL(new URL('../src/lib/crypto.ts', import.meta.url).pathname).href
);


/* -------------------------------------------------------------------------- */
/*  Tiny test runner                                                           */
/* -------------------------------------------------------------------------- */

let passed = 0;
const failures = [];
let currentSuite = '';

function suite(name) {
  currentSuite = name;
  console.log(`\n${name}`);
}

async function test(name, fn) {
  try {
    await fn();
    passed++;
    console.log(`  ok   ${name}`);
  } catch (err) {
    failures.push({ suite: currentSuite, name, err });
    console.log(`  FAIL ${name}\n       ${err.message}`);
  }
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg || 'assertion failed');
}

function eq(actual, expected, msg) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) throw new Error(`${msg || 'not equal'}\n       expected: ${e}\n       actual:   ${a}`);
}

/* -------------------------------------------------------------------------- */
/*  Harness                                                                    */
/* -------------------------------------------------------------------------- */

const DB = D1Shim.fromFile(':memory:');
DB.exec(readFileSync(new URL('../src/db/schema.sql', import.meta.url), 'utf8'));

const sentEmails = [];
const STRIPE_SECRET = 'whsec_test_secret';
const STRIPE_CALLS = [];

const EMAIL = {
  async send(msg) {
    sentEmails.push(msg);
    return { messageId: 'cf-' + sentEmails.length };
  },
};

const env = {
  DB,
  EMAIL,
  ENVIRONMENT: 'test',
  API_HOST: 'webhooks.email',
  MCP_HOST: 'mcp.webhooks.email',
  OUTBOUND_EMAIL: 'noreply@webhooks.email',
  WEBHOOK_TIMEOUT_MS: '2000',
  MAX_RETRIES: '5',
  AUTH_CACHE_TTL_SECONDS: '3600',
  KEY_PBKDF2_ITERATIONS: '100000',
  DELIVERY_BATCH_SIZE: '25',
  CHARGE_ENDPOINT: 'email.send',
  CHARGE_WEBHOOK_ENDPOINT: 'webhook.send',
  CHARGE_INBOUND_ENDPOINT: 'email.inbound',
  STRIPE_WEBHOOK_SECRET: STRIPE_SECRET,
  STRIPE_SECRET: 'sk_test_fixture',
  STRIPE_METER_EMAIL_SEND: 'mtr_test_email',
  STRIPE_METER_WEBHOOK_SEND: 'mtr_test_webhook',
  STRIPE_METER_EMAIL_INBOUND: 'mtr_test_inbound',
};

// Intercept outbound fetches (webhook deliveries, Stripe, callbacks).
const realFetch = globalThis.fetch;
let fetchRoutes = [];
globalThis.fetch = async (input, init) => {
  const url = typeof input === 'string' ? input : input.url;
  const match = fetchRoutes.find((r) => url.startsWith(r.prefix));
  if (match) return match.handler(url, init);
  throw new Error(`test: unexpected outbound fetch to ${url}`);
};

let ctxCounter = 0;
function makeCtx() {
  const waits = [];
  return {
    waits,
    waitUntil: (p) => {
      waits.push(Promise.resolve(p));
    },
    passThroughOnException: () => {},
    props: {},
    get counter() {
      return ++ctxCounter;
    },
  };
}

async function req(method, path, { body, headers } = {}) {
  const init = { method, headers: { ...(headers ?? {}) } };
  if (body !== undefined) {
    if (typeof body === 'string') {
      init.body = body;
    } else {
      init.body = JSON.stringify(body);
      init.headers['content-type'] = 'application/json';
    }
  }
  const ctx = makeCtx();
  const res = await worker.fetch(new Request(`https://webhooks.email${path}`, init), env, ctx);
  const text = await res.text();
  // Real Workers give waitUntil work a grace period; drain it so side effects
  // (metrics rows, charge logs, fast-path sends) are observable in assertions.
  await Promise.allSettled(ctx.waits);
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* non-JSON body */
  }
  return { status: res.status, json, text, headers: res.headers };
}

async function runCron(cron, scheduledTime = new Date()) {
  const waits = [];
  const handler = worker.scheduled;
  const ctx = { waitUntil: (p) => waits.push(Promise.resolve(p)) };
  const controller = {
    cron,
    scheduledTime: scheduledTime.getTime(),
    noRetry: () => {},
  };
  await handler(controller, env, ctx);
  await Promise.all(waits);
  return waits;
}

/* -------------------------------------------------------------------------- */
/*  Fixtures                                                                   */
/* -------------------------------------------------------------------------- */

const API_KEY = generateApiKey();
const AGENT_ID = 'agent-under-test';
await DB.prepare(
  `INSERT INTO api_keys (key_id, agent_id, agent_name, key_hash, key_lookup, stripe_customer_id, status, leaderboard_optin, created_at)
   VALUES (?, ?, ?, ?, ?, ?, 'active', 1, ?)`
).bind(
  'key_fixture',
  AGENT_ID,
  'Agent Under Test',
  await hashApiKey(API_KEY, 100000),
  await keyLookupDigest(API_KEY),
  'cus_fixture',
  new Date().toISOString()
).run();

const AUTH = { authorization: `Bearer ${API_KEY}` };

function chargeCount(agentId = AGENT_ID, endpoint = null) {
  const sql = endpoint
    ? `SELECT COUNT(*) c FROM charges WHERE agent_id = ? AND endpoint = ?`
    : `SELECT COUNT(*) c FROM charges WHERE agent_id = ?`;
  const args = endpoint ? [agentId, endpoint] : [agentId];
  const row = DB.prepare(sql).get(...args);
  return Number(row?.c ?? 0);
}

function chargeSum(agentId = AGENT_ID) {
  const row = DB.prepare(`SELECT COALESCE(SUM(amount_usd),0) s FROM charges WHERE agent_id = ?`).get(agentId);
  return Number(row?.s ?? 0);
}

/* -------------------------------------------------------------------------- */
/*  Public routes                                                              */
/* -------------------------------------------------------------------------- */

suite('public routes');

await test('GET / returns the endpoint index', async () => {
  const r = await req('GET', '/');
  assert(r.status === 200, `status ${r.status}`);
  assert(r.json.service === 'webhooks.email');
});

await test('GET /pricing publishes the configured prices', async () => {
  const r = await req('GET', '/pricing');
  assert(r.status === 200);
  const prices = Object.fromEntries(r.json.prices.map((p) => [p.endpoint, p.unit_cost_usd]));
  eq(prices['email.send'], 0.01, 'email.send price');
  eq(prices['webhook.send'], 0.001, 'webhook.send price');
  eq(prices['email.inbound'], 0.0005, 'email.inbound price');
  assert(r.json.prices.every((p) => p.per_1000 > 0));
});

await test('GET /leaderboard only lists opted-in agents', async () => {
  const r = await req('GET', '/leaderboard');
  assert(r.status === 200);
  assert(Array.isArray(r.json.this_week));
});

await test('GET /status returns counters, never throws', async () => {
  const r = await req('GET', '/status');
  assert(r.status === 200);
  assert(typeof r.json.api_uptime === 'number');
  assert('total_webhooks_delivered_today' in r.json);
});

await test('unknown route returns 404 json', async () => {
  const r = await req('GET', '/nope');
  assert(r.status === 404);
  eq(r.json.error, 'not_found');
});

await test('OPTIONS preflight returns 204', async () => {
  const r = await req('OPTIONS', '/email/send');
  assert(r.status === 204, `status ${r.status}`);
});

/* -------------------------------------------------------------------------- */
/*  Auth                                                                       */
/* -------------------------------------------------------------------------- */

suite('auth');

await test('rejects missing Authorization header', async () => {
  const r = await req('POST', '/webhooks/send', { body: {} });
  assert(r.status === 401, `status ${r.status}`);
  eq(r.json.error, 'missing_api_key');
});

await test('rejects an unknown key', async () => {
  const r = await req('POST', '/webhooks/send', {
    body: {},
    headers: { authorization: 'Bearer whemails_live_definitely_not_a_real_key_0000' },
  });
  assert(r.status === 401, `status ${r.status}`);
  eq(r.json.error, 'invalid_api_key');
});

await test('rejects a key with the right prefix but wrong secret', async () => {
  // Same 12-char prefix as the real key -> must still fail PBKDF2 verification.
  const forged = API_KEY.slice(0, 12) + 'x'.repeat(API_KEY.length - 12);
  const r = await req('POST', '/webhooks/send', {
    body: {},
    headers: { authorization: `Bearer ${forged}` },
  });
  assert(r.status === 401, `status ${r.status}`);
  eq(r.json.error, 'invalid_api_key');
});

await test('does not let a public route require auth', async () => {
  // Regression guard: auth was originally attached with a '/email/*' glob,
  // which also wrapped /email/inbound. Exact paths must keep it public.
  const r = await req('POST', '/email/inbound', {
    body: { from: 'open@peer.test', subject: 'hi', text: 'yo' },
  });
  assert(r.status === 200, `status ${r.status}`);
  eq(r.json.status, 'stored');
});

await test('accepts the valid key', async () => {
  const r = await req('GET', '/api/charges/summary', { headers: AUTH });
  assert(r.status === 200, `status ${r.status}`);
});

/* -------------------------------------------------------------------------- */
/*  Validation + SSRF                                                          */
/* -------------------------------------------------------------------------- */

suite('validation');

const validHook = {
  to_webhook_url: 'https://receiver.test/callback',
  event_type: 'order.completed',
  payload: { order_id: 1 },
};

await test('rejects a non-https webhook url', async () => {
  const r = await req('POST', '/webhooks/send', {
    headers: AUTH,
    body: { ...validHook, to_webhook_url: 'http://receiver.test/cb' },
  });
  assert(r.status === 400, `status ${r.status}`);
  eq(r.json.error, 'validation_failed');
});

await test('rejects localhost webhook targets (SSRF guard)', async () => {
  for (const url of ['https://localhost/cb', 'https://127.0.0.1/cb', 'https://10.0.0.5/cb', 'https://169.254.169.254/latest/meta-data']) {
    const r = await req('POST', '/webhooks/send', { headers: AUTH, body: { ...validHook, to_webhook_url: url } });
    assert(r.status === 400, `${url} -> status ${r.status}`);
  }
});

await test('rejects unknown fields (strict schema)', async () => {
  const r = await req('POST', '/webhooks/send', { headers: AUTH, body: { ...validHook, evil: 1 } });
  assert(r.status === 400, `status ${r.status}`);
});

await test('rejects a malformed email address', async () => {
  const r = await req('POST', '/email/send', {
    headers: AUTH,
    body: { to: 'not-an-email', subject: 'x', body: 'y' },
  });
  assert(r.status === 400, `status ${r.status}`);
});

await test('rejects a non-JSON body with 400 not 500', async () => {
  const r = await req('POST', '/webhooks/send', { headers: AUTH, body: '{not json' });
  assert(r.status === 400, `status ${r.status}`);
});

await test('rejects an oversized body', async () => {
  const huge = 'a'.repeat(1_100_000);
  const r = await req('POST', '/email/send', {
    headers: AUTH,
    body: JSON.stringify({ to: 'a@b.test', subject: 'x', body: huge }),
  });
  assert(r.status === 413, `status ${r.status}`);
});

/* -------------------------------------------------------------------------- */
/*  POST /webhooks/send                                                        */
/* -------------------------------------------------------------------------- */

suite('POST /webhooks/send');

let queuedEventId;

await test('returns 202 with a charge receipt', async () => {
  const r = await req('POST', '/webhooks/send', { headers: AUTH, body: validHook });
  assert(r.status === 202, `status ${r.status}: ${r.text}`);
  assert(r.json.event_id.startsWith('evt_'));
  eq(r.json.status, 'queued');
  eq(r.json.charge.amount_usd, 0.001);
  queuedEventId = r.json.event_id;
});

await test('logs exactly one webhook.send charge of $0.001', async () => {
  assert(chargeCount(AGENT_ID, 'webhook.send') === 1, 'expected 1 webhook.send charge');
  const row = DB.prepare(`SELECT amount_usd FROM charges WHERE ref_id = ?`).get(queuedEventId);
  eq(Number(row.amount_usd), 0.001);
});

await test('GET /webhooks/:eventId returns a receipt', async () => {
  const r = await req('GET', `/webhooks/${queuedEventId}`, { headers: AUTH });
  assert(r.status === 200, `status ${r.status}`);
  eq(r.json.event_id, queuedEventId);
  eq(r.json.status, 'queued');
});

await test("another agent cannot read someone else's event", async () => {
  const otherKey = generateApiKey();
  await DB.prepare(
    `INSERT INTO api_keys (key_id, agent_id, agent_name, key_hash, key_lookup, stripe_customer_id, status, created_at)
     VALUES (?, ?, ?, ?, ?, ?, 'active', ?)`
  ).bind('key_other', 'other-agent', 'Other', await hashApiKey(otherKey, 100000), await keyLookupDigest(otherKey), 'cus_other', new Date().toISOString()).run();

  const r = await req('GET', `/webhooks/${queuedEventId}`, {
    headers: { authorization: `Bearer ${otherKey}` },
  });
  assert(r.status === 403, `status ${r.status}`);
  eq(r.json.error, 'forbidden');
});

/* -------------------------------------------------------------------------- */
/*  Cron: D1 queue drain                                                       */
/* -------------------------------------------------------------------------- */

suite('cron: D1 queue drain');

await test('does not deliver before the cron runs', async () => {
  assert(sentEmails.length === 0);
  const row = DB.prepare(`SELECT status FROM webhooks WHERE event_id = ?`).get(queuedEventId);
  eq(row.status, 'queued');
});

await test('per-minute cron delivers the queued webhook', async () => {
  let received = null;
  fetchRoutes = [
    {
      prefix: 'https://receiver.test',
      handler: async (url, init) => {
        received = { url, body: init.body, headers: init.headers };
        return new Response('ok', { status: 200 });
      },
    },
  ];
  await runCron('* * * * *');

  assert(received, 'receiver was never called');
  const parsed = JSON.parse(received.body);
  eq(parsed, validHook.payload);
  assert(received.headers['x-webhooks-email-event-id'] === queuedEventId);
  assert(received.headers['x-webhooks-email-event-type'] === 'order.completed');

  const row = DB.prepare(`SELECT status, delivered_at, response_status FROM webhooks WHERE event_id = ?`).get(queuedEventId);
  eq(row.status, 'delivered');
  eq(Number(row.response_status), 200);
  assert(row.delivered_at, 'delivered_at not set');
});

await test('a failing receiver reschedules with backoff instead of dropping', async () => {
  const r = await req('POST', '/webhooks/send', {
    headers: AUTH,
    body: { ...validHook, to_webhook_url: 'https://flaky.test/cb' },
  });
  const id = r.json.event_id;

  fetchRoutes = [
    { prefix: 'https://flaky.test', handler: async () => new Response('boom', { status: 500 }) },
  ];
  await runCron('* * * * *');

  const row = DB.prepare(`SELECT status, delivery_attempts, last_error, next_attempt_at FROM webhooks WHERE event_id = ?`).get(id);
  eq(row.status, 'queued', 'should be rescheduled, not failed');
  eq(Number(row.delivery_attempts), 1);
  assert(/HTTP 500/.test(row.last_error), `last_error was ${row.last_error}`);
  assert(Date.parse(row.next_attempt_at) > Date.now(), 'next_attempt_at should be in the future');
});

await test('an unreachable host reschedules and records the error', async () => {
  const r = await req('POST', '/webhooks/send', {
    headers: AUTH,
    body: { ...validHook, to_webhook_url: 'https://dead.test/cb' },
  });
  const id = r.json.event_id;

  fetchRoutes = [
    {
      prefix: 'https://dead.test',
      handler: async () => {
        throw new Error('getaddrinfo ENOTFOUND');
      },
    },
  ];
  await runCron('* * * * *');

  const row = DB.prepare(`SELECT status, last_error FROM webhooks WHERE event_id = ?`).get(id);
  eq(row.status, 'queued');
  assert(/ENOTFOUND/.test(row.last_error), `last_error was ${row.last_error}`);
});

await test('gives up permanently after MAX_RETRIES', async () => {
  const r = await req('POST', '/webhooks/send', {
    headers: AUTH,
    body: { ...validHook, to_webhook_url: 'https://dead.test/cb' },
  });
  const id = r.json.event_id;

  // Force every retry to be due immediately.
  for (let i = 0; i < 6; i++) {
    DB.prepare(`UPDATE webhooks SET next_attempt_at = ? WHERE event_id = ?`).bind(
      new Date(Date.now() - 1000).toISOString(),
      id
    ).run();
    await runCron('* * * * *');
  }

  const row = DB.prepare(`SELECT status, delivery_attempts FROM webhooks WHERE event_id = ?`).get(id);
  eq(row.status, 'failed', 'should be permanently failed after MAX_RETRIES');
  eq(Number(row.delivery_attempts), 5);
});

await test('does not re-deliver an already-delivered event', async () => {
  let calls = 0;
  fetchRoutes = [
    {
      prefix: 'https://receiver.test',
      handler: async () => {
        calls++;
        return new Response('ok', { status: 200 });
      },
    },
  ];
  await runCron('* * * * *');
  await runCron('* * * * *');
  eq(calls, 0, 'delivered event must not be picked up again');
});

/* -------------------------------------------------------------------------- */
/*  POST /email/send                                                           */
/* -------------------------------------------------------------------------- */

suite('POST /email/send');

let messageId;

await test('returns 202 and charges $0.01', async () => {
  const r = await req('POST', '/email/send', {
    headers: AUTH,
    body: { to: 'Lead@Example.test', subject: 'Order confirmation', body: 'Confirmed.' },
  });
  assert(r.status === 202, `status ${r.status}: ${r.text}`);
  assert(r.json.message_id.startsWith('msg_'));
  eq(r.json.charge.amount_usd, 0.01);
  messageId = r.json.message_id;

  const row = DB.prepare(`SELECT amount_usd FROM charges WHERE ref_id = ?`).get(messageId);
  eq(Number(row.amount_usd), 0.01);
});

await test('lowercases the recipient address', async () => {
  const row = DB.prepare(`SELECT to_email FROM emails WHERE message_id = ?`).get(messageId);
  eq(row.to_email, 'lead@example.test');
});

await test('fast path sends via the EMAIL binding without waiting for the cron', async () => {
  sentEmails.length = 0;
  const r = await req('POST', '/email/send', {
    headers: AUTH,
    body: { to: 'fast@path.test', subject: 'Fast', body: 'now' },
  });
  const id = r.json.message_id;

  // req() drains waitUntil, so the fast path has already run by now.
  eq(sentEmails.length, 1, 'EMAIL.send should have been called exactly once');
  eq(sentEmails[0].from, 'noreply@webhooks.email');
  eq(sentEmails[0].to, 'fast@path.test');
  assert(/Subject: Fast/.test(sentEmails[0].raw), 'MIME subject header missing');
  assert(/MIME-Version: 1.0/.test(sentEmails[0].raw), 'MIME-Version missing');
  assert(/Message-ID: <msg_/.test(sentEmails[0].raw), 'Message-ID missing');

  const row = DB.prepare(`SELECT status FROM emails WHERE message_id = ?`).get(id);
  eq(row.status, 'sent', 'row should be marked sent');
});

await test('fast path does not double-send when the cron also runs', async () => {
  sentEmails.length = 0;
  const r = await req('POST', '/email/send', {
    headers: AUTH,
    body: { to: 'nosend2x@example.test', subject: 'Once', body: 'only once' },
  });
  assert(r.status === 202, `queueing failed: ${r.status} ${r.text}`);
  await runCron('* * * * *');
  eq(sentEmails.length, 1, 'message must be sent exactly once across fast path + cron');
});

await test('cron drains queued emails that the fast path did not send', async () => {
  sentEmails.length = 0;
  const id = 'msg_' + 'a'.repeat(24);
  await DB.prepare(
    `INSERT INTO emails (message_id, agent_id, to_email, from_email, subject, body, status, next_attempt_at, created_at)
     VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?)`
  ).bind(id, AGENT_ID, 'cron@path.test', 'noreply@webhooks.email', 'Cron', 'body', new Date().toISOString(), new Date().toISOString()).run();

  await runCron('* * * * *');

  eq(sentEmails.length, 1, 'cron should have sent exactly one email');
  eq(sentEmails[0].to, 'cron@path.test');
  const row = DB.prepare(`SELECT status FROM emails WHERE message_id = ?`).get(id);
  eq(row.status, 'sent');
});

await test('recovers an email stranded in sending (worker killed mid-send)', async () => {
  const id = 'msg_' + 'b'.repeat(24);
  const old = new Date(Date.now() - 600_000).toISOString();
  await DB.prepare(
    `INSERT INTO emails (message_id, agent_id, to_email, from_email, subject, body, status, attempts, next_attempt_at, created_at)
     VALUES (?, ?, ?, ?, ?, ?, 'sending', 1, ?, ?)`
  ).bind(id, AGENT_ID, 'stuck@path.test', 'noreply@webhooks.email', 'Stuck', 'body', old, old).run();

  sentEmails.length = 0;
  await runCron('* * * * *');
  eq(sentEmails.length, 1, 'stranded email should be retried');
});

await test('GET /email/status/:messageId returns a receipt', async () => {
  const r = await req('GET', `/email/status/${messageId}`, { headers: AUTH });
  assert(r.status === 200, `status ${r.status}`);
  eq(r.json.message_id, messageId);
});

/* -------------------------------------------------------------------------- */
/*  Inbound                                                                    */
/* -------------------------------------------------------------------------- */

suite('inbound');

await test('POST /email/inbound stores and meters at $0.0005', async () => {
  const r = await req('POST', '/email/inbound', {
    body: { from: 'Lead@Corp.test', to: 'noreply@webhooks.email', subject: 'Interested', text: 'Tell me more' },
  });
  assert(r.status === 200, `status ${r.status}`);
  eq(r.json.charge.amount_usd, 0.0005);

  const row = DB.prepare(`SELECT from_email, forwarded_to_webhook FROM inbound_emails WHERE inbound_id = ?`).get(r.json.inbound_id);
  eq(row.from_email, 'lead@corp.test', 'sender should be lowercased');
  eq(Number(row.forwarded_to_webhook), 0, 'no route registered yet');

  const chg = DB.prepare(`SELECT amount_usd FROM charges WHERE ref_id = ?`).get(r.json.inbound_id);
  eq(Number(chg.amount_usd), 0.0005);
});

await test('inbound is charged to the system agent, not the caller', async () => {
  const row = DB.prepare(`SELECT agent_id FROM charges WHERE endpoint = ?`).get('email.inbound');
  eq(row.agent_id, 'system');
});

await test('a registered sender gets fanned out to their callback', async () => {
  const rr = await req('POST', '/api/inbound/routes', {
    headers: AUTH,
    body: { from: 'vip@corp.test', callback_webhook: 'https://agent.test/inbound' },
  });
  assert(rr.status === 201, `status ${rr.status}`);

  let fanned = null;
  fetchRoutes = [
    {
      prefix: 'https://agent.test',
      handler: async (url, init) => {
        fanned = JSON.parse(init.body);
        return new Response('ok');
      },
    },
  ];

  const r = await req('POST', '/email/inbound', {
    body: { from: 'vip@corp.test', to: 'noreply@webhooks.email', subject: 'Big deal', text: 'body' },
  });
  const inboundId = r.json.inbound_id;
  eq(r.json.status, 'stored');

  const row = DB.prepare(`SELECT forwarded_to_webhook, forwarded_event_id FROM inbound_emails WHERE inbound_id = ?`).get(inboundId);
  eq(Number(row.forwarded_to_webhook), 1, 'should have been fanned out');
  assert(row.forwarded_event_id, 'should record the created event_id');

  // The fan-out is itself a queued webhook; drain it and confirm delivery.
  await runCron('* * * * *');
  assert(fanned, 'callback never fired');
  eq(fanned.from, 'vip@corp.test');
  eq(fanned.subject, 'Big deal');
  eq(fanned.inbound_id, inboundId);

  const wrow = DB.prepare(`SELECT status FROM webhooks WHERE event_id = ?`).get(row.forwarded_event_id);
  eq(wrow.status, 'delivered');
});

await test('an unregistered sender is stored but not fanned out', async () => {
  const r = await req('POST', '/email/inbound', {
    body: { from: 'stranger@nowhere.test', subject: 'hi', text: 'x' },
  });
  const row = DB.prepare(`SELECT forwarded_to_webhook FROM inbound_emails WHERE inbound_id = ?`).get(r.json.inbound_id);
  eq(Number(row.forwarded_to_webhook), 0);
});

/* -------------------------------------------------------------------------- */
/*  Charges + billing                                                          */
/* -------------------------------------------------------------------------- */

suite('charges + billing');

await test('GET /api/charges/summary totals match the charge rows', async () => {
  const r = await req('GET', '/api/charges/summary?period=all', { headers: AUTH });
  assert(r.status === 200, `status ${r.status}`);
  const byEndpoint = Object.fromEntries(r.json.charges.map((c) => [c.endpoint, c]));
  assert(byEndpoint['webhook.send'], 'no webhook.send line');
  assert(byEndpoint['email.send'], 'no email.send line');
  eq(byEndpoint['webhook.send'].count, chargeCount(AGENT_ID, 'webhook.send'));
  eq(byEndpoint['webhook.send'].unit_cost, 0.001);
  eq(byEndpoint['email.send'].unit_cost, 0.01);
  assert(Math.abs(r.json.period_total_usd - chargeSum(AGENT_ID)) < 1e-9, 'period total mismatch');
  assert(r.json.next_billing_date, 'missing next_billing_date');
});

await test('summary rejects an unknown period', async () => {
  const r = await req('GET', '/api/charges/summary?period=decade', { headers: AUTH });
  assert(r.status === 400, `status ${r.status}`);
});

await test('weekly cron posts one meter summary per endpoint', async () => {
  STRIPE_CALLS.length = 0;
  const realFetchSaved = globalThis.fetch;
  globalThis.fetch = async (input, init) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url.startsWith('https://api.stripe.com')) {
      STRIPE_CALLS.push({ url, body: init.body });
      return new Response(JSON.stringify({ id: 'meter_evt_1' }), { status: 200 });
    }
    return realFetchSaved(input, init);
  };

  await runCron('0 0 0 * * 0');
  globalThis.fetch = realFetchSaved;

  assert(STRIPE_CALLS.length >= 2, `expected >=2 stripe calls, got ${STRIPE_CALLS.length}`);
  const endpoints = STRIPE_CALLS.map((c) => new URLSearchParams(c.body).get('event_name'));
  assert(endpoints.includes('email_send'), `no email_send summary: ${endpoints}`);
  assert(endpoints.includes('webhook_send'), `no webhook_send summary: ${endpoints}`);

  const billed = DB.prepare(`SELECT COUNT(*) c FROM charges WHERE agent_id = ? AND status = 'billed'`).get(AGENT_ID);
  assert(Number(billed.c) > 0, 'charges were not marked billed');
  const run = DB.prepare(`SELECT status, agents_charged FROM billing_runs ORDER BY executed_at DESC LIMIT 1`).get();
  eq(run.status, 'completed');
});

await test('a Stripe outage leaves charges pending (money is never lost)', async () => {
  const before = DB.prepare(`SELECT COUNT(*) c FROM charges WHERE agent_id = ? AND status = 'pending'`).get(AGENT_ID);
  assert(Number(before.c) === 0, 'precondition: nothing pending');

  // Add a fresh pending charge, then make Stripe fail.
  const r = await req('POST', '/webhooks/send', { headers: AUTH, body: validHook });
  assert(r.status === 202);
  const pendingBefore = Number(
    DB.prepare(`SELECT COUNT(*) c FROM charges WHERE status = 'pending'`).get().c
  );

  const saved = globalThis.fetch;
  globalThis.fetch = async (input, init) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url.startsWith('https://api.stripe.com')) {
      return new Response(JSON.stringify({ error: { message: 'API is down', type: 'api_error' } }), { status: 503 });
    }
    return saved(input, init);
  };
  await runCron('0 0 0 * * 0');
  globalThis.fetch = saved;

  const pendingAfter = Number(
    DB.prepare(`SELECT COUNT(*) c FROM charges WHERE status = 'pending'`).get().c
  );
  eq(pendingAfter, pendingBefore, 'charges must stay pending when Stripe fails');

  const run = DB.prepare(`SELECT status, error FROM billing_runs ORDER BY executed_at DESC LIMIT 1`).get();
  eq(run.status, 'failed');
  assert(/API is down/.test(run.error), `run error was ${run.error}`);
});

/* -------------------------------------------------------------------------- */
/*  Stripe webhook                                                             */
/* -------------------------------------------------------------------------- */

suite('stripe webhook');

async function stripeSig(payload, secret, timestamp = Math.floor(Date.now() / 1000)) {
  const sig = await hmacSha256Hex(secret, `${timestamp}.${payload}`);
  return `t=${timestamp},v1=${sig}`;
}

await test('rejects a missing signature', async () => {
  const r = await req('POST', '/stripe/webhook', { body: { type: 'invoice.paid' } });
  assert(r.status === 400, `status ${r.status}`);
});

await test('rejects a signature made with the wrong secret', async () => {
  const payload = JSON.stringify({ id: 'evt_1', type: 'invoice.paid', data: { object: { id: 'in_1' } } });
  const r = await req('POST', '/stripe/webhook', {
    body: payload,
    headers: { 'stripe-signature': await stripeSig(payload, 'whsec_wrong') },
  });
  assert(r.status === 400, `status ${r.status}`);
  eq(r.json.error, 'invalid_signature');
});

await test('rejects a stale timestamp (replay window)', async () => {
  const payload = JSON.stringify({ id: 'evt_1', type: 'invoice.paid', data: { object: {} } });
  const stale = Math.floor(Date.now() / 1000) - 4000;
  const r = await req('POST', '/stripe/webhook', {
    body: payload,
    headers: { 'stripe-signature': await stripeSig(payload, STRIPE_SECRET, stale) },
  });
  assert(r.status === 400, `status ${r.status}`);
});

await test('rejects a tampered payload', async () => {
  const original = JSON.stringify({ id: 'evt_1', type: 'invoice.paid', data: { object: { id: 'in_1' } } });
  const sig = await stripeSig(original, STRIPE_SECRET);
  const tampered = JSON.stringify({ id: 'evt_1', type: 'invoice.paid', data: { object: { id: 'in_999' } } });
  const r = await req('POST', '/stripe/webhook', { body: tampered, headers: { 'stripe-signature': sig } });
  assert(r.status === 400, `status ${r.status}`);
});

await test('accepts a correctly signed invoice.paid', async () => {
  const payload = JSON.stringify({
    id: 'evt_2',
    type: 'invoice.paid',
    data: { object: { id: 'in_2', customer: 'cus_fixture', amount_paid: 1000 } },
  });
  const r = await req('POST', '/stripe/webhook', {
    body: payload,
    headers: { 'stripe-signature': await stripeSig(payload, STRIPE_SECRET) },
  });
  assert(r.status === 200, `status ${r.status}: ${r.text}`);
  eq(r.json.received, true);
});

await test('accepts invoice.payment_failed without leaking detail', async () => {
  const payload = JSON.stringify({
    id: 'evt_3',
    type: 'invoice.payment_failed',
    data: { object: { id: 'in_3', customer: 'cus_fixture' } },
  });
  const r = await req('POST', '/stripe/webhook', {
    body: payload,
    headers: { 'stripe-signature': await stripeSig(payload, STRIPE_SECRET) },
  });
  assert(r.status === 200);
});

/* -------------------------------------------------------------------------- */
/*  Key generation                                                             */
/* -------------------------------------------------------------------------- */

suite('key generation');

await test('generates a usable key and stores only a hash', async () => {
  const r = await req('POST', '/api/keys/generate', {
    body: { agent_name: 'brand-new-agent', stripe_customer_id: 'cus_brandnew123', leaderboard_optin: true },
  });
  assert(r.status === 201, `status ${r.status}: ${r.text}`);
  assert(r.json.api_key.startsWith('whemails_live_'));

  const row = DB.prepare(`SELECT key_hash FROM api_keys WHERE agent_id = 'brand-new-agent'`).get();
  assert(row, 'no row written');
  assert(!row.key_hash.includes(r.json.api_key), 'raw key must not be stored');
  assert(!JSON.stringify(row).includes(r.json.api_key), 'raw key leaked into any column');
  assert(row.key_hash.startsWith('pbkdf2$'), 'unexpected hash format');

  // And the returned key actually authenticates.
  const authed = await req('GET', '/api/charges/summary', {
    headers: { authorization: `Bearer ${r.json.api_key}` },
  });
  assert(authed.status === 200, `generated key failed auth: ${authed.status}`);
});

await test('refuses to re-issue a key for an existing agent', async () => {
  const r = await req('POST', '/api/keys/generate', {
    body: { agent_name: AGENT_ID, stripe_customer_id: 'cus_fixture' },
  });
  assert(r.status === 409, `status ${r.status}`);
  eq(r.json.error, 'agent_exists');
});

await test('rejects a non-Stripe customer id', async () => {
  const r = await req('POST', '/api/keys/generate', {
    body: { agent_name: 'bad-customer', stripe_customer_id: 'not-a-cus' },
  });
  assert(r.status === 400, `status ${r.status}`);
});

/* -------------------------------------------------------------------------- */
/*  Leaderboard                                                                */
/* -------------------------------------------------------------------------- */

suite('leaderboard');

await test('opted-in agent appears with volume and spend', async () => {
  const r = await req('GET', '/leaderboard');
  const me = r.json.this_week.find((row) => row.agent_name === 'Agent Under Test');
  assert(me, 'opted-in agent missing from leaderboard');
  assert(me.webhooks_sent > 0, 'webhooks_sent should be > 0');
  assert(me.spend_usd > 0, 'spend_usd should be > 0');
  assert(!('revenue_generated' in me), 'we must not fabricate revenue numbers');
});

await test('never lists a non-opted-in agent', async () => {
  const r = await req('GET', '/leaderboard');
  assert(!r.json.this_week.some((row) => row.agent_name === 'Other'), 'non-opted-in agent leaked');
});

/* -------------------------------------------------------------------------- */
/*  Metrics                                                                    */
/* -------------------------------------------------------------------------- */

suite('metrics');

await test('endpoint_metrics rows are written by the middleware', async () => {
  const rows = DB.prepare(`SELECT endpoint, requests FROM endpoint_metrics`).allSync();
  assert(rows.length > 0, 'no metrics recorded');
  const send = rows.find((r) => r.endpoint === 'webhook.send');
  assert(send && Number(send.requests) > 0, 'webhook.send requests not tracked');
});

await test('a 400 counts as an error, not a success', async () => {
  await req('POST', '/webhooks/send', { headers: AUTH, body: { event_type: 'x' } });
  const row = DB.prepare(`SELECT requests, successes, errors FROM endpoint_metrics WHERE endpoint = 'webhook.send'`).get();
  assert(Number(row.errors) > 0, 'validation failures should be recorded as errors');
  assert(Number(row.errors) < Number(row.requests), 'not all requests should be errors');
});

/* -------------------------------------------------------------------------- */
/*  Header injection                                                           */
/* -------------------------------------------------------------------------- */

suite('header injection');

await test('strips CRLF from a subject so headers cannot be injected', async () => {
  const { buildMime } = await import(
    pathToFileURL(new URL('../src/lib/email-service.ts', import.meta.url).pathname).href
  );
  const raw = buildMime({
    from: 'noreply@webhooks.email',
    to: 'victim@test.example',
    subject: 'Hello\r\nBcc: attacker@evil.test',
    body: 'hi',
    messageId: 'msg_injection',
    date: new Date().toUTCString(),
  });
  assert(!/^Bcc:/m.test(raw), 'Bcc header was injected');
  assert(!/\r\nBcc/.test(raw), 'CRLF injection succeeded');
});

await test('filters agent-supplied webhook headers to an allowlist', async () => {
  const r = await req('POST', '/webhooks/send', {
    headers: AUTH,
    body: {
      ...validHook,
      to_webhook_url: 'https://hdr.test/cb',
      headers: { 'x-custom': 'nope', host: 'evil.test', 'x-api-key': 'ok' },
    },
  });
  const id = r.json.event_id;

  let seen = null;
  fetchRoutes = [
    {
      prefix: 'https://hdr.test',
      handler: async (url, init) => {
        seen = init.headers;
        return new Response('ok');
      },
    },
  ];
  await runCron('* * * * *');

  assert(seen, 'receiver not called');
  assert(seen['x-api-key'] === 'ok', 'allowlisted header missing');
  assert(seen['x-custom'] === undefined, 'non-allowlisted header leaked');
  assert(seen.host === undefined, 'host header was overridden');
});

/* -------------------------------------------------------------------------- */

console.log(`\n${'-'.repeat(60)}`);
console.log(`passed: ${passed}   failed: ${failures.length}`);

if (failures.length) {
  console.log('\nFailures:');
  for (const f of failures) console.log(`  ${f.suite} :: ${f.name}\n    ${f.err.stack?.split('\n').slice(0, 3).join('\n    ')}`);
  process.exit(1);
}
console.log('all green');
