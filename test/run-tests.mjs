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
const { WEEKLY_BILLING_CRON, DRAIN_CRON } = await import(
  pathToFileURL(new URL('../src/config/schedules.ts', import.meta.url).pathname).href
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
  KEYS_PER_IP_PER_DAY: '5',
  SEND_PER_AGENT_PER_MINUTE: '60',
  INBOUND_PER_ROUTE_PER_DAY: '1000',
  INBOUND_WILDCARD_PER_DAY: '200',
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
/*  Cron: schedule alignment                                                   */
/* -------------------------------------------------------------------------- */

suite('cron: schedule alignment');

await test('wrangler.jsonc crons are all handled by scheduled()', async () => {
  const raw = readFileSync(new URL('../wrangler.jsonc', import.meta.url), 'utf8');
  const wrangler = JSON.parse(
    raw.replace(/^\s*\/\/.*$/gm, '').replace(/,(\s*[}\]])/g, '$1')
  );
  const crons = wrangler.triggers.crons;
  assert(Array.isArray(crons), 'triggers.crons missing');

  assert(
    crons.includes(WEEKLY_BILLING_CRON),
    `WEEKLY_BILLING_CRON (${WEEKLY_BILLING_CRON}) not in wrangler crons: ${crons}`
  );
  // scheduled() has exactly two branches: weekly billing and the drain. Every
  // configured cron must map to one of them, or a tick would be handled blindly.
  for (const cron of crons) {
    assert(
      cron === WEEKLY_BILLING_CRON || cron === DRAIN_CRON,
      `cron ${cron} is not handled by scheduled()`
    );
  }
});

await test('billing cron runs billing, not the drain', async () => {
  const id = 'msg_' + 'c'.repeat(24);
  await DB.prepare(
    `INSERT INTO emails (message_id, agent_id, to_email, from_email, subject, body, status, next_attempt_at, created_at)
     VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?)`
  ).bind(id, AGENT_ID, 'sched@path.test', 'noreply@webhooks.email', 'Sched', 'body', new Date().toISOString(), new Date().toISOString()).run();

  sentEmails.length = 0;
  const runsBefore = Number(DB.prepare(`SELECT COUNT(*) c FROM billing_runs`).get().c);
  await runCron(WEEKLY_BILLING_CRON);
  eq(sentEmails.length, 0, 'billing cron must not drain the email queue');
  eq(DB.prepare(`SELECT status FROM emails WHERE message_id = ?`).get(id).status, 'queued');
  assert(
    Number(DB.prepare(`SELECT COUNT(*) c FROM billing_runs`).get().c) === runsBefore + 1,
    'billing cron should have recorded a billing run'
  );

  await runCron(DRAIN_CRON);
  eq(sentEmails.length, 1, 'drain cron should have sent the queued email');
  eq(DB.prepare(`SELECT status FROM emails WHERE message_id = ?`).get(id).status, 'sent');
  sentEmails.length = 0;
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

await test('POST /email/inbound stores unrouted mail without charging', async () => {
  const r = await req('POST', '/email/inbound', {
    body: { from: 'Lead@Corp.test', to: 'noreply@webhooks.email', subject: 'Interested', text: 'Tell me more' },
  });
  assert(r.status === 200, `status ${r.status}`);

  const row = DB.prepare(`SELECT from_email, agent_id, forwarded_to_webhook FROM inbound_emails WHERE inbound_id = ?`).get(r.json.inbound_id);
  eq(row.from_email, 'lead@corp.test', 'sender should be lowercased');
  eq(row.agent_id, 'system', 'unrouted mail is stored under system');
  eq(Number(row.forwarded_to_webhook), 0, 'no route registered yet');

  const chg = DB.prepare(`SELECT COUNT(*) c FROM charges WHERE ref_id = ?`).get(r.json.inbound_id);
  eq(Number(chg.c), 0, 'unrouted inbound must not be charged');
});

await test('an unrouted sender is stored under system and never charged', async () => {
  const chg = DB.prepare(`SELECT COUNT(*) c FROM charges WHERE endpoint = ? AND agent_id = 'system'`).get('email.inbound');
  eq(Number(chg.c), 0, 'system must never accumulate inbound charges');
});

await test('a registered sender is fanned out and billed to the route owner', async () => {
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
  assert(r.json.forwarded_event_id, 'should return the created event_id');

  const row = DB.prepare(`SELECT agent_id, forwarded_to_webhook, forwarded_event_id FROM inbound_emails WHERE inbound_id = ?`).get(inboundId);
  eq(row.agent_id, AGENT_ID, 'inbound row is attributed to the route owner');
  eq(Number(row.forwarded_to_webhook), 1, 'should have been fanned out');
  assert(row.forwarded_event_id, 'should record the created event_id');

  // Exactly one email.inbound charge, to the owner.
  const chgRows = DB.prepare(`SELECT agent_id, amount_usd FROM charges WHERE ref_id = ?`).allSync(inboundId);
  eq(chgRows.length, 1, 'exactly one charge for the inbound message');
  eq(chgRows[0].agent_id, AGENT_ID);
  eq(Number(chgRows[0].amount_usd), 0.0005);

  // The fan-out webhook is owned by the route owner and is NOT separately charged.
  const wrow = DB.prepare(`SELECT agent_id, status FROM webhooks WHERE event_id = ?`).get(row.forwarded_event_id);
  eq(wrow.agent_id, AGENT_ID, 'fan-out webhook belongs to the route owner');
  const sendCharge = DB.prepare(`SELECT COUNT(*) c FROM charges WHERE ref_id = ? AND endpoint = 'webhook.send'`).get(row.forwarded_event_id);
  eq(Number(sendCharge.c), 0, 'fan-out must not add a webhook.send charge');

  // The fan-out is itself a queued webhook; drain it and confirm delivery.
  await runCron(DRAIN_CRON);
  assert(fanned, 'callback never fired');
  eq(fanned.from, 'vip@corp.test');
  eq(fanned.subject, 'Big deal');
  eq(fanned.inbound_id, inboundId);
  eq(DB.prepare(`SELECT status FROM webhooks WHERE event_id = ?`).get(row.forwarded_event_id).status, 'delivered');
});

await test('an unregistered sender is stored but not fanned out', async () => {
  const r = await req('POST', '/email/inbound', {
    body: { from: 'stranger@nowhere.test', subject: 'hi', text: 'x' },
  });
  const row = DB.prepare(`SELECT agent_id, forwarded_to_webhook FROM inbound_emails WHERE inbound_id = ?`).get(r.json.inbound_id);
  eq(row.agent_id, 'system');
  eq(Number(row.forwarded_to_webhook), 0);
  eq(Number(DB.prepare(`SELECT COUNT(*) c FROM charges WHERE ref_id = ?`).get(r.json.inbound_id).c), 0, 'no charge');
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

  await runCron(WEEKLY_BILLING_CRON);
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
  await runCron(WEEKLY_BILLING_CRON);
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
/*  Inbound billing                                                            */
/* -------------------------------------------------------------------------- */

suite('inbound billing');

const INBOUND_KEY = generateApiKey();
const INBOUND_AGENT = 'inbound-owner';
await DB.prepare(
  `INSERT INTO api_keys (key_id, agent_id, agent_name, key_hash, key_lookup, stripe_customer_id, status, created_at)
   VALUES (?, ?, ?, ?, ?, ?, 'active', ?)`
).bind(
  'key_inbound_owner',
  INBOUND_AGENT,
  'Inbound Owner',
  await hashApiKey(INBOUND_KEY, 100000),
  await keyLookupDigest(INBOUND_KEY),
  'cus_inbound',
  new Date().toISOString()
).run();

const AUTH_IN = { authorization: `Bearer ${INBOUND_KEY}` };

await test('routed inbound charges the owner once and not for the fan-out', async () => {
  const rr = await req('POST', '/api/inbound/routes', {
    headers: AUTH_IN,
    body: { from: 'paid@corp.test', callback_webhook: 'https://inbound-owner.test/cb' },
  });
  assert(rr.status === 201, `route create ${rr.status}`);

  const r = await req('POST', '/email/inbound', {
    body: { from: 'paid@corp.test', to: 'noreply@webhooks.email', subject: 'Pay up', text: 'body' },
  });
  assert(r.status === 200, `status ${r.status}`);
  assert(r.json.forwarded_event_id, 'routed inbound should be forwarded');

  const inboundId = r.json.inbound_id;
  const chg = DB.prepare(`SELECT agent_id, endpoint, amount_usd FROM charges WHERE ref_id = ?`).allSync(inboundId);
  eq(chg.length, 1, 'exactly one charge for a routed inbound');
  eq(chg[0].agent_id, INBOUND_AGENT);
  eq(chg[0].endpoint, 'email.inbound');
  eq(Number(chg[0].amount_usd), 0.0005);

  const wrow = DB.prepare(`SELECT agent_id FROM webhooks WHERE event_id = ?`).get(r.json.forwarded_event_id);
  eq(wrow.agent_id, INBOUND_AGENT, 'fan-out webhook owned by the route owner');
  eq(
    Number(DB.prepare(`SELECT COUNT(*) c FROM charges WHERE ref_id = ? AND endpoint = 'webhook.send'`).get(r.json.forwarded_event_id).c),
    0,
    'fan-out is not separately charged'
  );

  const ok = await req('GET', `/webhooks/${r.json.forwarded_event_id}`, { headers: AUTH_IN });
  assert(ok.status === 200, `owner should read own inbound webhook: ${ok.status}`);
  const bad = await req('GET', `/webhooks/${r.json.forwarded_event_id}`, { headers: AUTH });
  assert(bad.status === 403, `other agent must be forbidden: ${bad.status}`);
});

await test('unrouted inbound stays under system with no charge', async () => {
  const r = await req('POST', '/email/inbound', {
    body: { from: 'orphan@nowhere.test', subject: 'x', text: 'y' },
  });
  const row = DB.prepare(`SELECT agent_id FROM inbound_emails WHERE inbound_id = ?`).get(r.json.inbound_id);
  eq(row.agent_id, 'system');
  eq(Number(DB.prepare(`SELECT COUNT(*) c FROM charges WHERE ref_id = ?`).get(r.json.inbound_id).c), 0);
});

await test('a wildcard route catches any sender and bills its owner', async () => {
  // The routes API validates `from` as an email address, so register the
  // catch-all directly. The DB and fan-out path both support '*'.
  await DB.prepare(
    `INSERT INTO inbound_routes (from_email, agent_id, callback_webhook, created_at)
     VALUES ('*', ?, 'https://wild.test/cb', ?)`
  ).bind(AGENT_ID, new Date().toISOString()).run();

  const r = await req('POST', '/email/inbound', {
    body: { from: 'anyone@random.test', subject: 'wild', text: 'z' },
  });
  assert(r.json.forwarded_event_id, 'wildcard should forward');
  const chg = DB.prepare(`SELECT agent_id, endpoint FROM charges WHERE ref_id = ?`).allSync(r.json.inbound_id);
  eq(chg.length, 1);
  eq(chg[0].agent_id, AGENT_ID);
  eq(chg[0].endpoint, 'email.inbound');
});

await test('weekly billing meters routed inbound as email_inbound for its owner', async () => {
  STRIPE_CALLS.length = 0;
  const saved = globalThis.fetch;
  globalThis.fetch = async (input, init) => {
    const url = typeof input === 'string' ? input : input.url;
    if (url.startsWith('https://api.stripe.com')) {
      STRIPE_CALLS.push({ url, body: init.body });
      return new Response(JSON.stringify({ id: 'meter_evt_inbound' }), { status: 200 });
    }
    return saved(input, init);
  };
  await runCron(WEEKLY_BILLING_CRON);
  globalThis.fetch = saved;

  const inbound = STRIPE_CALLS.map((c) => new URLSearchParams(c.body)).filter(
    (p) => p.get('event_name') === 'email_inbound'
  );
  const mine = inbound.find(
    (p) => p.get('payload[summarized_by][stripe_customer_id]') === 'cus_inbound'
  );
  assert(mine, `no email_inbound summary for the inbound owner: ${inbound.map((p) => p.toString())}`);
  eq(mine.get('payload[value]'), '1', 'owner should be metered exactly one inbound email');
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
  fetchRoutes = [
    {
      prefix: 'https://api.stripe.com/v1/customers/cus_brandnew123',
      handler: async () =>
        new Response(JSON.stringify({ id: 'cus_brandnew123', object: 'customer' }), {
          status: 200,
        }),
    },
  ];

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
/*  Abuse controls                                                             */
/* -------------------------------------------------------------------------- */

suite('abuse controls');

async function insertAgent(keyId, agentId, key, customerId) {
  await DB.prepare(
    `INSERT INTO api_keys (key_id, agent_id, agent_name, key_hash, key_lookup, stripe_customer_id, status, created_at)
     VALUES (?, ?, ?, ?, ?, ?, 'active', ?)`
  ).bind(
    keyId,
    agentId,
    agentId,
    await hashApiKey(key, 100000),
    await keyLookupDigest(key),
    customerId,
    new Date().toISOString()
  ).run();
}

async function deliverRawEmail(raw, from, to = 'inbox@webhooks.email') {
  const waits = [];
  const ctx = {
    waitUntil: (p) => waits.push(Promise.resolve(p)),
    passThroughOnException: () => {},
  };
  const message = {
    from,
    to,
    raw: new Response(raw).body,
    headers: new Headers(),
  };
  await worker.email(message, env, ctx);
  await Promise.allSettled(waits);
}

async function seedLimit(bucket, count) {
  await DB.prepare(
    `INSERT INTO rate_limits (bucket, count, window_start) VALUES (?, ?, ?)
     ON CONFLICT(bucket) DO UPDATE
       SET count = excluded.count, window_start = excluded.window_start`
  ).bind(bucket, count, new Date().toISOString()).run();
}

function rawEmail({ from, to = 'inbox@webhooks.email', subject = 'hello', auth = null }) {
  return [
    `From: ${from}`,
    `To: ${to}`,
    `Subject: ${subject}`,
    ...(auth ? [`Authentication-Results: ${auth}`] : []),
    'Content-Type: text/plain; charset=utf-8',
    '',
    'body text',
  ].join('\r\n');
}

await test('key creation rejects a customer Stripe reports as missing', async () => {
  fetchRoutes = [
    {
      prefix: 'https://api.stripe.com/v1/customers/cus_missing',
      handler: async () =>
        new Response(
          JSON.stringify({ error: { message: 'No such customer', type: 'invalid_request_error' } }),
          { status: 404 }
        ),
    },
  ];

  const r = await req('POST', '/api/keys/generate', {
    headers: { 'cf-connecting-ip': '198.51.100.11' },
    body: { agent_name: 'abuse-unknown-customer', stripe_customer_id: 'cus_missing' },
  });
  assert(r.status === 400, `status ${r.status}: ${r.text}`);
  eq(r.json.error, 'unknown_customer');
  assert(
    !DB.prepare(`SELECT 1 FROM api_keys WHERE agent_id = ?`).get('abuse-unknown-customer'),
    'key must not be created for an unknown customer'
  );
});

await test('a Stripe outage during key creation returns 503 and creates no key', async () => {
  fetchRoutes = [
    {
      prefix: 'https://api.stripe.com/v1/customers/cus_outage',
      handler: async () =>
        new Response(
          JSON.stringify({ error: { message: 'upstream exploded', type: 'api_error' } }),
          { status: 500 }
        ),
    },
  ];

  const r = await req('POST', '/api/keys/generate', {
    headers: { 'cf-connecting-ip': '198.51.100.12' },
    body: { agent_name: 'abuse-outage-customer', stripe_customer_id: 'cus_outage' },
  });
  assert(r.status === 503, `status ${r.status}: ${r.text}`);
  eq(r.json.error, 'billing_unavailable');
  assert(
    !DB.prepare(`SELECT 1 FROM api_keys WHERE agent_id = ?`).get('abuse-outage-customer'),
    'key must not be created when Stripe is unavailable'
  );
});

await test('the 6th key request from one IP in a UTC day returns 429', async () => {
  fetchRoutes = [
    {
      prefix: 'https://api.stripe.com/v1/customers/',
      handler: async (url) =>
        new Response(JSON.stringify({ id: url.split('/').pop(), object: 'customer' }), {
          status: 200,
        }),
    },
  ];

  for (let i = 0; i < 5; i++) {
    const r = await req('POST', '/api/keys/generate', {
      headers: { 'cf-connecting-ip': '198.51.100.13' },
      body: { agent_name: `abuse-key-${i}`, stripe_customer_id: `cus_bulk${i}` },
    });
    assert(r.status === 201, `request ${i + 1} -> ${r.status}: ${r.text}`);
  }

  const limited = await req('POST', '/api/keys/generate', {
    headers: { 'cf-connecting-ip': '198.51.100.13' },
    body: { agent_name: 'abuse-key-5', stripe_customer_id: 'cus_bulk5' },
  });
  assert(limited.status === 429, `status ${limited.status}: ${limited.text}`);
  eq(limited.json.error, 'rate_limited');
  assert(Number(limited.headers.get('retry-after')) > 0, 'missing retry-after');
  assert(
    !DB.prepare(`SELECT 1 FROM api_keys WHERE agent_id = ?`).get('abuse-key-5'),
    'the rate-limited request must not create a key'
  );
});

const ABUSE_KEY = generateApiKey();
const ABUSE_AGENT = 'abuse-sender';
await insertAgent('key_abuse_sender', ABUSE_AGENT, ABUSE_KEY, 'cus_abuse');
const AUTH_ABUSE = { authorization: `Bearer ${ABUSE_KEY}` };

await test('60 sends in one minute succeed and the 61st returns 429 without a charge', async () => {
  const before = chargeCount(ABUSE_AGENT, 'webhook.send');

  for (let i = 0; i < 60; i++) {
    const r = await req('POST', '/webhooks/send', {
      headers: AUTH_ABUSE,
      body: { ...validHook, to_webhook_url: 'https://rate-limited.test/cb' },
    });
    assert(r.status === 202, `send ${i + 1} -> ${r.status}: ${r.text}`);
  }

  await new Promise((resolve) => setTimeout(resolve, 20));
  eq(chargeCount(ABUSE_AGENT, 'webhook.send'), before + 60, 'all 60 sends should be metered');

  const limited = await req('POST', '/webhooks/send', {
    headers: AUTH_ABUSE,
    body: { ...validHook, to_webhook_url: 'https://rate-limited.test/cb' },
  });
  assert(limited.status === 429, `status ${limited.status}: ${limited.text}`);
  eq(limited.json.error, 'rate_limited');
  assert(Number(limited.headers.get('retry-after')) > 0, 'missing retry-after');

  await new Promise((resolve) => setTimeout(resolve, 20));
  eq(
    chargeCount(ABUSE_AGENT, 'webhook.send'),
    before + 60,
    'a rejected send must not add a charge'
  );
});

await test('the send limit is per agent', async () => {
  const otherKey = generateApiKey();
  const otherId = 'abuse-sender-other';
  await insertAgent('key_abuse_other', otherId, otherKey, 'cus_abuse_other');

  const r = await req('POST', '/webhooks/send', {
    headers: { authorization: `Bearer ${otherKey}` },
    body: validHook,
  });
  assert(r.status === 202, `status ${r.status}: ${r.text}`);
});

await test('a counter failure fails open and logs rate_limit.error', async () => {
  const failKey = generateApiKey();
  const failId = 'abuse-fail-open';
  await insertAgent('key_abuse_fail_open', failId, failKey, 'cus_abuse_fail');

  const realDB = env.DB;
  const logs = [];
  const realConsoleError = console.error;
  console.error = (...args) => {
    logs.push(args.map((a) => String(a)).join(' '));
    realConsoleError(...args);
  };
  env.DB = {
    prepare(sql) {
      if (/rate_limits/i.test(sql)) throw new Error('simulated counter table failure');
      return realDB.prepare(sql);
    },
    batch: (...args) => realDB.batch(...args),
    exec: (...args) => realDB.exec(...args),
  };

  let r;
  try {
    r = await req('POST', '/webhooks/send', {
      headers: { authorization: `Bearer ${failKey}` },
      body: validHook,
    });
  } finally {
    env.DB = realDB;
    console.error = realConsoleError;
  }

  assert(r.status === 202, `a counter outage must not block a paid send: ${r.status} ${r.text}`);
  assert(
    logs.some((line) => line.includes('rate_limit.error')),
    `rate_limit.error was not logged: ${logs.join(' | ')}`
  );
  eq(chargeCount(failId, 'webhook.send'), 1, 'the send should still be metered');
});

await test('mail that fails DMARC/SPF and DKIM is rejected before billing', async () => {
  const webhooksBefore = Number(DB.prepare(`SELECT COUNT(*) c FROM webhooks`).get().c);

  await deliverRawEmail(
    rawEmail({
      from: 'spoofed@dmarc.test',
      auth: 'mx.cloudflare.net; spf=fail; dkim=fail; dmarc=fail',
    }),
    'spoofed@dmarc.test'
  );

  const row = DB.prepare(
    `SELECT * FROM inbound_emails WHERE from_email = ? ORDER BY rowid DESC LIMIT 1`
  ).get('spoofed@dmarc.test');
  assert(row, 'rejected message should still be recorded');
  eq(row.status, 'rejected_auth');
  eq(row.agent_id, 'system');
  eq(Number(row.forwarded_to_webhook), 0, 'rejected mail must not be fanned out');
  eq(
    Number(DB.prepare(`SELECT COUNT(*) c FROM charges WHERE ref_id = ?`).get(row.inbound_id).c),
    0,
    'rejected mail must not be charged'
  );
  eq(Number(DB.prepare(`SELECT COUNT(*) c FROM webhooks`).get().c), webhooksBefore);
});

await test('mail with no Authentication-Results header is not rejected', async () => {
  await DB.prepare(
    `INSERT INTO inbound_routes (from_email, agent_id, callback_webhook, created_at)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(from_email) DO UPDATE SET callback_webhook = excluded.callback_webhook`
  ).bind('noauth@corp.test', AGENT_ID, 'https://noauth.test/cb', new Date().toISOString()).run();

  await deliverRawEmail(rawEmail({ from: 'noauth@corp.test' }), 'noauth@corp.test');

  const row = DB.prepare(
    `SELECT * FROM inbound_emails WHERE from_email = ? ORDER BY rowid DESC LIMIT 1`
  ).get('noauth@corp.test');
  assert(row, 'missing header should still be accepted');
  eq(row.status, 'accepted');
  eq(Number(row.forwarded_to_webhook), 1, 'accepted mail should still fan out');
  await new Promise((resolve) => setTimeout(resolve, 20));
  eq(
    Number(DB.prepare(`SELECT COUNT(*) c FROM charges WHERE ref_id = ?`).get(row.inbound_id).c),
    1,
    'accepted mail should still be charged once'
  );
});

await test('a wildcard route stops at 200 inbound messages per UTC day', async () => {
  const day = new Date().toISOString().slice(0, 10);
  await seedLimit(`inbound:*:${day}`, 199);

  const accepted = await req('POST', '/email/inbound', {
    body: { from: 'wildcap@random.test', subject: '199', text: 'ok' },
  });
  assert(accepted.status === 200, `accepted status ${accepted.status}`);
  assert(accepted.json.forwarded_event_id, 'the 200th message should fan out');
  const acceptedRow = DB.prepare(`SELECT status FROM inbound_emails WHERE inbound_id = ?`).get(
    accepted.json.inbound_id
  );
  eq(acceptedRow.status, 'accepted');

  const limited = await req('POST', '/email/inbound', {
    body: { from: 'wildcap2@random.test', subject: '201', text: 'nope' },
  });
  assert(limited.status === 200, `limited status ${limited.status}`);
  assert(!limited.json.forwarded_event_id, 'the 201st message must not fan out');

  const limitedRow = DB.prepare(`SELECT * FROM inbound_emails WHERE inbound_id = ?`).get(
    limited.json.inbound_id
  );
  eq(limitedRow.status, 'rate_limited');
  eq(Number(limitedRow.forwarded_to_webhook), 0);
  await new Promise((resolve) => setTimeout(resolve, 20));
  eq(
    Number(DB.prepare(`SELECT COUNT(*) c FROM charges WHERE ref_id = ?`).get(limited.json.inbound_id).c),
    0,
    'rate-limited inbound must not be charged'
  );
});

await test('a non-wildcard route stops at 1000 inbound messages per UTC day', async () => {
  const day = new Date().toISOString().slice(0, 10);
  await DB.prepare(
    `INSERT INTO inbound_routes (from_email, agent_id, callback_webhook, created_at)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(from_email) DO UPDATE SET callback_webhook = excluded.callback_webhook`
  ).bind('cap@corp.test', AGENT_ID, 'https://cap.test/cb', new Date().toISOString()).run();
  await seedLimit(`inbound:cap@corp.test:${day}`, 999);

  const accepted = await req('POST', '/email/inbound', {
    body: { from: 'cap@corp.test', subject: '1000', text: 'ok' },
  });
  assert(accepted.json.forwarded_event_id, 'the 1000th message should fan out');
  eq(
    DB.prepare(`SELECT status FROM inbound_emails WHERE inbound_id = ?`).get(accepted.json.inbound_id)
      .status,
    'accepted'
  );
  await new Promise((resolve) => setTimeout(resolve, 20));
  eq(
    Number(DB.prepare(`SELECT COUNT(*) c FROM charges WHERE ref_id = ?`).get(accepted.json.inbound_id).c),
    1,
    'the 1000th accepted message should be charged once'
  );

  const limited = await req('POST', '/email/inbound', {
    body: { from: 'cap@corp.test', subject: '1001', text: 'nope' },
  });
  assert(!limited.json.forwarded_event_id, 'the 1001st message must not fan out');
  const limitedRow = DB.prepare(`SELECT * FROM inbound_emails WHERE inbound_id = ?`).get(
    limited.json.inbound_id
  );
  eq(limitedRow.status, 'rate_limited');
  await new Promise((resolve) => setTimeout(resolve, 20));
  eq(
    Number(DB.prepare(`SELECT COUNT(*) c FROM charges WHERE ref_id = ?`).get(limited.json.inbound_id).c),
    0,
    'rate-limited inbound must not be charged'
  );
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
