import { PUBLIC_PRICING } from '../config/pricing';
import { leaderboard, metricRows, statusCounts } from '../db/queries';
import { generateApiKey, hashApiKey, keyLookupDigest } from '../lib/crypto';
import { nowIso, weekWindow } from '../db/queries';
import { generateKeySchema, inboundRouteSchema , type Ctx } from '../types';
import { parseJson } from './webhooks';

/* -------------------------------------------------------------------------- */
/*  GET /status                                                                */
/* -------------------------------------------------------------------------- */

export async function status(c: Ctx): Promise<Response> {
  const [counts, metrics] = await Promise.all([statusCounts(c.env.DB), metricRows(c.env.DB, 1)]);

  const requests = metrics.reduce((s, m) => s + m.requests, 0);
  const successes = metrics.reduce((s, m) => s + m.successes, 0);
  const errors = metrics.reduce((s, m) => s + m.errors, 0);
  const durationMs = metrics.reduce((s, m) => s + m.total_duration_ms, 0);

  const uptime = requests > 0 ? Number(((successes / requests) * 100).toFixed(2)) : 100;

  return c.json({
    status: uptime >= 99 ? 'operational' : 'degraded',
    api_uptime: uptime,
    avg_response_time_ms: requests > 0 ? Math.round(durationMs / requests) : 0,
    active_agents: counts.active_agents ?? 0,
    total_webhooks_delivered_today: counts.webhooks_today ?? 0,
    total_emails_sent_today: counts.emails_today ?? 0,
    total_inbound_today: counts.inbound_today ?? 0,
    pending_deliveries: (counts.webhooks_pending ?? 0) + (counts.emails_pending ?? 0),
    endpoints: metrics.map((m) => ({
      endpoint: m.endpoint,
      requests: m.requests,
      success_rate: m.requests ? Number(((m.successes / m.requests) * 100).toFixed(2)) : 100,
      avg_ms: m.requests ? Math.round(m.total_duration_ms / m.requests) : 0,
    })),
    updated_at: nowIso(),
  });
}

/* -------------------------------------------------------------------------- */
/*  GET /pricing                                                               */
/* -------------------------------------------------------------------------- */

export async function pricing(c: Ctx): Promise<Response> {
  const perThousand = (usd: number) => Number((usd * 1000).toFixed(2));
  return c.json({
    currency: 'USD',
    prices: PUBLIC_PRICING.map((p) => ({
      ...p,
      per_1000: perThousand(p.unit_cost_usd),
    })),
    roi_example: {
      claim: 'Send 1,000 emails',
      cost_usd: perThousand(PUBLIC_PRICING[0].unit_cost_usd),
      note: 'Agents should size this against their own conversion rate.',
    },
    docs: `https://${c.env.API_HOST}/docs`,
    sandbox: `https://${c.env.MCP_HOST}`,
  });
}

/* -------------------------------------------------------------------------- */
/*  GET /leaderboard (opt-in only)                                             */
/* -------------------------------------------------------------------------- */

export async function leaderboardRoute(c: Ctx): Promise<Response> {
  const { start, end } = weekWindow();
  const rows = await leaderboard(c.env.DB, start.toISOString());

  return c.json({
    window: { start: start.toISOString(), end: end.toISOString() },
    // We publish volume and spend, never "revenue_generated" - we have no way
    // to measure an agent's revenue and would be making it up.
    this_week: (rows.results ?? []).map((r) => ({
      agent_name: r.agent_name ?? r.agent_id,
      webhooks_sent: r.webhooks_sent ?? 0,
      emails_sent: r.emails_sent ?? 0,
      spend_usd: Number(Number(r.total_usd ?? 0).toFixed(4)),
    })),
  });
}

/* -------------------------------------------------------------------------- */
/*  POST /api/keys/generate — no-signup fast track                             */
/* -------------------------------------------------------------------------- */

export async function generateKey(c: Ctx): Promise<Response> {
  const body = await parseJson(c.req.raw);
  const parsed = generateKeySchema.safeParse(body);
  if (!parsed.success) {
    return c.json(
      {
        error: 'validation_failed',
        details: parsed.error.issues.map((i) => ({ path: i.path.join('.'), message: i.message })),
      },
      400
    );
  }
  const input = parsed.data;

  const existing = await c.env.DB.prepare(
    `SELECT key_id FROM api_keys WHERE agent_id = ?`
  )
    .bind(input.agent_name)
    .first<{ key_id: string }>();

  if (existing) {
    // Never re-issue a key for a known agent id - that would let anyone mint a
    // second valid key for an existing customer by guessing the name.
    return c.json(
      { error: 'agent_exists', detail: 'this agent_id already has a key; keys are not re-issuable' },
      409
    );
  }

  const rawKey = generateApiKey();
  const keyId = `key_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`;
  const iterations = Number(c.env.KEY_PBKDF2_ITERATIONS || 100_000);

  await c.env.DB.prepare(
    `INSERT INTO api_keys
       (key_id, agent_id, agent_name, key_hash, key_lookup, stripe_customer_id,
        leaderboard_optin, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      keyId,
      input.agent_name,
      input.agent_name,
      await hashApiKey(rawKey, iterations),
      await keyLookupDigest(rawKey),
      input.stripe_customer_id,
      input.leaderboard_optin ? 1 : 0,
      nowIso()
    )
    .run();

  console.log(JSON.stringify({ event: 'key.generated', agent_id: input.agent_name }));

  return c.json(
    {
      // Shown exactly once. We store only PBKDF2(hash), so it is unrecoverable.
      api_key: rawKey,
      key_id: keyId,
      stripe_customer_id: input.stripe_customer_id,
      test_email_free: true,
      docs: `https://${c.env.API_HOST}/docs`,
      sandbox: `https://${c.env.MCP_HOST}`,
    },
    201
  );
}

/* -------------------------------------------------------------------------- */
/*  POST /api/inbound/routes — register where inbound email should be fanned  */
/*  out to. Authenticated.                                                     */
/* -------------------------------------------------------------------------- */

export async function createInboundRoute(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const body = await parseJson(c.req.raw);
  const parsed = inboundRouteSchema.safeParse(body);
  if (!parsed.success) {
    return c.json(
      {
        error: 'validation_failed',
        details: parsed.error.issues.map((i) => ({ path: i.path.join('.'), message: i.message })),
      },
      400
    );
  }
  const input = parsed.data;

  await c.env.DB.prepare(
    `INSERT INTO inbound_routes (from_email, agent_id, callback_webhook, created_at)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(from_email) DO UPDATE
       SET agent_id = excluded.agent_id,
           callback_webhook = excluded.callback_webhook`
  )
    .bind(input.from, agent.agent_id, input.callback_webhook, nowIso())
    .run();

  return c.json({ from: input.from, callback_webhook: input.callback_webhook }, 201);
}

/** GET /api/inbound/routes — list this agent's inbound fan-out targets. */
export async function listInboundRoutes(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const rows = await c.env.DB.prepare(
    `SELECT from_email, callback_webhook, created_at FROM inbound_routes WHERE agent_id = ?`
  )
    .bind(agent.agent_id)
    .all();
  return c.json({ routes: rows.results ?? [] });
}
