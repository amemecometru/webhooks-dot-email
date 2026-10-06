import { PRICING } from '../config/pricing';
import {
  chargesSummary,
  enqueueEmail,
  getEmail,
  nowIso,
  periodStartFor,
  weekWindow,
} from '../db/queries';
import { logChargeAsync } from '../lib/charge-logger';
import { fastPathSend } from '../lib/queue';
import { chargesSummaryQuerySchema, sendEmailSchema , type Ctx } from '../types';
import { parseJson } from './webhooks';

/** POST /email/send — 202 Accepted, charged $0.01 */
export async function sendEmailRoute(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const body = await parseJson(c.req.raw);

  const parsed = sendEmailSchema.safeParse(body);
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

  const messageId = `msg_${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`;
  const from = input.from ?? c.env.OUTBOUND_EMAIL;
  const callbackWebhook = input.callback_webhook ?? agent.default_callback_webhook ?? null;

  await enqueueEmail(c.env.DB, {
    messageId,
    agentId: agent.agent_id,
    to: input.to,
    from,
    replyTo: input.reply_to ?? null,
    subject: input.subject,
    body: input.body,
    html: input.html ?? null,
    callbackWebhook,
  });

  logChargeAsync(c.env, agent.agent_id, 'email.send', messageId);

  // Fast path: attempt the actual send now so latency is ~1s, not up-to-60s.
  // The cron drain remains the safety net for whatever this does not finish.
  c.executionCtx.waitUntil(fastPathSend(c.env, messageId).catch((err) => {
    console.error(`fast-path send failed (${messageId}):`, (err as Error).message);
  }));

  console.log(
    JSON.stringify({
      event: 'email.queued',
      message_id: messageId,
      agent_id: agent.agent_id,
      to: input.to,
      from,
    })
  );

  return c.json(
    {
      message_id: messageId,
      status: 'queued',
      charge: { amount_usd: PRICING['email.send'], description: 'email.send' },
      status_url: `${c.env.API_HOST}/email/status/${messageId}`,
    },
    202
  );
}

/** GET /email/status/:messageId */
export async function emailStatus(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const messageId = c.req.param('messageId');
  if (!messageId) return c.json({ error: 'not_found' }, 404);
  const row = await getEmail(c.env.DB, messageId);

  if (!row) return c.json({ error: 'not_found' }, 404);
  if (row.agent_id !== agent.agent_id) return c.json({ error: 'forbidden' }, 403);

  return c.json({
    message_id: row.message_id,
    status: row.status,
    to: row.to_email,
    from: row.from_email,
    subject: row.subject,
    attempts: row.attempts,
    last_error: row.last_error,
    created_at: row.created_at,
    delivered_at: row.delivered_at,
  });
}

/**
 * GET /api/charges/summary?period=week
 * Returns per-endpoint counts and totals plus the current billing window.
 */
export async function chargesSummaryRoute(c: Ctx): Promise<Response> {
  const agent = c.get('agent');

  const parsed = chargesSummaryQuerySchema.safeParse({
    period: c.req.query('period') ?? 'week',
  });
  if (!parsed.success) return c.json({ error: 'invalid_period' }, 400);
  const { period } = parsed.data;

  const since = periodStartFor(period);
  const [rows, keyRow] = await Promise.all([
    chargesSummary(c.env.DB, agent.agent_id, since),
    c.env.DB.prepare(
      `SELECT stripe_customer_id FROM api_keys WHERE agent_id = ?`
    )
      .bind(agent.agent_id)
      .first<{ stripe_customer_id: string }>(),
  ]);

  // Fill in unit cost from the pricing config rather than from the DB, so the
  // reported unit price always matches what we actually charged.
  const charges = rows.map((r) => ({
    endpoint: r.endpoint,
    count: r.count,
    unit_cost: PRICING[r.endpoint as keyof typeof PRICING] ?? 0,
    total: Number(r.total.toFixed(6)),
  }));

  const periodTotal = charges.reduce((sum, c2) => sum + c2.total, 0);
  const { start, end } = weekWindow();
  const nextBilling = new Date(end.getTime() + 7 * 24 * 60 * 60 * 1000);

  return c.json({
    agent_id: agent.agent_id,
    period,
    window: { start: since, end: nowIso() },
    charges,
    period_total_usd: Number(periodTotal.toFixed(6)),
    current_billing_period: `${start.toISOString()} to ${end.toISOString()}`,
    next_billing_date: nextBilling.toISOString(),
    stripe_customer_id: keyRow?.stripe_customer_id ?? agent.stripe_customer_id,
  });
}
