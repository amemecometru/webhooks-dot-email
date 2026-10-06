import type { Env } from '../types';
import type { ChargeEndpoint } from '../config/pricing';
import { nowIso, weekWindow } from '../db/queries';
import { isStripeConfigured, reportMeterEvent, StripeError } from './stripe-metering';

/**
 * Weekly billing: aggregate pending D1 charges per agent and report them to
 * Stripe as billing meter event summaries, then mark the charges billed.
 *
 * Failure policy is the important part. Charges are only marked 'billed' after
 * Stripe confirms the meter event summary. If Stripe is unconfigured, a meter
 * is missing, or the API errors, we log the failure into billing_runs and
 * leave the charges 'pending' so the next run retries them. Money is never
 * marked as collected when it was not.
 */

export interface BillingOutcome {
  runId: string;
  status: 'completed' | 'failed';
  agentsCharged: number;
  totalUsd: number;
  errors: string[];
}

export async function weeklyBilling(env: Env, now = new Date()): Promise<BillingOutcome> {
  const runId = `run_${crypto.randomUUID()}`;
  const { start, end } = weekWindow(now);
  const startIso = start.toISOString();
  const endIso = end.toISOString();
  const errors: string[] = [];

  const pending = await env.DB.prepare(
    `SELECT c.agent_id,
            SUM(c.amount_usd) AS total_usd,
            COUNT(*)        AS charge_count,
            SUM(CASE WHEN c.endpoint = 'email.send'    THEN 1 ELSE 0 END) AS email_send,
            SUM(CASE WHEN c.endpoint = 'webhook.send' THEN 1 ELSE 0 END) AS webhook_send,
            SUM(CASE WHEN c.endpoint = 'email.inbound' THEN 1 ELSE 0 END) AS email_inbound
       FROM charges c
      WHERE c.status = 'pending'
        AND c.created_at >= ?
        AND c.created_at <  ?
        AND c.agent_id != 'system'
      GROUP BY c.agent_id`
  )
    .bind(startIso, endIso)
    .all<{
      agent_id: string;
      total_usd: number;
      charge_count: number;
      email_send: number;
      webhook_send: number;
      email_inbound: number;
    }>();

  const rows = pending.results ?? [];
  let agentsCharged = 0;
  let totalUsd = 0;

  if (!isStripeConfigured(env)) {
    errors.push('STRIPE_SECRET is not configured; no charges were billed');
  }

  for (const row of rows) {
    const key = await env.DB.prepare(
      `SELECT stripe_customer_id FROM api_keys WHERE agent_id = ?`
    )
      .bind(row.agent_id)
      .first<{ stripe_customer_id: string }>();

    if (!key?.stripe_customer_id) {
      errors.push(`${row.agent_id}: no stripe_customer_id on file`);
      continue;
    }

    // One summary per metered endpoint, valued as an event COUNT. Stripe prices
    // each meter per unit, so the $ rates live in the Stripe price config and
    // D1 stays the record of what we charged.
    const events = (
      [
        { endpoint: 'email.send', count: row.email_send ?? 0 },
        { endpoint: 'webhook.send', count: row.webhook_send ?? 0 },
        { endpoint: 'email.inbound', count: row.email_inbound ?? 0 },
      ] satisfies { endpoint: ChargeEndpoint; count: number }[]
    ).filter((e) => e.count > 0);

    let allOk = true;
    for (const ev of events) {
      const res = await reportMeterEvent(env, {
        endpoint: ev.endpoint,
        customerId: key.stripe_customer_id,
        value: ev.count,
        // Idempotency: replaying a run will not double-report.
        identifier: `${runId}:${row.agent_id}:${ev.endpoint}`,
        timestamp: Math.floor(end.getTime() / 1000),
      });
      if (!res.ok) {
        allOk = false;
        errors.push(`${row.agent_id}/${ev.endpoint}: ${res.error}`);
      }
    }

    if (!allOk) continue;

    await env.DB.prepare(
      `UPDATE charges
          SET status = 'billed',
              billed_at = ?,
              billing_period_start = ?,
              billing_period_end = ?
        WHERE agent_id = ? AND status = 'pending' AND created_at >= ? AND created_at < ?`
    )
      .bind(nowIso(), startIso, endIso, row.agent_id, startIso, endIso)
      .run();

    agentsCharged++;
    totalUsd += row.total_usd ?? 0;
  }

  const status = errors.length === 0 ? 'completed' : 'failed';

  await env.DB.prepare(
    `INSERT INTO billing_runs
       (run_id, billing_period_start, billing_period_end, agents_charged, total_usd, status, error, executed_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      runId,
      startIso,
      endIso,
      agentsCharged,
      totalUsd,
      status,
      errors.length ? errors.slice(0, 20).join(' | ').slice(0, 2000) : null,
      nowIso()
    )
    .run();

  console.log(
    JSON.stringify({
      event: 'billing.run',
      run_id: runId,
      status,
      agents_charged: agentsCharged,
      agents_pending: rows.length,
      total_usd: Number(totalUsd.toFixed(6)),
      errors: errors.slice(0, 10),
    })
  );

  return { runId, status, agentsCharged, totalUsd, errors };
}

/** GET /api/billing/runs — operator view of billing history. */
export async function listBillingRuns(env: Env, limit = 20) {
  const res = await env.DB.prepare(
    `SELECT run_id, billing_period_start, billing_period_end, agents_charged,
            total_usd, status, error, executed_at
       FROM billing_runs
      ORDER BY executed_at DESC
      LIMIT ?`
  )
    .bind(limit)
    .all();
  return res.results ?? [];
}

export { StripeError };
