import type { EmailRow, WebhookRow } from '../types';

/* -------------------------------------------------------------------------- */
/*  Time helpers                                                               */
/* -------------------------------------------------------------------------- */

export function nowIso(): string {
  return new Date().toISOString();
}

export function utcDay(d: Date = new Date()): string {
  return d.toISOString().slice(0, 10);
}

/** ISO timestamp `seconds` in the future. */
export function isoIn(seconds: number): string {
  return new Date(Date.now() + seconds * 1000).toISOString();
}

/**
 * Monday-anchored UTC week boundary. The weekly billing cron runs Sunday 00:00
 * UTC, so the billable window is the 7 days ending at that instant.
 */
export function weekWindow(now = new Date()): { start: Date; end: Date } {
  const end = new Date(now);
  const start = new Date(end.getTime() - 7 * 24 * 60 * 60 * 1000);
  return { start, end };
}

export function periodStartFor(period: 'day' | 'week' | 'month' | 'all', now = new Date()): string {
  const ms: Record<string, number> = {
    day: 24 * 60 * 60 * 1000,
    week: 7 * 24 * 60 * 60 * 1000,
    month: 30 * 24 * 60 * 60 * 1000,
    all: 100 * 365 * 24 * 60 * 60 * 1000,
  };
  return new Date(now.getTime() - ms[period]).toISOString();
}

/** Exponential backoff with a 1h ceiling. Attempt is 1-based. */
export function backoffSeconds(attempt: number): number {
  const base = Math.min(2 ** Math.max(0, attempt - 1), 3600);
  const jitter = 0.75 + Math.random() * 0.5; // 75%..125% to avoid thundering herd
  return Math.max(1, Math.round(base * jitter));
}

/* -------------------------------------------------------------------------- */
/*  Webhook queue                                                              */
/* -------------------------------------------------------------------------- */

export async function enqueueWebhook(
  db: D1Database,
  row: {
    eventId: string;
    agentId: string;
    toWebhookUrl: string;
    eventType: string;
    payload: unknown;
    headers?: Record<string, string> | null;
    notifyUrl?: string | null;
  }
): Promise<void> {
  const ts = nowIso();
  await db
    .prepare(
      `INSERT INTO webhooks
         (event_id, agent_id, to_webhook_url, event_type, payload, headers, notify_url,
          status, next_attempt_at, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)`
    )
    .bind(
      row.eventId,
      row.agentId,
      row.toWebhookUrl,
      row.eventType,
      JSON.stringify(row.payload ?? null),
      row.headers ? JSON.stringify(row.headers) : null,
      row.notifyUrl ?? null,
      ts,
      ts
    )
    .run();
}

/**
 * Claim a batch of due rows by flipping them to 'delivering' in a single
 * conditional UPDATE. Doing the claim in SQL (rather than read-then-write)
 * means two overlapping cron ticks cannot deliver the same row twice.
 */
export async function claimDueWebhooks(db: D1Database, limit: number): Promise<WebhookRow[]> {
  const now = nowIso();
  const rows = await db
    .prepare(
      `UPDATE webhooks
          SET status = 'delivering', delivery_attempts = delivery_attempts + 1
        WHERE event_id IN (
          SELECT event_id FROM webhooks
           WHERE status = 'queued' AND next_attempt_at <= ?
           ORDER BY next_attempt_at ASC
           LIMIT ?
        )
        RETURNING *`
    )
    .bind(now, limit)
    .all<WebhookRow>();

  return rows.results ?? [];
}

export async function markWebhookDelivered(
  db: D1Database,
  eventId: string,
  responseStatus: number
): Promise<void> {
  await db
    .prepare(
      `UPDATE webhooks SET status = 'delivered', response_status = ?, delivered_at = ?, last_error = NULL
        WHERE event_id = ?`
    )
    .bind(responseStatus, nowIso(), eventId)
    .run();
}

export async function markWebhookRetry(
  db: D1Database,
  eventId: string,
  attempt: number,
  maxAttempts: number,
  error: string
): Promise<void> {
  if (attempt >= maxAttempts) {
    await db
      .prepare(`UPDATE webhooks SET status = 'failed', last_error = ? WHERE event_id = ?`)
      .bind(error.slice(0, 1000), eventId)
      .run();
    return;
  }
  await db
    .prepare(
      `UPDATE webhooks SET status = 'queued', last_error = ?, next_attempt_at = ? WHERE event_id = ?`
    )
    .bind(error.slice(0, 1000), isoIn(backoffSeconds(attempt)), eventId)
    .run();
}

export async function getWebhook(db: D1Database, eventId: string): Promise<WebhookRow | null> {
  return db
    .prepare(`SELECT * FROM webhooks WHERE event_id = ?`)
    .bind(eventId)
    .first<WebhookRow>();
}

/* -------------------------------------------------------------------------- */
/*  Email queue                                                                */
/* -------------------------------------------------------------------------- */

export async function enqueueEmail(
  db: D1Database,
  row: {
    messageId: string;
    agentId: string;
    to: string;
    from: string;
    replyTo?: string | null;
    subject: string;
    body: string;
    html?: string | null;
    callbackWebhook?: string | null;
  }
): Promise<void> {
  const ts = nowIso();
  await db
    .prepare(
      `INSERT INTO emails
         (message_id, agent_id, to_email, from_email, reply_to, subject, body, html,
          status, callback_webhook, next_attempt_at, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)`
    )
    .bind(
      row.messageId,
      row.agentId,
      row.to,
      row.from,
      row.replyTo ?? null,
      row.subject,
      row.body,
      row.html ?? null,
      row.callbackWebhook ?? null,
      ts,
      ts
    )
    .run();
}

/** Claim ONE row by id - used by the fast path so we do not race the cron. */
export async function claimEmailById(db: D1Database, messageId: string): Promise<EmailRow | null> {
  const row = await db
    .prepare(
      `UPDATE emails SET status = 'sending', attempts = attempts + 1
        WHERE message_id = ? AND status IN ('queued')
        RETURNING *`
    )
    .bind(messageId)
    .first<EmailRow>();
  return row ?? null;
}

export async function claimDueEmails(db: D1Database, limit: number): Promise<EmailRow[]> {
  const rows = await db
    .prepare(
      `UPDATE emails SET status = 'sending', attempts = attempts + 1
        WHERE message_id IN (
          SELECT message_id FROM emails
           WHERE status = 'queued' AND next_attempt_at <= ?
           ORDER BY next_attempt_at ASC
           LIMIT ?
        )
        RETURNING *`
    )
    .bind(nowIso(), limit)
    .all<EmailRow>();
  return rows.results ?? [];
}

export async function markEmailSent(db: D1Database, messageId: string): Promise<void> {
  await db
    .prepare(
      `UPDATE emails SET status = 'sent', delivered_at = ?, last_error = NULL WHERE message_id = ?`
    )
    .bind(nowIso(), messageId)
    .run();
}

export async function markEmailRetry(
  db: D1Database,
  messageId: string,
  attempt: number,
  maxAttempts: number,
  error: string
): Promise<void> {
  if (attempt >= maxAttempts) {
    await db
      .prepare(`UPDATE emails SET status = 'failed', last_error = ? WHERE message_id = ?`)
      .bind(error.slice(0, 1000), messageId)
      .run();
    return;
  }
  await db
    .prepare(
      `UPDATE emails SET status = 'queued', last_error = ?, next_attempt_at = ? WHERE message_id = ?`
    )
    .bind(error.slice(0, 1000), isoIn(backoffSeconds(attempt)), messageId)
    .run();
}

export async function getEmail(db: D1Database, messageId: string): Promise<EmailRow | null> {
  return db.prepare(`SELECT * FROM emails WHERE message_id = ?`).bind(messageId).first<EmailRow>();
}

/* -------------------------------------------------------------------------- */
/*  Charges                                                                    */
/* -------------------------------------------------------------------------- */

export async function insertCharge(
  db: D1Database,
  charge: { agentId: string; endpoint: string; amountUsd: number; refId?: string | null }
): Promise<string> {
  const chargeId = `chr_${crypto.randomUUID()}`;
  const ts = nowIso();
  // period_start buckets charges into billing weeks (Monday-anchored UTC).
  const weekMs = 7 * 24 * 60 * 60 * 1000;
  const periodStart = new Date(
    Math.floor(Date.parse(ts) / weekMs) * weekMs
  ).toISOString();

  await db
    .prepare(
      `INSERT INTO charges
         (charge_id, agent_id, endpoint, amount_usd, status, period_start, created_at, ref_id)
       VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)`
    )
    .bind(chargeId, charge.agentId, charge.endpoint, charge.amountUsd, periodStart, ts, charge.refId ?? null)
    .run();

  return chargeId;
}

export async function chargesSummary(
  db: D1Database,
  agentId: string,
  sinceIso: string
): Promise<{ endpoint: string; count: number; unit_cost: number; total: number }[]> {
  const res = await db
    .prepare(
      `SELECT endpoint, COUNT(*) AS count, SUM(amount_usd) AS total
         FROM charges
        WHERE agent_id = ? AND created_at >= ? AND status != 'failed'
        GROUP BY endpoint
        ORDER BY total DESC`
    )
    .bind(agentId, sinceIso)
    .all<{ endpoint: string; count: number; total: number }>();
  return (res.results ?? []).map((r) => ({
    endpoint: r.endpoint,
    count: r.count,
    unit_cost: 0,
    total: r.total ?? 0,
  }));
}

/* -------------------------------------------------------------------------- */
/*  Leaderboard / status                                                       */
/* -------------------------------------------------------------------------- */

export async function leaderboard(db: D1Database, weekStart: string) {
  return db
    .prepare(
      `SELECT k.agent_name, k.agent_id,
              SUM(CASE WHEN c.endpoint = 'webhook.send' THEN 1 ELSE 0 END) AS webhooks_sent,
              SUM(CASE WHEN c.endpoint = 'email.send'   THEN 1 ELSE 0 END) AS emails_sent,
              SUM(c.amount_usd) AS total_usd
         FROM charges c
         JOIN api_keys k ON k.agent_id = c.agent_id
        WHERE c.created_at >= ? AND k.leaderboard_optin = 1
        GROUP BY k.agent_id
        ORDER BY total_usd DESC
        LIMIT 25`
    )
    .bind(weekStart)
    .all();
}

export async function statusCounts(db: D1Database) {
  const res = await db
    .prepare(
      `SELECT
         (SELECT COUNT(*) FROM webhooks WHERE date(created_at) = date('now'))  AS webhooks_today,
         (SELECT COUNT(*) FROM emails   WHERE date(created_at) = date('now'))  AS emails_today,
         (SELECT COUNT(*) FROM inbound_emails WHERE date(received_at) = date('now')) AS inbound_today,
         (SELECT COUNT(*) FROM webhooks WHERE status = 'queued')               AS webhooks_pending,
         (SELECT COUNT(*) FROM emails   WHERE status IN ('queued','sending'))  AS emails_pending,
         (SELECT COUNT(*) FROM api_keys WHERE status = 'active')               AS active_agents`
    )
    .first<Record<string, number>>();
  return res ?? {};
}

export async function metricRows(db: D1Database, days: number) {
  const res = await db
    .prepare(
      `SELECT endpoint,
              SUM(requests)          AS requests,
              SUM(successes)         AS successes,
              SUM(errors)            AS errors,
              SUM(total_duration_ms) AS total_duration_ms
         FROM endpoint_metrics
        WHERE day >= date('now', ?)
        GROUP BY endpoint`
    )
    .bind(`-${days} days`)
    .all<{
      endpoint: string;
      requests: number;
      successes: number;
      errors: number;
      total_duration_ms: number;
    }>();
  return res.results ?? [];
}
