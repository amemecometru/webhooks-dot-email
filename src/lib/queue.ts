import type { EmailRow, Env, WebhookRow } from '../types';
import { PRICING } from '../config/pricing';
import { sendEmail } from './email-service';
import {
  claimDueEmails,
  claimDueWebhooks,
  enqueueWebhook,
  getEmail,
  markEmailRetry,
  markEmailSent,
  markWebhookDelivered,
  markWebhookRetry,
  nowIso,
} from '../db/queries';

/**
 * The D1-backed delivery engine. This replaces Cloudflare Queues.
 *
 * Request path: insert row (status 'queued') -> return 202. Nothing is held in
 * memory, so a Worker eviction between the 202 and the delivery loses nothing.
 *
 * Drain path: the `* * * * *` cron claims due rows with a conditional UPDATE
 * (so two overlapping ticks cannot double-send), performs I/O, and writes the
 * outcome. Failures go back to 'queued' with exponential backoff up to
 * MAX_RETRIES, then to 'failed'.
 */

const USER_AGENT = 'webhooks.email/1.0 (+https://webhooks.email)';

export interface DrainResult {
  webhooks: { claimed: number; delivered: number; failed: number; rescheduled: number };
  emails: { claimed: number; sent: number; failed: number; rescheduled: number };
}

export async function drainWebhooks(env: Env, limit: number): Promise<DrainResult['webhooks']> {
  const maxAttempts = Number(env.MAX_RETRIES || 5);
  const timeoutMs = Number(env.WEBHOOK_TIMEOUT_MS || 10_000);

  const rows = await claimDueWebhooks(env.DB, limit);
  const result = { claimed: rows.length, delivered: 0, failed: 0, rescheduled: 0 };
  if (rows.length === 0) return result;

  const outcomes = await Promise.all(rows.map((row) => deliverWebhook(env, row, timeoutMs)));

  for (const [row, outcome] of rows.map((r, i) => [r, outcomes[i]] as const)) {
    if (outcome.ok) {
      await markWebhookDelivered(env.DB, row.event_id, outcome.status);
      result.delivered++;
      await fanOutStatusCallback(env, row, 'webhook.delivered', outcome.status);
    } else {
      await markWebhookRetry(env.DB, row.event_id, row.delivery_attempts, maxAttempts, outcome.error);
      if (row.delivery_attempts >= maxAttempts) {
        result.failed++;
        await fanOutStatusCallback(env, row, 'webhook.failed', null);
      } else {
        result.rescheduled++;
      }
    }
  }

  return result;
}

type Outcome = { ok: true; status: number } | { ok: false; error: string };

async function deliverWebhook(env: Env, row: WebhookRow, timeoutMs: number): Promise<Outcome> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(row.to_webhook_url, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'user-agent': USER_AGENT,
        'x-webhooks-email-event-id': row.event_id,
        'x-webhooks-email-event-type': row.event_type,
        'x-webhooks-email-attempt': String(row.delivery_attempts),
        ...(safeHeaders(row.headers) as Record<string, string>),
      },
      body: row.payload,
      signal: controller.signal,
    });
    if (res.ok) return { ok: true, status: res.status };
    const snippet = (await res.text().catch(() => '')).slice(0, 200);
    return { ok: false, error: `HTTP ${res.status}${snippet ? `: ${snippet}` : ''}` };
  } catch (err) {
    const e = err as Error;
    return {
      ok: false,
      error: e.name === 'AbortError' ? `timeout after ${timeoutMs}ms` : e.message,
    };
  } finally {
    clearTimeout(timer);
  }
}

/** Only allow a conservative subset of headers from agent input. */
function safeHeaders(json: string | null): Record<string, string> {
  if (!json) return {};
  const ALLOW = new Set([
    'authorization',
    'x-api-key',
    'x-signature',
    'x-agent-id',
    'x-request-id',
    'content-type',
    'user-agent',
  ]);
  try {
    const parsed = JSON.parse(json) as Record<string, unknown>;
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(parsed)) {
      const key = k.toLowerCase();
      if (!ALLOW.has(key)) continue;
      if (typeof v !== 'string') continue;
      // strip CR/LF to prevent header injection
      out[key] = v.replace(/[\r\n]/g, '');
    }
    return out;
  } catch {
    return {};
  }
}

/**
 * Delivery proof (brief section 3C): tell the agent's callback that their
 * webhook landed or permanently failed. Failure here is logged, never thrown -
 * a dead callback must not fail the delivery we just completed.
 */
async function fanOutStatusCallback(
  env: Env,
  row: WebhookRow,
  event: 'webhook.delivered' | 'webhook.failed',
  status: number | null
): Promise<void> {
  if (!row.notify_url) return;

  try {
    await fetch(row.notify_url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'user-agent': USER_AGENT },
      body: JSON.stringify({
        event,
        event_id: row.event_id,
        event_type: row.event_type,
        to_webhook_url: row.to_webhook_url,
        status,
        proof: {
          timestamp: nowIso(),
          delivery_attempts: row.delivery_attempts,
          charge_amount_usd: PRICING['webhook.send'],
        },
      }),
    });
  } catch (err) {
    console.error('status callback failed:', (err as Error).message);
  }
}

/* -------------------------------------------------------------------------- */
/*  Emails                                                                     */
/* -------------------------------------------------------------------------- */

export async function drainEmails(env: Env, limit: number): Promise<DrainResult['emails']> {
  const maxAttempts = Number(env.MAX_RETRIES || 5);
  const rows = await claimDueEmails(env.DB, limit);
  const result = { claimed: rows.length, sent: 0, failed: 0, rescheduled: 0 };
  if (rows.length === 0) return result;

  const outcomes = await Promise.all(rows.map((row) => attemptSend(env, row, maxAttempts)));

  for (const [row, outcome] of rows.map((r, i) => [r, outcomes[i]] as const)) {
    if (outcome.ok) {
      result.sent++;
    } else if (row.attempts >= maxAttempts) {
      result.failed++;
    } else {
      result.rescheduled++;
    }
  }

  return result;
}

/**
 * Attempt one send. On success marks 'sent'. On failure decides retry vs.
 * permanent failure. Used by both the fast path and the cron drain.
 */
export async function attemptSend(env: Env, row: EmailRow, maxAttempts: number): Promise<Outcome> {
  try {
    await sendEmail(env, {
      from: row.from_email,
      to: row.to_email,
      subject: row.subject,
      body: row.body,
      html: row.html,
      replyTo: row.reply_to,
      messageId: row.message_id,
    });
    await markEmailSent(env.DB, row.message_id);
    if (row.callback_webhook) {
      notifyEmailStatus(env, row, 'email.sent').catch((e) =>
        console.error('email status callback failed:', e.message)
      );
    }
    return { ok: true, status: 202 };
  } catch (err) {
    const error = (err as Error).message;
    await markEmailRetry(env.DB, row.message_id, row.attempts, maxAttempts, error);
    return { ok: false, error };
  }
}

/**
 * Fast path: claim this specific message by id and send it now, inside
 * waitUntil(). The cron is only the safety net, so callers normally get
 * sub-second delivery rather than up-to-60s.
 */
export async function fastPathSend(env: Env, messageId: string): Promise<void> {
  const claimed = await claimById(env.DB, messageId);
  if (!claimed) return; // cron already owns it, or it went out
  await attemptSend(env, claimed, Number(env.MAX_RETRIES || 5));
}

async function claimById(db: D1Database, messageId: string): Promise<EmailRow | null> {
  return db
    .prepare(
      `UPDATE emails SET status = 'sending', attempts = attempts + 1
        WHERE message_id = ? AND status = 'queued'
        RETURNING *`
    )
    .bind(messageId)
    .first<EmailRow>();
}

/**
 * Recover rows stuck in 'sending' - a Worker killed mid-send would otherwise
 * strand them forever, since the claim filter only looks at 'queued'.
 */
export async function requeueStuckSends(env: Env, olderThanSeconds = 300): Promise<number> {
  const cutoff = new Date(Date.now() - olderThanSeconds * 1000).toISOString();
  const res = await env.DB.prepare(
    `UPDATE emails SET status = 'queued', next_attempt_at = ?
      WHERE status = 'sending' AND created_at <= ?
        AND attempts < ?`
  )
    .bind(nowIso(), cutoff, Number(env.MAX_RETRIES || 5))
    .run();
  return res.meta?.changes ?? 0;
}

export async function notifyEmailStatus(
  env: Env,
  row: EmailRow,
  event: 'email.sent' | 'email.bounced' | 'email.failed'
): Promise<void> {
  if (!row.callback_webhook) return;
  try {
    await fetch(row.callback_webhook, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'user-agent': USER_AGENT },
      body: JSON.stringify({
        event,
        message_id: row.message_id,
        to: row.to_email,
        from: row.from_email,
        subject: row.subject,
        status: row.status,
        proof: {
          timestamp: nowIso(),
          charge_amount_usd: PRICING['email.send'],
          invoice_line_item: null,
        },
      }),
    });
  } catch (err) {
    console.error('notifyEmailStatus failed:', (err as Error).message);
  }
}

/** Fan an inbound email out to the registered callback for that sender. */
export async function fanOutInbound(
  env: Env,
  args: { from: string; inboundId: string; subject: string; body: string; to: string }
): Promise<string | null> {
  const route = await env.DB.prepare(
    `SELECT callback_webhook FROM inbound_routes
      WHERE from_email = ? OR from_email = '*'
      ORDER BY (from_email = '*') ASC
      LIMIT 1`
  )
    .bind(args.from)
    .first<{ callback_webhook: string }>()
    .catch(() => null);

  if (!route?.callback_webhook) return null;

  const eventId = `evt_${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`;
  await enqueueWebhook(env.DB, {
    eventId,
    agentId: 'system',
    toWebhookUrl: route.callback_webhook,
    eventType: 'email.inbound',
    payload: {
      event: 'email.inbound',
      inbound_id: args.inboundId,
      from: args.from,
      to: args.to,
      subject: args.subject,
      text: args.body,
    },
  });
  return eventId;
}

export { getEmail };
