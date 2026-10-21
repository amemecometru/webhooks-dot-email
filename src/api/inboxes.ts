import { z } from 'zod';
import type { Ctx, Env } from '../types';
import { inboxCreateSchema, inboxPatchSchema } from '../types';
import { nowIso, utcDay } from '../db/queries';
import { enqueueEmail, markEmailSent } from '../db/queries';
import { consumeLimit, rateLimitResponse } from '../lib/rate-limit';
import { sendEmail } from '../lib/email-service';
import { logChargeAsync } from '../lib/charge-logger';
import { parseJson } from './webhooks';

const BASE32 = 'abcdefghijklmnopqrstuvwxyz234567';

function genInboxId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(20));
  let out = 'ib_';
  for (let i = 0; i < 20; i++) {
    out += BASE32[bytes[i] % 32];
  }
  return out;
}

function inboxLimit(env: Env, plan: 'free' | 'paid'): number {
  if (plan === 'free') return Number(env.FREE_INBOXES_MAX || 3);
  return Number(env.PAID_INBOXES_MAX || 100);
}

/* -------------------------------------------------------------------------- */
/*  POST /api/inboxes                                                         */
/* -------------------------------------------------------------------------- */

export async function createInbox(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const body = await parseJson(c.req.raw);
  const parsed = inboxCreateSchema.safeParse(body);
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

  const limit = inboxLimit(c.env, agent.plan);
  const countRow = await c.env.DB.prepare(
    `SELECT COUNT(*) AS c FROM inboxes WHERE agent_id = ? AND status = 'active'`
  )
    .bind(agent.agent_id)
    .first<{ c: number }>();
  const activeCount = Number(countRow?.c ?? 0);
  if (activeCount >= limit) {
    return c.json({ error: 'inbox_limit_reached', limit }, 403);
  }

  const inboxId = genInboxId();
  const address = `${inboxId}@webhooks.email`;
  const createdAt = nowIso();

  await c.env.DB.prepare(
    `INSERT INTO inboxes (inbox_id, agent_id, address, name, callback_url, status, created_at)
     VALUES (?, ?, ?, ?, ?, 'active', ?)`
  )
    .bind(inboxId, agent.agent_id, address, input.name ?? null, input.callback_url, createdAt)
    .run();

  return c.json(
    {
      inbox_id: inboxId,
      address,
      callback_url: input.callback_url,
      status: 'active',
      created_at: createdAt,
    },
    201
  );
}

/* -------------------------------------------------------------------------- */
/*  GET /api/inboxes                                                          */
/* -------------------------------------------------------------------------- */

export async function listInboxes(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const rows = await c.env.DB.prepare(
    `SELECT inbox_id, address, name, callback_url, status, created_at
       FROM inboxes
      WHERE agent_id = ?
      ORDER BY created_at DESC`
  )
    .bind(agent.agent_id)
    .all();
  return c.json({ inboxes: rows.results ?? [] });
}

/* -------------------------------------------------------------------------- */
/*  PATCH /api/inboxes/:inboxId                                               */
/* -------------------------------------------------------------------------- */

export async function patchInbox(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const inboxId = c.req.param('inboxId');

  const existing = await c.env.DB.prepare(
    `SELECT inbox_id, agent_id FROM inboxes WHERE inbox_id = ?`
  )
    .bind(inboxId)
    .first<{ inbox_id: string; agent_id: string }>();

  if (!existing || existing.agent_id !== agent.agent_id) {
    return c.json({ error: 'not_found' }, 404);
  }

  const body = await parseJson(c.req.raw);
  const parsed = inboxPatchSchema.safeParse(body);
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

  const sets: string[] = [];
  const vals: unknown[] = [];
  if (input.callback_url !== undefined) {
    sets.push('callback_url = ?');
    vals.push(input.callback_url);
  }
  if (input.status !== undefined) {
    sets.push('status = ?');
    vals.push(input.status);
  }
  if (input.allowed_senders !== undefined) {
    let normalized: string[] | null = null;
    if (input.allowed_senders) {
      const out: string[] = [];
      const seen = new Set<string>();
      for (const entry of input.allowed_senders) {
        const e = String(entry).toLowerCase().trim();
        if (!e) continue;
        if (e.startsWith('@')) {
          if (!/^@[a-z0-9.-]+$/.test(e)) {
            return c.json({ error: 'validation_failed', details: [{ path: 'allowed_senders', message: 'invalid entry' }] }, 400);
          }
        } else {
          // basic email check
          if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e)) {
            return c.json({ error: 'validation_failed', details: [{ path: 'allowed_senders', message: 'invalid entry' }] }, 400);
          }
        }
        if (seen.has(e)) continue;
        seen.add(e);
        out.push(e);
      }
      normalized = out.length ? out : null;
    }
    sets.push('allowed_senders = ?');
    vals.push(normalized ? JSON.stringify(normalized) : null);
  }
  if (sets.length === 0) {
    return c.json({ error: 'validation_failed' }, 400);
  }
  vals.push(inboxId);
  await c.env.DB.prepare(`UPDATE inboxes SET ${sets.join(', ')} WHERE inbox_id = ?`)
    .bind(...vals)
    .run();

  const row = await c.env.DB.prepare(
    `SELECT inbox_id, address, callback_url, status, created_at FROM inboxes WHERE inbox_id = ?`
  )
    .bind(inboxId)
    .first();
  return c.json(row);
}

/* -------------------------------------------------------------------------- */
/*  DELETE /api/inboxes/:inboxId                                               */
/* -------------------------------------------------------------------------- */

export async function deleteInbox(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const inboxId = c.req.param('inboxId');

  const existing = await c.env.DB.prepare(
    `SELECT inbox_id, agent_id FROM inboxes WHERE inbox_id = ?`
  )
    .bind(inboxId)
    .first<{ inbox_id: string; agent_id: string }>();

  if (!existing || existing.agent_id !== agent.agent_id) {
    return c.json({ error: 'not_found' }, 404);
  }

  await c.env.DB.prepare(`UPDATE inboxes SET status = 'disabled' WHERE inbox_id = ?`)
    .bind(inboxId)
    .run();

  return c.json({ inbox_id: inboxId, status: 'disabled' }, 200);
}

/* -------------------------------------------------------------------------- */
/*  GET /api/inboxes/:inboxId/messages                                        */
/* -------------------------------------------------------------------------- */

export async function inboxMessages(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const inboxId = c.req.param('inboxId');

  const existing = await c.env.DB.prepare(
    `SELECT inbox_id, agent_id FROM inboxes WHERE inbox_id = ?`
  )
    .bind(inboxId)
    .first<{ inbox_id: string; agent_id: string }>();

  if (!existing || existing.agent_id !== agent.agent_id) {
    return c.json({ error: 'not_found' }, 404);
  }

  const limitRaw = c.req.query('limit');
  let limit = 20;
  if (limitRaw !== undefined && limitRaw !== null && limitRaw !== '') {
    const n = Number(limitRaw);
    if (!Number.isFinite(n) || n < 1 || n > 50) {
      return c.json({ error: 'validation_failed' }, 400);
    }
    limit = Math.floor(n);
  }
  const before = c.req.query('before');

  let sql = `SELECT inbound_id, from_email, to_email, subject, body, status, received_at
               FROM inbound_emails
              WHERE inbox_id = ?`;
  const binds: unknown[] = [inboxId];
  if (before) {
    sql += ` AND received_at < ?`;
    binds.push(before);
  }
  sql += ` ORDER BY received_at DESC LIMIT ?`;
  binds.push(limit + 1);

  const rows = await c.env.DB.prepare(sql).bind(...binds).all<{
    inbound_id: string;
    from_email: string;
    to_email: string;
    subject: string;
    body: string;
    status: string;
    received_at: string;
  }>();

  const results = rows.results ?? [];
  const hasMore = results.length > limit;
  const page = hasMore ? results.slice(0, limit) : results;

  const messages = page.map((r) => ({
    inbound_id: r.inbound_id,
    from: r.from_email,
    to: r.to_email,
    subject: r.subject,
    text_preview: (r.body ?? '').slice(0, 280),
    status: r.status,
    received_at: r.received_at,
    content_trust: 'external',
  }));

  const nextBefore = hasMore && page.length > 0 ? page[page.length - 1].received_at : null;

  return c.json({ messages, next_before: nextBefore });
}


/* -------------------------------------------------------------------------- */
/*  POST /api/inboxes/:inboxId/reply                                          */
/* -------------------------------------------------------------------------- */

const replyBodySchema = z
  .object({
    in_reply_to_inbound_id: z.string().min(1),
    text: z.string().min(1).max(1_000_000),
    html: z.string().max(1_000_000).optional(),
  })
  .strict();

export async function replyInbox(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const inboxId = c.req.param('inboxId');

  const inbox = await c.env.DB.prepare(
    `SELECT inbox_id, agent_id, address, allowed_senders FROM inboxes WHERE inbox_id = ?`
  )
    .bind(inboxId)
    .first<{ inbox_id: string; agent_id: string; address: string; allowed_senders: string | null }>();

  if (!inbox || inbox.agent_id !== agent.agent_id) {
    return c.json({ error: 'not_found' }, 404);
  }

  const body = await parseJson(c.req.raw);
  const parsed = replyBodySchema.safeParse(body);
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

  // Inbound must belong to this inbox.
  const inbound = await c.env.DB.prepare(
    `SELECT inbound_id, inbox_id, from_email, subject, message_id, in_reply_to, thread_id, received_at
       FROM inbound_emails
      WHERE inbound_id = ? AND inbox_id = ?`
  )
    .bind(input.in_reply_to_inbound_id, inboxId)
    .first<{
      inbound_id: string;
      inbox_id: string;
      from_email: string;
      subject: string;
      message_id: string | null;
      in_reply_to: string | null;
      thread_id: string | null;
      received_at: string;
    }>();

  if (!inbound) {
    return c.json({ error: 'not_found' }, 404);
  }

  const recipient = inbound.from_email;
  const replyWindowDays = Number(c.env.REPLY_WINDOW_DAYS || 30);
  const cutoff = new Date(Date.now() - replyWindowDays * 24 * 60 * 60 * 1000).toISOString();

  const allowed = await c.env.DB.prepare(
    `SELECT 1 FROM inbound_emails
      WHERE inbox_id = ? AND from_email = ? AND received_at >= ?
      LIMIT 1`
  )
    .bind(inboxId, recipient, cutoff)
    .first();

  if (!allowed) {
    return c.json({ error: 'recipient_not_in_thread' }, 403);
  }

  // Reply rate limit per inbox.
  const limit = Number(c.env.REPLY_PER_INBOX_PER_HOUR || 60);
  const bucket = `reply:${inboxId}:${Math.floor(Date.now() / 3_600_000)}`;
  try {
    const allowance = await consumeLimit(c.env.DB, bucket, limit, 3600);
    if (!allowance.allowed) {
      rateLimitResponse(c, allowance.retryAfter);
      return c.json({ error: 'rate_limited' }, 429);
    }
  } catch (err) {
    console.error('rate_limit.error', (err as Error).message);
    return c.json({ error: 'rate_limit_unavailable' }, 503);
  }

  // Free-plan send cap.
  if (agent.plan === 'free') {
    const now = new Date();
    const monthStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1)).toISOString();
    const freeLimit = Number(c.env.FREE_SEND_PER_MONTH || 100);
    let count = 0;
    try {
      const row = await c.env.DB.prepare(
        `SELECT COUNT(*) AS c FROM emails WHERE agent_id = ? AND created_at >= ? AND status != 'failed'`
      )
        .bind(agent.agent_id, monthStart)
        .first<{ c: number }>();
      count = Number(row?.c ?? 0);
    } catch (err) {
      console.error('free_send_count.error', (err as Error).message);
      return c.json({ error: 'send_unavailable' }, 503);
    }
    if (count >= freeLimit) {
      const resetsAt = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1)).toISOString();
      return c.json({ error: 'free_send_limit_reached', limit: freeLimit, resets_at: resetsAt }, 402);
    }
  }

  const messageId = `msg_${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`;
  const from = inbox.address;
  const subject = /^re:/i.test(inbound.subject) ? inbound.subject : `Re: ${inbound.subject}`;
  const originalMessageId = inbound.message_id || `<${inbound.inbound_id}@webhooks.email>`;
  const references = inbound.in_reply_to
    ? `${inbound.in_reply_to} ${originalMessageId}`
    : originalMessageId;

  await enqueueEmail(c.env.DB, {
    messageId,
    agentId: agent.agent_id,
    to: recipient,
    from,
    subject,
    body: input.text,
    html: input.html ?? null,
    callbackWebhook: null,
    tags: null,
  });

  logChargeAsync(c.env, agent.agent_id, 'email.send', messageId);

  // Fast path with threading headers.
  c.executionCtx.waitUntil(
    (async () => {
      try {
        await sendEmail(c.env, {
          from,
          to: recipient,
          subject,
          body: input.text,
          html: input.html ?? null,
          messageId,
          inReplyTo: originalMessageId,
          references,
        });
        await markEmailSent(c.env.DB, messageId);
      } catch (err) {
        console.error(`reply send failed (${messageId}):`, (err as Error).message);
      }
    })()
  );

  console.log(JSON.stringify({ event: 'inbox.reply', inbox_id: inboxId, message_id: messageId }));

  return c.json({ message_id: messageId, status: 'queued', thread_id: inbound.thread_id }, 202);
}
