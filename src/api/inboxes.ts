import { z } from 'zod';
import type { Ctx, Env } from '../types';
import { inboxCreateSchema, inboxPatchSchema } from '../types';
import { nowIso } from '../db/queries';
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
  }));

  const nextBefore = hasMore && page.length > 0 ? page[page.length - 1].received_at : null;

  return c.json({ messages, next_before: nextBefore });
}

