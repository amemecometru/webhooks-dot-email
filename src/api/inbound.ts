import parseMime from 'postal-mime';
import type { Env, Ctx } from '../types';
import { PRICING } from '../config/pricing';
import { fanOutInbound, resolveInboundRoute, resolveInbox } from '../lib/queue';
import { logChargeAsync, SYSTEM_AGENT_ID } from '../lib/charge-logger';
import { consumeLimit } from '../lib/rate-limit';
import { nowIso, utcDay } from '../db/queries';
import { inboundEmailSchema } from '../types';
import { extractReply } from '../lib/reply-parse';
import { parseJson } from './webhooks';

/**
 * Inbound email.
 *
 * Cloudflare Email Routing is configured with "Send to a Worker" -> `senders`,
 * so the PRIMARY path is the `email()` export in src/index.ts, which receives a
 * raw MIME `Message` stream. `POST /email/inbound` is kept as a secondary
 * programmatic path (and is the only one you can exercise with curl).
 *
 * Both paths funnel into storeInbound() so metering and fan-out behave
 * identically regardless of entry point.
 */

export type InboundRecord = {
  from: string;
  to: string;
  subject: string;
  body: string;
  /** Raw Authentication-Results header, when the ingress supplied one. */
  authResults?: string | null;
  messageId?: string | null;
  inReplyTo?: string | null;
  references?: string | null;
};

export type InboundResult = {
  inboundId: string;
  forwardedEventId: string | null;
};

type InboundStatus = 'accepted' | 'rejected_auth' | 'rejected_sender' | 'unrouted' | 'rate_limited';

/**
 * Drop mail that fails authentication only when both the domain-level check
 * (DMARC or SPF) and DKIM fail. A missing header is not evidence of failure.
 */
function authRejected(authResults: string | null | undefined): boolean {
  if (!authResults) return false;
  const value = authResults.toLowerCase();
  const dkimFailed = /\bdkim=fail\b/.test(value);
  const dmarcOrSpfFailed = /\bdmarc=fail\b/.test(value) || /\bspf=fail\b/.test(value);
  return dkimFailed && dmarcOrSpfFailed;
}

async function insertInbound(
  env: Env,
  inboundId: string,
  rec: InboundRecord,
  agentId: string,
  status: InboundStatus,
  inboxId: string | null = null,
  extras?: {
    messageId?: string | null;
    inReplyTo?: string | null;
    threadId?: string | null;
    replyText?: string | null;
    replyConfidence?: string | null;
  }
): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO inbound_emails
       (inbound_id, from_email, to_email, subject, body, agent_id, inbox_id, status, received_at,
        message_id, in_reply_to, thread_id, reply_text, reply_text_confidence)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(
      inboundId,
      rec.from,
      rec.to,
      rec.subject,
      rec.body,
      agentId,
      inboxId,
      status,
      nowIso(),
      extras?.messageId ?? null,
      extras?.inReplyTo ?? null,
      extras?.threadId ?? null,
      extras?.replyText ?? null,
      extras?.replyConfidence ?? null
    )
    .run();
}


async function getAgentPlan(db: D1Database, agentId: string): Promise<'free' | 'paid'> {
  const row = await db
    .prepare(`SELECT plan FROM api_keys WHERE agent_id = ?`)
    .bind(agentId)
    .first<{ plan: string }>()
    .catch(() => null);
  return row?.plan === 'free' ? 'free' : 'paid';
}

export async function storeInbound(env: Env, rec: InboundRecord): Promise<InboundResult> {
  const inboundId = `inb_${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`;

  // Normalise the destination address.
  rec.to = (rec.to ?? '').toLowerCase();

  const messageId = rec.messageId && rec.messageId.trim() ? rec.messageId.trim() : `<${inboundId}@webhooks.email>`;
  const { reply: replyText, confidence: replyConfidence } = extractReply(rec.body ?? '');
  const rootId =
    (rec.references || '').trim().split(/\s+/)[0] ||
    (rec.inReplyTo || '').trim() ||
    messageId;
  const threadDigest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(rootId));
  const threadId = [...new Uint8Array(threadDigest)]
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('')
    .slice(0, 16);

  const extras = { messageId, inReplyTo: rec.inReplyTo ?? null, threadId, replyText, replyConfidence };

  // Authentication failures are recorded for audit but never billed or fanned
  // out. This runs before route resolution so a spoofed sender cannot consume
  // the route owner's inbound quota.
  if (authRejected(rec.authResults)) {
    await insertInbound(env, inboundId, rec, SYSTEM_AGENT_ID, 'rejected_auth', null, extras);
    console.log(
      JSON.stringify({
        event: 'email.inbound.rejected_auth',
        inbound_id: inboundId,
        from: rec.from,
      })
    );
    return { inboundId, forwardedEventId: null };
  }

  // Resolve: inbox wins over sender routes.
  const inbox = await resolveInbox(env.DB, rec.to);
  const senderRoute = inbox ? null : await resolveInboundRoute(env.DB, rec.from);

  // Per-inbox sender allowlist.
  if (inbox) {
    let allowed: string[] | null = null;
    try {
      if (inbox.allowed_senders) allowed = JSON.parse(inbox.allowed_senders);
    } catch {
      allowed = null;
    }
    if (Array.isArray(allowed) && allowed.length > 0) {
      const sender = rec.from.toLowerCase();
      const ok = allowed.some((entry) => {
        const e = String(entry).toLowerCase();
        if (e.startsWith('@')) return sender.endsWith(e);
        return e === sender;
      });
      if (!ok) {
        await insertInbound(env, inboundId, rec, inbox.agent_id, 'rejected_sender', inbox.inbox_id, extras);
        console.log(
          JSON.stringify({
            event: 'email.inbound.rejected_sender',
            inbound_id: inboundId,
            from: rec.from,
          })
        );
        return { inboundId, forwardedEventId: null };
      }
    }
  }

  let target: {
    agent_id: string;
    callback_webhook: string;
    inbox_id: string | null;
    capKey: string;
    wildcard: boolean;
  } | null = null;

  if (inbox) {
    target = {
      agent_id: inbox.agent_id,
      callback_webhook: inbox.callback_url,
      inbox_id: inbox.inbox_id,
      capKey: rec.to,
      wildcard: false,
    };
  } else if (senderRoute) {
    target = {
      agent_id: senderRoute.agent_id,
      callback_webhook: senderRoute.callback_webhook,
      inbox_id: null,
      capKey: senderRoute.from_email,
      wildcard: senderRoute.from_email === '*',
    };
  }

  const ownerId = target?.agent_id ?? SYSTEM_AGENT_ID;

  if (target) {
    const limit = Number(
      target.wildcard
        ? env.INBOUND_WILDCARD_PER_DAY || 200
        : env.INBOUND_PER_ROUTE_PER_DAY || 1000
    );
    const bucket = `inbound:${target.capKey}:${utcDay()}`;

    let capped = false;
    try {
      const allowance = await consumeLimit(env.DB, bucket, limit, 24 * 60 * 60);
      capped = !allowance.allowed;
    } catch (err) {
      console.error('rate_limit.error', (err as Error).message);
      capped = true;
    }

    // Free-plan owners have an additional per-owner daily cap.
    if (!capped) {
      const plan = await getAgentPlan(env.DB, target.agent_id);
      if (plan === 'free') {
        const ownerLimit = Number(env.FREE_INBOUND_PER_DAY || 100);
        const ownerBucket = `inbound-owner:${target.agent_id}:${utcDay()}`;
        try {
          const allowance = await consumeLimit(env.DB, ownerBucket, ownerLimit, 24 * 60 * 60);
          capped = !allowance.allowed;
        } catch (err) {
          console.error('rate_limit.error', (err as Error).message);
          capped = true;
        }
      }
    }

    if (capped) {
      await insertInbound(env, inboundId, rec, ownerId, 'rate_limited', target.inbox_id, extras);
      console.log(
        JSON.stringify({
          event: 'email.inbound.rate_limited',
          inbound_id: inboundId,
          from: rec.from,
          route: target.capKey,
        })
      );
      return { inboundId, forwardedEventId: null };
    }
  }

  await insertInbound(
    env,
    inboundId,
    rec,
    ownerId,
    target ? 'accepted' : 'unrouted',
    target?.inbox_id ?? null,
    extras
  );

  if (target) {
    logChargeAsync(env, target.agent_id, 'email.inbound', inboundId);
  }

  let eventId: string | null = null;
  try {
    const fanned = await fanOutInbound(env, {
      from: rec.from,
      inboundId,
      subject: rec.subject,
      body: rec.body,
      to: rec.to,
      route: senderRoute,
      inboxId: target?.inbox_id ?? null,
      callbackWebhook: inbox?.callback_url ?? null,
      agentId: inbox?.agent_id ?? null,
      receivedAt: nowIso(),
      messageId,
      threadId,
      inReplyTo: rec.inReplyTo ?? null,
      replyText,
      replyConfidence,
    });
    eventId = fanned?.eventId ?? null;
  } catch (err) {
    console.error('inbound fan-out failed:', (err as Error).message);
  }

  if (eventId) {
    await env.DB.prepare(
      `UPDATE inbound_emails SET forwarded_to_webhook = 1, forwarded_event_id = ? WHERE inbound_id = ?`
    )
      .bind(eventId, inboundId)
      .run();
  }

  console.log(JSON.stringify({ event: 'email.inbound', inbound_id: inboundId, from: rec.from }));
  return { inboundId, forwardedEventId: eventId };
}

/** POST /email/inbound — no auth (Email Routing is the real ingress). */
export async function inboundRoute(c: Ctx): Promise<Response> {
  const body = await parseJson(c.req.raw);

  const parsed = inboundEmailSchema.safeParse(body);
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

  const { inboundId, forwardedEventId } = await storeInbound(c.env, {
    from: input.from,
    to: input.to ?? 'webhooks.email',
    subject: input.subject,
    body: input.text,
    messageId: input.message_id ?? null,
    inReplyTo: input.in_reply_to ?? null,
    references: input.references ?? null,
  });

  return c.json({
    inbound_id: inboundId,
    forwarded_event_id: forwardedEventId,
    status: 'stored',
    charge: { amount_usd: PRICING['email.inbound'], description: 'email.inbound' },
  });
}

/**
 * Handle a raw MIME message from Cloudflare Email Routing.
 * Exported for src/index.ts's `email()` export.
 */
export async function handleRawMessage(
  env: Env,
  message: ForwardableEmailMessage,
  ctx: ExecutionContext
): Promise<void> {
  const raw = await new Response(message.raw).text();

  let from = message.from.toLowerCase();
  let to = message.to.toLowerCase();
  let subject = '';
  let body = '';
  let authResults: string | null = null;
  let messageId: string | null = null;
  let inReplyTo: string | null = null;
  let references: string | null = null;

  try {
    const parsed = await parseMime.parse(raw);
    from = (parsed.from?.address ?? from).toLowerCase();
    to = (parsed.to?.[0]?.address ?? to).toLowerCase();
    subject = parsed.subject ?? '';
    body = parsed.text || stripHtml(parsed.html ?? '');
    authResults =
      parsed.headers
        ?.filter((h) => h.key.toLowerCase() === 'authentication-results')
        .map((h) => h.value)
        .join('; ') || null;
    messageId =
      parsed.headers
        ?.find((h) => h.key.toLowerCase() === 'message-id')
        ?.value?.trim() || null;
    inReplyTo =
      parsed.headers
        ?.find((h) => h.key.toLowerCase() === 'in-reply-to')
        ?.value?.trim() || null;
    references =
      parsed.headers
        ?.find((h) => h.key.toLowerCase() === 'references')
        ?.value?.trim() || null;
  } catch (err) {
    console.error('MIME parse failed, storing raw:', (err as Error).message);
    body = raw.slice(0, 100_000);
  }

  const task = storeInbound(env, { from, to, subject, body, authResults, messageId, inReplyTo, references }).catch((err) =>
    console.error('storeInbound failed:', (err as Error).message)
  );

  // Email Routing gives us a short deadline; waitUntil lets us finish the DB
  // write after the response is returned.
  ctx.waitUntil(task);
}

function stripHtml(html: string): string {
  return html
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/\s+/g, ' ')
    .trim();
}
