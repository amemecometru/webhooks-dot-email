import parseMime from 'postal-mime';
import type { Env, Ctx } from '../types';
import { PRICING } from '../config/pricing';
import { fanOutInbound, resolveInboundRoute } from '../lib/queue';
import { logChargeAsync, SYSTEM_AGENT_ID } from '../lib/charge-logger';
import { consumeLimit } from '../lib/rate-limit';
import { nowIso, utcDay } from '../db/queries';
import { inboundEmailSchema } from '../types';
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
};

export type InboundResult = {
  inboundId: string;
  forwardedEventId: string | null;
};

type InboundStatus = 'accepted' | 'rejected_auth' | 'unrouted' | 'rate_limited';

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
  status: InboundStatus
): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO inbound_emails
       (inbound_id, from_email, to_email, subject, body, agent_id, status, received_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(inboundId, rec.from, rec.to, rec.subject, rec.body, agentId, status, nowIso())
    .run();
}

export async function storeInbound(env: Env, rec: InboundRecord): Promise<InboundResult> {
  const inboundId = `inb_${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`;

  // Authentication failures are recorded for audit but never billed or fanned
  // out. This runs before route resolution so a spoofed sender cannot consume
  // the route owner's inbound quota.
  if (authRejected(rec.authResults)) {
    await insertInbound(env, inboundId, rec, SYSTEM_AGENT_ID, 'rejected_auth');
    console.log(
      JSON.stringify({
        event: 'email.inbound.rejected_auth',
        inbound_id: inboundId,
        from: rec.from,
      })
    );
    return { inboundId, forwardedEventId: null };
  }

  // Resolve once: the route owner pays for inbound and owns the fan-out
  // webhook. Unrouted mail is stored for audit under 'system' but not charged.
  const route = await resolveInboundRoute(env.DB, rec.from);
  const ownerId = route?.agent_id ?? SYSTEM_AGENT_ID;

  if (route) {
    const wildcard = route.from_email === '*';
    const limit = Number(
      wildcard
        ? env.INBOUND_WILDCARD_PER_DAY || 200
        : env.INBOUND_PER_ROUTE_PER_DAY || 1000
    );
    const bucket = `inbound:${route.from_email}:${utcDay()}`;

    // Fail closed for inbound: an unusable counter must not turn into an
    // uncapped fan-out on the route owner's bill.
    let capped = false;
    try {
      const allowance = await consumeLimit(env.DB, bucket, limit, 24 * 60 * 60);
      capped = !allowance.allowed;
    } catch (err) {
      console.error('rate_limit.error', (err as Error).message);
      capped = true;
    }

    if (capped) {
      await insertInbound(env, inboundId, rec, ownerId, 'rate_limited');
      console.log(
        JSON.stringify({
          event: 'email.inbound.rate_limited',
          inbound_id: inboundId,
          from: rec.from,
          route: route.from_email,
        })
      );
      return { inboundId, forwardedEventId: null };
    }
  }

  await insertInbound(env, inboundId, rec, ownerId, route ? 'accepted' : 'unrouted');

  if (route) {
    logChargeAsync(env, route.agent_id, 'email.inbound', inboundId);
  }

  let eventId: string | null = null;
  try {
    const fanned = await fanOutInbound(env, {
      from: rec.from,
      inboundId,
      subject: rec.subject,
      body: rec.body,
      to: rec.to,
      route,
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
  } catch (err) {
    console.error('MIME parse failed, storing raw:', (err as Error).message);
    body = raw.slice(0, 100_000);
  }

  const task = storeInbound(env, { from, to, subject, body, authResults }).catch((err) =>
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
