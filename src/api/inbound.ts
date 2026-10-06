import parseMime from 'postal-mime';
import type { Env, Ctx } from '../types';
import { PRICING } from '../config/pricing';
import { fanOutInbound } from '../lib/queue';
import { logChargeAsync, SYSTEM_AGENT_ID } from '../lib/charge-logger';
import { nowIso } from '../db/queries';
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
};

export type InboundResult = {
  inboundId: string;
  forwardedEventId: string | null;
};

export async function storeInbound(env: Env, rec: InboundRecord): Promise<InboundResult> {
  const inboundId = `inb_${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`;

  await env.DB.prepare(
    `INSERT INTO inbound_emails
       (inbound_id, from_email, to_email, subject, body, agent_id, received_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`
  )
    .bind(inboundId, rec.from, rec.to, rec.subject, rec.body, SYSTEM_AGENT_ID, nowIso())
    .run();

  // We absorb inbound cost; it is metered against 'system'.
  logChargeAsync(env, SYSTEM_AGENT_ID, 'email.inbound', inboundId);

  let eventId: string | null = null;
  try {
    eventId = await fanOutInbound(env, {
      from: rec.from,
      inboundId,
      subject: rec.subject,
      body: rec.body,
      to: rec.to,
    });
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

  try {
    const parsed = await parseMime.parse(raw);
    from = (parsed.from?.address ?? from).toLowerCase();
    to = (parsed.to?.[0]?.address ?? to).toLowerCase();
    subject = parsed.subject ?? '';
    body = parsed.text || stripHtml(parsed.html ?? '');
  } catch (err) {
    console.error('MIME parse failed, storing raw:', (err as Error).message);
    body = raw.slice(0, 100_000);
  }

  const task = storeInbound(env, { from, to, subject, body }).catch((err) =>
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
