import { z } from 'zod';
import { PRICING } from '../config/pricing';
import { enqueueWebhook, getWebhook } from '../db/queries';
import { logChargeAsync } from '../lib/charge-logger';
import { sendWebhookSchema , type Ctx } from '../types';

/** POST /webhooks/send  — 202 Accepted, charged $0.001 */
export async function sendWebhook(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const body = await parseJson(c.req.raw);

  const parsed = sendWebhookSchema.safeParse(body);
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

  const eventId = `evt_${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`;

  // 1. Persist the delivery. This IS the queue - no in-memory waitUntil, so an
  //    isolate eviction between here and the cron cannot lose the event.
  await enqueueWebhook(c.env.DB, {
    eventId,
    agentId: agent.agent_id,
    toWebhookUrl: input.to_webhook_url,
    eventType: input.event_type,
    payload: input.payload,
    headers: input.headers ?? null,
    notifyUrl: input.callback_webhook ?? agent.default_callback_webhook ?? null,
  });

  // 2. Meter it.
  logChargeAsync(c.env, agent.agent_id, 'webhook.send', eventId);

  console.log(
    JSON.stringify({
      event: 'webhook.queued',
      event_id: eventId,
      agent_id: agent.agent_id,
      to: input.to_webhook_url,
      type: input.event_type,
    })
  );

  return c.json(
    {
      event_id: eventId,
      status: 'queued',
      charge: { amount_usd: PRICING['webhook.send'], description: 'webhook.send' },
      status_url: `${c.env.API_HOST}/webhooks/${eventId}`,
    },
    202
  );
}

/** GET /webhooks/:eventId — delivery receipt for an agent polling for proof. */
export async function webhookStatus(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const eventId = z.string().min(1).max(80).parse(c.req.param('eventId'));

  const row = await getWebhook(c.env.DB, eventId);
  if (!row) return c.json({ error: 'not_found' }, 404);
  if (row.agent_id !== agent.agent_id && row.agent_id !== 'system') {
    return c.json({ error: 'forbidden' }, 403);
  }

  return c.json({
    event_id: row.event_id,
    status: row.status,
    event_type: row.event_type,
    delivery_attempts: row.delivery_attempts,
    response_status: row.response_status,
    last_error: row.last_error,
    created_at: row.created_at,
    delivered_at: row.delivered_at,
  });
}

/** Shared JSON body parser with a hard size cap. */
export async function parseJson(req: Request): Promise<unknown> {
  const MAX_BYTES = 1_000_000;
  const len = Number(req.headers.get('content-length') ?? 0);
  if (len > MAX_BYTES) throw new PayloadTooLarge();

  const text = await req.text();
  if (text.length > MAX_BYTES) throw new PayloadTooLarge();
  if (!text.trim()) return {};

  try {
    return JSON.parse(text);
  } catch {
    throw new BadJson();
  }
}

export class BadJson extends Error {
  constructor() {
    super('Body is not valid JSON');
    this.name = 'BadJson';
  }
}

export class PayloadTooLarge extends Error {
  constructor() {
    super('Request body too large');
    this.name = 'PayloadTooLarge';
  }
}
