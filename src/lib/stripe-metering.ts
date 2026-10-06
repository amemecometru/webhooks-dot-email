import type { Env } from '../types';
import type { ChargeEndpoint } from '../config/pricing';

/**
 * Stripe Billing Meters integration, written against the REST API with fetch
 * rather than the `stripe` SDK.
 *
 * Why not the SDK: it needs `nodejs_compat` and pulls a large dependency tree
 * into the Worker bundle, and we use exactly four endpoints. A typed fetch
 * wrapper is ~80 lines and removes the compat flag entirely.
 *
 * API shape note: the brief's pseudo-code used
 * `stripe.billing.meter.event_summaries.create({meter_event_id, value})`, which
 * is the deprecated v1 Meter Events API. The current API is
 * `POST /v1/billing/meters/{meter}/event_summaries` with
 * `payload: { value, summarized_by: { stripe_customer_id } }` and an
 * `identifier` used as the idempotency key. We use the current API.
 */

const STRIPE_API = 'https://api.stripe.com/v1';

export class StripeError extends Error {
  readonly status: number;
  readonly type?: string;

  constructor(message: string, status: number, type?: string) {
    super(message);
    this.name = 'StripeError';
    this.status = status;
    this.type = type;
  }
}

function authHeader(env: Env): string {
  const key = env.STRIPE_SECRET;
  if (!key) throw new StripeError('STRIPE_SECRET is not configured', 0, 'missing_secret');
  return `Bearer ${key}`;
}

async function stripeFetch<T>(env: Env, path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${STRIPE_API}${path}`, {
    ...init,
    headers: {
      authorization: authHeader(env),
      ...(init?.body ? { 'content-type': 'application/x-www-form-urlencoded' } : {}),
      ...(init?.headers ?? {}),
    },
  });

  const text = await res.text();
  const json = text ? (JSON.parse(text) as Record<string, unknown>) : {};

  if (!res.ok) {
    const err = (json.error ?? {}) as { message?: string; type?: string };
    throw new StripeError(err.message ?? `Stripe ${res.status}`, res.status, err.type);
  }
  return json as T;
}

function meterIdFor(env: Env, endpoint: ChargeEndpoint): string | undefined {
  switch (endpoint) {
    case 'email.send':
      return env.STRIPE_METER_EMAIL_SEND;
    case 'webhook.send':
      return env.STRIPE_METER_WEBHOOK_SEND;
    case 'email.inbound':
      return env.STRIPE_METER_EMAIL_INBOUND;
  }
}

export function isStripeConfigured(env: Env): boolean {
  return Boolean(env.STRIPE_SECRET);
}

/**
 * Report one aggregated usage event to a meter.
 *
 * `identifier` is the idempotency key - replaying the same billing run will not
 * double-report to Stripe.
 */
export async function reportMeterEvent(
  env: Env,
  args: { endpoint: ChargeEndpoint; customerId: string; value: number; identifier: string; timestamp: number }
): Promise<{ ok: true } | { ok: false; error: string }> {
  const meter = meterIdFor(env, args.endpoint);
  if (!meter) {
    return { ok: false, error: `no Stripe meter configured for ${args.endpoint}` };
  }
  if (!args.customerId || args.customerId === 'system') {
    return { ok: false, error: `invalid stripe_customer_id "${args.customerId}"` };
  }

  const body = new URLSearchParams({
    event_name: args.endpoint.replace('.', '_'),
    identifier: args.identifier,
    timestamp: String(args.timestamp),
    'payload[value]': String(args.value),
    'payload[summarized_by][stripe_customer_id]': args.customerId,
  });

  try {
    await stripeFetch(env, `/billing/meters/${meter}/event_summaries`, {
      method: 'POST',
      body: body.toString(),
    });
    return { ok: true };
  } catch (err) {
    const e = err as StripeError;
    return { ok: false, error: `${e.message} (${e.status}${e.type ? ` ${e.type}` : ''})` };
  }
}

/** Used by GET /api/billing/meters for operators to confirm wiring. */
export async function listMeters(env: Env) {
  return stripeFetch<{ data: { id: string; event_name: string; display_name: string }[] }>(
    env,
    '/billing/meters?limit=20'
  );
}
