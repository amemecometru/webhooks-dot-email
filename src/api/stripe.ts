import type { Ctx } from '../types';
import { verifyStripeSignature } from '../lib/crypto';
import { nowIso } from '../db/queries';

/**
 * POST /stripe/webhook — no auth header, authenticated by Stripe signature.
 * Subscribed events: invoice.created, invoice.paid, invoice.payment_failed,
 * invoice.payment_succeeded.
 */
export async function stripeWebhook(c: Ctx): Promise<Response> {
  const secret = c.env.STRIPE_WEBHOOK_SECRET;
  if (!secret) {
    console.error('STRIPE_WEBHOOK_SECRET not configured; rejecting webhook');
    return c.json({ error: 'webhook_not_configured' }, 503);
  }

  const signature = c.req.header('stripe-signature');
  const payload = await c.req.text();

  const valid = await verifyStripeSignature(payload, signature, secret);
  if (!valid) {
    // Deliberately vague, and no detail about which check failed.
    return c.json({ error: 'invalid_signature' }, 400);
  }

  let event: StripeEvent;
  try {
    event = JSON.parse(payload) as StripeEvent;
  } catch {
    return c.json({ error: 'invalid_payload' }, 400);
  }

  const object = (event.data?.object ?? {}) as Record<string, unknown>;

  switch (event.type) {
    case 'invoice.paid':
    case 'invoice.payment_succeeded': {
      const customer = String(object.customer ?? 'unknown');
      console.log(
        JSON.stringify({
          event: event.type,
          invoice_id: object.id,
          customer,
          amount_paid: object.amount_paid,
          at: nowIso(),
        })
      );
      break;
    }

    case 'invoice.payment_failed': {
      const customer = String(object.customer ?? 'unknown');
      console.error(
        JSON.stringify({
          event: 'invoice.payment_failed',
          invoice_id: object.id,
          customer,
          at: nowIso(),
        })
      );
      break;
    }

    case 'invoice.created':
      console.log(
        JSON.stringify({ event: event.type, invoice_id: object.id, at: nowIso() })
      );
      break;

    default:
      console.log(JSON.stringify({ event: 'stripe.unhandled', type: event.type }));
  }

  return c.json({ received: true });
}

type StripeEvent = {
  id: string;
  type: string;
  created: number;
  data: { object: Record<string, unknown> };
};
