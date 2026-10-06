/**
 * Single source of truth for pricing. Anything that reports, charges, or
 * meters money must read from here - never hardcode a number.
 *
 * Resolved 2026-10-03 from webhooksCONFIGURATION.md:
 *   email.send   $0.01
 *   webhook.send $0.001
 *   email.inbound $0.0005
 */

export type ChargeEndpoint = 'email.send' | 'webhook.send' | 'email.inbound';

export const PRICING = {
  'email.send': 0.01,
  'webhook.send': 0.001,
  'email.inbound': 0.0005,
} as const satisfies Record<ChargeEndpoint, number>;

export const PUBLIC_PRICING = [
  {
    endpoint: 'email.send' as const,
    label: 'Outbound email',
    unit_cost_usd: PRICING['email.send'],
    unit: 'per email sent',
    stripe_meter_event: 'email_send',
  },
  {
    endpoint: 'webhook.send' as const,
    label: 'Webhook delivery',
    unit_cost_usd: PRICING['webhook.send'],
    unit: 'per webhook delivered',
    stripe_meter_event: 'webhook_send',
  },
  {
    endpoint: 'email.inbound' as const,
    label: 'Inbound email',
    unit_cost_usd: PRICING['email.inbound'],
    unit: 'per inbound email received',
    stripe_meter_event: 'email_inbound',
  },
];

export function priceOf(endpoint: ChargeEndpoint): number {
  return PRICING[endpoint];
}

/** Maps a charge endpoint to its Stripe billing meter event name. */
export const STRIPE_METER_EVENT: Record<ChargeEndpoint, string> = {
  'email.send': 'email_send',
  'webhook.send': 'webhook_send',
  'email.inbound': 'email_inbound',
};
