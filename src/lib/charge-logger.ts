import type { ChargeEndpoint, Env } from '../types';
import { priceOf } from '../config/pricing';
import { insertCharge } from '../db/queries';

/**
 * Charge logging is the billing system's source of truth. It writes to D1
 * synchronously in the request path (a D1 write is ~5ms and the row is small),
 * but we deliberately do NOT await it on the response path in the API routes -
 * the caller gets its 202 and the charge lands via waitUntil(). D1 writes are
 * durable, so a killed isolate still leaves the row behind.
 */

export type ChargeReceipt = {
  charge_id: string;
  amount_usd: number;
  description: string;
};

export async function logCharge(
  db: D1Database,
  agentId: string,
  endpoint: ChargeEndpoint,
  refId?: string | null
): Promise<ChargeReceipt> {
  const amountUsd = priceOf(endpoint);
  const chargeId = await insertCharge(db, {
    agentId,
    endpoint,
    amountUsd,
    refId: refId ?? null,
  });
  return { charge_id: chargeId, amount_usd: amountUsd, description: endpoint };
}

/**
 * Fire-and-forget variant for the request path. Failures are logged but never
 * surface to the caller - a metering hiccup must not fail a paid request.
 * Charges that fail to write stay unrecorded, which the /status endpoint
 * exposes rather than hiding.
 */
export function logChargeAsync(env: Env, agentId: string, endpoint: ChargeEndpoint, refId?: string): void {
  logCharge(env.DB, agentId, endpoint, refId).catch((err) =>
    console.error(`charge log failed (${endpoint}, agent=${agentId}):`, (err as Error).message)
  );
}

/** Charge we absorb on the agent's behalf (e.g. inbound email we route). */
export const SYSTEM_AGENT_ID = 'system';
