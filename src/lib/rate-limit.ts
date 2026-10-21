import type { Env } from '../types';

/**
 * D1-backed fixed-window counters for abuse controls.
 *
 * The bucket name carries the window identity (agent+minute, ip+day, route+day)
 * so a single row per bucket is enough. Window start is stored only so the
 * Monday cron can garbage-collect old buckets.
 */

export type LimitResult = {
  allowed: boolean;
  remaining: number;
  retryAfter: number;
};

export async function consumeLimit(
  db: D1Database,
  bucket: string,
  limit: number,
  windowSeconds: number,
  now = new Date()
): Promise<LimitResult> {
  const windowMs = windowSeconds * 1000;
  const windowStart = new Date(Math.floor(now.getTime() / windowMs) * windowMs).toISOString();

  const row = await db
    .prepare(
      `INSERT INTO rate_limits (bucket, count, window_start)
       VALUES (?, 1, ?)
       ON CONFLICT(bucket) DO UPDATE
         SET count = count + 1,
             window_start = excluded.window_start
       RETURNING count`
    )
    .bind(bucket, windowStart)
    .first<{ count: number }>();

  const count = Number(row?.count ?? 0);
  const secondsIntoWindow = (now.getTime() / 1000) % windowSeconds;
  const retryAfter = Math.max(1, Math.ceil(windowSeconds - secondsIntoWindow));

  return {
    allowed: count <= limit,
    remaining: Math.max(0, limit - count),
    retryAfter,
  };
}

/** Delete counters older than a day. Run from the Monday cron. */
export async function cleanupRateLimits(db: D1Database, now = new Date()): Promise<number> {
  const cutoff = new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString();
  const res = await db
    .prepare(`DELETE FROM rate_limits WHERE window_start < ?`)
    .bind(cutoff)
    .run();
  return res.meta?.changes ?? 0;
}

/** Retry-After for a send window, used by the API routes. */
export function rateLimitResponse(c: { header: (name: string, value: string) => void }, retryAfter: number) {
  c.header('Retry-After', String(retryAfter));
}

export function sendBucket(agentId: string, now = new Date()): string {
  return `send:${agentId}:${Math.floor(now.getTime() / 60_000)}`;
}

export function sendLimit(env: Env): number {
  return Number(env.SEND_PER_AGENT_PER_MINUTE || 60);
}
