import type { Context, MiddlewareHandler, Next } from 'hono';
import type { Ctx } from '../types';
import { validateApiKey } from '../lib/auth';
import { utcDay } from '../db/queries';

/**
 * Auth middleware. Applied per-route (not app-wide) because
 * /status, /pricing, /leaderboard, /stripe/webhook and /api/keys/generate are
 * deliberately public. See ROUTES in src/index.ts.
 */
export const requireAuth = (): MiddlewareHandler => async (c: Ctx, next: Next) => {
  const header = c.req.header('authorization');
  if (!header?.startsWith('Bearer ')) {
    return c.json({ error: 'missing_api_key', detail: 'Use Authorization: Bearer <api_key>' }, 401);
  }

  const key = header.slice(7).trim();
  const agent = await validateApiKey(key, c.env);

  if (!agent) {
    return c.json({ error: 'invalid_api_key' }, 401);
  }

  c.set('agent', agent);
  await next();
  return undefined;
};

/**
 * Metrics middleware. Records per-endpoint counts and latency into
 * endpoint_metrics (upsert, one row per endpoint per UTC day) which powers
 * GET /status. Written with waitUntil so it never blocks the response.
 */
export const trackMetrics = (endpoint: string): MiddlewareHandler =>
  async (c: Ctx, next: Next) => {
    const started = Date.now();
    let status = 500;
    try {
      await next();
      status = c.res?.status ?? 200;
    } finally {
      const duration = Date.now() - started;
      const day = utcDay();
      const ok = status < 400 ? 1 : 0;
      c.executionCtx.waitUntil(
        c.env.DB.prepare(
          `INSERT INTO endpoint_metrics (endpoint, day, requests, successes, errors, total_duration_ms)
           VALUES (?, ?, 1, ?, ?, ?)
           ON CONFLICT(endpoint, day) DO UPDATE SET
             requests = requests + 1,
             successes = successes + excluded.successes,
             errors = errors + excluded.errors,
             total_duration_ms = total_duration_ms + excluded.total_duration_ms`
        )
          .bind(endpoint, day, ok, ok ? 0 : 1, duration)
          .run()
          .catch((err: unknown) => console.error('metrics write failed:', (err as Error).message))
      );
    }
  };

/** CORS + request id, applied to everything. */
export const base = async (c: Ctx, next: Next): Promise<void> => {
  c.header('Access-Control-Allow-Origin', '*');
  c.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  c.header('Access-Control-Allow-Headers', 'Authorization, Content-Type, X-Agent-Name');
  c.header('X-Content-Type-Options', 'nosniff');

  const requestId = c.req.header('cf-ray') ?? crypto.randomUUID();
  c.set('requestId', requestId);
  c.header('X-Request-Id', requestId);

  if (c.req.method === 'OPTIONS') {
    c.res = new Response(null, { status: 204 });
    return;
  }

  await next();
};

/** Terminal error handler: never leak stack traces to callers. */
export const onError = (err: Error, c: Ctx): Response => {
  const requestId = c.get('requestId');

  if (err?.name === 'BadJson' || /JSON/i.test(err?.message ?? '')) {
    return c.json({ error: 'invalid_json' }, 400);
  }
  if (err?.name === 'PayloadTooLarge') {
    return c.json({ error: 'payload_too_large' }, 413);
  }
  if (err instanceof Error && /ZodError/.test(err.name)) {
    return c.json({ error: 'validation_failed' }, 400);
  }

  console.error(
    JSON.stringify({
      event: 'request.error',
      request_id: requestId,
      path: c.req.path,
      method: c.req.method,
      error: err?.message,
    })
  );
  return c.json({ error: 'internal_error', request_id: requestId }, 500);
};

export const notFound = (c: Ctx): Response =>
  c.json(
    {
      error: 'not_found',
      path: c.req.path,
      hint: 'GET / returns the endpoint index',
    },
    404
  );
