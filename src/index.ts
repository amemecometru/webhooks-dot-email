import { Hono } from 'hono';
import type { Context } from 'hono';
import type { AppVars, Env } from './types';
import { ensureDB } from './db/init';
import { base, notFound, onError, requireAuth, trackMetrics } from './middleware';
import { sendWebhook, webhookStatus } from './api/webhooks';
import { chargesSummaryRoute, emailStatus, sendEmailRoute } from './api/email';
import { handleRawMessage, inboundRoute } from './api/inbound';
import { stripeWebhook } from './api/stripe';
import {
  createInboundRoute,
  generateKey,
  leaderboardRoute,
  listInboundRoutes,
  pricing,
  status,
} from './api/meta';
import { drainEmails, drainWebhooks, requeueStuckSends } from './lib/queue';
import { weeklyBilling } from './lib/billing';

const app = new Hono<{ Bindings: Env; Variables: AppVars }>();

app.onError(onError);
app.notFound(notFound);
app.use('*', base);

/**
 * Schema self-heal. ensureDB batches into a single D1 round trip and is
 * memoised per isolate, so this costs one round trip per cold start, not one
 * per request. A failure here is logged, not thrown: we would rather serve
 * traffic against a partially-created schema than 500 every request.
 */
app.use('*', async (c, next) => {
  await ensureDB(c.env.DB).catch((err) =>
    console.error('ensureDB skipped:', (err as Error).message)
  );
  await next();
});

/* -------------------------------------------------------------------------- */
/*  Public routes                                                              */
/*  Registered BEFORE any auth middleware.                                     */
/* -------------------------------------------------------------------------- */

const index = (c: Context) =>
  c.json({
    service: 'webhooks.email',
    version: '1.0.0',
    endpoints: {
      'POST /webhooks/send': 'queue a webhook delivery ($0.001)',
      'GET /webhooks/:eventId': 'delivery receipt',
      'POST /email/send': 'send an email ($0.01)',
      'GET /email/status/:messageId': 'send receipt',
      'POST /email/inbound': 'programmatic inbound email',
      'GET /api/charges/summary': 'your metered usage',
      'POST /api/keys/generate': 'no-signup key',
      'POST /api/inbound/routes': 'where to fan out inbound email',
      'POST /stripe/webhook': 'Stripe events',
      'GET /status': 'public uptime + counters',
      'GET /pricing': 'public price list',
      'GET /leaderboard': 'opt-in volume leaderboard',
    },
    auth: 'Authorization: Bearer <api_key>',
    sandbox: 'https://mcp.webhooks.email',
  });

app.get('/', index);
app.get('/healthz', (c) => c.json({ ok: true }));
app.get('/status', status);
app.get('/pricing', pricing);
app.get('/leaderboard', leaderboardRoute);

// Signature-authenticated, not bearer-authenticated.
app.post('/stripe/webhook', stripeWebhook);

// No-signup fast track (brief: agent has a working key in 10 seconds).
app.post('/api/keys/generate', generateKey);

// Public. Cloudflare Email Routing also enters via the `email()` export below;
// this endpoint exists so inbound can be exercised with curl.
app.post('/email/inbound', trackMetrics('email.inbound'), inboundRoute);

/* -------------------------------------------------------------------------- */
/*  Authenticated routes                                                       */
/*                                                                             */
/*  NOTE: auth is attached with EXACT paths, never a '/segment/*' glob.        */
/*  A glob like '/email/*' would also wrap the public /email/inbound route     */
/*  above, because Hono runs every matching middleware in registration order  */
/*  until one returns a response. Exact paths make that impossible.            */
/* -------------------------------------------------------------------------- */

const auth = requireAuth();

app.use('/webhooks/send', auth);
app.post('/webhooks/send', trackMetrics('webhook.send'), sendWebhook);

app.use('/webhooks/:eventId', auth);
app.get('/webhooks/:eventId', trackMetrics('webhook.read'), webhookStatus);

app.use('/email/send', auth);
app.post('/email/send', trackMetrics('email.send'), sendEmailRoute);

app.use('/email/status/:messageId', auth);
app.get('/email/status/:messageId', trackMetrics('email.read'), emailStatus);

app.use('/api/charges/summary', auth);
app.get('/api/charges/summary', trackMetrics('charges.summary'), chargesSummaryRoute);

app.use('/api/inbound/routes', auth);
app.get('/api/inbound/routes', listInboundRoutes);
app.post('/api/inbound/routes', createInboundRoute);

/* -------------------------------------------------------------------------- */
/*  Exports                                                                    */
/* -------------------------------------------------------------------------- */

export default {
  fetch: app.fetch,

  /**
   * Cloudflare Email Routing "Send to a Worker" -> worker `senders`.
   * This is the PRIMARY inbound path. It receives a raw MIME stream, not JSON.
   */
  async email(message: ForwardableEmailMessage, env: Env, ctx: ExecutionContext) {
    await ensureDB(env.DB).catch((err) =>
      console.error('ensureDB skipped:', (err as Error).message)
    );
    await handleRawMessage(env, message, ctx);
  },

  /**
   * Cron schedules (see wrangler.jsonc triggers):
   *   "* * * * *"   -> drain the D1 delivery queue, every minute
   *   "0 0 0 * * 0" -> weekly Stripe billing, Sunday 00:00 UTC
   */
  async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext) {
    const task = (async () => {
      if (controller.cron === WEEKLY_BILLING_CRON) {
        await weeklyBilling(env, new Date(controller.scheduledTime));
        return;
      }

      const batch = Number(env.DELIVERY_BATCH_SIZE || 25);
      const requeued = await requeueStuckSends(env);

      const webhooks = await drainWebhooks(env, batch);
      const emails = await drainEmails(env, batch);

      console.log(
        JSON.stringify({
          event: 'queue.drain',
          at: new Date().toISOString(),
          requeued_stuck_sends: requeued,
          webhooks,
          emails,
        })
      );
    })().catch((err) => {
      console.error(
        JSON.stringify({
          event: 'cron.failed',
          cron: controller.cron,
          error: (err as Error).message,
        })
      );
    });

    ctx.waitUntil(task);
  },
} satisfies ExportedHandler<Env>;

export const WEEKLY_BILLING_CRON = '0 0 0 * * 0';
export { app };
