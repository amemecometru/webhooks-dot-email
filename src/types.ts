import type { Context } from 'hono';
import { z } from 'zod';
import type { ChargeEndpoint } from './config/pricing';

export type { ChargeEndpoint };

/* -------------------------------------------------------------------------- */
/*  Bindings                                                                   */
/* -------------------------------------------------------------------------- */

export interface Env {
  DB: D1Database;
  EMAIL: SendEmail;
  AI: Ai;

  ENVIRONMENT: string;
  API_HOST: string;
  MCP_HOST: string;
  OUTBOUND_EMAIL: string;
  WEBHOOK_TIMEOUT_MS: string;
  MAX_RETRIES: string;
  AUTH_CACHE_TTL_SECONDS: string;
  KEY_PBKDF2_ITERATIONS: string;
  DELIVERY_BATCH_SIZE: string;
  CHARGE_ENDPOINT: string;
  CHARGE_WEBHOOK_ENDPOINT: string;
  CHARGE_INBOUND_ENDPOINT: string;

  STRIPE_SECRET?: string;
  STRIPE_WEBHOOK_SECRET?: string;
  STRIPE_METER_EMAIL_SEND?: string;
  STRIPE_METER_WEBHOOK_SEND?: string;
  STRIPE_METER_EMAIL_INBOUND?: string;
}

export type Agent = {
  agent_id: string;
  key_id: string;
  stripe_customer_id: string;
  agent_name: string | null;
  default_callback_webhook: string | null;
};

/* -------------------------------------------------------------------------- */
/*  Shared primitives                                                          */
/* -------------------------------------------------------------------------- */

export const idSchema = z
  .string()
  .min(1)
  .max(64)
  .regex(/^[A-Za-z0-9._-]+$/, 'must be alphanumeric plus . _ -');

export const emailSchema = z
  .string()
  .email()
  .max(320)
  .transform((v) => v.toLowerCase());

const httpsUrl = z
  .string()
  .url()
  .max(2048)
  .refine((v) => v.startsWith('https://'), 'must be https')
  // Reject loopback / private ranges so an agent cannot use us as an SSRF pivot
  // into our own D1 admin plane or the Cloudflare API.
  .refine(
    (v) => {
      try {
        const h = new URL(v).hostname.toLowerCase();
        if (h === 'localhost' || h.endsWith('.localhost') || h.endsWith('.internal')) return false;
        if (h === 'metadata.google.internal') return false;
        const ip = h.replace(/^\[|\]$/g, '');
        if (/^\d+\.\d+\.\d+\.\d+$/.test(ip)) {
          const [a, b] = ip.split('.').map(Number);
          if (a === 10 || a === 127 || a === 0) return false;
          if (a === 172 && b >= 16 && b <= 31) return false;
          if (a === 192 && b === 168) return false;
          if (a === 169 && b === 254) return false;
        }
        return true;
      } catch {
        return false;
      }
    },
    { message: 'must not target localhost or private networks' }
  );

export const httpsUrlSchema = httpsUrl;

/* -------------------------------------------------------------------------- */
/*  POST /webhooks/send                                                        */
/* -------------------------------------------------------------------------- */

export const sendWebhookSchema = z
  .object({
    to_webhook_url: httpsUrl,
    event_type: z.string().min(1).max(128),
    payload: z.unknown(),
    headers: z.record(z.string().max(256)).optional(),
    callback_webhook: httpsUrl.optional(),
  })
  .strict();

export type SendWebhookRequest = z.infer<typeof sendWebhookSchema>;

/* -------------------------------------------------------------------------- */
/*  POST /email/send                                                           */
/* -------------------------------------------------------------------------- */

export const sendEmailSchema = z
  .object({
    to: emailSchema,
    subject: z.string().min(1).max(998),
    body: z.string().min(1).max(1_000_000),
    html: z.string().max(1_000_000).optional(),
    from: emailSchema.optional(),
    reply_to: emailSchema.optional(),
    callback_webhook: httpsUrl.optional(),
  })
  .strict();

export type SendEmailRequest = z.infer<typeof sendEmailSchema>;

/* -------------------------------------------------------------------------- */
/*  POST /email/inbound (programmatic; Email Routing uses the email() handler)   */
/* -------------------------------------------------------------------------- */

export const inboundEmailSchema = z
  .object({
    from: emailSchema,
    to: emailSchema.optional(),
    subject: z.string().max(998).default(''),
    text: z.string().max(1_000_000).default(''),
    html: z.string().max(2_000_000).optional(),
    message_id: z.string().max(512).optional(),
  })
  .strict();

export type InboundEmailRequest = z.infer<typeof inboundEmailSchema>;

/* -------------------------------------------------------------------------- */
/*  GET /api/charges/summary                                                   */
/* -------------------------------------------------------------------------- */

export const chargesSummaryQuerySchema = z.object({
  period: z.enum(['day', 'week', 'month', 'all']).default('week'),
});

/* -------------------------------------------------------------------------- */
/*  POST /api/keys/generate                                                     */
/* -------------------------------------------------------------------------- */

export const generateKeySchema = z
  .object({
    agent_name: idSchema,
    stripe_customer_id: z.string().regex(/^cus_[A-Za-z0-9]+$/, 'must be a Stripe customer id'),
    leaderboard_optin: z.boolean().default(false),
  })
  .strict();

export type GenerateKeyRequest = z.infer<typeof generateKeySchema>;

/* -------------------------------------------------------------------------- */
/*  POST /api/inbound/routes                                                    */
/* -------------------------------------------------------------------------- */

export const inboundRouteSchema = z
  .object({
    from: emailSchema,
    callback_webhook: httpsUrl,
  })
  .strict();

export type InboundRouteRequest = z.infer<typeof inboundRouteSchema>;

/* -------------------------------------------------------------------------- */
/*  Row shapes                                                                  */
/* -------------------------------------------------------------------------- */

export type WebhookRow = {
  event_id: string;
  agent_id: string;
  to_webhook_url: string;
  event_type: string;
  payload: string;
  headers: string | null;
  notify_url: string | null;
  status: 'queued' | 'delivering' | 'delivered' | 'failed';
  delivery_attempts: number;
  next_attempt_at: string;
  last_error: string | null;
  response_status: number | null;
  created_at: string;
  delivered_at: string | null;
};

export type EmailRow = {
  message_id: string;
  agent_id: string;
  to_email: string;
  from_email: string;
  subject: string;
  body: string;
  html: string | null;
  reply_to: string | null;
  status: 'queued' | 'sent' | 'delivered' | 'bounce' | 'failed';
  callback_webhook: string | null;
  attempts: number;
  next_attempt_at: string;
  last_error: string | null;
  created_at: string;
  delivered_at: string | null;
};

/* -------------------------------------------------------------------------- */
/*  Hono context variables                                                      */
/* -------------------------------------------------------------------------- */

export type AppVars = {
  agent: Agent;
  requestId: string;
};

/** Hono context with our bindings and variables bound. */
export type Ctx = Context<{ Bindings: Env; Variables: AppVars }>;
