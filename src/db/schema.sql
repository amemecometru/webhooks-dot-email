-- webhooks.email :: D1 schema
-- Applied with: npm run db:apply
--
-- Design note: there is NO Cloudflare Queue. The `webhooks` and `emails` tables
-- ARE the queues. Rows are written in the request path (status 'queued') and the
-- per-minute cron (`* * * * *`) drains them with exponential backoff. This keeps
-- the "return 202 immediately" contract while surviving Worker eviction, because
-- pending work lives in D1 rather than in an in-memory waitUntil().

PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------------------
-- API keys
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api_keys (
  key_id            TEXT PRIMARY KEY,
  agent_id          TEXT UNIQUE NOT NULL,
  agent_name        TEXT,
  -- format: pbkdf2$<iterations>$<saltBase64>$<hashBase64>
  key_hash          TEXT NOT NULL,
  -- SHA-256(raw key) hex; indexed. Narrows to one row before the CPU-expensive
  -- PBKDF2 verification. NOT a string prefix: every key shares the literal
  -- "whemails_live_" so prefixes are constant across all agents.
  key_lookup        TEXT NOT NULL,
  stripe_customer_id TEXT NOT NULL,
  -- used for delivery-proof callbacks when a request omits callback_webhook
  default_callback_webhook TEXT,
  status            TEXT NOT NULL DEFAULT 'active',  -- active | disabled
  leaderboard_optin INTEGER NOT NULL DEFAULT 0,
  created_at        TEXT NOT NULL,
  last_used_at      TEXT
);

-- Inbound email and the webhook we fan it out to are metered/attributed to
-- agent_id 'system', and charges/webhooks both carry a FOREIGN KEY onto
-- api_keys(agent_id). This sentinel row is what makes those inserts legal.
-- status is 'system' (not 'active') so it never counts as a real agent.
INSERT OR IGNORE INTO api_keys
  (key_id, agent_id, agent_name, key_hash, key_lookup, stripe_customer_id, status, created_at)
VALUES ('key_system', 'system', 'system', 'pbkdf2$1$AA$AA', 'system-lookup', 'cus_system', 'system', '1970-01-01T00:00:00.000Z');

-- ---------------------------------------------------------------------------
-- Outbound webhook deliveries (also the D1-backed queue)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS webhooks (
  event_id          TEXT PRIMARY KEY,
  agent_id          TEXT NOT NULL,
  to_webhook_url    TEXT NOT NULL,
  event_type        TEXT NOT NULL,
  payload           TEXT NOT NULL,          -- JSON
  headers           TEXT,                   -- JSON object of extra headers
  notify_url        TEXT,                   -- delivery-proof callback (brief section 3C)
  status            TEXT NOT NULL DEFAULT 'queued', -- queued|delivering|delivered|failed
  delivery_attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at   TEXT NOT NULL,          -- cron only picks rows where this <= now
  last_error        TEXT,
  response_status   INTEGER,
  created_at        TEXT NOT NULL,
  delivered_at      TEXT,
  FOREIGN KEY(agent_id) REFERENCES api_keys(agent_id)
);

-- ---------------------------------------------------------------------------
-- Outbound email (also the D1-backed queue)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS emails (
  message_id        TEXT PRIMARY KEY,
  agent_id          TEXT NOT NULL,
  to_email          TEXT NOT NULL,
  from_email        TEXT NOT NULL,
  reply_to          TEXT,
  subject           TEXT NOT NULL,
  body              TEXT NOT NULL,
  html              TEXT,
  status            TEXT NOT NULL DEFAULT 'queued', -- queued|sent|delivered|bounce|failed
  callback_webhook  TEXT,
  attempts          INTEGER NOT NULL DEFAULT 0,
  next_attempt_at   TEXT NOT NULL,
  last_error        TEXT,
  created_at        TEXT NOT NULL,
  delivered_at      TEXT,
  FOREIGN KEY(agent_id) REFERENCES api_keys(agent_id)
);

-- ---------------------------------------------------------------------------
-- Inbound email
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inbound_emails (
  inbound_id          TEXT PRIMARY KEY,
  from_email          TEXT NOT NULL,
  to_email            TEXT NOT NULL,
  subject             TEXT NOT NULL,
  body                TEXT NOT NULL,
  agent_id            TEXT NOT NULL DEFAULT 'system',
  forwarded_event_id  TEXT,               -- webhooks.event_id if we fanned it out
  forwarded_to_webhook INTEGER NOT NULL DEFAULT 0,
  received_at         TEXT NOT NULL
);

-- ---------------------------------------------------------------------------
-- Which agent gets an inbound email fanned out to their callback_webhook.
-- 'from' may be an exact address or '*' to catch every sender.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inbound_routes (
  from_email      TEXT PRIMARY KEY,       -- lowercased address, or '*'
  agent_id        TEXT NOT NULL,
  callback_webhook TEXT NOT NULL,
  created_at      TEXT NOT NULL,
  FOREIGN KEY(agent_id) REFERENCES api_keys(agent_id)
);

-- ---------------------------------------------------------------------------
-- Charges (metering)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS charges (
  charge_id            TEXT PRIMARY KEY,
  agent_id             TEXT NOT NULL,      -- 'system' for inbound we absorb
  endpoint             TEXT NOT NULL,      -- email.send | webhook.send | email.inbound
  amount_usd           REAL NOT NULL,
  status               TEXT NOT NULL DEFAULT 'pending', -- pending|billed|failed
  period_start         TEXT NOT NULL,
  created_at           TEXT NOT NULL,
  billed_at            TEXT,
  billing_period_start TEXT,
  billing_period_end   TEXT,
  -- reference to the queued work this charge was for, for auditability
  ref_id               TEXT,
  FOREIGN KEY(agent_id) REFERENCES api_keys(agent_id)
);

-- ---------------------------------------------------------------------------
-- Billing runs (audit trail)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS billing_runs (
  run_id               TEXT PRIMARY KEY,
  billing_period_start TEXT NOT NULL,
  billing_period_end   TEXT NOT NULL,
  agents_charged       INTEGER NOT NULL,
  total_usd            REAL NOT NULL,
  status               TEXT NOT NULL DEFAULT 'completed', -- completed|failed
  error                TEXT,
  executed_at          TEXT NOT NULL
);

-- ---------------------------------------------------------------------------
-- Per-endpoint counters powering GET /status
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS endpoint_metrics (
  endpoint        TEXT NOT NULL,
  day             TEXT NOT NULL,          -- YYYY-MM-DD (UTC)
  requests        INTEGER NOT NULL DEFAULT 0,
  successes       INTEGER NOT NULL DEFAULT 0,
  errors          INTEGER NOT NULL DEFAULT 0,
  total_duration_ms INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (endpoint, day)
);

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_webhooks_agent  ON webhooks(agent_id);
CREATE INDEX IF NOT EXISTS idx_webhooks_status ON webhooks(status);
CREATE INDEX IF NOT EXISTS idx_webhooks_due    ON webhooks(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_emails_agent    ON emails(agent_id);
CREATE INDEX IF NOT EXISTS idx_emails_status   ON emails(status);
CREATE INDEX IF NOT EXISTS idx_emails_due      ON emails(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_inbound_from     ON inbound_emails(from_email);
CREATE INDEX IF NOT EXISTS idx_inbound_received ON inbound_emails(received_at DESC);
CREATE INDEX IF NOT EXISTS idx_charges_agent   ON charges(agent_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_charges_period  ON charges(status, period_start);
CREATE INDEX IF NOT EXISTS idx_charges_run     ON charges(agent_id, period_start);
CREATE INDEX IF NOT EXISTS idx_keys_lookup    ON api_keys(key_lookup);
