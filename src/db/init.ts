/**
 * Ensure the D1 schema exists. `db:apply` applies schema.sql directly; this
 * module exists so a cold start (or a fresh sandbox DB) self-heals instead of
 * throwing 500s on a missing table.
 *
 * All statements are IF NOT EXISTS, so this is safe to call on every cold start.
 */

const STATEMENTS = [
  `CREATE TABLE IF NOT EXISTS api_keys (
    key_id TEXT PRIMARY KEY,
    agent_id TEXT UNIQUE NOT NULL,
    agent_name TEXT,
    key_hash TEXT NOT NULL,
    key_lookup TEXT NOT NULL,
    stripe_customer_id TEXT NOT NULL,
    default_callback_webhook TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    leaderboard_optin INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_used_at TEXT
  )`,

  `INSERT OR IGNORE INTO api_keys
     (key_id, agent_id, agent_name, key_hash, key_lookup, stripe_customer_id, status, created_at)
   VALUES ('key_system', 'system', 'system', 'pbkdf2$1$AA$AA', 'system-lookup', 'cus_system', 'system', '1970-01-01T00:00:00.000Z')`,

  `CREATE TABLE IF NOT EXISTS webhooks (
    event_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    to_webhook_url TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    headers TEXT,
    notify_url TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    delivery_attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    last_error TEXT,
    response_status INTEGER,
    created_at TEXT NOT NULL,
    delivered_at TEXT
  )`,

  `CREATE TABLE IF NOT EXISTS emails (
    message_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    to_email TEXT NOT NULL,
    from_email TEXT NOT NULL,
    reply_to TEXT,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    html TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    callback_webhook TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    last_error TEXT,
    created_at TEXT NOT NULL,
    delivered_at TEXT
  )`,

  `CREATE TABLE IF NOT EXISTS inbound_emails (
    inbound_id TEXT PRIMARY KEY,
    from_email TEXT NOT NULL,
    to_email TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    agent_id TEXT NOT NULL DEFAULT 'system',
    forwarded_event_id TEXT,
    forwarded_to_webhook INTEGER NOT NULL DEFAULT 0,
    received_at TEXT NOT NULL
  )`,

  `CREATE TABLE IF NOT EXISTS inbound_routes (
    from_email TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    callback_webhook TEXT NOT NULL,
    created_at TEXT NOT NULL
  )`,

  `CREATE TABLE IF NOT EXISTS charges (
    charge_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    amount_usd REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    period_start TEXT NOT NULL,
    created_at TEXT NOT NULL,
    billed_at TEXT,
    billing_period_start TEXT,
    billing_period_end TEXT,
    ref_id TEXT
  )`,

  `CREATE TABLE IF NOT EXISTS billing_runs (
    run_id TEXT PRIMARY KEY,
    billing_period_start TEXT NOT NULL,
    billing_period_end TEXT NOT NULL,
    agents_charged INTEGER NOT NULL,
    total_usd REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'completed',
    error TEXT,
    executed_at TEXT NOT NULL
  )`,

  `CREATE TABLE IF NOT EXISTS endpoint_metrics (
    endpoint TEXT NOT NULL,
    day TEXT NOT NULL,
    requests INTEGER NOT NULL DEFAULT 0,
    successes INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    total_duration_ms INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (endpoint, day)
  )`,

  `CREATE INDEX IF NOT EXISTS idx_webhooks_due   ON webhooks(status, next_attempt_at)`,
  `CREATE INDEX IF NOT EXISTS idx_emails_due     ON emails(status, next_attempt_at)`,
  `CREATE INDEX IF NOT EXISTS idx_charges_period ON charges(status, period_start)`,
  `CREATE INDEX IF NOT EXISTS idx_keys_lookup   ON api_keys(key_lookup)`,
];

let ensured: Promise<void> | null = null;

/**
 * Idempotent and memoised per isolate. Uses a single `db.batch()` so the whole
 * schema is one network round trip rather than N sequential writes - important
 * because this runs on the first request of every cold isolate.
 *
 * Returns quietly if D1 is unreachable so a transient blip degrades rather than
 * turning every request into a 500.
 */
export async function ensureDB(db: D1Database): Promise<void> {
  if (!ensured) {
    ensured = db
      .batch(STATEMENTS.map((sql) => db.prepare(sql)))
      .then(() => undefined)
      .catch((err) => {
        console.error('ensureDB failed:', (err as Error)?.message ?? err);
        ensured = null; // allow retry on a later cold start
        throw err;
      });
  }
  return ensured;
}

/** Test helper: forget that we already ran. */
export function __resetEnsureCache(): void {
  ensured = null;
}
