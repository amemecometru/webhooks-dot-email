/**
 * Seeds a test API key into the remote D1 database.
 *
 * Run:  npm run db:seed
 *
 * We generate the PBKDF2 hash HERE rather than in SQL because the hash must
 * match what src/lib/crypto.ts produces (pbkdf2$100000$salt$hash). Writing a
 * placeholder hash from SQL would produce a key that authenticates as invalid.
 *
 * The raw key is printed once. Only the hash is stored, so it cannot be
 * recovered later - re-run this script for a new one.
 */

const ACCOUNT_ID = process.env.CF_ACCOUNT_ID ?? '8a460817bc554362e040644c8e003fb9';
const DATABASE_ID = process.env.D1_DATABASE_ID ?? '03cddfda-aac2-4c7e-bfdf-d0b78a98ab30';
const TOKEN = process.env.CLOUDFLARE_API_TOKEN;

const AGENT_ID = process.env.SEED_AGENT_ID ?? 'test-agent';
const STRIPE_CUSTOMER = process.env.SEED_STRIPE_CUSTOMER ?? 'cus_test_seed';
const ITERATIONS = 100_000;

if (!TOKEN) {
  console.error('CLOUDFLARE_API_TOKEN is required. Source .env first.');
  process.exit(1);
}

function b64(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('base64');
}

async function hashApiKey(key: string): Promise<string> {
  const salt = new Uint8Array(16);
  crypto.getRandomValues(salt);
  const hash = await crypto.subtle.deriveBits(
    { name: 'PBKDF2', hash: 'SHA-256', salt, iterations: ITERATIONS },
    await crypto.subtle.importKey(
      'raw',
      new TextEncoder().encode(key),
      'PBKDF2',
      false,
      ['deriveBits']
    ),
    256
  );
  return `pbkdf2$${ITERATIONS}$${b64(salt)}$${b64(new Uint8Array(hash))}`;
}

const rawKey = `whemails_live_${Buffer.from(crypto.getRandomValues(new Uint8Array(32)))
  .toString('base64')
  .replace(/\+/g, '-')
  .replace(/\//g, '_')
  .replace(/=+$/, '')}`;

const keyHash = await hashApiKey(rawKey);
const keyLookupDigest = async (k: string): Promise<string> => {
  const d = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(k));
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, '0')).join('');
};
const now = new Date().toISOString();

const sql = `INSERT OR REPLACE INTO api_keys
  (key_id, agent_id, agent_name, key_hash, key_lookup, stripe_customer_id, status, leaderboard_optin, created_at)
  VALUES (?, ?, ?, ?, ?, ?, 'active', 0, ?)`;

const res = await fetch(
  `https://api.cloudflare.com/client/v4/accounts/${ACCOUNT_ID}/d1/database/${DATABASE_ID}/query`,
  {
    method: 'POST',
    headers: {
      authorization: `Bearer ${TOKEN}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      sql,
      params: [
        `key_${crypto.randomUUID().slice(0, 8)}`,
        AGENT_ID,
        AGENT_ID,
        keyHash,
        await keyLookupDigest(rawKey),
        STRIPE_CUSTOMER,
        now,
      ],
    }),
  }
);

const json = (await res.json()) as {
  success: boolean;
  errors?: { message: string }[];
  result?: { success: boolean }[];
};

if (!json.success || json.result?.[0]?.success === false) {
  console.error('seed failed:', JSON.stringify(json.errors ?? json, null, 2));
  process.exit(1);
}

console.log('Seeded API key.\n');
console.log(`  agent_id : ${AGENT_ID}`);
console.log(`  api_key  : ${rawKey}`);
console.log(`  customer : ${STRIPE_CUSTOMER}`);
console.log('\nThis key is shown once. Store it now - only the PBKDF2 hash is in D1.');

export {};
