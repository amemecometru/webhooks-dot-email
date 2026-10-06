#!/usr/bin/env node
/**
 * Deploy-time preflight: verify every required Worker secret actually exists.
 *
 * Why this exists instead of a `secrets.required` block in wrangler.jsonc:
 * wrangler's config schema has no `secrets` key at all (see
 * node_modules/wrangler/config-schema.json), and a deploy --dry-run with one
 * injected is silently ignored -- no error, no warning. It would look like
 * validation while checking nothing. This script does the real check.
 *
 * The required list is derived from every `env.*` read in src/ that is neither
 * a `vars` entry in wrangler.jsonc nor a platform binding (EMAIL / DB / AI).
 * If you add a new `env.FOO` read in src/, add it here or this check is stale.
 *
 * Behaviour:
 *   - Worker not deployed yet -> warn and pass, so the first bootstrap deploy
 *     is not blocked by secrets that cannot exist before the Worker exists.
 *   - Worker deployed, secret missing -> fail with the exact `wrangler secret
 *     put` command to run.
 *   - SKIP_SECRET_CHECK=1 -> pass with a loud notice.
 */

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';

const RETRIES = 4;

const REQUIRED = [
  ['STRIPE_SECRET', 'Stripe API key used by the weekly billing cron.'],
  ['STRIPE_WEBHOOK_SECRET', 'Verifies POST /stripe/webhook signatures.'],
  ['STRIPE_METER_EMAIL_SEND', 'Billing meter id for email.send ($0.01).'],
  ['STRIPE_METER_WEBHOOK_SEND', 'Billing meter id for webhook.send ($0.001).'],
  ['STRIPE_METER_EMAIL_INBOUND', 'Billing meter id for email.inbound ($0.0005).'],
];

const WORKER = JSON.parse(
  readFileSync(new URL('../wrangler.jsonc', import.meta.url), 'utf8')
    .replace(/^\s*\/\/.*$/gm, '')
    .replace(/,(\s*[}\]])/g, '$1'),
).name;

if (process.env.SKIP_SECRET_CHECK === '1') {
  console.warn('SKIP_SECRET_CHECK=1 -- skipping secret preflight.');
  process.exit(0);
}

/**
 * Retried deliberately: this network intermittently fails with "fetch failed",
 * and a flaky call that happens to return an empty list would report every
 * secret as missing -- a false alarm that trains people to ignore the check.
 * We only trust a response that parses as JSON.
 */
function secretList() {
  let last = '';
  for (let attempt = 1; attempt <= RETRIES; attempt++) {
    try {
      const out = execFileSync(
        'npx',
        ['wrangler', 'secret', 'list', '--name', WORKER, '--format', 'json'],
        { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] },
      );
      const parsed = JSON.parse(out);
      const list = Array.isArray(parsed) ? parsed : (parsed.result ?? []);
      if (Array.isArray(list)) return list;
      last = `non-array response: ${out.slice(0, 200)}`;
    } catch (err) {
      last = `${err.stderr ?? ''}${err.stdout ?? ''}${err.message ?? ''}`;
      if (attempt < RETRIES) {
        console.warn(`  secret list attempt ${attempt}/${RETRIES} failed, retrying...`);
        execFileSync('sleep', ['3']);
      }
    }
  }
  throw new Error(last);
}

let present;
try {
  present = new Set(secretList().map((s) => s.name));
} catch (err) {
  const msg = err.message ?? '';
  if (/not found|no such worker|10007/i.test(msg)) {
    console.warn(
      `Worker "${WORKER}" does not exist yet -- skipping secret preflight.\n` +
        `This is the expected result of a first deploy. Re-run \`npm run deploy\`\n` +
        `after setting secrets to get real validation.`,
    );
    process.exit(0);
  }
  console.error(
    'Could not read secrets from Cloudflare after ' + RETRIES + ' attempts:\n' + msg.trim() +
      '\n\nRefusing to guess. Re-run once the network is stable.',
  );
  process.exit(1);
}

const missing = REQUIRED.filter(([name]) => !present.has(name));

if (missing.length === 0) {
  console.log(`Secret preflight OK -- all ${REQUIRED.length} required secrets present.`);
  process.exit(0);
}

console.error(`\nSecret preflight FAILED -- ${missing.length} missing:\n`);
for (const [name, why] of missing) {
  console.error(`  ${name}`);
  console.error(`      ${why}`);
}
console.error('\nSet them with:\n');
for (const [name] of missing) {
  console.error(`  npx wrangler secret put ${name} --name ${WORKER}`);
}
console.error('');
process.exit(1);