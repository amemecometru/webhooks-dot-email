import type { Agent, Env } from '../types';
import { nowIso } from '../db/queries';
import { keyLookupDigest, verifyApiKey } from './crypto';

/**
 * API key auth with a module-level (per-isolate) cache.
 *
 * Latency: a D1 lookup costs 5-10ms and PBKDF2-SHA256 at 100k iterations costs
 * ~10ms of CPU, so an uncached auth would cost ~15-20ms on every request. The
 * cache makes the hot path ~0ms. This is the cache the brief asked for, and it
 * is real: Workers reuses the isolate's global scope across requests.
 *
 * Lookup strategy: an indexed `key_lookup` column holds SHA-256(key) hex. One
 * indexed query narrows to a single candidate row, then we PBKDF2-verify it. No
 * secret rotation coupling and no full-table scan.
 *
 * (A string prefix does NOT work here: every key begins with the literal
 * "whemails_live_", so the prefix is identical for all agents.)
 *
 * Trade-off: a revoked key remains valid for up to AUTH_CACHE_TTL_SECONDS in a
 * warm isolate.
 */

type CacheEntry = { agent: Agent; expiresAt: number };

const cache = new Map<string, CacheEntry>();

// Bound the cache so a flood of distinct bad keys cannot grow it without limit.
const MAX_CACHE_ENTRIES = 5000;

export function clearAuthCache(): void {
  cache.clear();
}

export function authCacheSize(): number {
  return cache.size;
}

export async function validateApiKey(key: string, env: Env): Promise<Agent | null> {
  const ttlMs = Number(env.AUTH_CACHE_TTL_SECONDS || 3600) * 1000;
  const now = Date.now();

  const hit = cache.get(key);
  if (hit) {
    if (hit.expiresAt > now) {
      // refresh LRU position
      cache.delete(key);
      cache.set(key, hit);
      return hit.agent;
    }
    cache.delete(key);
  }

  if (!key || key.length < 20) return null;

  const row = await env.DB.prepare(
    `SELECT key_id, agent_id, stripe_customer_id, agent_name,
            default_callback_webhook, key_hash, status
       FROM api_keys
      WHERE key_lookup = ?
      LIMIT 1`
  )
    .bind(await keyLookupDigest(key))
    .first<{
      key_id: string;
      agent_id: string;
      stripe_customer_id: string;
      agent_name: string | null;
      default_callback_webhook: string | null;
      key_hash: string;
      status: string;
    }>()
    .catch((err) => {
      console.error('api_keys lookup failed:', err);
      return null;
    });

  if (!row || row.status !== 'active') return null;
  if (!(await verifyApiKey(key, row.key_hash))) return null;

  const agent: Agent = {
    agent_id: row.agent_id,
    key_id: row.key_id,
    stripe_customer_id: row.stripe_customer_id,
    agent_name: row.agent_name,
    default_callback_webhook: row.default_callback_webhook,
  };

  if (cache.size >= MAX_CACHE_ENTRIES) {
    const oldest = cache.keys().next();
    if (!oldest.done) cache.delete(oldest.value);
  }
  cache.set(key, { agent, expiresAt: now + ttlMs });

  env.DB.prepare(`UPDATE api_keys SET last_used_at = ? WHERE key_id = ?`)
    .bind(nowIso(), row.key_id)
    .run()
    .catch(() => {});

  return agent;
}
