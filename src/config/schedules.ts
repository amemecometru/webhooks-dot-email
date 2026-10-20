/**
 * Cron schedules. These values MUST match `triggers.crons` in wrangler.jsonc;
 * the test suite asserts that alignment.
 */

/** Weekly Stripe billing, Monday 00:00 UTC. */
export const WEEKLY_BILLING_CRON = '0 0 * * 1';

/** Per-minute D1 queue drain (the default branch in `scheduled()`). */
export const DRAIN_CRON = '* * * * *';
