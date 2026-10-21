import type { Env } from '../types';

/**
 * Purge message content older than the plan's retention window.
 * Billing records (charges, billing_runs) are never touched.
 */
export async function purgeOldMessages(env: Env, now = new Date()): Promise<void> {
  const freeDays = Number(env.RETENTION_DAYS_FREE || 7);
  const paidDays = Number(env.RETENTION_DAYS_PAID || 30);
  const freeCutoff = new Date(now.getTime() - freeDays * 24 * 60 * 60 * 1000).toISOString();
  const paidCutoff = new Date(now.getTime() - paidDays * 24 * 60 * 60 * 1000).toISOString();

  let emailsDeleted = 0;
  let inboundDeleted = 0;

  try {
    const r1 = await env.DB.prepare(
      `DELETE FROM emails
        WHERE created_at < ?
          AND agent_id IN (SELECT agent_id FROM api_keys WHERE plan = 'free')`
    )
      .bind(freeCutoff)
      .run();
    emailsDeleted += r1.meta?.changes ?? 0;

    const r2 = await env.DB.prepare(
      `DELETE FROM emails
        WHERE created_at < ?
          AND agent_id IN (SELECT agent_id FROM api_keys WHERE plan = 'paid')`
    )
      .bind(paidCutoff)
      .run();
    emailsDeleted += r2.meta?.changes ?? 0;

    const r3 = await env.DB.prepare(
      `DELETE FROM inbound_emails
        WHERE received_at < ?
          AND agent_id IN (SELECT agent_id FROM api_keys WHERE plan = 'free')`
    )
      .bind(freeCutoff)
      .run();
    inboundDeleted += r3.meta?.changes ?? 0;

    const r4 = await env.DB.prepare(
      `DELETE FROM inbound_emails
        WHERE received_at < ?
          AND agent_id IN (SELECT agent_id FROM api_keys WHERE plan = 'paid')`
    )
      .bind(paidCutoff)
      .run();
    inboundDeleted += r4.meta?.changes ?? 0;

    console.log(
      JSON.stringify({ event: 'retention.purge', emails: emailsDeleted, inbound: inboundDeleted })
    );
  } catch (err) {
    console.error('retention.purge failed:', (err as Error).message);
  }
}
