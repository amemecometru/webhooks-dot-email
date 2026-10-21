import type { Ctx } from '../types';
import { TAG_RE } from '../lib/tags';
import { periodStartFor, nowIso } from '../db/queries';

export async function emailStatsRoute(c: Ctx): Promise<Response> {
  const agent = c.get('agent');

  const periodRaw = c.req.query('period') ?? 'week';
  if (periodRaw !== 'day' && periodRaw !== 'week' && periodRaw !== 'month') {
    return c.json({ error: 'validation_failed', details: [{ path: 'period', message: 'invalid period' }] }, 400);
  }
  const period = periodRaw as 'day' | 'week' | 'month';

  const tagRaw = c.req.query('tag');
  const tag = tagRaw && tagRaw.trim() !== '' ? tagRaw : undefined;
  if (tag !== undefined && !TAG_RE.test(tag)) {
    return c.json({ error: 'validation_failed', details: [{ path: 'tag', message: 'invalid tag' }] }, 400);
  }

  const startIso = periodStartFor(period);
  const endIso = nowIso();

  // Totals (with optional tag filter)
  const totalsRows = await c.env.DB.prepare(
    `SELECT status, COUNT(*) AS n
       FROM emails
      WHERE agent_id = ?1
        AND created_at >= ?2
        AND (?3 IS NULL OR EXISTS (
              SELECT 1 FROM json_each(emails.tags) WHERE json_each.value = ?3))
      GROUP BY status`
  )
    .bind(agent.agent_id, startIso, tag ?? null)
    .all<{ status: string; n: number }>();

  const totals = { queued: 0, sending: 0, sent: 0, failed: 0, total: 0 };
  for (const row of totalsRows.results ?? []) {
    const n = Number(row.n ?? 0);
    if (row.status in totals) {
      (totals as Record<string, number>)[row.status] += n;
    }
    totals.total += n;
  }

  // Per-tag breakdown (only when no tag filter)
  let byTagMap = new Map<string, { total: number; sent: number; failed: number; queued: number }>();
  let truncated = false;
  if (!tag) {
    const byTagRows = await c.env.DB.prepare(
      `SELECT j.value AS tag, e.status, COUNT(*) AS n
         FROM emails e, json_each(e.tags) j
        WHERE e.agent_id = ?1
          AND e.created_at >= ?2
          AND e.tags IS NOT NULL
        GROUP BY j.value, e.status`
    )
      .bind(agent.agent_id, startIso)
      .all<{ tag: string; status: string; n: number }>();

    for (const row of byTagRows.results ?? []) {
      const t = row.tag;
      if (!byTagMap.has(t)) {
        byTagMap.set(t, { total: 0, sent: 0, failed: 0, queued: 0 });
      }
      const agg = byTagMap.get(t)!;
      const n = Number(row.n ?? 0);
      agg.total += n;
      if (row.status === 'sent') agg.sent += n;
      if (row.status === 'failed') agg.failed += n;
      if (row.status === 'queued') agg.queued += n;
    }

    if (byTagMap.size > 20) {
      truncated = true;
    }
  }

  // Sort by total desc, take 20
  const byTagSorted = [...byTagMap.entries()]
    .map(([t, agg]) => ({ tag: t, total: agg.total, sent: agg.sent, failed: agg.failed, queued: agg.queued }))
    .sort((a, b) => b.total - a.total)
    .slice(0, 20);

  // Untagged
  let untagged: { total: number; sent: number; failed: number; queued: number } | null = null;
  if (!tag) {
    const untaggedRows = await c.env.DB.prepare(
      `SELECT status, COUNT(*) AS n
         FROM emails
        WHERE agent_id = ?1
          AND created_at >= ?2
          AND tags IS NULL
        GROUP BY status`
    )
      .bind(agent.agent_id, startIso)
      .all<{ status: string; n: number }>();

    const u = { total: 0, sent: 0, failed: 0, queued: 0 };
    for (const row of untaggedRows.results ?? []) {
      const n = Number(row.n ?? 0);
      u.total += n;
      if (row.status === 'sent') u.sent += n;
      if (row.status === 'failed') u.failed += n;
      if (row.status === 'queued') u.queued += n;
    }
    untagged = u;
  }

  return c.json({
    period,
    window: { start: startIso, end: endIso },
    tag: tag ?? null,
    totals,
    by_tag: tag ? [] : byTagSorted,
    untagged,
    truncated,
  });
}
