export const TAG_RE = /^[a-z0-9_:.-]{1,32}$/;

/**
 * Normalise a tags input. Returns:
 *   null  -> no tags
 *   string[] -> validated, deduped, lowercased tags
 *   undefined -> invalid input (caller returns 400)
 */
export function normaliseTags(input: unknown): string[] | null | undefined {
  if (input === undefined || input === null) return null;
  if (!Array.isArray(input)) return undefined;
  if (input.length > 5) return undefined;

  const out: string[] = [];
  const seen = new Set<string>();
  for (const entry of input) {
    if (typeof entry !== 'string') return undefined;
    const tag = entry.toLowerCase();
    if (!TAG_RE.test(tag)) return undefined;
    if (seen.has(tag)) continue;
    seen.add(tag);
    out.push(tag);
  }

  if (out.length === 0) return null;
  return out;
}
