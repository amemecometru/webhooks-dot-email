import type { Ctx, Env } from '../types';
import { nowIso } from '../db/queries';
import { parseJson } from './webhooks';
import { timingSafeEqualStr } from '../lib/crypto';

function b64urlEncode(bytes: Uint8Array): string {
  let s = btoa(String.fromCharCode(...bytes));
  s = s.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  return s;
}

function b64urlDecode(s: string): Uint8Array {
  let t = s.replace(/-/g, '+').replace(/_/g, '/');
  while (t.length % 4) t += '=';
  const bin = atob(t);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

async function issueToken(secret: string, agentId: string, exp: number): Promise<string> {
  const payload = { agent_id: agentId, exp };
  const payloadBytes = new TextEncoder().encode(JSON.stringify(payload));
  const payloadB64 = b64urlEncode(payloadBytes);

  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(payloadB64));
  const sigB64 = b64urlEncode(new Uint8Array(sig));
  return `${payloadB64}.${sigB64}`;
}

async function verifyToken(
  secret: string | undefined,
  token: string
): Promise<{ agent_id: string; exp: number } | null> {
  if (!secret) return null;
  const parts = token.split('.');
  if (parts.length !== 2) return null;
  const [payloadB64, sigB64] = parts;

  let payload: { agent_id?: unknown; exp?: unknown };
  try {
    payload = JSON.parse(new TextDecoder().decode(b64urlDecode(payloadB64)));
  } catch {
    return null;
  }
  if (typeof payload.agent_id !== 'string' || typeof payload.exp !== 'number') return null;

  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const expected = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(payloadB64));
  const expectedB64 = b64urlEncode(new Uint8Array(expected));
  if (!timingSafeEqualStr(expectedB64, sigB64)) return null;
  if (payload.exp < Math.floor(Date.now() / 1000)) return null;

  return { agent_id: payload.agent_id, exp: payload.exp };
}

async function fetchMessages(env: Env, agentId: string, before?: string | null) {
  const limit = 25;
  let sql = `
    SELECT 'sent' AS direction, message_id AS id, subject, from_email AS from_addr, to_email AS to_addr, status, created_at, body, html
      FROM emails
     WHERE agent_id = ?
  `;
  const binds: unknown[] = [agentId];
  if (before) {
    sql += ` AND created_at < ?`;
    binds.push(before);
  }
  sql += `
    UNION ALL
    SELECT 'received' AS direction, inbound_id AS id, subject, from_email AS from_addr, to_email AS to_addr, status, received_at AS created_at, body, NULL AS html
      FROM inbound_emails
     WHERE agent_id = ?
  `;
  if (before) {
    // bind placeholder appended after agent id; need inject condition before union? reconstruct below instead
  }
  // Rebuild properly with before on second half:
  sql = `
    SELECT 'sent' AS direction, message_id AS id, subject, from_email AS from_addr, to_email AS to_addr, status, created_at, body, html
      FROM emails
     WHERE agent_id = ?${before ? ' AND created_at < ?' : ''}
    UNION ALL
    SELECT 'received' AS direction, inbound_id AS id, subject, from_email AS from_addr, to_email AS to_addr, status, received_at AS created_at, body, NULL AS html
      FROM inbound_emails
     WHERE agent_id = ?${before ? ' AND received_at < ?' : ''}
    ORDER BY created_at DESC
    LIMIT ?
  `;
  const binds2: unknown[] = [agentId];
  if (before) binds2.push(before);
  binds2.push(agentId);
  if (before) binds2.push(before);
  binds2.push(limit + 1);

  const rows = await env.DB.prepare(sql).bind(...binds2).all<{
    direction: string;
    id: string;
    subject: string;
    from_addr: string;
    to_addr: string;
    status: string;
    created_at: string;
    body: string;
    html: string | null;
  }>();

  const results = rows.results ?? [];
  const hasMore = results.length > limit;
  const page = hasMore ? results.slice(0, limit) : results;
  const nextBefore = hasMore && page.length > 0 ? page[page.length - 1].created_at : null;

  const messages = page.map((r) => ({
    id: r.id,
    direction: r.direction as 'sent' | 'received',
    subject: r.subject,
    from: r.from_addr,
    to: r.to_addr,
    status: r.status,
    created_at: r.created_at,
    text: r.body ?? '',
    html: r.html,
  }));

  return { messages, next_before: nextBefore };
}

function escHtml(s: string): string {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function escSrcdoc(s: string): string {
  return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;');
}

function renderPage(
  messages: { id: string; direction: string; subject: string; from: string; to: string; status: string; created_at: string; text: string; html: string | null }[],
  nextBefore: string | null,
  token: string
): string {
  const rows = messages
    .map((m) => {
      const body =
        m.html != null
          ? `<iframe sandbox="" srcdoc="${escSrcdoc(m.html)}"></iframe>`
          : `<pre>${escHtml(m.text)}</pre>`;
      return `<details><summary>${escHtml(m.direction)} · ${escHtml(m.subject || '(no subject)')} · ${escHtml(m.from)} → ${escHtml(m.to)} · ${escHtml(m.status)} · ${escHtml(m.created_at)}</summary><div><p>From an external sender</p>${body}</div></details>`;
    })
    .join('\n');

  const nextLink = nextBefore
    ? `<p><a href="?before=${encodeURIComponent(nextBefore)}">Next page</a></p>`
    : '';

  return `<!doctype html><html><head><meta charset="utf-8"><title>Sandbox viewer</title><style>body{font-family:system-ui,sans-serif;margin:2rem;}details{margin:.5rem 0;border:1px solid #ddd;padding:.5rem;}summary{cursor:pointer;}pre{white-space:pre-wrap;}iframe{width:100%;min-height:200px;border:1px solid #ccc;}</style></head><body><h1>Messages</h1>${rows}${nextLink}</body></html>`;
}

/* -------------------------------------------------------------------------- */
/*  POST /api/sandbox/session                                                 */
/* -------------------------------------------------------------------------- */

export async function sandboxSession(c: Ctx): Promise<Response> {
  const agent = c.get('agent');
  const secret = c.env.SANDBOX_SIGNING_SECRET;
  if (!secret) return c.json({ error: 'sandbox_unavailable' }, 503);

  const ttl = Number(c.env.SANDBOX_LINK_TTL_SECONDS || 86400);
  const exp = Math.floor(Date.now() / 1000) + ttl;
  const token = await issueToken(secret, agent.agent_id, exp);
  const url = `https://${c.env.API_HOST}/sandbox/${token}`;

  return c.json({ url, expires_at: new Date(exp * 1000).toISOString() });
}

/* -------------------------------------------------------------------------- */
/*  GET /sandbox/:token                                                       */
/* -------------------------------------------------------------------------- */

export async function sandboxPage(c: Ctx): Promise<Response> {
  const token = c.req.param('token') as string;
  const payload = await verifyToken(c.env.SANDBOX_SIGNING_SECRET, token);
  if (!payload) {
    return c.html('<!doctype html><html><body><p>link expired or invalid</p></body></html>', 404);
  }

  const before = c.req.query('before');
  const { messages, next_before } = await fetchMessages(c.env, payload.agent_id, before || null);

  const html = renderPage(messages, next_before, token);

  c.header('Content-Security-Policy', "default-src 'none'; style-src 'unsafe-inline'; frame-src 'self'; img-src 'none'");
  c.header('X-Frame-Options', 'DENY');
  c.header('Referrer-Policy', 'no-referrer');
  c.header('Cache-Control', 'private, no-store');

  return c.html(html, 200);
}

/* -------------------------------------------------------------------------- */
/*  GET /sandbox/:token/messages.json                                         */
/* -------------------------------------------------------------------------- */

export async function sandboxMessages(c: Ctx): Promise<Response> {
  const token = c.req.param('token') as string;
  const payload = await verifyToken(c.env.SANDBOX_SIGNING_SECRET, token);
  if (!payload) {
    return c.json({ error: 'not_found' }, 404);
  }

  const before = c.req.query('before');
  const { messages, next_before } = await fetchMessages(c.env, payload.agent_id, before || null);

  c.header('Cache-Control', 'private, no-store');
  return c.json({ messages, next_before });
}
