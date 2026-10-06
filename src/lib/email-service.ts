import { EmailMessage } from 'cloudflare:email';
import type { Env } from '../types';

/**
 * Thin wrapper over the Cloudflare Email binding.
 *
 * The binding's `send()` takes a raw MIME string, so we build the message here
 * rather than depending on a MIME construction library. Headers are escaped and
 * CRLF sequences are stripped from every user-controlled field: without that an
 * agent could inject arbitrary headers (Bcc, or a second Content-Type) via a
 * subject/body containing newlines.
 */

function stripCrlf(s: string): string {
  return s.replace(/[\r\n]+/g, ' ').trim();
}

/** Escape a value used in a header. Also neutralises NUL and other controls. */
function headerValue(s: string): string {
  return stripCrlf(s).replace(/[\u0000-\u001f\u007f]/g, '');
}

function encodeHeaderValue(s: string): string {
  const clean = headerValue(s);
  // RFC 2047 encoded-word if non-ASCII
  // eslint-disable-next-line no-control-regex
  if (/^[\x20-\x7e]*$/.test(clean)) return clean;
  return `=?UTF-8?B?${btoa(String.fromCharCode(...new TextEncoder().encode(clean)))}?=`;
}

function buildMime(opts: {
  from: string;
  to: string;
  subject: string;
  body: string;
  html?: string | null;
  replyTo?: string | null;
  messageId: string;
  date: string;
}): string {
  const boundary = `wh_${opts.messageId.replace(/[^a-zA-Z0-9]/g, '')}`;
  const hasHtml = Boolean(opts.html);

  const headers = [
    `From: ${opts.from}`,
    `To: ${opts.to}`,
    `Subject: ${encodeHeaderValue(opts.subject)}`,
    `Date: ${opts.date}`,
    `Message-ID: <${opts.messageId}@webhooks.email>`,
    `MIME-Version: 1.0`,
  ];
  if (opts.replyTo) headers.push(`Reply-To: ${opts.replyTo}`);

  let body: string;
  if (hasHtml) {
    headers.push(`Content-Type: multipart/alternative; boundary="${boundary}"`);
    body =
      `--${boundary}\r\n` +
      `Content-Type: text/plain; charset="utf-8"\r\n` +
      `Content-Transfer-Encoding: base64\r\n\r\n` +
      `${btoa(String.fromCharCode(...new TextEncoder().encode(opts.body)))}\r\n` +
      `--${boundary}\r\n` +
      `Content-Type: text/html; charset="utf-8"\r\n` +
      `Content-Transfer-Encoding: base64\r\n\r\n` +
      `${btoa(String.fromCharCode(...new TextEncoder().encode(opts.html as string)))}\r\n` +
      `--${boundary}--\r\n`;
  } else {
    headers.push(`Content-Type: text/plain; charset="utf-8"`);
    headers.push(`Content-Transfer-Encoding: base64`);
    body = `${btoa(String.fromCharCode(...new TextEncoder().encode(opts.body)))}\r\n`;
  }

  return `${headers.join('\r\n')}\r\n\r\n${body}`;
}

export async function sendEmail(
  env: Env,
  msg: {
    from: string;
    to: string;
    subject: string;
    body: string;
    html?: string | null;
    replyTo?: string | null;
    messageId: string;
  }
): Promise<void> {
  const raw = buildMime({ ...msg, date: new Date().toUTCString() });

  const result = await env.EMAIL.send(
    new EmailMessage(msg.from, msg.to, raw)
  );

  // The binding resolves even for some rejections, so surface any message.
  if (result && typeof result === 'object' && 'message' in result && result.message) {
    throw new Error(String((result as { message: unknown }).message));
  }
}

export { buildMime };
