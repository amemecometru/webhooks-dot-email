export type ReplyExtraction = { reply: string; confidence: 'high' | 'low' };

/**
 * Deterministic extraction of the new text from a plain-text reply.
 * Pure function: no I/O, no mutation of input.
 */
export function extractReply(text: string): ReplyExtraction {
  const normalised = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const lines = normalised.split('\n');

  let cutIndex = -1;

  // Helper: is a line a quote marker?
  const isQuoteLine = (line: string) => /^>/.test(line);

  // Scan for earliest marker.
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    // 1. On ... wrote:
    if (/^On .+ wrote:$/.test(line)) {
      cutIndex = i;
      break;
    }
    // 2. Original Message
    if (/^-{2,} ?Original Message ?-{2,}$/i.test(line)) {
      cutIndex = i;
      break;
    }
    // 3. From: ... followed within 3 lines by Sent: or Date:
    if (/^From: .+/i.test(line)) {
      for (let j = i + 1; j <= Math.min(i + 3, lines.length - 1); j++) {
        if (/^Sent:/i.test(lines[j]) || /^Date:/i.test(lines[j])) {
          cutIndex = i;
          break;
        }
      }
      if (cutIndex === i) break;
    }
    // 4. > quoted line, when it follows at least one non-quoted line
    if (isQuoteLine(line) && i > 0) {
      // ensure at least one previous non-quoted, non-empty line
      let hasNonQuoted = false;
      for (let k = 0; k < i; k++) {
        if (lines[k].trim() !== '' && !isQuoteLine(lines[k])) {
          hasNonQuoted = true;
          break;
        }
      }
      if (hasNonQuoted) {
        cutIndex = i;
        break;
      }
    }
    // 5. separator lines
    if (/^_{5,}$/.test(line) || /^-{5,}$/.test(line)) {
      cutIndex = i;
      break;
    }
    // 6. standard signature delimiter -- (last such line handled below; treat as cut if present)
    if (/^--\s*$/.test(line)) {
      // only cut if it is the last such delimiter in the message
      let lastDelim = -1;
      for (let k = 0; k < lines.length; k++) {
        if (/^--\s*$/.test(lines[k])) lastDelim = k;
      }
      if (lastDelim === i) {
        cutIndex = i;
        break;
      }
    }
  }

  let reply: string;
  if (cutIndex >= 0) {
    reply = lines.slice(0, cutIndex).join('\n');
  } else {
    reply = normalised;
  }

  // Trim trailing whitespace and blank lines.
  reply = reply.replace(/\s+$/g, '');
  // remove trailing blank lines
  reply = reply.replace(/\n{2,}$/g, '\n').trimEnd();

  if (cutIndex < 0) {
    // nothing cut
    if (reply.trim() === '') return { reply: normalised.trimEnd(), confidence: 'low' };
    return { reply: reply === '' ? normalised : reply, confidence: 'low' };
  }

  if (reply.trim() === '') {
    // never return empty
    return { reply: normalised, confidence: 'low' };
  }

  return { reply, confidence: 'high' };
}
