/** Stub for the runtime-only `cloudflare:email` builtin. */
export class EmailMessage {
  constructor(from, to, raw) {
    this.from = from;
    this.to = to;
    this.raw = raw;
  }
}
