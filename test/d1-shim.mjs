/**
 * A D1-compatible adapter over node:sqlite.
 *
 * Lets the Worker run under plain Node for tests, so routing, auth, validation,
 * queueing, metering and SQL can be exercised end to end in CI without
 * workerd. Implements the subset of the D1 surface this project uses:
 * prepare/bind/run/all/first/batch.
 */

import { DatabaseSync } from 'node:sqlite';

function toPlain(row) {
  return row == null ? null : { ...row };
}

export class D1Shim {
  constructor(db) {
    this.db = db;
  }

  static fromFile(path = ':memory:') {
    return new D1Shim(new DatabaseSync(path));
  }

  prepare(sql) {
    return new D1PreparedShim(this.db, sql);
  }

  /** Mirrors D1's transactional batch. */
  async batch(statements) {
    const results = [];
    this.db.exec('BEGIN');
    try {
      for (const stmt of statements) results.push(await stmt.run());
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
    return results;
  }

  exec(sql) {
    return this.db.exec(sql);
  }
}

class D1PreparedShim {
  constructor(db, sql) {
    this.db = db;
    this.sql = sql;
    this.params = [];
    this._stmt = null;
  }

  bind(...values) {
    this.params = values;
    return this;
  }

  _prepare() {
    // node:sqlite rejects some binding types; normalise to what it accepts.
    const normalised = this.params.map((v) => {
      if (v === undefined || v === null) return null;
      if (typeof v === 'boolean') return v ? 1 : 0;
      if (typeof v === 'number' || typeof v === 'bigint') return v;
      if (v instanceof ArrayBuffer || ArrayBuffer.isView(v)) return v;
      if (typeof v === 'object') return JSON.stringify(v);
      return String(v);
    });
    return this.db.prepare(this.sql).all(...normalised);
  }

  async run() {
    const stmt = this.db.prepare(this.sql);
    const normalised = this.params.map((v) => {
      if (v === undefined || v === null) return null;
      if (typeof v === 'boolean') return v ? 1 : 0;
      if (typeof v === 'number' || typeof v === 'bigint') return v;
      if (typeof v === 'object') return JSON.stringify(v);
      return String(v);
    });
    const res = stmt.run(...normalised);
    return {
      success: true,
      meta: {
        changes: Number(res.changes ?? 0),
        last_row_id: Number(res.lastInsertRowid ?? 0),
        duration_ms: 0.5,
      },
      results: [],
    };
  }

  async all() {
    const rows = this._prepare();
    return { success: true, results: rows.map(toPlain), meta: { duration_ms: 0.5 } };
  }

  async first() {
    const rows = this._prepare();
    return rows.length ? toPlain(rows[0]) : null;
  }

  /** Synchronous convenience for direct assertions in tests (not a D1 method). */
  get(...values) {
    this.params = values;
    const rows = this._prepare();
    return rows.length ? toPlain(rows[0]) : null;
  }

  /** Synchronous convenience for direct assertions in tests (not a D1 method). */
  allSync(...values) {
    this.params = values;
    return this._prepare().map(toPlain);
  }
}

/** Minimal stand-in for the `cloudflare:email` module, which Node cannot resolve. */
export class EmailMessageShim {
  constructor(from, to, raw) {
    this.from = from;
    this.to = to;
    this.raw = raw;
  }
}
