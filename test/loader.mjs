/**
 * Resolver hook so the Worker source can be imported directly by Node under test.
 *
 * Two things Node cannot do on its own that a bundler (and wrangler) do:
 *   1. resolve `cloudflare:email`, a runtime-only Cloudflare builtin
 *   2. resolve extensionless relative imports like './db/init' -> './db/init.ts'
 *
 * Everything else (hono, zod, postal-mime) resolves from node_modules normally.
 */

import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const ALIASES = {
  'cloudflare:email': new URL('./stubs/cloudflare-email.mjs', import.meta.url).href,
};

const EXTENSIONS = ['.ts', '.tsx', '.mjs', '.js'];

// Node's type-stripping is keyed off the resolved format, so we must not claim
// plain 'module' for a .ts file or it gets compiled as JavaScript.
const formatFor = (href) => (/\.tsx?$/.test(href) ? 'module-typescript' : 'module');

// Directory imports resolve to their index file (bundler behaviour).
const INDEX_FILES = ['/index.ts', '/index.tsx', '/index.mjs', '/index.js'];

export async function resolve(specifier, context, nextResolve) {
  if (ALIASES[specifier]) {
    return { url: ALIASES[specifier], shortCircuit: true, format: 'module' };
  }

  // Extensionless relative import -> try adding an extension.
  if (
    (specifier.startsWith('./') || specifier.startsWith('../')) &&
    !/\.[a-z]+$/i.test(specifier)
  ) {
    const parentURL = context.parentURL ?? import.meta.url;
    const base = new URL(specifier, parentURL);
    for (const ext of EXTENSIONS) {
      const candidate = new URL(base.href + ext);
      if (existsSync(fileURLToPath(candidate))) {
        return { url: candidate.href, shortCircuit: true, format: formatFor(candidate.href) };
      }
    }
    for (const idx of INDEX_FILES) {
      const candidate = new URL(base.href.replace(/\/$/, '') + idx);
      if (existsSync(fileURLToPath(candidate))) {
        return { url: candidate.href, shortCircuit: true, format: formatFor(candidate.href) };
      }
    }
  }

  return nextResolve(specifier, context);
}
