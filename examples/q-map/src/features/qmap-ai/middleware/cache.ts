/**
 * Unified cache utilities: bounded collections, async mutex, mutation idempotency.
 *
 * Consolidates cache-utils.ts + async-mutex.ts + tool-idempotency.ts into a
 * single module for cache-related concerns.
 */

import {DATASET_VALIDATION_MUTATING_TOOLS} from '../services/execution-tracking';

// ─── Bounded collections ──────────────────────────────────────────────────────

export function rememberBoundedSetValue(target: Set<string>, value: unknown, maxSize: number): void {
  const normalized = String(value || '').trim();
  if (!normalized) return;
  if (target.has(normalized)) {
    target.delete(normalized);
  }
  target.add(normalized);
  while (target.size > maxSize) {
    const oldest = target.values().next().value as string | undefined;
    if (!oldest) break;
    target.delete(oldest);
  }
}

export function setBoundedMapValue<K, V>(target: Map<K, V>, key: K, value: V, maxSize: number): void {
  if (target.has(key)) {
    target.delete(key);
  }
  target.set(key, value);
  while (target.size > maxSize) {
    const oldest = target.keys().next().value as K | undefined;
    if (oldest === undefined) break;
    target.delete(oldest);
  }
}

export function stableSerializeForCache(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(item => stableSerializeForCache(item)).join(',')}]`;
  }
  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b));
    return `{${entries.map(([key, entryValue]) => `${JSON.stringify(key)}:${stableSerializeForCache(entryValue)}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

// ─── Async mutex ──────────────────────────────────────────────────────────────

export class AsyncMutex {
  private _tail: Promise<void> = Promise.resolve();
  private _held = false;
  private _queueDepth = 0;

  get held(): boolean {
    return this._held;
  }
  get queueDepth(): number {
    return this._queueDepth;
  }

  async acquire(): Promise<() => void> {
    this._queueDepth++;
    let release: () => void;
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    const prev = this._tail;
    this._tail = gate;
    await prev;
    this._held = true;
    this._queueDepth--;
    return () => {
      this._held = false;
      release!();
    };
  }
}

// ─── Mutation idempotency ─────────────────────────────────────────────────────

export const MUTATION_IDEMPOTENCY_CACHE_LIMIT = 256;

export type MutationIdempotencyCacheEntry = {
  dedupHash: string;
  toolName: string;
  toolCallId: string;
  cachedAtMs: number;
  hits: number;
  normalizedResult: Record<string, unknown>;
};

function normalizeForStableSignature(value: unknown, seen: WeakSet<object> = new WeakSet()): unknown {
  if (value === undefined || typeof value === 'function' || typeof value === 'symbol') return undefined;
  if (value === null) return null;
  if (typeof value === 'bigint') return value.toString();
  if (typeof value === 'number' && !Number.isFinite(value)) return String(value);
  if (Array.isArray(value)) return value.map(item => normalizeForStableSignature(item, seen));
  if (value && typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    if (seen.has(obj)) return '[circular]';
    seen.add(obj);
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(obj).sort((a, b) => a.localeCompare(b))) {
      const next = normalizeForStableSignature(obj[key], seen);
      if (next === undefined) continue;
      out[key] = next;
    }
    return out;
  }
  return value;
}

function stableSerializeForDedupHash(value: unknown): string {
  try {
    return JSON.stringify(normalizeForStableSignature(value)) || '';
  } catch {
    return String(value || '');
  }
}

function hashStringFnv1a32(value: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, '0');
}

export function isMutationToolEligibleForIdempotency(toolName: string): boolean {
  return DATASET_VALIDATION_MUTATING_TOOLS.has(String(toolName || '').trim());
}

export function buildMutationDedupHash(toolName: string, args: Record<string, unknown>): string {
  const canonicalArgs = stableSerializeForDedupHash(args);
  const signature = `${String(toolName || '').trim().toLowerCase()}|${canonicalArgs}`;
  return `mut:${hashStringFnv1a32(signature)}`;
}

export function cloneNormalizedToolResult(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  try {
    return JSON.parse(JSON.stringify(value)) as Record<string, unknown>;
  } catch {
    return {...(value as Record<string, unknown>)};
  }
}

export function putMutationCacheEntry(
  cache: Map<string, MutationIdempotencyCacheEntry>,
  entry: MutationIdempotencyCacheEntry
) {
  setBoundedMapValue(cache, entry.dedupHash, entry, MUTATION_IDEMPOTENCY_CACHE_LIMIT);
}
