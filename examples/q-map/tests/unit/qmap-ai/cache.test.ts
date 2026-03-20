import {describe, it, expect} from 'vitest';
import {
  stableSerializeForCache,
  rememberBoundedSetValue,
  setBoundedMapValue,
  AsyncMutex,
  buildMutationDedupHash,
  cloneNormalizedToolResult,
  isMutationToolEligibleForIdempotency,
  MUTATION_IDEMPOTENCY_CACHE_LIMIT,
  putMutationCacheEntry,
  type MutationIdempotencyCacheEntry
} from '../../../src/features/qmap-ai/middleware/cache';

// ─── stableSerializeForCache ────────────────────────────────────────────────

describe('stableSerializeForCache', () => {
  it('produces identical output for objects with different key order', () => {
    const a = stableSerializeForCache({b: 2, a: 1});
    const b = stableSerializeForCache({a: 1, b: 2});
    expect(a).toBe(b);
  });

  it('produces identical output for deeply nested objects with different key order', () => {
    const a = stableSerializeForCache({z: {b: 2, a: 1}, y: 3});
    const b = stableSerializeForCache({y: 3, z: {a: 1, b: 2}});
    expect(a).toBe(b);
  });

  it('serializes arrays preserving element order', () => {
    const a = stableSerializeForCache([1, 2, 3]);
    const b = stableSerializeForCache([3, 2, 1]);
    expect(a).not.toBe(b);
    expect(stableSerializeForCache([1, 2, 3])).toBe(stableSerializeForCache([1, 2, 3]));
  });

  it('serializes primitives correctly', () => {
    expect(stableSerializeForCache(null)).toBe('null');
    expect(stableSerializeForCache(42)).toBe('42');
    expect(stableSerializeForCache('hello')).toBe('"hello"');
    expect(stableSerializeForCache(true)).toBe('true');
    expect(stableSerializeForCache(false)).toBe('false');
  });

  it('serializes undefined via JSON.stringify', () => {
    // JSON.stringify(undefined) returns the JS value undefined, not a string.
    // stableSerializeForCache delegates to JSON.stringify for primitives.
    const result = stableSerializeForCache(undefined);
    expect(result).toBeUndefined();
  });

  it('handles empty objects and arrays', () => {
    expect(stableSerializeForCache({})).toBe('{}');
    expect(stableSerializeForCache([])).toBe('[]');
  });

  it('handles arrays of objects with different key orders', () => {
    const a = stableSerializeForCache([{b: 2, a: 1}]);
    const b = stableSerializeForCache([{a: 1, b: 2}]);
    expect(a).toBe(b);
  });

  it('distinguishes between different values', () => {
    expect(stableSerializeForCache({a: 1})).not.toBe(stableSerializeForCache({a: 2}));
    expect(stableSerializeForCache({a: 1})).not.toBe(stableSerializeForCache({b: 1}));
  });
});

// ─── rememberBoundedSetValue ────────────────────────────────────────────────

describe('rememberBoundedSetValue', () => {
  it('adds a value to the set', () => {
    const s = new Set<string>();
    rememberBoundedSetValue(s, 'hello', 10);
    expect(s.has('hello')).toBe(true);
  });

  it('trims whitespace from value', () => {
    const s = new Set<string>();
    rememberBoundedSetValue(s, '  hello  ', 10);
    expect(s.has('hello')).toBe(true);
    expect(s.size).toBe(1);
  });

  it('converts non-string values to string', () => {
    const s = new Set<string>();
    rememberBoundedSetValue(s, 42, 10);
    expect(s.has('42')).toBe(true);
  });

  it('ignores null, undefined, and empty strings', () => {
    const s = new Set<string>();
    rememberBoundedSetValue(s, null, 10);
    rememberBoundedSetValue(s, undefined, 10);
    rememberBoundedSetValue(s, '', 10);
    rememberBoundedSetValue(s, '   ', 10);
    expect(s.size).toBe(0);
  });

  it('enforces max size by evicting oldest entries', () => {
    const s = new Set<string>();
    rememberBoundedSetValue(s, 'a', 3);
    rememberBoundedSetValue(s, 'b', 3);
    rememberBoundedSetValue(s, 'c', 3);
    rememberBoundedSetValue(s, 'd', 3);
    expect(s.size).toBe(3);
    expect(s.has('a')).toBe(false);
    expect(s.has('b')).toBe(true);
    expect(s.has('c')).toBe(true);
    expect(s.has('d')).toBe(true);
  });

  it('re-adding existing value moves it to the end (LRU)', () => {
    const s = new Set<string>();
    rememberBoundedSetValue(s, 'a', 3);
    rememberBoundedSetValue(s, 'b', 3);
    rememberBoundedSetValue(s, 'c', 3);
    // Re-add 'a' — should move it to the end
    rememberBoundedSetValue(s, 'a', 3);
    // Now add 'd' — 'b' (oldest) should be evicted, not 'a'
    rememberBoundedSetValue(s, 'd', 3);
    expect(s.size).toBe(3);
    expect(s.has('b')).toBe(false);
    expect(s.has('a')).toBe(true);
  });

  it('handles maxSize of 1', () => {
    const s = new Set<string>();
    rememberBoundedSetValue(s, 'a', 1);
    rememberBoundedSetValue(s, 'b', 1);
    expect(s.size).toBe(1);
    expect(s.has('b')).toBe(true);
    expect(s.has('a')).toBe(false);
  });
});

// ─── setBoundedMapValue ─────────────────────────────────────────────────────

describe('setBoundedMapValue', () => {
  it('sets a value in the map', () => {
    const m = new Map<string, number>();
    setBoundedMapValue(m, 'a', 1, 10);
    expect(m.get('a')).toBe(1);
  });

  it('enforces max size by evicting oldest entries', () => {
    const m = new Map<string, number>();
    setBoundedMapValue(m, 'a', 1, 3);
    setBoundedMapValue(m, 'b', 2, 3);
    setBoundedMapValue(m, 'c', 3, 3);
    setBoundedMapValue(m, 'd', 4, 3);
    expect(m.size).toBe(3);
    expect(m.has('a')).toBe(false);
    expect(m.get('d')).toBe(4);
  });

  it('re-adding existing key moves it to the end (LRU)', () => {
    const m = new Map<string, number>();
    setBoundedMapValue(m, 'a', 1, 3);
    setBoundedMapValue(m, 'b', 2, 3);
    setBoundedMapValue(m, 'c', 3, 3);
    // Re-add 'a' with new value — moves to end
    setBoundedMapValue(m, 'a', 10, 3);
    // Add 'd' — 'b' should be evicted
    setBoundedMapValue(m, 'd', 4, 3);
    expect(m.has('b')).toBe(false);
    expect(m.get('a')).toBe(10);
    expect(m.get('d')).toBe(4);
  });

  it('handles maxSize of 1', () => {
    const m = new Map<string, number>();
    setBoundedMapValue(m, 'a', 1, 1);
    setBoundedMapValue(m, 'b', 2, 1);
    expect(m.size).toBe(1);
    expect(m.has('a')).toBe(false);
    expect(m.get('b')).toBe(2);
  });

  it('updates existing value without changing size when below limit', () => {
    const m = new Map<string, number>();
    setBoundedMapValue(m, 'a', 1, 10);
    setBoundedMapValue(m, 'a', 2, 10);
    expect(m.size).toBe(1);
    expect(m.get('a')).toBe(2);
  });
});

// ─── AsyncMutex ─────────────────────────────────────────────────────────────

describe('AsyncMutex', () => {
  it('starts with held=false and queueDepth=0', () => {
    const mutex = new AsyncMutex();
    expect(mutex.held).toBe(false);
    expect(mutex.queueDepth).toBe(0);
  });

  it('acquire sets held=true, release sets held=false', async () => {
    const mutex = new AsyncMutex();
    const release = await mutex.acquire();
    expect(mutex.held).toBe(true);
    release();
    expect(mutex.held).toBe(false);
  });

  it('sequential acquire/release does not deadlock', async () => {
    const mutex = new AsyncMutex();
    const release1 = await mutex.acquire();
    release1();
    const release2 = await mutex.acquire();
    release2();
    const release3 = await mutex.acquire();
    release3();
    expect(mutex.held).toBe(false);
  });

  it('concurrent acquires execute in FIFO order', async () => {
    const mutex = new AsyncMutex();
    const order: number[] = [];

    const release1 = await mutex.acquire();

    const p2 = mutex.acquire().then(release => {
      order.push(2);
      release();
    });
    const p3 = mutex.acquire().then(release => {
      order.push(3);
      release();
    });

    expect(mutex.queueDepth).toBe(2);
    order.push(1);
    release1();

    await Promise.all([p2, p3]);
    expect(order).toEqual([1, 2, 3]);
  });

  it('queueDepth increments and decrements correctly', async () => {
    const mutex = new AsyncMutex();
    const release1 = await mutex.acquire();
    expect(mutex.queueDepth).toBe(0); // acquired, not queued

    const p2 = mutex.acquire();
    const p3 = mutex.acquire();
    expect(mutex.queueDepth).toBe(2);

    release1();
    const release2 = await p2;
    expect(mutex.queueDepth).toBe(1);

    release2();
    const release3 = await p3;
    expect(mutex.queueDepth).toBe(0);

    release3();
    expect(mutex.held).toBe(false);
  });
});

// ─── buildMutationDedupHash ─────────────────────────────────────────────────

describe('buildMutationDedupHash', () => {
  it('produces stable hash regardless of key order', () => {
    const a = buildMutationDedupHash('clipQMapDatasetByGeometry', {
      datasetName: 'foo',
      opts: {b: 2, a: 1}
    });
    const b = buildMutationDedupHash('clipQMapDatasetByGeometry', {
      opts: {a: 1, b: 2},
      datasetName: 'foo'
    });
    expect(a).toBe(b);
  });

  it('produces different hash when args differ', () => {
    const a = buildMutationDedupHash('clipQMapDatasetByGeometry', {datasetName: 'foo'});
    const b = buildMutationDedupHash('clipQMapDatasetByGeometry', {datasetName: 'bar'});
    expect(a).not.toBe(b);
  });

  it('produces different hash when tool name differs', () => {
    const a = buildMutationDedupHash('clipQMapDatasetByGeometry', {datasetName: 'foo'});
    const b = buildMutationDedupHash('overlayIntersection', {datasetName: 'foo'});
    expect(a).not.toBe(b);
  });

  it('starts with mut: prefix', () => {
    const hash = buildMutationDedupHash('test', {});
    expect(hash.startsWith('mut:')).toBe(true);
  });
});

// ─── isMutationToolEligibleForIdempotency ───────────────────────────────────

describe('isMutationToolEligibleForIdempotency', () => {
  it('returns true for known mutative tools', () => {
    expect(isMutationToolEligibleForIdempotency('clipQMapDatasetByGeometry')).toBe(true);
    expect(isMutationToolEligibleForIdempotency('overlayIntersection')).toBe(true);
    expect(isMutationToolEligibleForIdempotency('tassellateSelectedGeometry')).toBe(true);
  });

  it('returns false for non-mutative tools', () => {
    expect(isMutationToolEligibleForIdempotency('listQMapDatasets')).toBe(false);
    expect(isMutationToolEligibleForIdempotency('previewQMapDatasetRows')).toBe(false);
  });

  it('returns false for empty or undefined', () => {
    expect(isMutationToolEligibleForIdempotency('')).toBe(false);
    expect(isMutationToolEligibleForIdempotency(undefined as any)).toBe(false);
  });
});

// ─── cloneNormalizedToolResult ──────────────────────────────────────────────

describe('cloneNormalizedToolResult', () => {
  it('returns a detached deep copy', () => {
    const source = {llmResult: {success: true, nested: {count: 1}}};
    const cloned = cloneNormalizedToolResult(source);
    expect(cloned).toEqual(source);
    (cloned.llmResult as any).nested.count = 999;
    expect((source.llmResult as any).nested.count).toBe(1);
  });

  it('returns empty object for non-object input', () => {
    expect(cloneNormalizedToolResult(null)).toEqual({});
    expect(cloneNormalizedToolResult(undefined)).toEqual({});
    expect(cloneNormalizedToolResult([1, 2])).toEqual({});
  });

  it('returns empty object for primitive input', () => {
    expect(cloneNormalizedToolResult('hello' as any)).toEqual({});
    expect(cloneNormalizedToolResult(42 as any)).toEqual({});
  });
});

// ─── putMutationCacheEntry ──────────────────────────────────────────────────

describe('putMutationCacheEntry', () => {
  it('adds entry to cache', () => {
    const cache = new Map<string, MutationIdempotencyCacheEntry>();
    putMutationCacheEntry(cache, {
      dedupHash: 'mut:abc',
      toolName: 'test',
      toolCallId: 'call-1',
      cachedAtMs: Date.now(),
      hits: 0,
      normalizedResult: {success: true}
    });
    expect(cache.size).toBe(1);
    expect(cache.has('mut:abc')).toBe(true);
  });

  it('enforces MUTATION_IDEMPOTENCY_CACHE_LIMIT', () => {
    const cache = new Map<string, MutationIdempotencyCacheEntry>();
    for (let i = 0; i < MUTATION_IDEMPOTENCY_CACHE_LIMIT + 8; i++) {
      putMutationCacheEntry(cache, {
        dedupHash: `mut:${i}`,
        toolName: 'test',
        toolCallId: `call-${i}`,
        cachedAtMs: i,
        hits: 0,
        normalizedResult: {success: true}
      });
    }
    expect(cache.size).toBe(MUTATION_IDEMPOTENCY_CACHE_LIMIT);
    expect(cache.has('mut:0')).toBe(false);
    expect(cache.has(`mut:${MUTATION_IDEMPOTENCY_CACHE_LIMIT + 7}`)).toBe(true);
  });
});
