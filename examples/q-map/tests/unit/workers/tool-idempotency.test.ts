import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildMutationDedupHash,
  cloneNormalizedToolResult,
  isMutationToolEligibleForIdempotency,
  MUTATION_IDEMPOTENCY_CACHE_LIMIT,
  putMutationCacheEntry,
  type MutationIdempotencyCacheEntry
} from '../../../src/features/qmap-ai/middleware/cache';

test('buildMutationDedupHash is stable across key order', () => {
  const left = buildMutationDedupHash('clipQMapDatasetByGeometry', {
    datasetName: 'foo',
    opts: {b: 2, a: 1}
  });
  const right = buildMutationDedupHash('clipQMapDatasetByGeometry', {
    opts: {a: 1, b: 2},
    datasetName: 'foo'
  });
  assert.equal(left, right);
});

test('buildMutationDedupHash changes when args or tool differ', () => {
  const base = buildMutationDedupHash('clipQMapDatasetByGeometry', {datasetName: 'foo'});
  const changedArgs = buildMutationDedupHash('clipQMapDatasetByGeometry', {datasetName: 'bar'});
  const changedTool = buildMutationDedupHash('overlayIntersection', {datasetName: 'foo'});
  assert.notEqual(base, changedArgs);
  assert.notEqual(base, changedTool);
});

test('isMutationToolEligibleForIdempotency maps known mutative tools', () => {
  assert.equal(isMutationToolEligibleForIdempotency('clipQMapDatasetByGeometry'), true);
  assert.equal(isMutationToolEligibleForIdempotency('listQMapDatasets'), false);
});

test('cloneNormalizedToolResult returns detached copy', () => {
  const source = {
    llmResult: {
      success: true,
      details: 'ok',
      nested: {count: 1}
    }
  };
  const cloned = cloneNormalizedToolResult(source);
  assert.deepEqual(cloned, source);
  (cloned.llmResult as any).nested.count = 2;
  assert.equal((source.llmResult as any).nested.count, 1);
});

test('putMutationCacheEntry enforces max size and keeps most recent entries', () => {
  const cache = new Map<string, MutationIdempotencyCacheEntry>();
  for (let i = 0; i < MUTATION_IDEMPOTENCY_CACHE_LIMIT + 8; i += 1) {
    const dedupHash = `mut:${i}`;
    putMutationCacheEntry(cache, {
      dedupHash,
      toolName: 'clipQMapDatasetByGeometry',
      toolCallId: `call-${i}`,
      cachedAtMs: i,
      hits: 0,
      normalizedResult: {llmResult: {success: true, details: `ok-${i}`}}
    });
  }
  assert.equal(cache.size, MUTATION_IDEMPOTENCY_CACHE_LIMIT);
  assert.equal(cache.has('mut:0'), false);
  assert.equal(cache.has(`mut:${MUTATION_IDEMPOTENCY_CACHE_LIMIT + 7}`), true);
});
