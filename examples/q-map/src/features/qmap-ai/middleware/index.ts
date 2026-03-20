/**
 * Barrel re-export for middleware modules.
 *
 * Actual implementations:
 *   - middleware/tool-pipeline.ts — 5-stage tool execution pipeline
 *   - middleware/cache.ts — bounded collections, async mutex, mutation idempotency
 *   - middleware/dedup-cache.ts — stateless/mutation/non-actionable dedup (internal)
 *   - middleware/policy-gate.ts — contract + phase gate (internal)
 *   - middleware/schema-preprocess.ts — z.preprocess arg normalization (internal)
 */
export {wrapToolsWithPipeline, type ToolPipelineOptions} from './tool-pipeline';

export {
  stableSerializeForCache,
  rememberBoundedSetValue,
  setBoundedMapValue,
  AsyncMutex,
  type MutationIdempotencyCacheEntry,
  MUTATION_IDEMPOTENCY_CACHE_LIMIT,
  isMutationToolEligibleForIdempotency,
  buildMutationDedupHash,
  cloneNormalizedToolResult,
  putMutationCacheEntry
} from './cache';
