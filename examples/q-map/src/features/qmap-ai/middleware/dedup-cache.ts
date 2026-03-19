/**
 * Stage 3 — Dedup cache: stateless dedup + mutation idempotency + non-actionable failure.
 *
 * Returns either `null` (proceed to execution) or a cached/blocked result.
 */
import {
  buildMutationDedupHash,
  cloneNormalizedToolResult,
  isMutationToolEligibleForIdempotency,
  putMutationCacheEntry
} from './cache';
import {
  isStatelessToolEligibleForDedup,
  putStatelessToolCacheEntry,
  normalizeToolResult
} from '../tool-result-normalization';
import type {
  StatelessToolCallCacheEntry
} from '../tool-schema-utils';
import type {MutationIdempotencyCacheEntry} from './cache';

export type DedupCacheOptions = {
  toolName: string;
  normalizedArgs: Record<string, unknown>;
  toolCallId: string;
  isInternalValidationRun: boolean;
  bypassStateMachine: boolean;
  currentMutationRevision: number;
  mutationIdempotencyCache?: Map<string, MutationIdempotencyCacheEntry>;
  nonActionableFailureCache?: Map<string, {toolName: string; details: string; failedAtMs: number}>;
  statelessToolCallCache?: Map<string, StatelessToolCallCacheEntry>;
};

export type DedupResult =
  | {hit: false}
  | {hit: true; result: any; requiresValidation: false};

/**
 * Check all dedup caches. Returns a cached result if hit, or {hit: false} to proceed.
 */
export function checkDedupCaches(opts: DedupCacheOptions): DedupResult {
  const {
    toolName,
    normalizedArgs,
    isInternalValidationRun,
    bypassStateMachine,
    currentMutationRevision,
    mutationIdempotencyCache,
    nonActionableFailureCache,
    statelessToolCallCache,
    toolCallId
  } = opts;

  // ─── Stateless read-only dedup ──────────────────────────────────────────────
  const statelessDedupHash =
    !isInternalValidationRun &&
    !bypassStateMachine &&
    isStatelessToolEligibleForDedup(toolName) &&
    !isMutationToolEligibleForIdempotency(toolName)
      ? `${buildMutationDedupHash(toolName, normalizedArgs)}:rev:${currentMutationRevision}`
      : '';

  if (statelessDedupHash && statelessToolCallCache) {
    const cached = statelessToolCallCache.get(statelessDedupHash);
    if (cached?.normalizedResult) {
      const cachedResultClone = cloneNormalizedToolResult(cached.normalizedResult);
      const cachedLlmResult =
        cachedResultClone.llmResult &&
        typeof cachedResultClone.llmResult === 'object' &&
        !Array.isArray(cachedResultClone.llmResult)
          ? {...(cachedResultClone.llmResult as Record<string, unknown>)}
          : {};
      const dedupResult = normalizeToolResult(toolName, {
        ...cachedResultClone,
        llmResult: {
          ...cachedLlmResult,
          success: Boolean(cachedResultClone.success),
          details:
            `Dedup cache hit: skipped duplicate read-only call "${toolName}" ` +
            'with identical arguments in the same turn.'
        }
      });
      return {hit: true, result: dedupResult, requiresValidation: false};
    }
  }

  // ─── Non-actionable failure dedup (e.g. qCumber territorial units) ──────────
  const nonActionableFailureDedupHash =
    !isInternalValidationRun && !bypassStateMachine && toolName === 'queryQCumberTerritorialUnits'
      ? buildMutationDedupHash(toolName, normalizedArgs)
      : '';

  if (nonActionableFailureDedupHash && nonActionableFailureCache) {
    const cachedFailure = nonActionableFailureCache.get(nonActionableFailureDedupHash);
    if (cachedFailure) {
      return {
        hit: true,
        result: normalizeToolResult(toolName, {
          llmResult: {
            success: false,
            details:
              `Previous call to "${toolName}" with identical arguments already failed in this turn. ` +
              `Last error: ${cachedFailure.details} ` +
              'Change expectedAdminType/lv or filters before retrying.'
          }
        }),
        requiresValidation: false
      };
    }
  }

  // ─── Mutation idempotency ───────────────────────────────────────────────────
  const eligibleForMutationIdempotency =
    !isInternalValidationRun &&
    !bypassStateMachine &&
    isMutationToolEligibleForIdempotency(toolName);
  const mutationDedupHash = eligibleForMutationIdempotency
    ? buildMutationDedupHash(toolName, normalizedArgs)
    : '';

  if (mutationDedupHash && mutationIdempotencyCache) {
    const cached = mutationIdempotencyCache.get(mutationDedupHash);
    if (cached && cached.normalizedResult) {
      const nextHits = Math.max(0, Number(cached.hits || 0)) + 1;
      const sourceToolCallId = String(cached.toolCallId || '').trim();
      const sourceCachedAtMs = Number(cached.cachedAtMs || Date.now());
      const dedupDetails =
        `Idempotency cache hit: skipped duplicate mutation "${toolName}" ` +
        `with hash ${mutationDedupHash}.`;
      const cachedResultClone = cloneNormalizedToolResult(cached.normalizedResult);
      const cachedLlmResult =
        cachedResultClone.llmResult &&
        typeof cachedResultClone.llmResult === 'object' &&
        !Array.isArray(cachedResultClone.llmResult)
          ? {...(cachedResultClone.llmResult as Record<string, unknown>)}
          : {};
      const dedupResult = normalizeToolResult(toolName, {
        ...cachedResultClone,
        llmResult: {
          ...cachedLlmResult,
          success: true,
          details: dedupDetails,
          idempotency: {
            mode: 'cache_hit',
            dedupHash: mutationDedupHash,
            sourceToolCallId,
            sourceCachedAtMs,
            cacheReuseCount: nextHits
          }
        }
      });
      putMutationCacheEntry(mutationIdempotencyCache, {
        ...cached,
        hits: nextHits
      });
      return {hit: true, result: dedupResult, requiresValidation: false};
    }
  }

  return {hit: false};
}

/**
 * After successful execution, persist results to relevant caches.
 */
export function persistToCache(opts: {
  toolName: string;
  normalizedArgs: Record<string, unknown>;
  toolCallId: string;
  result: any;
  currentMutationRevision: number;
  isInternalValidationRun: boolean;
  bypassStateMachine: boolean;
  mutationIdempotencyCache?: Map<string, MutationIdempotencyCacheEntry>;
  nonActionableFailureCache?: Map<string, {toolName: string; details: string; failedAtMs: number}>;
  statelessToolCallCache?: Map<string, StatelessToolCallCacheEntry>;
  mutationRevisionRef?: {current: number};
}): any {
  const {
    toolName,
    normalizedArgs,
    toolCallId,
    result,
    currentMutationRevision,
    isInternalValidationRun,
    bypassStateMachine,
    mutationIdempotencyCache,
    nonActionableFailureCache,
    statelessToolCallCache,
    mutationRevisionRef
  } = opts;

  // ─── Persist mutation success for idempotency ───────────────────────────────
  const eligibleForMutationIdempotency =
    !isInternalValidationRun &&
    !bypassStateMachine &&
    isMutationToolEligibleForIdempotency(toolName);
  const mutationDedupHash = eligibleForMutationIdempotency
    ? buildMutationDedupHash(toolName, normalizedArgs)
    : '';

  let persisted = result;
  if (mutationDedupHash && mutationIdempotencyCache && result?.success === true) {
    const llmResult =
      result.llmResult && typeof result.llmResult === 'object' && !Array.isArray(result.llmResult)
        ? {...result.llmResult}
        : {};
    const cachedAtMs = Date.now();
    persisted = normalizeToolResult(toolName, {
      ...result,
      llmResult: {
        ...llmResult,
        idempotency: {
          mode: 'executed',
          dedupHash: mutationDedupHash,
          toolCallId,
          cachedAtMs,
          cacheReuseCount: 0
        }
      }
    });
    putMutationCacheEntry(mutationIdempotencyCache, {
      dedupHash: mutationDedupHash,
      toolName,
      toolCallId,
      cachedAtMs,
      hits: 0,
      normalizedResult: cloneNormalizedToolResult(persisted as Record<string, unknown>)
    });
    if (mutationRevisionRef) {
      mutationRevisionRef.current = Math.max(0, Number(mutationRevisionRef.current || 0)) + 1;
    }
  }

  // ─── Persist stateless read-only result ─────────────────────────────────────
  const statelessDedupHash =
    !isInternalValidationRun &&
    !bypassStateMachine &&
    isStatelessToolEligibleForDedup(toolName) &&
    !isMutationToolEligibleForIdempotency(toolName)
      ? `${buildMutationDedupHash(toolName, normalizedArgs)}:rev:${currentMutationRevision}`
      : '';

  if (
    statelessDedupHash &&
    statelessToolCallCache &&
    persisted &&
    typeof persisted === 'object' &&
    !Array.isArray(persisted)
  ) {
    putStatelessToolCacheEntry(statelessToolCallCache, {
      toolName,
      dedupHash: statelessDedupHash,
      mutationRevision: currentMutationRevision,
      cachedAtMs: Date.now(),
      normalizedResult: cloneNormalizedToolResult(persisted as Record<string, unknown>)
    });
  }

  // ─── Cache non-actionable failures ──────────────────────────────────────────
  const nonActionableFailureDedupHash =
    !isInternalValidationRun && !bypassStateMachine && toolName === 'queryQCumberTerritorialUnits'
      ? buildMutationDedupHash(toolName, normalizedArgs)
      : '';

  if (nonActionableFailureDedupHash && nonActionableFailureCache && persisted?.success === false) {
    nonActionableFailureCache.set(nonActionableFailureDedupHash, {
      toolName,
      details: String(persisted.details || '').trim(),
      failedAtMs: Date.now()
    });
  }

  return persisted;
}
