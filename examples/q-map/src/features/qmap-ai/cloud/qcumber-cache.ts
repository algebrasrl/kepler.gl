/**
 * Cache state and accessor functions for q-cumber queries, dataset help, and provider catalog.
 */
import {rememberBoundedSetValue, setBoundedMapValue, stableSerializeForCache} from '../middleware/cache';

// --- Module-level cache state ---

const EXECUTED_QCUMBER_QUERY_KEYS = new Set<string>();
const EXECUTED_QCUMBER_QUERY_KEYS_MAX_SIZE = 4096;

const QCUMBER_DATASET_HELP_CACHE = new Map<string, any | null>();
const QCUMBER_DATASET_HELP_CACHE_MAX_SIZE = 512;

const QCUMBER_SUCCESS_QUERY_CACHE = new Map<string, {cachedAt: number; llmResult: any}>();
const QCUMBER_SUCCESS_QUERY_CACHE_TTL_MS = 120000;
const QCUMBER_SUCCESS_QUERY_CACHE_MAX_SIZE = 512;

export const QCUMBER_PROVIDER_CATALOG_CACHE_TTL_MS = 5000;
export let qcumberProviderCatalogCache: {expiresAt: number; key: string; items: any[]} | null = null;

// --- Success query cache ---

export function getCachedSuccessfulQcumberQuery(key: string): {cachedAt: number; llmResult: any} | null {
  if (!key) return null;
  const cached = QCUMBER_SUCCESS_QUERY_CACHE.get(key) || null;
  if (!cached) return null;
  if (Date.now() - cached.cachedAt > QCUMBER_SUCCESS_QUERY_CACHE_TTL_MS) {
    QCUMBER_SUCCESS_QUERY_CACHE.delete(key);
    return null;
  }
  return cached;
}

export function setCachedSuccessfulQcumberQuery(key: string, llmResult: any) {
  if (!key || !llmResult?.success) return;
  setBoundedMapValue(
    QCUMBER_SUCCESS_QUERY_CACHE,
    key,
    {
      cachedAt: Date.now(),
      llmResult
    },
    QCUMBER_SUCCESS_QUERY_CACHE_MAX_SIZE
  );
}

// --- Dataset help cache ---

export function qcumberDatasetHelpCacheKey(providerId: string, datasetId: string): string {
  return `${String(providerId || '').trim().toLowerCase()}::${String(datasetId || '').trim().toLowerCase()}`;
}

export function getDatasetHelpFromCache(key: string): any | undefined {
  return QCUMBER_DATASET_HELP_CACHE.get(key);
}

export function hasDatasetHelpInCache(key: string): boolean {
  return QCUMBER_DATASET_HELP_CACHE.has(key);
}

export function setDatasetHelpInCache(key: string, value: any | null): void {
  setBoundedMapValue(QCUMBER_DATASET_HELP_CACHE, key, value, QCUMBER_DATASET_HELP_CACHE_MAX_SIZE);
}

// --- Executed query keys ---

export function hasExecutedQueryKey(key: string): boolean {
  return EXECUTED_QCUMBER_QUERY_KEYS.has(key);
}

export function rememberExecutedQueryKey(key: string): void {
  rememberBoundedSetValue(EXECUTED_QCUMBER_QUERY_KEYS, key, EXECUTED_QCUMBER_QUERY_KEYS_MAX_SIZE);
}

// --- Provider catalog cache ---

export function setProviderCatalogCache(cache: {expiresAt: number; key: string; items: any[]} | null): void {
  qcumberProviderCatalogCache = cache;
}

export function getProviderCatalogCache(): {expiresAt: number; key: string; items: any[]} | null {
  return qcumberProviderCatalogCache;
}

// Re-export stableSerializeForCache so downstream modules can use it without importing from middleware directly.
export {stableSerializeForCache};
