import {rememberBoundedSetValue, setBoundedMapValue} from './middleware/cache';
import type {
  QMapToolResultEnvelope,
  ToolAutoRetryDirective,
  StatelessToolCallCacheEntry
} from './tool-schema-utils';
import {normalizeToolDetails, dedupeNonEmpty} from './dataset-utils';

// ─── Section A: module-level state + small helpers ───────────────────────────

export const DEFAULT_PROVIDER = 'q-storage-backend';

export const EXECUTED_FILTER_TOOL_SIGNATURES = new Set<string>();
export const EXECUTED_TOOL_COMPONENT_KEYS = new Set<string>();
export const EXECUTED_FILTER_TOOL_SIGNATURES_MAX_SIZE = 2048;
export const EXECUTED_TOOL_COMPONENT_KEYS_MAX_SIZE = 4096;

export const QMAP_TOOL_RESULT_SCHEMA = 'qmap.tool_result.v1';

export const STATELESS_TOOL_CALL_CACHE_LIMIT = 256;
export const STATELESS_TOOL_DEDUP_ELIGIBLE_TOOLS = new Set([
  'listQMapDatasets',
  'listQCumberProviders',
  'listQCumberDatasets',
  'getQCumberDatasetHelp',
  // queryQCumberTerritorialUnits, queryQCumberDataset, queryQCumberDatasetSpatial
  // excluded: loadToMap=true mutates map state, dedup cache would return stale
  // results from a previous query and prevent the new dataset from being created.
  'previewQMapDatasetRows',
  'rankQMapDatasetRows',
  'distinctQMapFieldValues',
  'searchQMapFieldValues',
  // countQMapRows: local-only (no backend call), must be deduped to prevent
  // infinite loops when the model calls it repeatedly with identical args.
  // waitForQMapDataset excluded: must allow retry with longer timeout after timeout.
  'countQMapRows'
]);

export function shouldUseLoadingIndicator(): boolean {
  if (typeof window === 'undefined') return true;
  return !(window as any).__QMAP_E2E_TOOLS__;
}

export function makeExecutionKey(prefix: string) {
  return `${prefix}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;
}

export function rememberExecutedToolComponentKey(value: unknown) {
  rememberBoundedSetValue(EXECUTED_TOOL_COMPONENT_KEYS, value, EXECUTED_TOOL_COMPONENT_KEYS_MAX_SIZE);
}

export function rememberExecutedFilterToolSignature(value: unknown) {
  rememberBoundedSetValue(EXECUTED_FILTER_TOOL_SIGNATURES, value, EXECUTED_FILTER_TOOL_SIGNATURES_MAX_SIZE);
}

export function isStatelessToolEligibleForDedup(toolName: string): boolean {
  return STATELESS_TOOL_DEDUP_ELIGIBLE_TOOLS.has(String(toolName || '').trim());
}

export function putStatelessToolCacheEntry(
  cache: Map<string, StatelessToolCallCacheEntry>,
  entry: StatelessToolCallCacheEntry
) {
  setBoundedMapValue(cache, entry.dedupHash, entry, STATELESS_TOOL_CALL_CACHE_LIMIT);
}

// ─── Section B: result normalization + wrapping ───────────────────────────────

function normalizeMessageList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value
      .map(item => normalizeToolDetails(item))
      .filter(Boolean);
  }
  const single = normalizeToolDetails(value);
  return single ? [single] : [];
}

function toCanonicalDatasetRef(value: unknown, asId = false): string {
  const raw = normalizeToolDetails(value);
  if (!raw) return '';
  if (/^id:/i.test(raw)) return `id:${raw.replace(/^id:\s*/i, '').trim()}`;
  if (asId) return `id:${raw}`;
  return raw;
}

function extractProducedDatasetRefs(base: Record<string, unknown>, llmResult: Record<string, unknown>): string[] {
  const refs: string[] = [];
  const push = (value: unknown, asId = false) => {
    const ref = toCanonicalDatasetRef(value, asId);
    if (ref) refs.push(ref);
  };

  const datasetRefKeys = [
    'datasetRef',
    'loadedDatasetRef',
    'outputDatasetRef',
    'newDatasetRef',
    'targetDatasetRef',
    'joinedDatasetRef',
    'aggregateDatasetRef',
    'resultDatasetRef',
    'materializedDatasetRef',
    'intermediateDatasetRef',
    'tessellationDatasetRef',
    'tassellationDatasetRef'
  ];
  const datasetIdKeys = [
    'datasetId',
    'outputDatasetId',
    'newDatasetId',
    'targetDatasetId',
    'joinedDatasetId',
    'aggregateDatasetId',
    'resultDatasetId',
    'materializedDatasetId',
    'intermediateDatasetId',
    'tessellationDatasetId',
    'tassellationDatasetId'
  ];
  const datasetNameKeys = [
    'dataset',
    'loadedDatasetName',
    'datasetName',
    'outputDatasetName',
    'newDatasetName',
    'targetDatasetName',
    'joinedDatasetName',
    'aggregateDatasetName',
    'resultDataset',
    'materializedDataset',
    'intermediateDataset',
    'tessellationDatasetName',
    'tassellationDatasetName'
  ];

  for (const key of datasetRefKeys) {
    push(base[key], false);
    push(llmResult[key], false);
  }
  for (const key of datasetIdKeys) {
    push(base[key], true);
    push(llmResult[key], true);
  }
  for (const key of datasetNameKeys) {
    push(base[key], false);
    push(llmResult[key], false);
  }
  return dedupeNonEmpty(refs);
}

export function extractToolAutoRetryDirective(rawResult: any): ToolAutoRetryDirective | null {
  const llmResult =
    rawResult?.llmResult && typeof rawResult.llmResult === 'object' && !Array.isArray(rawResult.llmResult)
      ? rawResult.llmResult
      : null;
  if (!llmResult || llmResult.success !== false) return null;
  const retryToolName = String((llmResult as any).retryWithTool || '').trim();
  if (!retryToolName) return null;
  const retryArgsRaw = (llmResult as any).retryWithArgs;
  const retryArgs =
    retryArgsRaw && typeof retryArgsRaw === 'object' && !Array.isArray(retryArgsRaw)
      ? {...retryArgsRaw}
      : {};
  const retryReason = String((llmResult as any).retryReason || '').trim();
  return {retryToolName, retryArgs, retryReason};
}

export function normalizeToolResult(
  toolName: string,
  rawResult: any,
  thrownError?: unknown
): {
  llmResult: Record<string, unknown>;
  qmapToolResult: QMapToolResultEnvelope;
  success: boolean;
  details: string;
  [key: string]: unknown;
} {
  const base =
    rawResult && typeof rawResult === 'object' && !Array.isArray(rawResult)
      ? {...rawResult}
      : {};
  const llmResult =
    base.llmResult && typeof base.llmResult === 'object' && !Array.isArray(base.llmResult)
      ? {...base.llmResult}
      : {};
  const currentEnvelope =
    base.qmapToolResult && typeof base.qmapToolResult === 'object' && !Array.isArray(base.qmapToolResult)
      ? {...base.qmapToolResult}
      : {};

  let success =
    typeof currentEnvelope.success === 'boolean'
      ? currentEnvelope.success
      : typeof llmResult.success === 'boolean'
      ? llmResult.success
      : typeof base.success === 'boolean'
      ? base.success
      : false;

  let details =
    normalizeToolDetails(currentEnvelope.details) ||
    normalizeToolDetails(llmResult.details) ||
    normalizeToolDetails(base.details);

  if (!details && thrownError) {
    const errorMessage =
      thrownError instanceof Error
        ? thrownError.message
        : normalizeToolDetails(String(thrownError || ''));
    details = errorMessage ? `Tool "${toolName}" failed: ${errorMessage}` : `Tool "${toolName}" failed.`;
  }

  if (!details) {
    details = success ? `Tool "${toolName}" completed.` : `Tool "${toolName}" failed.`;
  }
  if (thrownError) {
    success = false;
  }

  const qmapToolResult: QMapToolResultEnvelope = {
    schema: QMAP_TOOL_RESULT_SCHEMA,
    toolName,
    success,
    details,
    error: success ? null : {message: details},
    objectiveReached:
      typeof currentEnvelope.objectiveReached === 'boolean'
        ? currentEnvelope.objectiveReached
        : typeof (llmResult as any)?.objectiveReached === 'boolean'
        ? Boolean((llmResult as any)?.objectiveReached)
        : typeof (base as any)?.objectiveReached === 'boolean'
        ? Boolean((base as any)?.objectiveReached)
        : success,
    warnings: dedupeNonEmpty([
      ...normalizeMessageList((currentEnvelope as any)?.warnings),
      ...normalizeMessageList((llmResult as any)?.warnings),
      ...normalizeMessageList((base as any)?.warnings)
    ]),
    blockingErrors: dedupeNonEmpty([
      ...normalizeMessageList((currentEnvelope as any)?.blockingErrors),
      ...normalizeMessageList((llmResult as any)?.blockingErrors),
      ...normalizeMessageList((base as any)?.blockingErrors),
      ...(!success ? [details] : [])
    ]),
    producedDatasetRefs: extractProducedDatasetRefs(base, llmResult as Record<string, unknown>)
  };

  const normalizedLlmResult = {
    ...llmResult,
    success,
    details,
    objectiveReached: qmapToolResult.objectiveReached,
    warnings: qmapToolResult.warnings,
    blockingErrors: qmapToolResult.blockingErrors,
    producedDatasetRefs: qmapToolResult.producedDatasetRefs
  };

  return {
    ...base,
    llmResult: normalizedLlmResult,
    qmapToolResult,
    success,
    details
  };
}

