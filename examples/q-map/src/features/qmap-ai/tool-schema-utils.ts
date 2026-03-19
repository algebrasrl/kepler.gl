import {z} from 'zod';
import type {MutationIdempotencyCacheEntry} from './middleware/cache';
import type React from 'react';
import type {AsyncMutex} from './middleware/cache';
import type {QMapTurnExecutionPhase, QMapTurnExecutionState} from './guardrails';

export const QMAP_PALETTE_NAMES = ['redGreen', 'greenRed', 'blueRed', 'viridis', 'magma', 'yellowRed', 'yellowBlue'] as const;
export const QMAP_PALETTE_ERROR_MESSAGE =
  'Invalid palette. Use one of: redGreen, greenRed, blueRed, viridis, magma, yellowRed, yellowBlue.';

export type QMapPaletteName = (typeof QMAP_PALETTE_NAMES)[number];

export const QMAP_PALETTE_ALIAS_TO_CANONICAL: Record<string, QMapPaletteName> = {
  redgreen: 'redGreen',
  rdylgn: 'redGreen',
  greenred: 'greenRed',
  gnylrd: 'greenRed',
  bluered: 'blueRed',
  rdbu: 'blueRed',
  viridis: 'viridis',
  magma: 'magma',
  yellowred: 'yellowRed',
  ylorrd: 'yellowRed',
  yellowblue: 'yellowBlue',
  ylgnbu: 'yellowBlue'
};

export function normalizePaletteLookupToken(value: unknown): string {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

export function normalizeQMapPaletteName(value: unknown): QMapPaletteName | null {
  const token = normalizePaletteLookupToken(value);
  if (!token) return null;
  return QMAP_PALETTE_ALIAS_TO_CANONICAL[token] || null;
}

export const qMapPaletteSchema = z.string().optional().transform((value, ctx) => {
  if (value == null) return undefined;
  const normalized = normalizeQMapPaletteName(value);
  if (normalized) return normalized;
  ctx.addIssue({
    code: z.ZodIssueCode.custom,
    message: QMAP_PALETTE_ERROR_MESSAGE
  });
  return z.NEVER;
});

export function normalizeEnumLikeToken(value: unknown): string {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

export function buildOptionalLenientEnumSchema(
  values: readonly string[],
  aliases: Record<string, string>,
  fieldLabel: string
) {
  const allowed = new Set(values);
  const allowedList = values.join(', ');
  return z.any().optional().transform((value, ctx): string | undefined => {
    if (value === undefined || value === null) return undefined;
    const raw = String(value || '').trim();
    if (!raw) return undefined;
    const token = normalizeEnumLikeToken(raw);
    const normalized = aliases[token] || (allowed.has(token) ? token : '');
    if (normalized) return normalized;
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `Invalid ${fieldLabel}. Use one of: ${allowedList}.`
    });
    return z.NEVER;
  });
}

export function buildRequiredLenientEnumSchema(
  values: readonly string[],
  aliases: Record<string, string>,
  fieldLabel: string
) {
  const allowed = new Set(values);
  const allowedList = values.join(', ');
  return z.any().transform((value, ctx): string => {
    const raw = String(value || '').trim();
    if (!raw) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `Missing ${fieldLabel}. Use one of: ${allowedList}.`
      });
      return z.NEVER;
    }
    const token = normalizeEnumLikeToken(raw);
    const normalized = aliases[token] || (allowed.has(token) ? token : '');
    if (normalized) return normalized;
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `Invalid ${fieldLabel}. Use one of: ${allowedList}.`
    });
    return z.NEVER;
  });
}

export const QMAP_SORT_DIRECTION_SCHEMA = buildOptionalLenientEnumSchema(
  ['asc', 'desc'],
  {
    ascending: 'asc',
    ascend: 'asc',
    up: 'asc',
    crescente: 'asc',
    ascendente: 'asc',
    descending: 'desc',
    descend: 'desc',
    down: 'desc',
    decrescente: 'desc',
    discendente: 'desc'
  },
  'sortDirection'
);
export const QMAP_COLOR_SCALE_MODE_SCHEMA = buildOptionalLenientEnumSchema(
  ['linear', 'quantize', 'quantile', 'ordinal'],
  {
    quantized: 'quantize',
    quantized_scale: 'quantize',
    quantiles: 'quantile',
    quantili: 'quantile',
    categorical: 'ordinal',
    category: 'ordinal',
    unique: 'ordinal'
  },
  'mode'
);
export const QMAP_HEIGHT_SCALE_SCHEMA = buildOptionalLenientEnumSchema(
  ['linear', 'log', 'sqrt'],
  {
    logarithmic: 'log',
    logarithm: 'log',
    square_root: 'sqrt',
    squareroot: 'sqrt',
    radice: 'sqrt'
  },
  'scale'
);
export const QMAP_THRESHOLD_STRATEGY_SCHEMA = buildRequiredLenientEnumSchema(
  ['mean', 'median', 'mode', 'quantiles'],
  {
    average: 'mean',
    media: 'mean',
    mediana: 'median',
    moda: 'mode',
    quantile: 'quantiles',
    quantili: 'quantiles',
    quartiles: 'quantiles',
    percentile: 'quantiles',
    percentili: 'quantiles'
  },
  'strategy'
);
export const QMAP_POSITION_SCHEMA = buildRequiredLenientEnumSchema(
  ['top', 'bottom', 'above', 'below'],
  {
    up: 'top',
    upper: 'top',
    down: 'bottom',
    lower: 'bottom',
    over: 'above',
    under: 'below'
  },
  'position'
);
export const QMAP_GEOMETRY_MODE_SCHEMA = buildOptionalLenientEnumSchema(
  ['auto', 'preserve_only', 'derive_from_latlon', 'none'],
  {
    preserve: 'preserve_only',
    preserveonly: 'preserve_only',
    derive_from_lat_lon: 'derive_from_latlon',
    derive_latlon: 'derive_from_latlon',
    latlon: 'derive_from_latlon',
    lat_lon: 'derive_from_latlon'
  },
  'geometryMode'
);
export const QMAP_CLIP_MODE_SCHEMA = buildOptionalLenientEnumSchema(
  ['intersects', 'centroid', 'within'],
  {
    intersect: 'intersects',
    intersection: 'intersects',
    centroids: 'centroid',
    inside: 'within'
  },
  'mode'
);
export const QMAP_SPATIAL_PREDICATE_SCHEMA = buildOptionalLenientEnumSchema(
  ['intersects', 'within', 'contains', 'touches'],
  {
    intersect: 'intersects',
    intersection: 'intersects',
    contain: 'contains',
    touch: 'touches',
    inside: 'within'
  },
  'predicate'
);
export const QMAP_TOUCH_PREDICATE_SCHEMA = buildOptionalLenientEnumSchema(
  ['touches', 'intersects'],
  {
    touch: 'touches',
    intersect: 'intersects',
    intersection: 'intersects'
  },
  'predicate'
);
export const QMAP_AGGREGATION_SCHEMA = buildOptionalLenientEnumSchema(
  ['count', 'sum', 'avg', 'min', 'max'],
  {
    total: 'sum',
    average: 'avg',
    mean: 'avg',
    minimum: 'min',
    massimo: 'max',
    maximum: 'max',
    minimo: 'min'
  },
  'aggregation'
);
export const QMAP_AGGREGATION_BASIC_SCHEMA = buildOptionalLenientEnumSchema(
  ['count', 'sum', 'avg'],
  {
    total: 'sum',
    average: 'avg',
    mean: 'avg'
  },
  'aggregation'
);
export const QMAP_AGGREGATION_REQUIRED_SCHEMA = buildRequiredLenientEnumSchema(
  ['count', 'sum', 'avg', 'min', 'max'],
  {
    total: 'sum',
    average: 'avg',
    mean: 'avg',
    minimum: 'min',
    maximum: 'max'
  },
  'aggregation'
);
export const QMAP_AGGREGATION_WITH_DISTINCT_REQUIRED_SCHEMA = buildRequiredLenientEnumSchema(
  ['count', 'distinct_count', 'sum', 'avg', 'min', 'max'],
  {
    distinct: 'distinct_count',
    distinctcount: 'distinct_count',
    total: 'sum',
    average: 'avg',
    mean: 'avg',
    minimum: 'min',
    maximum: 'max'
  },
  'operation'
);
export const QMAP_WEIGHT_MODE_SCHEMA = buildOptionalLenientEnumSchema(
  ['intersects', 'centroid', 'area_weighted'],
  {
    intersect: 'intersects',
    centroids: 'centroid',
    areaweighted: 'area_weighted',
    area_weight: 'area_weighted',
    area_weighting: 'area_weighted'
  },
  'weightMode'
);
export const QMAP_JOIN_TYPE_SCHEMA = buildOptionalLenientEnumSchema(
  ['inner', 'left'],
  {
    left_join: 'left',
    leftjoin: 'left',
    inner_join: 'inner',
    innerjoin: 'inner',
    outer: 'left'
  },
  'joinType'
);
export const QMAP_H3_JOIN_METRIC_SCHEMA = buildOptionalLenientEnumSchema(
  ['avg', 'sum', 'max', 'first'],
  {
    average: 'avg',
    mean: 'avg',
    total: 'sum',
    maximum: 'max'
  },
  'metric'
);
export const QMAP_AVG_SUM_SCHEMA = buildOptionalLenientEnumSchema(
  ['avg', 'sum'],
  {
    average: 'avg',
    mean: 'avg',
    total: 'sum'
  },
  'aggregation'
);
export const QMAP_VALUE_SEMANTICS_SCHEMA = buildOptionalLenientEnumSchema(
  ['intensive', 'extensive', 'count'],
  {
    intensivo: 'intensive',
    estensivo: 'extensive',
    counting: 'count'
  },
  'valueSemantics'
);
export const QMAP_ALLOCATION_MODE_SCHEMA = buildOptionalLenientEnumSchema(
  ['centroid', 'intersects'],
  {
    centroids: 'centroid',
    intersect: 'intersects',
    intersection: 'intersects'
  },
  'allocationMode'
);

export type QMapBounds = {minLng: number; minLat: number; maxLng: number; maxLat: number};

export type QMapToolResultEnvelope = {
  schema: string;
  toolName: string;
  success: boolean;
  details: string;
  error: {message: string} | null;
  objectiveReached: boolean;
  warnings: string[];
  blockingErrors: string[];
  producedDatasetRefs: string[];
};

export type ToolAutoRetryDirective = {
  retryToolName: string;
  retryArgs: Record<string, unknown>;
  retryReason: string;
};

export type QMapToolConcurrencyClass = 'read' | 'mutation' | 'validation';

export type QMapToolPhaseMetadata = {
  executionPhase: QMapTurnExecutionPhase;
  concurrencyClass: QMapToolConcurrencyClass;
  queuePosition: number | null;
  deferredReason: string | null;
};

export type QMapToolExecutionPolicyDecision = {
  allow: boolean;
  details?: string;
  gateType?: 'phase' | 'snapshot_expired' | 'ambiguous_ref' | 'hard';
};

export type QMapToolExecutionEvent = {
  phase: 'start' | 'finish' | 'blocked';
  toolName: string;
  toolCallId: string;
  success?: boolean;
  details?: string;
  requiresDatasetValidation?: boolean;
  datasetName?: string;
};

export type WrapToolRuntimeOptions = {
  shouldAllowTool?: (
    toolName: string,
    args: Record<string, unknown>,
    context: Record<string, unknown>
  ) => QMapToolExecutionPolicyDecision;
  onToolEvent?: (event: QMapToolExecutionEvent) => void;
  resolveCanonicalDatasetRef?: (datasetCandidate: string) => string;
  resolveFallbackDatasetRef?: () => string;
  onNormalizedToolResult?: (toolName: string, normalizedResult: Record<string, unknown>) => void;
  mutationIdempotencyCache?: Map<string, MutationIdempotencyCacheEntry>;
  nonActionableFailureCache?: Map<string, {toolName: string; details: string; failedAtMs: number}>;
  statelessToolCallCache?: Map<string, StatelessToolCallCacheEntry>;
  mutationRevisionRef?: React.MutableRefObject<number>;
  turnExecutionStateRef?: { current: QMapTurnExecutionState };
  mutationMutex?: AsyncMutex;
  /** Per-tool call counter for cross-turn circuit breaker. Reset each turn. */
  toolCallCounter?: Map<string, number>;
  /** Batch tracker for single-tool-per-response enforcement. Reset on each LLM response. */
  responseBatchTracker?: { current: { batchId: number; callsInBatch: number } };
};

export type StatelessToolCallCacheEntry = {
  toolName: string;
  dedupHash: string;
  mutationRevision: number;
  cachedAtMs: number;
  normalizedResult: Record<string, unknown>;
};
