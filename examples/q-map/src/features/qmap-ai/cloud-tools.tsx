import {useEffect} from 'react';
import {useDispatch} from 'react-redux';
import {addDataToMap, loadCloudMap, wrapTo} from '@kepler.gl/actions';
import {extendedTool} from './tool-shim';
import {z} from 'zod';
import CustomCloudProvider from '../../cloud-providers/custom-cloud-provider';
import {callMcpToolParsed} from './mcp-client';
import {DEFAULT_QMAP_ASSISTANT_BASE_URL} from '../../utils/assistant-config';
import {rememberBoundedSetValue, setBoundedMapValue, stableSerializeForCache} from './middleware/cache';
import {
  qcumberListProviders,
  qcumberListDatasets,
  qcumberGetDatasetHelp,
  qcumberQuery
} from './services/qcumber-api';

const DEFAULT_QCUMBER_BACKEND_PROVIDER = 'q-cumber-backend';
const DEFAULT_QCUMBER_TOKEN_KEY = 'qmap_qcumber_backend_token';
const STATIC_QCUMBER_TOKEN = import.meta.env.VITE_QCUMBER_CLOUD_TOKEN || '';
const DEFAULT_QSTORAGE_BACKEND_PROVIDER = 'q-storage-backend';
const DEFAULT_QSTORAGE_TOKEN_KEY = 'qmap_qstorage_backend_token';
const STATIC_QSTORAGE_TOKEN = import.meta.env.VITE_QSTORAGE_CLOUD_TOKEN || '';
const DEFAULT_CLOUD_MAP_PROVIDER = DEFAULT_QSTORAGE_BACKEND_PROVIDER;
const DEFAULT_ASSISTANT_BASE = (
  import.meta.env.VITE_QMAP_AI_PROXY_BASE ||
  import.meta.env.VITE_QMAP_AI_API_BASE ||
  DEFAULT_QMAP_ASSISTANT_BASE_URL
).replace(/\/+$/, '');
const EXECUTED_QCUMBER_QUERY_KEYS = new Set<string>();
const EXECUTED_QCUMBER_QUERY_KEYS_MAX_SIZE = 4096;
// QCUMBER_HTTP_TIMEOUT_MS removed — timeout handled by qcumber-api.ts proxy client
const QCUMBER_MAX_AUTO_LAYER_GEOMETRY_ROWS = Math.max(
  1000,
  Number(import.meta.env.VITE_QMAP_AI_QUERY_MAX_AUTO_LAYER_GEOMETRY_ROWS || 15000) || 15000
);
const QCUMBER_INCLUDE_LATLON_FALLBACK_FIELDS =
  String(import.meta.env.VITE_QMAP_AI_QUERY_INCLUDE_LATLON_FALLBACK || 'false').toLowerCase() === 'true';
const ITALY_DEFAULT_SPATIAL_BBOX: [number, number, number, number] = [6.6272658, 35.2889616, 18.7844746, 47.0921462];
const QCUMBER_PROVIDER_ROUTING_HINTS: Record<string, string> = {
  'local-assets-it': 'Use for Italian administrative boundaries (regions/provinces/municipalities, Kontur).',
  'geoapi-q-cumber':
    'Use for GeoAPI collections: events-data for events, feature-data for heterogeneous geospatial features.',
  'q-cumber': 'Use for platform APIs (devices/sensors/readings/emission factors), not administrative boundaries.'
};
const QCUMBER_DATASET_HELP_CACHE = new Map<string, any | null>();
const QCUMBER_DATASET_HELP_CACHE_MAX_SIZE = 512;
const QCUMBER_SUCCESS_QUERY_CACHE = new Map<string, {cachedAt: number; llmResult: any}>();
const QCUMBER_SUCCESS_QUERY_CACHE_TTL_MS = 120000;
const QCUMBER_SUCCESS_QUERY_CACHE_MAX_SIZE = 512;
const QCUMBER_PROVIDER_CATALOG_CACHE_TTL_MS = 5000;
let qcumberProviderCatalogCache: {expiresAt: number; key: string; items: any[]} | null = null;

function getCachedSuccessfulQcumberQuery(key: string): {cachedAt: number; llmResult: any} | null {
  if (!key) return null;
  const cached = QCUMBER_SUCCESS_QUERY_CACHE.get(key) || null;
  if (!cached) return null;
  if (Date.now() - cached.cachedAt > QCUMBER_SUCCESS_QUERY_CACHE_TTL_MS) {
    QCUMBER_SUCCESS_QUERY_CACHE.delete(key);
    return null;
  }
  return cached;
}

function setCachedSuccessfulQcumberQuery(key: string, llmResult: any) {
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

export function normalizeCloudMapProvider(rawProvider?: string): string {
  const normalized = String(rawProvider || '').trim().toLowerCase();
  if (!normalized) return DEFAULT_CLOUD_MAP_PROVIDER;
  return normalized;
}

export function createCloudStorageProvider(rawProvider?: string) {
  const providerName = normalizeCloudMapProvider(rawProvider);
  if (providerName === DEFAULT_QCUMBER_BACKEND_PROVIDER) {
    const apiBase = String(import.meta.env.VITE_QCUMBER_CLOUD_API_BASE || '').replace(/\/+$/, '');
    if (!apiBase) {
      throw new Error('Q-cumber cloud API base URL is not configured');
    }
    return new CustomCloudProvider({
      name: DEFAULT_QCUMBER_BACKEND_PROVIDER,
      apiBaseUrl: apiBase,
      displayName: import.meta.env.VITE_QCUMBER_CLOUD_DISPLAY_NAME || 'Q-cumber',
      managementUrl: import.meta.env.VITE_QCUMBER_CLOUD_MANAGEMENT_URL,
      staticToken: STATIC_QCUMBER_TOKEN,
      tokenStorageKey: DEFAULT_QCUMBER_TOKEN_KEY
    });
  }

  if (providerName === DEFAULT_QSTORAGE_BACKEND_PROVIDER) {
    const apiBase = String(import.meta.env.VITE_QSTORAGE_CLOUD_API_BASE || '').replace(/\/+$/, '');
    if (!apiBase) {
      throw new Error('Q-storage cloud API base URL is not configured');
    }
    return new CustomCloudProvider({
      name: DEFAULT_QSTORAGE_BACKEND_PROVIDER,
      apiBaseUrl: apiBase,
      displayName: import.meta.env.VITE_QSTORAGE_CLOUD_DISPLAY_NAME || 'My Maps',
      managementUrl: import.meta.env.VITE_QSTORAGE_CLOUD_MANAGEMENT_URL,
      staticToken: STATIC_QSTORAGE_TOKEN,
      tokenStorageKey: DEFAULT_QSTORAGE_TOKEN_KEY,
      privateStorage: true
    });
  }

  throw new Error(`Unsupported cloud provider "${providerName}"`);
}

function withUniqueMapDatasetIdentity(dataset: any, executionKey?: string) {
  if (!dataset || typeof dataset !== 'object' || !executionKey) {
    return dataset;
  }
  const info = dataset.info;
  if (!info || typeof info !== 'object') {
    return dataset;
  }
  const baseId = String((info as any).id || 'q-cumber-query').trim() || 'q-cumber-query';
  const baseLabel = String((info as any).label || baseId).trim() || baseId;
  const safeKey = String(executionKey);
  const keyParts = safeKey.split('-').filter(Boolean);
  const shortKey = keyParts.length ? keyParts[keyParts.length - 1] : safeKey.slice(-8);
  return {
    ...dataset,
    info: {
      ...(info as Record<string, unknown>),
      id: `${baseId}-${safeKey}`,
      label: `${baseLabel} [${shortKey}]`
    }
  };
}

function deriveDatasetIdentity(dataset: any, executionKey?: string) {
  const withIdentity = withUniqueMapDatasetIdentity(dataset, executionKey);
  const info = withIdentity?.info || {};
  const id = String(info?.id || '').trim();
  const label = String(info?.label || id || '').trim();
  return {
    id,
    label,
    ref: id ? `id:${id}` : ''
  };
}

function rebuildQcumberMapDatasetRows(dataset: any, rows: any[], fields: any[]): any {
  if (!dataset || typeof dataset !== 'object') {
    return dataset;
  }
  const data = dataset?.data && typeof dataset.data === 'object' ? dataset.data : {};
  const existingFields = Array.isArray(data?.fields) ? data.fields : [];
  const resolvedFieldNames = (
    existingFields.length
      ? existingFields.map((field: any) => String(field?.name || '').trim()).filter(Boolean)
      : (Array.isArray(fields) ? fields : []).map((field: any) => String(field || '').trim()).filter(Boolean)
  ) as string[];
  const normalizedFieldDefs = resolvedFieldNames.map(fieldName => {
    const exact = existingFields.find((field: any) => String(field?.name || '').trim() === fieldName);
    if (exact) return exact;
    const ci = existingFields.find(
      (field: any) => String(field?.name || '').trim().toLowerCase() === fieldName.toLowerCase()
    );
    if (ci) return ci;
    return {name: fieldName, type: fieldName === '_geojson' ? 'geojson' : 'string'};
  });
  const normalizedRows = (Array.isArray(rows) ? rows : []).map((row: any) => {
    const rowObject = row && typeof row === 'object' && !Array.isArray(row) ? row : {};
    return resolvedFieldNames.map(fieldName => {
      if (Object.prototype.hasOwnProperty.call(rowObject, fieldName)) {
        return rowObject[fieldName];
      }
      const ciEntry = Object.entries(rowObject).find(([key]) => key.toLowerCase() === fieldName.toLowerCase());
      return ciEntry ? ciEntry[1] : null;
    });
  });
  return {
    ...dataset,
    data: {
      ...data,
      fields: normalizedFieldDefs,
      rows: normalizedRows
    }
  };
}

function resolveQCumberProviderId(rawProviderId?: string): string {
  const raw = String(rawProviderId || '').trim();
  return raw;
}

function isInvalidProviderIdLiteral(rawProviderId?: string): boolean {
  const normalized = String(rawProviderId || '')
    .trim()
    .toLowerCase();
  return (
    normalized === '[object]' ||
    normalized === '[object object]' ||
    normalized === 'object object' ||
    normalized === 'null' ||
    normalized === 'undefined'
  );
}

function pickPreferredProviderIdFromCatalog(
  providerItems: any[],
  preferredProviderId?: string
): {providerId: string; autoSelected: boolean} {
  const ids = (Array.isArray(providerItems) ? providerItems : [])
    .map((item: any) => String(item?.id || '').trim())
    .filter(Boolean);
  const uniqueIds = Array.from(new Set(ids));
  const preferred = resolveQCumberProviderId(preferredProviderId);
  if (preferred && uniqueIds.includes(preferred)) {
    return {providerId: preferred, autoSelected: false};
  }
  // Keep provider selection strict when the caller passed an explicit id:
  // returning a silent fallback can hide provider mismatches and lead to tool loops.
  if (preferred) {
    return {providerId: preferred, autoSelected: false};
  }
  if (uniqueIds.length === 1) {
    return {providerId: uniqueIds[0], autoSelected: true};
  }
  return {providerId: '', autoSelected: false};
}

async function listQcumberProvidersCatalog(locale?: string, forceRefresh = false): Promise<any[]> {
  const suffix = locale ? `?locale=${encodeURIComponent(String(locale))}` : '';
  const cacheKey = String(locale || '').trim().toLowerCase();
  const now = Date.now();
  if (!forceRefresh && qcumberProviderCatalogCache && qcumberProviderCatalogCache.key === cacheKey) {
    if (qcumberProviderCatalogCache.expiresAt > now) {
      return qcumberProviderCatalogCache.items;
    }
  }
  const payload = await requestQcumberJson(`/providers${suffix}`);
  const items = Array.isArray(payload?.items) ? payload.items : [];
  qcumberProviderCatalogCache = {
    key: cacheKey,
    expiresAt: now + QCUMBER_PROVIDER_CATALOG_CACHE_TTL_MS,
    items
  };
  return items;
}

async function resolveExistingQCumberProviderId(
  rawProviderId?: string,
  options?: {forceRefresh?: boolean}
): Promise<{providerId: string; autoSelected: boolean; availableProviderIds: string[]}> {
  const invalidLiteral = isInvalidProviderIdLiteral(rawProviderId);
  if (invalidLiteral) {
    try {
      const items = await listQcumberProvidersCatalog(undefined, Boolean(options?.forceRefresh));
      const availableProviderIds = items.map((item: any) => String(item?.id || '').trim()).filter(Boolean);
      return {
        providerId: '',
        autoSelected: false,
        availableProviderIds
      };
    } catch {
      return {
        providerId: '',
        autoSelected: false,
        availableProviderIds: []
      };
    }
  }
  const preferred = resolveQCumberProviderId(rawProviderId);
  try {
    const items = await listQcumberProvidersCatalog(undefined, Boolean(options?.forceRefresh));
    const availableProviderIds = items.map((item: any) => String(item?.id || '').trim()).filter(Boolean);
    const picked = pickPreferredProviderIdFromCatalog(items, preferred);
    return {
      providerId: picked.providerId || preferred,
      autoSelected: picked.autoSelected,
      availableProviderIds
    };
  } catch {
    return {
      providerId: preferred,
      autoSelected: false,
      availableProviderIds: []
    };
  }
}

async function resolvePreferredTerritorialProviderId(
  rawProviderId: string | undefined,
  _rawDatasetId: unknown,
  fallbackProviderId: string
): Promise<string> {
  const raw = String(rawProviderId || '').trim();
  if (raw) {
    return resolveQCumberProviderId(rawProviderId);
  }
  return String(fallbackProviderId || '').trim();
}

function normalizeDatasetToken(value: unknown): string {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function normalizeQcumberEnumToken(value: unknown): string {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function buildOptionalQcumberEnumSchema(
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
    const token = normalizeQcumberEnumToken(raw);
    const normalized = aliases[token] || (allowed.has(token) ? token : '');
    if (normalized) return normalized;
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `Invalid ${fieldLabel}. Use one of: ${allowedList}.`
    });
    return z.NEVER;
  });
}

const NON_EMPTY_STRING_SCHEMA = z.string().trim().min(1);
const OPTIONAL_NON_EMPTY_STRING_SCHEMA = NON_EMPTY_STRING_SCHEMA.optional();

const QCUMBER_ORDER_DIRECTION_SCHEMA = buildOptionalQcumberEnumSchema(
  ['asc', 'desc'],
  {
    ascending: 'asc',
    ascend: 'asc',
    crescente: 'asc',
    ascendente: 'asc',
    descending: 'desc',
    descend: 'desc',
    decrescente: 'desc',
    discendente: 'desc'
  },
  'orderDirection'
);
const QCUMBER_EXPECTED_ADMIN_TYPE_VALUES = [
  'country',
  'region',
  'province',
  'municipality',
  'stato',
  'regione',
  'provincia',
  'comune'
] as const;
const QCUMBER_EXPECTED_ADMIN_TYPE_ALIASES: Record<string, string> = {
  countries: 'country',
  stato: 'country',
  stati: 'country',
  country_stato: 'country',
  stato_country: 'country',
  regions: 'region',
  regione: 'region',
  regioni: 'region',
  region_regione: 'region',
  regione_region: 'region',
  provinces: 'province',
  provincia: 'province',
  province_it: 'province',
  province_provincia: 'province',
  provincia_province: 'province',
  municipalities: 'municipality',
  municipalitys: 'municipality',
  comune: 'municipality',
  comuni: 'municipality',
  municipality_comune: 'municipality',
  comune_municipality: 'municipality',
  city: 'municipality',
  citta: 'municipality'
};
const QCUMBER_EXPECTED_ADMIN_TYPE_SCHEMA = buildOptionalQcumberEnumSchema(
  QCUMBER_EXPECTED_ADMIN_TYPE_VALUES,
  QCUMBER_EXPECTED_ADMIN_TYPE_ALIASES,
  'expectedAdminType'
);

function isFilterValueLikelyFieldName(fieldName: unknown, value: unknown): boolean {
  if (typeof value !== 'string') return false;
  const rawField = String(fieldName || '').trim();
  const rawValue = String(value || '').trim();
  if (!rawField || !rawValue) return false;

  const fieldToken = normalizeDatasetToken(rawField);
  const valueToken = normalizeDatasetToken(rawValue);
  if (!fieldToken || !valueToken || fieldToken !== valueToken) return false;

  const looksLikeFieldName = /[_-]/.test(rawValue) || rawValue.includes('__') || /(^|[_-])lv\d*/i.test(rawValue);
  return looksLikeFieldName;
}

type QcumberQueryPolicy = 'auto' | 'territorial' | 'thematic_spatial';

const QCUMBER_FILTER_OP_VALUES = [
  'eq',
  'ne',
  'gt',
  'gte',
  'lt',
  'lte',
  'in',
  'contains',
  'startswith',
  'endswith',
  'is_null',
  'not_null'
] as const;
const QCUMBER_FILTER_OP_INPUT_VALUES = [...QCUMBER_FILTER_OP_VALUES, 'neq', 'starts_with', 'ends_with'] as const;
const QCUMBER_FILTER_OPS = new Set<string>(QCUMBER_FILTER_OP_VALUES);
const QCUMBER_INLINE_NUMERIC_PATTERN = /^-?(?:0|[1-9]\d*)(?:\.\d+)?$/;

function normalizeQcumberFilterOp(raw: unknown): string {
  const rawOp = String(raw || 'eq')
    .trim()
    .toLowerCase();
  if (rawOp === 'neq') return 'ne';
  if (rawOp === 'starts_with') return 'startswith';
  if (rawOp === 'ends_with') return 'endswith';
  return rawOp || 'eq';
}

function parseQcumberInlineScalar(raw: unknown): unknown {
  if (raw === undefined) return undefined;
  if (raw === null || typeof raw === 'number' || typeof raw === 'boolean') return raw;
  if (typeof raw !== 'string') return undefined;
  const token = raw.trim();
  if (!token) return undefined;
  const lower = token.toLowerCase();
  if (lower === 'null') return null;
  if (lower === 'true') return true;
  if (lower === 'false') return false;
  if (QCUMBER_INLINE_NUMERIC_PATTERN.test(token)) {
    const parsed = Number(token);
    if (Number.isFinite(parsed)) return parsed;
  }
  if (
    (token.startsWith('"') && token.endsWith('"') && token.length >= 2) ||
    (token.startsWith("'") && token.endsWith("'") && token.length >= 2)
  ) {
    return token.slice(1, -1);
  }
  return token;
}

function parseQcumberInlineValues(raw: unknown): unknown[] {
  if (Array.isArray(raw)) return raw;
  if (typeof raw !== 'string') return [];
  const token = raw.trim();
  if (!token) return [];
  if (token.startsWith('[') && token.endsWith(']')) {
    try {
      const parsed = JSON.parse(token);
      if (Array.isArray(parsed)) {
        return parsed
          .map(item => parseQcumberInlineScalar(item))
          .filter((item): item is unknown => item !== undefined);
      }
    } catch {
      // Fallback to separator parsing below.
    }
  }
  const separator = token.includes('|') ? '|' : token.includes(';') ? ';' : ',';
  return token
    .split(separator)
    .map(item => parseQcumberInlineScalar(item))
    .filter((item): item is unknown => item !== undefined);
}

function normalizeQcumberFilterInput(raw: unknown): unknown {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return raw;
  const next: Record<string, unknown> = {...(raw as Record<string, unknown>)};
  const rawOp = typeof next.op === 'string' ? next.op.trim() : '';
  const hasExplicitValue = next.value !== undefined;
  const hasExplicitValues = Array.isArray(next.values);
  if (!rawOp) return next;

  const split = rawOp.match(/^([a-z_]+)\s*[,;|]\s*(.+)$/i);
  if (!split) return next;

  const opToken = normalizeQcumberFilterOp(split[1]);
  if (!QCUMBER_FILTER_OPS.has(opToken)) return next;

  next.op = opToken;
  if (hasExplicitValue || hasExplicitValues) return next;

  const tailToken = String(split[2] || '').trim();
  if (!tailToken) return next;
  const keyed = tailToken.match(/^(value|values)\s*[:=]\s*(.+)$/i);
  const key = keyed ? keyed[1].toLowerCase() : '';
  const rawValueToken = keyed ? keyed[2] : tailToken;

  if (opToken === 'in' || key === 'values') {
    const parsedValues = parseQcumberInlineValues(rawValueToken);
    if (parsedValues.length) next.values = parsedValues;
    return next;
  }

  const parsedScalar = parseQcumberInlineScalar(rawValueToken);
  if (parsedScalar !== undefined) next.value = parsedScalar;
  return next;
}

const QCUMBER_SCALAR_FILTER_VALUE_SCHEMA = z.union([z.string(), z.number(), z.boolean(), z.null()]);
const QCUMBER_FILTER_SCHEMA = z
  .preprocess(
    raw => normalizeQcumberFilterInput(raw),
    z
      .object({
        field: NON_EMPTY_STRING_SCHEMA,
        op: z.enum(QCUMBER_FILTER_OP_INPUT_VALUES).optional(),
        value: QCUMBER_SCALAR_FILTER_VALUE_SCHEMA.optional(),
        values: z.array(QCUMBER_SCALAR_FILTER_VALUE_SCHEMA).optional()
      })
      .strict()
      .superRefine((payload, ctx) => {
        const op = normalizeQcumberFilterOp(payload?.op);
        const hasValue = payload?.value !== undefined;
        const valuesArray = Array.isArray(payload?.values) ? payload.values : [];
        const hasValuesArray = Array.isArray(payload?.values);
        const hasValues = valuesArray.length > 0;
        if (op === 'is_null' || op === 'not_null') {
          if (hasValue || hasValuesArray) {
            ctx.addIssue({
              code: z.ZodIssueCode.custom,
              message: `Operator "${op}" does not accept value/values.`
            });
          }
          return;
        }
        if (op === 'in') {
          if (!hasValues) {
            ctx.addIssue({
              code: z.ZodIssueCode.custom,
              message: 'Operator "in" requires non-empty "values".'
            });
          }
          if (hasValue) {
            ctx.addIssue({
              code: z.ZodIssueCode.custom,
              message: 'Operator "in" must use "values", not "value".'
            });
          }
          return;
        }
        if (!hasValue) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: `Operator "${op}" requires "value".`
          });
        }
        if (hasValuesArray) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: `Operator "${op}" does not accept "values".`
          });
        }
      })
  );

async function getQcumberDatasetCatalogItem(providerId: string, datasetId: string): Promise<any | null> {
  const items = await listQcumberDatasetCatalogItems(providerId);
  const match = items.find(
    (item: any) => String(item?.id || '').trim().toLowerCase() === String(datasetId || '').trim().toLowerCase()
  );
  return match || null;
}

async function listQcumberDatasetCatalogItems(providerId: string): Promise<any[]> {
  const payload = await requestQcumberJson(`/providers/${encodeURIComponent(providerId)}/datasets`);
  return Array.isArray(payload?.items) ? payload.items : [];
}

function collectCatalogDatasetIds(items: any[]): string[] {
  return (Array.isArray(items) ? items : [])
    .map((item: any) => String(item?.id || '').trim())
    .filter(Boolean);
}

function formatAvailableDatasetIds(ids: string[], maxItems = 12): string {
  const list = (Array.isArray(ids) ? ids : []).map(id => String(id || '').trim()).filter(Boolean);
  if (!list.length) return '';
  const shown = list.slice(0, maxItems);
  const suffix = list.length > shown.length ? ', ...' : '';
  return `${shown.join(', ')}${suffix}`;
}

type QcumberDatasetIdResolution = {
  requestedDatasetId: string;
  resolvedDatasetId: string;
  availableDatasetIds: string[];
  exactMatch: boolean;
};

async function resolveQCumberDatasetId(providerId: string, rawDatasetId: unknown): Promise<QcumberDatasetIdResolution> {
  const raw = String(rawDatasetId || '').trim();
  const items = await listQcumberDatasetCatalogItems(providerId);
  const availableDatasetIds = collectCatalogDatasetIds(items);
  if (!raw) {
    return {
      requestedDatasetId: '',
      resolvedDatasetId: '',
      availableDatasetIds,
      exactMatch: false
    };
  }

  const byId = items.find((item: any) => String(item?.id || '').trim().toLowerCase() === raw.toLowerCase()) || null;
  if (!byId?.id) {
    return {
      requestedDatasetId: raw,
      resolvedDatasetId: '',
      availableDatasetIds,
      exactMatch: false
    };
  }
  return {
    requestedDatasetId: raw,
    resolvedDatasetId: String(byId.id).trim(),
    availableDatasetIds,
    exactMatch: true
  };
}

type QcumberCatalogDataset = {
  id: string;
  name: string;
  description: string;
  tags: string[];
  routing: any;
};

const QCUMBER_ROUTING_QUERY_TOOLS = new Set([
  'queryQCumberTerritorialUnits',
  'queryQCumberDatasetSpatial',
  'queryQCumberDataset'
]);

function getRoutingPreferredQueryTool(routing: any): string {
  const queryToolHint = String(routing?.queryToolHint?.preferredTool || '').trim();
  if (QCUMBER_ROUTING_QUERY_TOOLS.has(queryToolHint)) return queryToolHint;
  return '';
}

function normalizeMetricProfile(metricProfile: any, sourceFallback = 'backend_routing'): any | null {
  if (!metricProfile || typeof metricProfile !== 'object') return null;
  const asStringList = (value: unknown): string[] =>
    Array.isArray(value) ? value.map(item => String(item || '').trim()).filter(Boolean) : [];
  const asMetricList = (value: unknown): any[] => (Array.isArray(value) ? value.filter(item => item && typeof item === 'object') : []);
  const out: Record<string, unknown> = {
    source: String(metricProfile?.source || '').trim() || sourceFallback,
    confidence: String(metricProfile?.confidence || '').trim() || 'medium',
    metricSemantic: String(metricProfile?.metricSemantic || '').trim() || 'generic',
    biasRisk: String(metricProfile?.biasRisk || '').trim() || 'unknown',
    numeratorFieldCandidates: asStringList(metricProfile?.numeratorFieldCandidates),
    denominatorFieldCandidates: asStringList(metricProfile?.denominatorFieldCandidates),
    preferredRankingFieldCandidates: asStringList(metricProfile?.preferredRankingFieldCandidates),
    recommendedDerivedMetrics: asMetricList(metricProfile?.recommendedDerivedMetrics),
    analysisCaveats: asStringList(metricProfile?.analysisCaveats)
  };
  return out;
}

function inferMetricProfileFromContext(params: {
  aiProfile: any;
  datasetClass: string;
  text: string;
  normalizedFields: string[];
}): any | null {
  const declared = normalizeMetricProfile(params?.aiProfile?.analysisMetrics, 'descriptor_profile');
  if (declared) return declared;
  const text = String(params?.text || '').toLowerCase();
  const fields = Array.isArray(params?.normalizedFields) ? params.normalizedFields : [];
  const isClcLike =
    params?.datasetClass === 'land_cover' ||
    text.includes('clc') ||
    text.includes('land cover') ||
    text.includes('land-cover') ||
    fields.includes('code_18');
  if (!isClcLike) return null;
  return normalizeMetricProfile(
    {
      source: 'inferred_land_cover_proxy',
      confidence: 'medium',
      metricSemantic: 'proxy_environmental_pressure',
      biasRisk: 'absolute_only_bias',
      numeratorFieldCandidates: ['area_ha', 'zonal_value', 'sum', 'sum_area_m2'],
      denominatorFieldCandidates: ['area_region_m2', 'population'],
      preferredRankingFieldCandidates: ['pressure_pct_area', 'pressure_ha_per_100k', 'zonal_value', 'area_ha'],
      recommendedDerivedMetrics: [
        {
          name: 'pressure_pct_area',
          formulaHint: 'numerator_m2 / area_region_m2',
          unit: 'ratio'
        },
        {
          name: 'pressure_ha_per_100k',
          formulaHint: '(numerator_ha / population) * 100000',
          unit: 'ha_per_100k'
        }
      ],
      analysisCaveats: [
        'Proxy based on land-cover classes and not a direct clinical/sanitary risk measure.',
        'Avoid absolute-only ranking when denominator fields are available.'
      ]
    },
    'inferred_land_cover_proxy'
  );
}

function buildMetricProfileHint(metricProfile: any): string {
  const profile = normalizeMetricProfile(metricProfile);
  if (!profile) return '';
  const semantic = String(profile.metricSemantic || '').trim();
  const bias = String(profile.biasRisk || '').trim();
  const derived = Array.isArray(profile.recommendedDerivedMetrics)
    ? profile.recommendedDerivedMetrics
        .map((item: any) => String(item?.name || '').trim())
        .filter(Boolean)
        .slice(0, 2)
    : [];
  const caveat = Array.isArray(profile.analysisCaveats) ? String(profile.analysisCaveats[0] || '').trim() : '';
  const segments = [
    semantic ? `metricSemantic=${semantic}` : '',
    bias ? `biasRisk=${bias}` : '',
    derived.length ? `derived=${derived.join(',')}` : '',
    caveat ? `caveat=${caveat}` : ''
  ].filter(Boolean);
  return segments.length ? ` Metric profile: ${segments.join('; ')}.` : '';
}

async function listQcumberCatalogDatasetsWithRouting(providerId: string): Promise<QcumberCatalogDataset[]> {
  try {
    const payload = await requestQcumberJson(`/providers/${encodeURIComponent(providerId)}/datasets`);
    const items = Array.isArray(payload?.items) ? payload.items : [];
    return items
      .map((item: any) => ({
        id: String(item?.id || '').trim(),
        name: String(item?.name || '').trim(),
        description: String(item?.description || '').trim(),
        tags: Array.isArray(item?.tags)
          ? item.tags.map((tag: any) => String(tag || '').trim()).filter(Boolean)
          : [],
        routing: pickRoutingFromBackendOrInfer(item)
      }))
      .filter((item: QcumberCatalogDataset) => !!item.id);
  } catch {
    return [];
  }
}

async function resolveQCumberDatasetIdForQuery(params: {
  providerId: string;
  rawDatasetId: unknown;
  policyMode: QcumberQueryPolicy;
  normalizedExpectedAdminType: string | null;
  spatialBbox: number[] | undefined;
}): Promise<{
  datasetId: string;
  requestedDatasetId: string;
  requestedDatasetIdInvalid: boolean;
  availableDatasetIds: string[];
  autoSelected: boolean;
}> {
  const {providerId, rawDatasetId, policyMode, normalizedExpectedAdminType, spatialBbox} = params;
  const explicitResolution = await resolveQCumberDatasetId(providerId, rawDatasetId);
  if (explicitResolution.requestedDatasetId) {
    if (explicitResolution.resolvedDatasetId) {
      return {
        datasetId: explicitResolution.resolvedDatasetId,
        requestedDatasetId: explicitResolution.requestedDatasetId,
        requestedDatasetIdInvalid: false,
        availableDatasetIds: explicitResolution.availableDatasetIds,
        autoSelected: false
      };
    }
    return {
      datasetId: '',
      requestedDatasetId: explicitResolution.requestedDatasetId,
      requestedDatasetIdInvalid: true,
      availableDatasetIds: explicitResolution.availableDatasetIds,
      autoSelected: false
    };
  }

  const catalogItems = await listQcumberCatalogDatasetsWithRouting(providerId);
  const availableDatasetIds = catalogItems.map(item => String(item?.id || '').trim()).filter(Boolean);
  if (!catalogItems.length) {
    return {
      datasetId: '',
      requestedDatasetId: '',
      requestedDatasetIdInvalid: false,
      availableDatasetIds: [],
      autoSelected: false
    };
  }
  if (catalogItems.length === 1) {
    return {
      datasetId: catalogItems[0].id,
      requestedDatasetId: '',
      requestedDatasetIdInvalid: false,
      availableDatasetIds,
      autoSelected: true
    };
  }

  const wantsAdministrative = policyMode === 'territorial' || !!normalizedExpectedAdminType;
  if (wantsAdministrative) {
    const adminCandidates = catalogItems.filter(item => Boolean(item?.routing?.isAdministrative));
    if (adminCandidates.length === 1) {
      return {
        datasetId: adminCandidates[0].id,
        requestedDatasetId: '',
        requestedDatasetIdInvalid: false,
        availableDatasetIds,
        autoSelected: true
      };
    }
    const territorialPreferred = adminCandidates.filter(
      item => getRoutingPreferredQueryTool(item?.routing) === 'queryQCumberTerritorialUnits'
    );
    if (territorialPreferred.length === 1) {
      return {
        datasetId: territorialPreferred[0].id,
        requestedDatasetId: '',
        requestedDatasetIdInvalid: false,
        availableDatasetIds,
        autoSelected: true
      };
    }
  }

  const hasSpatialBbox = Array.isArray(spatialBbox) && spatialBbox.length === 4;
  const wantsThematicSpatial =
    policyMode === 'thematic_spatial' || (policyMode === 'auto' && hasSpatialBbox && !wantsAdministrative);
  if (wantsThematicSpatial) {
    const thematicCandidates = catalogItems.filter(item => {
      const preferredTool = getRoutingPreferredQueryTool(item?.routing);
      return !item?.routing?.isAdministrative || preferredTool === 'queryQCumberDatasetSpatial';
    });
    if (thematicCandidates.length === 1) {
      return {
        datasetId: thematicCandidates[0].id,
        requestedDatasetId: '',
        requestedDatasetIdInvalid: false,
        availableDatasetIds,
        autoSelected: true
      };
    }
    const thematicSpatialPreferred = thematicCandidates.filter(
      item => getRoutingPreferredQueryTool(item?.routing) === 'queryQCumberDatasetSpatial'
    );
    if (thematicSpatialPreferred.length === 1) {
      return {
        datasetId: thematicSpatialPreferred[0].id,
        requestedDatasetId: '',
        requestedDatasetIdInvalid: false,
        availableDatasetIds,
        autoSelected: true
      };
    }
  }

  return {
    datasetId: '',
    requestedDatasetId: '',
    requestedDatasetIdInvalid: false,
    availableDatasetIds,
    autoSelected: false
  };
}

function compactQcumberPreviewRows(rows: any[], maxRows = 8): any[] {
  return (Array.isArray(rows) ? rows : []).slice(0, maxRows).map((row: any) => {
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      return row;
    }
    const out: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row)) {
      const lowerKey = String(key || '').toLowerCase();
      if (lowerKey === '_geojson' || lowerKey === '_geometry') {
        out[key] = '[geojson omitted]';
        continue;
      }
      if (value === null || value === undefined) {
        out[key] = value;
        continue;
      }
      if (typeof value === 'string') {
        out[key] = value.length > 160 ? `${value.slice(0, 157)}...` : value;
        continue;
      }
      if (typeof value === 'number' || typeof value === 'boolean') {
        out[key] = value;
        continue;
      }
      out[key] = '[complex value omitted]';
    }
    return out;
  });
}

function buildRankingPreviewDetails(
  rows: any[],
  orderBy: unknown,
  orderDirection: unknown,
  maxRows = 10
): string {
  const list = (Array.isArray(rows) ? rows : []).filter(
    (row: any) => row && typeof row === 'object' && !Array.isArray(row)
  );
  if (!list.length) return ' Ranking preview: no rows returned.';

  const metricFieldRaw = String(orderBy || '').trim();
  const firstRow = list[0] as Record<string, unknown>;
  const rowKeys = Object.keys(firstRow || {});
  const metricField =
    rowKeys.find(key => key === metricFieldRaw) ||
    rowKeys.find(key => key.toLowerCase() === metricFieldRaw.toLowerCase()) ||
    metricFieldRaw;
  const lowerDirection = String(orderDirection || 'asc').toLowerCase();
  const directionLabel = lowerDirection === 'desc' ? 'desc' : 'asc';

  const preferredNameFields = ['name', 'name_en', 'name_it', 'nome', 'comune'];
  const nameField =
    preferredNameFields.find(candidate => rowKeys.some(key => key.toLowerCase() === candidate)) ||
    rowKeys.find(key => /(name|nome|comune)/i.test(key)) ||
    '';

  const sample = list.slice(0, Math.max(1, Math.min(maxRows, 10)));
  const entries = sample.map((row: Record<string, unknown>, index: number) => {
    const metricValue =
      row[metricField] ??
      Object.entries(row).find(([key]) => key.toLowerCase() === metricField.toLowerCase())?.[1];
    const rawName = nameField ? row[nameField] : '';
    const nameLabel =
      typeof rawName === 'string' && rawName.trim().length
        ? rawName.trim()
        : `row_${index + 1}`;
    const metricLabel =
      metricValue === null || metricValue === undefined ? 'n/a' : String(metricValue).trim() || 'n/a';
    return `${index + 1}) ${nameLabel} (${metricField}=${metricLabel})`;
  });

  return ` Ranking ${directionLabel} by ${metricField}: ${entries.join('; ')}.`;
}

function resolveRankingMetricFieldName(rows: any[], fields: any[], orderBy: unknown): string {
  const requested = String(orderBy || '').trim();
  if (!requested) return '';

  const fieldList = Array.isArray(fields)
    ? fields.map((field: any) => String(field || '').trim()).filter(Boolean)
    : [];
  const fieldLowerMap = new Map<string, string>();
  fieldList.forEach(fieldName => {
    fieldLowerMap.set(fieldName.toLowerCase(), fieldName);
  });

  if (fieldLowerMap.has(requested.toLowerCase())) {
    return fieldLowerMap.get(requested.toLowerCase()) || requested;
  }

  const row = (Array.isArray(rows) ? rows : []).find(
    (candidate: any) => candidate && typeof candidate === 'object' && !Array.isArray(candidate)
  ) as Record<string, unknown> | undefined;
  if (!row) return '';
  const rowMatch = Object.keys(row).find(key => key.toLowerCase() === requested.toLowerCase());
  return rowMatch || '';
}

function getRowFieldValueCaseInsensitive(row: Record<string, unknown>, fieldName: string): unknown {
  if (!fieldName) return undefined;
  if (Object.prototype.hasOwnProperty.call(row, fieldName)) return row[fieldName];
  const lowered = fieldName.toLowerCase();
  const matchedKey = Object.keys(row).find(key => key.toLowerCase() === lowered);
  return matchedKey ? row[matchedKey] : undefined;
}

function isComparableRankingValue(value: unknown): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === 'number') return Number.isFinite(value);
  if (typeof value === 'string') return value.trim().length > 0;
  if (typeof value === 'boolean') return true;
  return false;
}

function validateRankingMetric(rows: any[], fields: any[], orderBy: unknown): {
  ok: boolean;
  metricField: string;
  reason?: string;
} {
  const requested = String(orderBy || '').trim();
  if (!requested) {
    return {ok: false, metricField: '', reason: 'Missing orderBy field for ranking query.'};
  }

  const metricField = resolveRankingMetricFieldName(rows, fields, requested);
  if (!metricField) {
    return {
      ok: false,
      metricField: requested,
      reason: `Requested ranking field "${requested}" is not present in the returned schema.`
    };
  }

  const list = (Array.isArray(rows) ? rows : []).filter(
    (row: any) => row && typeof row === 'object' && !Array.isArray(row)
  ) as Record<string, unknown>[];
  if (!list.length) {
    return {ok: true, metricField};
  }

  const sampled = list.slice(0, Math.min(200, list.length));
  const values = sampled.map(row => getRowFieldValueCaseInsensitive(row, metricField));
  const nonNullValues = values.filter(value => value !== null && value !== undefined);
  if (!nonNullValues.length) {
    return {
      ok: false,
      metricField,
      reason: `Ranking field "${metricField}" has no usable values in returned rows (all null/undefined).`
    };
  }

  const comparableValues = nonNullValues.filter(isComparableRankingValue);
  if (!comparableValues.length) {
    return {
      ok: false,
      metricField,
      reason:
        `Ranking field "${metricField}" contains non-comparable values ` +
        '(for example geometry/object values).'
    };
  }

  return {ok: true, metricField};
}

function isGeometryLikeRankingField(fieldName: unknown): boolean {
  const token = String(fieldName || '')
    .trim()
    .toLowerCase();
  if (!token) return false;
  return token === '_geojson' || token === '_geometry' || token === 'geometry' || token === 'geom';
}

function isLikelyIdentifierRankingField(fieldName: unknown): boolean {
  const token = String(fieldName || '')
    .trim()
    .toLowerCase();
  if (!token) return false;
  if (token === 'id' || token === 'gid' || token === 'fid' || token === 'pk' || token === 'uuid') return true;
  if (token.endsWith('_id') || token.includes('__id')) return true;
  if (token.includes('hasc')) return true;
  return false;
}

function collectMetadataRankingFieldCandidates(datasetRouting: any, datasetHelp: any, datasetCatalogItem: any): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const pushCandidate = (value: unknown) => {
    const candidate = String(value || '').trim();
    if (!candidate || isGeometryLikeRankingField(candidate)) return;
    const key = candidate.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    out.push(candidate);
  };

  const metricProfile = normalizeMetricProfile(datasetRouting?.metricProfile || datasetHelp?.routing?.metricProfile);
  const metricPreferred = Array.isArray(metricProfile?.preferredRankingFieldCandidates)
    ? metricProfile.preferredRankingFieldCandidates
    : [];
  const metricNumerator = Array.isArray(metricProfile?.numeratorFieldCandidates)
    ? metricProfile.numeratorFieldCandidates
    : [];
  const metricDenominator = Array.isArray(metricProfile?.denominatorFieldCandidates)
    ? metricProfile.denominatorFieldCandidates
    : [];
  metricPreferred.forEach(pushCandidate);
  metricNumerator.forEach(pushCandidate);
  metricDenominator.forEach(pushCandidate);

  const routingOrder = Array.isArray(datasetRouting?.orderByCandidates) ? datasetRouting.orderByCandidates : [];
  routingOrder.forEach(pushCandidate);

  const aiHints =
    (datasetHelp && typeof datasetHelp === 'object' && datasetHelp?.aiHints) ||
    (datasetCatalogItem && typeof datasetCatalogItem === 'object' && datasetCatalogItem?.aiHints) ||
    null;
  const aiOrder = Array.isArray(aiHints?.orderByCandidates) ? aiHints.orderByCandidates : [];
  aiOrder.forEach(pushCandidate);

  const aiNumeric = Array.isArray(aiHints?.numericFields) ? aiHints.numericFields : [];
  aiNumeric.forEach(pushCandidate);

  const fieldCatalog = Array.isArray(aiHints?.fieldCatalog) ? aiHints.fieldCatalog : [];
  fieldCatalog.forEach((item: any) => {
    if (!item || typeof item !== 'object') return;
    const fieldName = String(item?.name || '').trim();
    if (!fieldName) return;
    const fieldType = String(item?.type || '').trim().toLowerCase();
    const sortable = Boolean(item?.sortable);
    const rankable = Boolean(item?.rankable);
    if (fieldType === 'number' || fieldType === 'integer' || (sortable && rankable && fieldType !== 'geojson')) {
      pushCandidate(fieldName);
    }
  });

  ['area_m2', 'population', 'value', 'count', 'name', 'name_en', 'gid', 'id'].forEach(pushCandidate);
  return out;
}

function pickBestMetadataRankingCandidate(candidates: string[]): string {
  const normalized = (Array.isArray(candidates) ? candidates : [])
    .map(value => String(value || '').trim())
    .filter(Boolean);
  if (!normalized.length) return '';
  const nonId = normalized.find(value => !isGeometryLikeRankingField(value) && !isLikelyIdentifierRankingField(value));
  if (nonId) return nonId;
  return normalized.find(value => !isGeometryLikeRankingField(value)) || '';
}

function resolveFallbackRankingOrderBy(
  rows: any[],
  fields: any[],
  candidates: string[]
): string {
  const normalizedFields = Array.isArray(fields)
    ? fields.map((field: any) => String(field || '').trim()).filter(Boolean)
    : [];
  const rowSample = (Array.isArray(rows) ? rows : []).filter(
    (row: any) => row && typeof row === 'object' && !Array.isArray(row)
  ) as Record<string, unknown>[];
  const sampledRows = rowSample.slice(0, Math.min(200, rowSample.length));

  const resolveCanonical = (fieldName: string) => {
    const fromFields = normalizedFields.find(field => field.toLowerCase() === fieldName.toLowerCase());
    if (fromFields) return fromFields;
    const fromRows = sampledRows.length
      ? Object.keys(sampledRows[0]).find(key => key.toLowerCase() === fieldName.toLowerCase())
      : '';
    return fromRows || '';
  };

  const fieldHasComparableValues = (fieldName: string) => {
    if (!sampledRows.length) return false;
    const values = sampledRows.map(row => getRowFieldValueCaseInsensitive(row, fieldName));
    const nonNullValues = values.filter(value => value !== null && value !== undefined);
    if (!nonNullValues.length) return false;
    return nonNullValues.some(isComparableRankingValue);
  };

  const fieldHasNumericComparableValues = (fieldName: string) => {
    if (!sampledRows.length) return false;
    const values = sampledRows
      .map(row => getRowFieldValueCaseInsensitive(row, fieldName))
      .filter(value => value !== null && value !== undefined);
    if (!values.length) return false;
    const numericCount = values.filter(value => {
      if (typeof value === 'number') return Number.isFinite(value);
      if (typeof value !== 'string') return false;
      const parsed = Number(value);
      return Number.isFinite(parsed);
    }).length;
    return numericCount > 0;
  };

  const tryCandidates = (predicate: (fieldName: string) => boolean) => {
    for (const rawCandidate of candidates || []) {
      const canonical = resolveCanonical(String(rawCandidate || '').trim());
      if (!canonical || isGeometryLikeRankingField(canonical)) continue;
      if (predicate(canonical)) return canonical;
    }
    return '';
  };

  // 1) Prefer explicit metadata candidates that are numeric and not identifier-like.
  const candidateNumericNonId = tryCandidates(
    fieldName => !isLikelyIdentifierRankingField(fieldName) && fieldHasNumericComparableValues(fieldName)
  );
  if (candidateNumericNonId) return candidateNumericNonId;

  // 2) Then explicit metadata candidates that are comparable and not identifier-like.
  const candidateComparableNonId = tryCandidates(
    fieldName => !isLikelyIdentifierRankingField(fieldName) && fieldHasComparableValues(fieldName)
  );
  if (candidateComparableNonId) return candidateComparableNonId;

  // 3) Then any non-identifier field that has numeric values.
  for (const fieldName of normalizedFields) {
    if (!fieldName || isGeometryLikeRankingField(fieldName) || isLikelyIdentifierRankingField(fieldName)) continue;
    if (fieldHasNumericComparableValues(fieldName)) return fieldName;
  }

  // 4) Then any non-identifier comparable field.
  for (const fieldName of normalizedFields) {
    if (!fieldName || isGeometryLikeRankingField(fieldName) || isLikelyIdentifierRankingField(fieldName)) continue;
    if (fieldHasComparableValues(fieldName)) return fieldName;
  }

  // 5) As last resort, allow identifier-like fields to avoid hard failure.
  const candidateNumericAny = tryCandidates(fieldName => fieldHasNumericComparableValues(fieldName));
  if (candidateNumericAny) return candidateNumericAny;
  const candidateComparableAny = tryCandidates(fieldName => fieldHasComparableValues(fieldName));
  if (candidateComparableAny) return candidateComparableAny;

  for (const rawCandidate of candidates || []) {
    const canonical = resolveCanonical(String(rawCandidate || '').trim());
    if (!canonical || isGeometryLikeRankingField(canonical)) continue;
    return canonical;
  }

  return '';
}

function inferQcumberH3Metadata(fields: any[], rows: any[]) {
  const normalizedFields = Array.isArray(fields) ? fields : [];
  const fieldNames = normalizedFields
    .map((f: any) => String(f || '').trim())
    .filter(Boolean);
  const lowerFieldNames = fieldNames.map(name => name.toLowerCase());
  const h3FieldCandidates = fieldNames.filter((name, idx) => {
    const lower = lowerFieldNames[idx];
    return lower === 'h3_id' || lower === 'h3__id' || lower.endsWith('__h3_id') || lower.endsWith('_h3_id');
  });
  const resolutionFieldCandidates = fieldNames.filter((name, idx) => {
    const lower = lowerFieldNames[idx];
    return (
      lower === 'h3_resolution' ||
      lower === 'resolution' ||
      lower.endsWith('__h3_resolution') ||
      lower.endsWith('_h3_resolution')
    );
  });

  const resolutionValues = new Set<number>();
  const sampled = (Array.isArray(rows) ? rows : []).slice(0, 5000);
  sampled.forEach((row: any) => {
    if (!row || typeof row !== 'object' || Array.isArray(row)) return;
    resolutionFieldCandidates.forEach(fieldName => {
      const raw = (row as any)[fieldName];
      const num = Number(raw);
      if (Number.isFinite(num) && num >= 0 && num <= 15) {
        resolutionValues.add(Math.trunc(num));
      }
    });
  });

  return {
    h3Fields: h3FieldCandidates,
    resolutionFields: resolutionFieldCandidates,
    h3Resolutions: Array.from(resolutionValues).sort((a, b) => a - b)
  };
}

function inferQcumberAdminMetadata(fields: any[], rows: any[]) {
  const normalizedFields = Array.isArray(fields) ? fields : [];
  const fieldNames = normalizedFields
    .map((f: any) => String(f || '').trim())
    .filter(Boolean);
  const levelField =
    fieldNames.find(name => {
      const lower = name.toLowerCase();
      return lower === 'lv' || lower.endsWith('__lv') || lower.endsWith('_lv') || /(^|[_-])level($|[_-])/.test(lower);
    }) || null;
  const levelCounts: Record<string, number> = {};
  const sampled = (Array.isArray(rows) ? rows : []).slice(0, 50000);
  if (levelField) {
    sampled.forEach((row: any) => {
      if (!row || typeof row !== 'object' || Array.isArray(row)) return;
      const raw = (row as any)[levelField];
      if (raw === null || raw === undefined || String(raw).trim() === '') return;
      const key = String(raw).trim();
      levelCounts[key] = (levelCounts[key] || 0) + 1;
    });
  }
  const hasLv9 = Number(levelCounts['9'] || 0) > 0;
  return {
    levelField,
    levelCounts,
    hasLv9,
    sampledRows: sampled.length
  };
}

type CanonicalAdminType = 'country' | 'region' | 'province' | 'municipality';

const CANONICAL_ADMIN_TYPES = new Set<CanonicalAdminType>(['country', 'region', 'province', 'municipality']);

function canonicalAdminType(raw: string | null): CanonicalAdminType | null {
  if (!raw) return null;
  if (raw === 'country' || raw === 'stato') return 'country';
  if (raw === 'region' || raw === 'regione') return 'region';
  if (raw === 'province' || raw === 'provincia') return 'province';
  if (raw === 'municipality' || raw === 'comune') return 'municipality';
  return null;
}

function normalizeExpectedAdminType(raw: unknown): string | null {
  const token = normalizeQcumberEnumToken(raw);
  if (!token) return null;
  const normalized = QCUMBER_EXPECTED_ADMIN_TYPE_ALIASES[token] || token;
  const canonical = canonicalAdminType(normalized);
  return canonical && CANONICAL_ADMIN_TYPES.has(canonical) ? canonical : null;
}

function parseAdminTypeToLevelMap(raw: unknown): Partial<Record<CanonicalAdminType, number>> {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  const out: Partial<Record<CanonicalAdminType, number>> = {};
  Object.entries(raw as Record<string, unknown>).forEach(([rawKey, rawValue]) => {
    const canonical = canonicalAdminType(normalizeQcumberEnumToken(rawKey));
    if (!canonical) return;
    const numeric = Number(rawValue);
    if (!Number.isFinite(numeric)) return;
    out[canonical] = Math.trunc(numeric);
  });
  return out;
}

function resolveAdminTypeToLevelMap(datasetCatalogItem: any, datasetHelp: any, datasetRouting: any) {
  return {
    ...parseAdminTypeToLevelMap(datasetRouting?.adminTypeToLevel),
    ...parseAdminTypeToLevelMap(datasetCatalogItem?.ai?.profile?.adminWorkflows?.adminTypeToLevel),
    ...parseAdminTypeToLevelMap(datasetHelp?.aiHints?.aiProfile?.adminWorkflows?.adminTypeToLevel)
  } as Partial<Record<CanonicalAdminType, number>>;
}

function resolveExpectedAdminLevel(
  raw: unknown,
  adminTypeToLevel: Partial<Record<CanonicalAdminType, number>>
): number | null {
  const normalized = normalizeExpectedAdminType(raw);
  if (!normalized) return null;
  const canonical = canonicalAdminType(normalized);
  if (!canonical) return null;
  const mapped = adminTypeToLevel[canonical];
  return Number.isFinite(Number(mapped)) ? Math.trunc(Number(mapped)) : null;
}

function inferExpectedAdminLevelFromAvailableLevels(
  normalizedExpectedAdminType: string | null,
  adminTypeToLevel: Partial<Record<CanonicalAdminType, number>>,
  levelCounts: Record<string, number> | undefined,
  fallbackLevel: number | null
): number | null {
  const canonical = canonicalAdminType(normalizedExpectedAdminType);
  if (!canonical) return fallbackLevel;
  const mapped = adminTypeToLevel[canonical];
  if (Number.isFinite(Number(mapped))) {
    return Math.trunc(Number(mapped));
  }
  void levelCounts;
  return fallbackLevel;
}

function isNameLikeField(fieldName: unknown): boolean {
  const token = normalizeFieldToken(String(fieldName || '').trim());
  if (!token) return false;
  return (
    token === 'name' ||
    token === 'name en' ||
    token === 'nome' ||
    token.endsWith(' name') ||
    token.endsWith(' nome') ||
    token.includes('municipality name') ||
    token.includes('province name')
  );
}

function extractExactNameFilterValues(filters: any[]): string[] {
  const out = new Set<string>();
  (Array.isArray(filters) ? filters : []).forEach(filterItem => {
    if (!isNameLikeField(filterItem?.field)) return;
    const op = String(filterItem?.op || 'eq').toLowerCase();
    if (op === 'eq') {
      const value = String(filterItem?.value || '').trim();
      if (value) out.add(value);
      return;
    }
    if (op === 'in') {
      const values = Array.isArray(filterItem?.values)
        ? filterItem.values
        : Array.isArray(filterItem?.value)
        ? filterItem.value
        : [filterItem?.value];
      values.forEach((candidate: unknown) => {
        const value = String(candidate || '').trim();
        if (value) out.add(value);
      });
    }
  });
  return Array.from(out);
}

function valuesEqualAdminLevel(raw: unknown, expected: number): boolean {
  const num = Number(raw);
  if (Number.isFinite(num)) return Math.trunc(num) === expected;
  return String(raw || '').trim() === String(expected);
}

function inferQcumberDatasetClass(dataset: any): string {
  return inferQcumberDatasetRouting(dataset).datasetClass;
}

function hasBackendRoutingMetadata(dataset: any, datasetHelp?: any): boolean {
  const helpRouting = datasetHelp && typeof datasetHelp?.routing === 'object' ? datasetHelp.routing : null;
  const datasetRouting = dataset && typeof dataset?.routing === 'object' ? dataset.routing : null;
  return Boolean(helpRouting || datasetRouting);
}

function pickRoutingFromBackendOrInfer(dataset: any, datasetHelp?: any) {
  const backendRouting =
    datasetHelp && typeof datasetHelp?.routing === 'object'
      ? datasetHelp.routing
      : dataset && typeof dataset?.routing === 'object'
      ? dataset.routing
      : null;
  if (backendRouting && typeof backendRouting === 'object') {
    const normalizedMetric = normalizeMetricProfile((backendRouting as any)?.metricProfile, 'backend_routing');
    if (normalizedMetric) {
      return {
        ...backendRouting,
        metricProfile: normalizedMetric
      };
    }
    return backendRouting;
  }
  return inferQcumberDatasetRouting(dataset, datasetHelp);
}

function getFieldNamesFromAiHints(aiHints: any): string[] {
  const hints = aiHints && typeof aiHints === 'object' ? aiHints : null;
  const rawCatalog = Array.isArray(hints?.fieldCatalog) ? hints.fieldCatalog : [];
  const out: string[] = [];
  rawCatalog.forEach((item: any) => {
    const name = String(item?.name || '').trim();
    if (name && !out.includes(name)) {
      out.push(name);
    }
  });
  return out;
}

function inferQcumberDatasetRouting(dataset: any, datasetHelp?: any) {
  const id = String(dataset?.id || datasetHelp?.datasetId || '').toLowerCase();
  const name = String(dataset?.name || datasetHelp?.datasetName || '').toLowerCase();
  const description = String(dataset?.description || '').toLowerCase();
  const tags = (Array.isArray(dataset?.tags) ? dataset.tags : []).map((tag: any) => String(tag || '').toLowerCase());
  const aiHints = datasetHelp?.aiHints || dataset?.aiHints || null;
  const aiProfile = aiHints && typeof aiHints?.aiProfile === 'object' ? aiHints.aiProfile : null;
  const fields = getFieldNamesFromAiHints(aiHints);
  const normalizedFields = fields.map(field => field.toLowerCase());
  const geometryFields = Array.isArray(aiHints?.geometryFields)
    ? aiHints.geometryFields.map((field: any) => String(field || '')).filter(Boolean)
    : [];
  const numericFields = Array.isArray(aiHints?.numericFields)
    ? aiHints.numericFields.map((field: any) => String(field || '')).filter(Boolean)
    : [];
  const text = `${id} ${name} ${description} ${tags.join(' ')} ${normalizedFields.join(' ')}`;

  const levelFieldCandidates = fields.filter(field => {
    const normalized = field.toLowerCase();
    return normalized === 'lv' || normalized.endsWith('__lv') || normalized.endsWith('_lv');
  });
  const parentIdFieldCandidates = fields.filter(field => {
    const normalized = field.toLowerCase();
    return normalized.includes('__lv') && normalized.endsWith('_id');
  });
  if (aiProfile && typeof aiProfile?.adminWorkflows === 'object') {
    const parentIdFields = (aiProfile.adminWorkflows as any)?.parentIdFields;
    if (Array.isArray(parentIdFields)) {
      parentIdFields.forEach((fieldName: any) => {
        const candidate = String(fieldName || '').trim();
        if (candidate && !parentIdFieldCandidates.includes(candidate)) {
          parentIdFieldCandidates.push(candidate);
        }
      });
    }
  }
  const nameFieldCandidates = fields.filter(field => {
    const normalized = field.toLowerCase();
    return normalized === 'name' || normalized === 'name_en' || normalized.endsWith('_name');
  });

  const hasAdministrativeSignals =
    levelFieldCandidates.length > 0 ||
    parentIdFieldCandidates.length > 0 ||
    text.includes('administrative') ||
    text.includes('confini') ||
    text.includes('province') ||
    text.includes('provincia') ||
    text.includes('municipalit') ||
    text.includes('comune') ||
    text.includes('region') ||
    text.includes('regione') ||
    text.includes('kontur') ||
    tags.includes('boundaries');

  let datasetClass = 'other';
  if (hasAdministrativeSignals) {
    datasetClass = 'administrative';
  } else if (text.includes('event')) {
    datasetClass = 'events';
  } else if (text.includes('feature')) {
    datasetClass = 'features';
  } else if (text.includes('land-cover') || text.includes('land cover') || text.includes('clc')) {
    datasetClass = 'land_cover';
  }
  const profileDatasetClass = String(aiProfile?.datasetClass || '').trim().toLowerCase();
  if (profileDatasetClass) {
    datasetClass = profileDatasetClass;
  }

  const queryRouting = aiProfile && typeof aiProfile?.queryRouting === 'object' ? aiProfile.queryRouting : null;
  const adminWorkflows = aiProfile && typeof aiProfile?.adminWorkflows === 'object' ? aiProfile.adminWorkflows : null;
  const adminTypeToLevel = parseAdminTypeToLevelMap(adminWorkflows?.adminTypeToLevel);
  const profilePreferredToolRaw = String(aiProfile?.queryRouting?.preferredTool || '').trim();
  const profilePreferredTool = QCUMBER_ROUTING_QUERY_TOOLS.has(profilePreferredToolRaw) ? profilePreferredToolRaw : '';
  const inferredPreferredQueryTool =
    hasAdministrativeSignals
      ? 'queryQCumberTerritorialUnits'
      : geometryFields.length
      ? 'queryQCumberDatasetSpatial'
      : 'queryQCumberDataset';
  const preferredQueryTool =
    profilePreferredTool ||
    inferredPreferredQueryTool;
  const parseLooseBool = (raw: unknown, fallback: boolean): boolean => {
    if (typeof raw === 'boolean') return raw;
    if (typeof raw === 'number') return raw !== 0;
    if (typeof raw === 'string') {
      const normalized = raw.trim().toLowerCase();
      if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) return true;
      if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) return false;
    }
    return fallback;
  };
  const requiresSpatialBbox = parseLooseBool(
    queryRouting?.requiresSpatialBbox,
    preferredQueryTool === 'queryQCumberDatasetSpatial' && !hasAdministrativeSignals
  );
  const expectedAdminTypeSupported = parseLooseBool(queryRouting?.expectedAdminTypeSupported, hasAdministrativeSignals);
  const forbiddenAdminConstraints = Array.isArray(queryRouting?.forbiddenAdminConstraints)
    ? queryRouting.forbiddenAdminConstraints.map((item: any) => String(item || '').trim()).filter(Boolean)
    : [];
  const queryToolHintSource = profilePreferredTool
    ? 'descriptor_profile'
    : hasAdministrativeSignals
    ? 'inferred_admin_signals'
    : geometryFields.length
    ? 'inferred_geometry'
    : 'inferred_fallback';
  const queryToolHintConfidence = profilePreferredTool
    ? 'high'
    : hasAdministrativeSignals || geometryFields.length
    ? 'medium'
    : 'low';
  const queryToolHintReason = profilePreferredTool
    ? `Preferred tool provided by dataset descriptor profile (${profilePreferredTool}).`
    : hasAdministrativeSignals
    ? 'Administrative signals detected from fields/tags/text.'
    : geometryFields.length
    ? 'Geometry fields detected in dataset metadata.'
    : 'No administrative/spatial signal; fallback to generic query tool.';
  const suggestedOps = Array.isArray(aiHints?.suggestedOps) ? aiHints.suggestedOps : [];
  const metricProfile = inferMetricProfileFromContext({
    aiProfile,
    datasetClass,
    text,
    normalizedFields
  });

  return {
    datasetClass,
    isAdministrative: hasAdministrativeSignals,
    queryToolHint: {
      preferredTool: preferredQueryTool,
      confidence: queryToolHintConfidence,
      source: queryToolHintSource,
      reason: queryToolHintReason,
      requiresSpatialBbox,
      expectedAdminTypeSupported,
      forbiddenAdminConstraints
    },
    adminTypeToLevel,
    levelFieldCandidates,
    parentIdFieldCandidates,
    nameFieldCandidates,
    geometryFields,
    numericFields,
    suggestedOps,
    metricProfile
  };
}

function normalizeFieldToken(value: string): string {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function parseLocaleNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value !== 'string') {
    return null;
  }
  const raw = value.trim();
  if (!raw) return null;
  // Handle both "43,166175" and "1.234,56" style values.
  const normalized = raw.includes(',')
    ? raw.replace(/\./g, '').replace(',', '.')
    : raw;
  const num = Number(normalized);
  return Number.isFinite(num) ? num : null;
}

function normalizeSpatialBboxInput(value: unknown): number[] | undefined {
  if (!Array.isArray(value) || value.length !== 4) return undefined;
  const bbox = value.map(entry => Number(entry));
  if (bbox.some(entry => !Number.isFinite(entry))) return undefined;
  if (bbox.every(entry => entry === 0)) return undefined;
  const [minLon, minLat, maxLon, maxLat] = bbox;
  if (minLon < -180 || minLon > 180 || maxLon < -180 || maxLon > 180) return undefined;
  if (minLat < -90 || minLat > 90 || maxLat < -90 || maxLat > 90) return undefined;
  if (minLon > maxLon || minLat > maxLat) return undefined;
  return bbox;
}

function withGeometryFieldsForMapLoad(selectFields: string[]): string[] {
  const geometryRequired = ['_geojson', '_geometry', 'geometry', 'geom'];
  const latLonFallback = [
    'latitude',
    'longitude',
    'lat',
    'lon',
    'lng',
    'latitude GEO',
    'longitude GEO'
  ];
  const required = QCUMBER_INCLUDE_LATLON_FALLBACK_FIELDS
    ? [...geometryRequired, ...latLonFallback]
    : geometryRequired;
  const out = [...selectFields];
  const seen = new Set(out.map(field => String(field || '').toLowerCase()));
  required.forEach(field => {
    const key = field.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      out.push(field);
    }
  });
  return out;
}

function withAdminLevelFieldsForValidation(selectFields: string[]): string[] {
  const required = ['lv', 'level', 'admin_level', 'adm_level'];
  const out = [...selectFields];
  const seen = new Set(out.map(field => String(field || '').toLowerCase()));
  required.forEach(field => {
    const key = field.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      out.push(field);
    }
  });
  return out;
}

function ensureGeojsonPointsFromLatLon(datasetPayload: any): any {
  const fields = datasetPayload?.data?.fields;
  const rows = datasetPayload?.data?.rows;
  if (!Array.isArray(fields) || !Array.isArray(rows) || !fields.length) {
    return datasetPayload;
  }
  const existingGeoFieldIdx = fields.findIndex((f: any) => String(f?.name || '').toLowerCase() === '_geojson');
  if (existingGeoFieldIdx >= 0) {
    return datasetPayload;
  }

  const names = fields.map((f: any) => String(f?.name || ''));
  const norm = names.map(normalizeFieldToken);

  const findIdx = (candidates: string[]) =>
    norm.findIndex(token => candidates.some(candidate => token === candidate || token.includes(candidate)));

  // Prefer WGS84 textual GEO columns when present (e.g. "latitude GEO"/"longitude GEO").
  let latIdx = findIdx(['latitude geo', 'lat geo', 'latitude wgs84', 'lat wgs84']);
  let lonIdx = findIdx(['longitude geo', 'lon geo', 'lng geo', 'longitude wgs84', 'lon wgs84', 'lng wgs84']);

  if (latIdx < 0 || lonIdx < 0) {
    latIdx = findIdx(['latitude', 'lat']);
    lonIdx = findIdx(['longitude', 'lon', 'lng']);
  }

  if (latIdx < 0 || lonIdx < 0) {
    return datasetPayload;
  }

  const nextFields = [...fields, {name: '_geojson', type: 'geojson'}];
  const nextRows = rows.map((row: any) => {
    if (!Array.isArray(row)) return row;
    const lat = parseLocaleNumber(row[latIdx]);
    const lon = parseLocaleNumber(row[lonIdx]);
    const valid = lat !== null && lon !== null && Math.abs(lat) <= 90 && Math.abs(lon) <= 180;
    const geojson = valid ? {type: 'Point', coordinates: [lon, lat]} : null;
    return [...row, geojson];
  });

  return {
    ...datasetPayload,
    data: {
      ...datasetPayload.data,
      fields: nextFields,
      rows: nextRows
    }
  };
}

function valuesEqualLoose(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  const leftNum = Number(left);
  const rightNum = Number(right);
  if (Number.isFinite(leftNum) && Number.isFinite(rightNum)) {
    return leftNum === rightNum;
  }
  return String(left ?? '').toLowerCase() === String(right ?? '').toLowerCase();
}

function rowMatchesFilter(row: any, filter: any): boolean {
  if (!row || typeof row !== 'object' || Array.isArray(row)) return true;
  const field = String(filter?.field || '').trim();
  if (!field) return true;
  const op = String(filter?.op || 'eq').toLowerCase();
  const rowValue = (row as any)[field];
  const rhs = filter?.value;
  const rhsValues = Array.isArray(filter?.values) ? filter.values : Array.isArray(rhs) ? rhs : [rhs];

  if (op === 'in') {
    return rhsValues.some((v: unknown) => valuesEqualLoose(rowValue, v));
  }
  if (op === 'eq') return valuesEqualLoose(rowValue, rhs);
  if (op === 'ne' || op === 'neq') return !valuesEqualLoose(rowValue, rhs);

  const leftNum = Number(rowValue);
  const rightNum = Number(rhs);
  if (!Number.isFinite(leftNum) || !Number.isFinite(rightNum)) return true;
  if (op === 'gt') return leftNum > rightNum;
  if (op === 'gte') return leftNum >= rightNum;
  if (op === 'lt') return leftNum < rightNum;
  if (op === 'lte') return leftNum <= rightNum;
  return true;
}

function isLikelyMunicipalFilter(filters: any[]): boolean {
  const list = Array.isArray(filters) ? filters : [];
  return list.some((filter: any) => {
    const field = String(filter?.field || '').toLowerCase();
    const op = String(filter?.op || 'eq').toLowerCase();
    const value = filter?.value;
    const values = Array.isArray(filter?.values) ? filter.values : Array.isArray(value) ? value : [value];
    const isLevelField = field === 'lv' || field.endsWith('__lv') || field.endsWith('_lv');
    if (!isLevelField) return false;
    if (op !== 'eq' && op !== 'in') return false;
    return values.some((v: any) => Number(v) === 9 || String(v).trim() === '9');
  });
}

function isGenericParentIdFieldName(fieldName: unknown): boolean {
  const normalized = normalizeFieldToken(String(fieldName || ''));
  return normalized === 'parent id' || normalized === 'parentid';
}

function hasGenericParentIdFilter(filters: any[]): boolean {
  return (Array.isArray(filters) ? filters : []).some(filterItem =>
    isGenericParentIdFieldName((filterItem as any)?.field)
  );
}

function extractRequestedAdminLevelFromFilters(filters: any[]): number | null {
  const list = Array.isArray(filters) ? filters : [];
  for (const filterItem of list) {
    const field = String(filterItem?.field || '').trim();
    if (!field || !isLikelyAdminLevelFieldName(field)) continue;
    const op = String(filterItem?.op || 'eq').toLowerCase();
    if (op === 'eq') {
      const raw = filterItem?.value;
      const num = Number(raw);
      if (Number.isFinite(num)) return Math.trunc(num);
      const text = String(raw || '').trim();
      if (/^\d+$/.test(text)) return Math.trunc(Number(text));
      continue;
    }
    if (op === 'in') {
      const values = Array.isArray(filterItem?.values)
        ? filterItem.values
        : Array.isArray(filterItem?.value)
        ? filterItem.value
        : [filterItem?.value];
      if (values.length !== 1) continue;
      const raw = values[0];
      const num = Number(raw);
      if (Number.isFinite(num)) return Math.trunc(num);
      const text = String(raw || '').trim();
      if (/^\d+$/.test(text)) return Math.trunc(Number(text));
    }
  }
  return null;
}

function collectParentIdRetryFieldCandidates(
  datasetRouting: any,
  datasetHelp: any,
  datasetCatalogItem: any,
  filters: any[]
): string[] {
  const out: string[] = [];
  const push = (fieldName: unknown) => {
    const candidate = String(fieldName || '').trim();
    if (!candidate) return;
    if (isGenericParentIdFieldName(candidate)) return;
    if (!out.includes(candidate)) out.push(candidate);
  };
  const pushList = (value: unknown) => {
    if (!Array.isArray(value)) return;
    value.forEach(push);
  };

  const aiHints =
    (datasetHelp && typeof datasetHelp === 'object' && datasetHelp?.aiHints) ||
    (datasetCatalogItem && typeof datasetCatalogItem === 'object' && datasetCatalogItem?.aiHints) ||
    null;
  const aiProfile = aiHints && typeof aiHints?.aiProfile === 'object' ? aiHints.aiProfile : null;
  const requestedLevel = extractRequestedAdminLevelFromFilters(filters);

  const priorityByLevel =
    aiProfile &&
    typeof aiProfile?.adminWorkflows === 'object' &&
    aiProfile.adminWorkflows &&
    typeof aiProfile.adminWorkflows?.parentIdPriorityForChildLevel === 'object'
      ? aiProfile.adminWorkflows.parentIdPriorityForChildLevel
      : null;
  if (priorityByLevel && requestedLevel !== null) {
    const levelKey = String(requestedLevel);
    const raw = (priorityByLevel as any)[levelKey];
    if (typeof raw === 'string') {
      push(raw);
    } else {
      pushList(raw);
    }
  }

  if (aiProfile && typeof aiProfile?.adminWorkflows === 'object') {
    const explicitParentFields = (aiProfile.adminWorkflows as any)?.parentIdFields;
    pushList(explicitParentFields);
  }

  pushList(datasetRouting?.parentIdFieldCandidates);

  const fieldCatalog = Array.isArray(aiHints?.fieldCatalog) ? aiHints.fieldCatalog : [];
  fieldCatalog.forEach((item: any) => {
    if (!item || typeof item !== 'object') return;
    const role = String(item?.semanticRole || '').trim().toLowerCase();
    if (role === 'admin_parent_id') {
      push(item?.name);
    }
  });

  return out;
}

function rewriteGenericParentIdFilters(filters: any[], targetFieldName: string): any[] {
  const target = String(targetFieldName || '').trim();
  if (!target) return Array.isArray(filters) ? [...filters] : [];
  return (Array.isArray(filters) ? filters : []).map(filterItem => {
    if (!isGenericParentIdFieldName((filterItem as any)?.field)) {
      return filterItem;
    }
    if (filterItem && typeof filterItem === 'object' && !Array.isArray(filterItem)) {
      return {
        ...filterItem,
        field: target
      };
    }
    return filterItem;
  });
}

function isMissingFilterFieldError(error: unknown, fieldName: string): boolean {
  const message = String((error as any)?.message || error || '')
    .trim()
    .toLowerCase();
  if (!message) return false;
  const field = String(fieldName || '')
    .trim()
    .toLowerCase();
  if (!field) return false;
  return (
    message.includes(`filter field '${field}' is not available`) ||
    message.includes(`filter field "${field}" is not available`) ||
    message.includes(`column "${field}"`) ||
    message.includes(`column '${field}'`)
  );
}

function isLikelyAdminLevelFieldName(fieldName: unknown): boolean {
  const field = String(fieldName || '')
    .trim()
    .toLowerCase();
  if (!field) return false;
  return (
    field === 'lv' ||
    field.endsWith('__lv') ||
    field.endsWith('_lv') ||
    /(^|[_-])level($|[_-])/.test(field) ||
    field === 'admin_level' ||
    field === 'adm_level'
  );
}

function verifyRowsAgainstFilters(rows: any[], filters: any[]): {ok: boolean; failedFilter?: string} {
  const normalizedRows = Array.isArray(rows) ? rows : [];
  const normalizedFilters = Array.isArray(filters) ? filters : [];
  if (!normalizedRows.length || !normalizedFilters.length) return {ok: true};
  for (const filter of normalizedFilters) {
    const field = String(filter?.field || '').trim();
    if (!field) continue;
    const seenField = normalizedRows.some((row: any) => row && typeof row === 'object' && field in row);
    if (!seenField) continue;
    const allMatch = normalizedRows.every((row: any) => rowMatchesFilter(row, filter));
    if (!allMatch) {
      return {ok: false, failedFilter: `${field}:${String(filter?.op || 'eq')}`};
    }
  }
  return {ok: true};
}

function datasetHasRenderableGeometry(datasetPayload: any): boolean {
  const fields = datasetPayload?.data?.fields;
  const rows = datasetPayload?.data?.rows;
  if (!Array.isArray(fields) || !Array.isArray(rows) || !fields.length) {
    return false;
  }
  const geoIdx = fields.findIndex((f: any) => String(f?.type || '').toLowerCase() === 'geojson');
  if (geoIdx < 0) {
    return false;
  }
  const sample = rows.slice(0, 2000);
  for (const row of sample) {
    if (!Array.isArray(row)) continue;
    const geom = row[geoIdx];
    if (geom && typeof geom === 'object') {
      return true;
    }
  }
  return false;
}

/**
 * Route q-cumber requests through the q-assistant backend proxy.
 * Replaces the previous direct-to-q-cumber fetch with auth token handling.
 */
async function requestQcumberJson(path: string, init: RequestInit = {}) {
  // POST /datasets/query
  if (init.method === 'POST' && path === '/datasets/query') {
    const body = init.body ? JSON.parse(String(init.body)) : {};
    return qcumberQuery(body);
  }
  // GET /providers or /providers?flat=true
  if (/^\/providers(\?|$)/.test(path)) {
    const flat = path.includes('flat=true');
    return qcumberListProviders(flat);
  }
  // GET /providers/:id/datasets/:did/help (or just /datasets/:did)
  const helpMatch = path.match(/^\/providers\/([^/]+)\/datasets\/([^/]+)\/help/);
  if (helpMatch) {
    return qcumberGetDatasetHelp(decodeURIComponent(helpMatch[1]), decodeURIComponent(helpMatch[2]));
  }
  // GET /providers/:id/datasets
  const dsMatch = path.match(/^\/providers\/([^/]+)\/datasets/);
  if (dsMatch) {
    return qcumberListDatasets(decodeURIComponent(dsMatch[1]));
  }
  // Fallback: unknown path — throw descriptive error
  throw new Error(`Unknown q-cumber proxy path: ${path}`);
}

function qcumberDatasetHelpCacheKey(providerId: string, datasetId: string): string {
  return `${String(providerId || '').trim().toLowerCase()}::${String(datasetId || '').trim().toLowerCase()}`;
}

async function requestQcumberDatasetHelp(providerId: string, datasetId: string): Promise<any> {
  return requestQcumberJson(
    `/providers/${encodeURIComponent(String(providerId || '').trim())}/datasets/${encodeURIComponent(
      String(datasetId || '').trim()
    )}/help`
  );
}

async function getQcumberDatasetHelpCached(providerId: string, datasetId: string): Promise<any | null> {
  const key = qcumberDatasetHelpCacheKey(providerId, datasetId);
  if (QCUMBER_DATASET_HELP_CACHE.has(key)) {
    return QCUMBER_DATASET_HELP_CACHE.get(key) || null;
  }
  try {
    const payload = await requestQcumberDatasetHelp(providerId, datasetId);
    setBoundedMapValue(QCUMBER_DATASET_HELP_CACHE, key, payload || null, QCUMBER_DATASET_HELP_CACHE_MAX_SIZE);
    return payload || null;
  } catch {
    setBoundedMapValue(QCUMBER_DATASET_HELP_CACHE, key, null, QCUMBER_DATASET_HELP_CACHE_MAX_SIZE);
    return null;
  }
}

async function listMapsFromCloud(apiBaseUrl: string, provider?: string) {
  const resolvedProvider = normalizeCloudMapProvider(provider);
  const payload = await callMcpToolParsed(apiBaseUrl, ['list_qmap_cloud_maps'], {provider: resolvedProvider});

  const items = payload?.maps;
  if (!Array.isArray(items)) {
    return [];
  }

  return items.map((item: any) => ({
    id: String(item?.id || item?.mapId || ''),
    title: String(item?.title || item?.name || 'Untitled map'),
    description: String(item?.description || '')
  }));
}

function createListQMapCloudMapsTool(apiBaseUrl: string) {
  return extendedTool({
    description: 'List available maps from q-map cloud storage provider.',
    parameters: z
      .object({
        provider: OPTIONAL_NON_EMPTY_STRING_SCHEMA
      })
      .strict(),
    execute: async ({provider}) => {
      const resolvedProvider = normalizeCloudMapProvider(provider);
      const maps = await listMapsFromCloud(apiBaseUrl, resolvedProvider);
      return {
        llmResult: {
          success: true,
          provider: resolvedProvider,
          maps,
          details: maps.length
            ? `Found ${maps.length} cloud maps in provider ${resolvedProvider}.`
            : `No cloud maps were found for current account in provider ${resolvedProvider}.`
        }
      };
    }
  });
}

function createLoadQMapCloudMapTool(apiBaseUrl: string) {
  return extendedTool({
    description:
      'Load a map from q-map cloud storage into q-hive. Provide mapId from listQMapCloudMaps.',
    parameters: z
      .object({
        provider: OPTIONAL_NON_EMPTY_STRING_SCHEMA,
        mapId: NON_EMPTY_STRING_SCHEMA
      })
      .strict(),
    execute: async ({provider, mapId}) => {
      const resolvedProvider = normalizeCloudMapProvider(provider);
      let resolvedId = (mapId || '').trim();

      if (!resolvedId) {
        return {
          llmResult: {
            success: false,
            details: 'Missing mapId. Call listQMapCloudMaps first and pass a valid mapId.'
          }
        };
      }

      const actionPayload = await callMcpToolParsed(
        apiBaseUrl,
        ['build_load_cloud_map_action'],
        {
          provider: resolvedProvider,
          mapId: resolvedId
        }
      );

      const args = actionPayload?.action?.args || {};
      const providerName = normalizeCloudMapProvider(String(args.provider || resolvedProvider));
      const actionMapId = String(args.mapId || resolvedId);

      return {
        llmResult: {
          success: true,
          details: `Loading cloud map ${actionMapId}...`,
          mapId: actionMapId
        },
        additionalData: {
          provider: providerName,
          loadParams: {
            id: actionMapId,
            path: `/maps/${actionMapId}`
          }
        }
      };
    },
    component: LoadQMapCloudMapComponent
  });
}

function createListQCumberDatasetsTool() {
  return extendedTool({
    description: 'List datasets available from q-cumber dynamic provider catalog.',
    parameters: z
      .object({
        providerId: OPTIONAL_NON_EMPTY_STRING_SCHEMA
      })
      .strict(),
    execute: async ({providerId}) => {
      if (isInvalidProviderIdLiteral(providerId)) {
        const providerSelection = await resolveExistingQCumberProviderId(undefined, {forceRefresh: true});
        return {
          llmResult: {
            success: false,
            providerId: String(providerId || ''),
            datasets: [],
            details:
              `Invalid providerId "${String(providerId || '')}". ` +
              `Use an exact provider id from listQCumberProviders (e.g. ${providerSelection.availableProviderIds.join(', ') || 'local-assets-it'}).`
          }
        };
      }
      let providerSelection = await resolveExistingQCumberProviderId(providerId);
      let resolvedProviderId = providerSelection.providerId;
      if (!resolvedProviderId) {
        const availableHint = providerSelection.availableProviderIds.length
          ? ` Available providers: ${providerSelection.availableProviderIds.join(', ')}.`
          : '';
        return {
          llmResult: {
            success: false,
            providerId: '',
            datasets: [],
            details:
              `Missing providerId. Call listQCumberProviders and pass a valid providerId.${availableHint}`
          }
        };
      }
      try {
        const payload = await requestQcumberJson(`/providers/${encodeURIComponent(resolvedProviderId)}/datasets`);
        const items = Array.isArray(payload?.items) ? payload.items : [];
        const datasets = items.map((item: any) => {
          const routing = pickRoutingFromBackendOrInfer(item);
          const preferredQueryTool = getRoutingPreferredQueryTool(routing);
          const metricProfile = normalizeMetricProfile(routing?.metricProfile);
          return {
            id: String(item?.id || ''),
            name: String(item?.name || item?.id || ''),
            description: String(item?.description || ''),
            format: String(item?.format || ''),
            tags: Array.isArray(item?.tags) ? item.tags : [],
            aiHints: item?.aiHints || null,
            routingClass: String(routing?.datasetClass || inferQcumberDatasetClass(item) || 'other'),
            routing,
            metricProfile,
            helpAvailable: true
          };
        });

        return {
          llmResult: {
            success: true,
            providerId: resolvedProviderId,
            datasets,
            routingHint: QCUMBER_PROVIDER_ROUTING_HINTS[resolvedProviderId] || '',
            details: datasets.length
              ? `Found ${datasets.length} datasets in provider ${resolvedProviderId}.${providerSelection.autoSelected ? ' (provider auto-selected)' : ''}`
              : `No datasets found for provider ${resolvedProviderId}.`
          }
        };
      } catch (error: any) {
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasets: [],
            details: `Failed to list datasets for provider ${resolvedProviderId}: ${String(
              error?.message || error || 'Unknown error'
            )}.${providerSelection.availableProviderIds.length ? ` Available providers: ${providerSelection.availableProviderIds.join(', ')}.` : ''}`
          }
        };
      }
    }
  });
}

function createGetQCumberDatasetHelpTool() {
  return extendedTool({
    description:
      'Get backend AI help/metadata for a specific q-cumber dataset. Use this to choose the right query tool and field-level routing before querying.',
    parameters: z
      .object({
        providerId: NON_EMPTY_STRING_SCHEMA,
        datasetId: NON_EMPTY_STRING_SCHEMA
      })
      .strict(),
    execute: async ({providerId, datasetId}) => {
      if (isInvalidProviderIdLiteral(providerId)) {
        const providerSelection = await resolveExistingQCumberProviderId(undefined, {forceRefresh: true});
        return {
          llmResult: {
            success: false,
            providerId: String(providerId || ''),
            datasetId: String(datasetId || ''),
            details:
              `Invalid providerId "${String(providerId || '')}". ` +
              `Use an exact provider id from listQCumberProviders (e.g. ${providerSelection.availableProviderIds.join(', ') || 'local-assets-it'}).`
          }
        };
      }
      let providerSelection = await resolveExistingQCumberProviderId(providerId);
      let resolvedProviderId = providerSelection.providerId || resolveQCumberProviderId(providerId);
      const datasetResolution = await resolveQCumberDatasetId(resolvedProviderId, datasetId);
      let resolvedDatasetId = datasetResolution.resolvedDatasetId;
      if (!resolvedDatasetId) {
        const availableDatasets = formatAvailableDatasetIds(datasetResolution.availableDatasetIds);
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasetId: String(datasetResolution.requestedDatasetId || datasetId || ''),
            details:
              `Invalid datasetId "${String(datasetResolution.requestedDatasetId || datasetId || '').trim()}". ` +
              'Use an exact datasetId from listQCumberDatasets(providerId).' +
              (availableDatasets ? ` Available datasetIds: ${availableDatasets}.` : '')
          }
        };
      }
      let catalogItem: any | null = null;
      try {
        catalogItem = await getQcumberDatasetCatalogItem(resolvedProviderId, resolvedDatasetId);
      } catch {
        catalogItem = null;
      }
      try {
        const help = await requestQcumberDatasetHelp(resolvedProviderId, resolvedDatasetId);
        const cacheKey = qcumberDatasetHelpCacheKey(resolvedProviderId, resolvedDatasetId);
        setBoundedMapValue(QCUMBER_DATASET_HELP_CACHE, cacheKey, help || null, QCUMBER_DATASET_HELP_CACHE_MAX_SIZE);
        const routing = pickRoutingFromBackendOrInfer(catalogItem || {id: resolvedDatasetId}, help || null);
        const metricProfile = normalizeMetricProfile(routing?.metricProfile);
        return {
          llmResult: {
            success: true,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            datasetName: String(help?.datasetName || catalogItem?.name || resolvedDatasetId),
            aiHints: help?.aiHints || null,
            routing,
            metricProfile,
            details:
              `Loaded dataset help for ${resolvedDatasetId}. ` +
              `Preferred query tool: ${getRoutingPreferredQueryTool(routing) || 'queryQCumberDataset'}.` +
              `${buildMetricProfileHint(metricProfile)}${providerSelection.autoSelected ? ' (provider auto-selected)' : ''}`
          }
        };
      } catch (error: any) {
        const routing = pickRoutingFromBackendOrInfer(catalogItem || {id: resolvedDatasetId}, null);
        const metricProfile = normalizeMetricProfile(routing?.metricProfile);
        const hasUsableRouting = Boolean(
          routing &&
            (routing.isAdministrative !== undefined ||
              routing.queryToolHint?.preferredTool ||
              routing.datasetClass)
        );
        const errorNote = `Help endpoint unavailable (${String(
          error?.message || error || 'Unknown error'
        ).slice(0, 80)}); routing inferred from catalog.`;
        return {
          llmResult: {
            success: hasUsableRouting,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            routing,
            metricProfile,
            details: hasUsableRouting
              ? `${errorNote} Preferred query tool: ${getRoutingPreferredQueryTool(routing) || 'queryQCumberDataset'}.${buildMetricProfileHint(metricProfile)}`
              : `Failed to load dataset help for ${resolvedDatasetId}: ${String(
                  error?.message || error || 'Unknown error'
                )}.${providerSelection.availableProviderIds.length ? ` Available providers: ${providerSelection.availableProviderIds.join(', ')}.` : ''}`
          }
        };
      }
    }
  });
}

function createListQCumberProvidersTool() {
  return extendedTool({
    description: 'List available dataset providers from q-cumber backend (e.g. q-cumber, ckan, esri).',
    parameters: z
      .object({
        locale: OPTIONAL_NON_EMPTY_STRING_SCHEMA.describe('Optional locale (e.g. it, en)')
      })
      .strict(),
    execute: async ({locale}) => {
      try {
        const items = await listQcumberProvidersCatalog(locale, true);
        const providers = items.map((item: any) => ({
          id: String(item?.id || ''),
          name: String(item?.name || item?.id || ''),
          locale: String(item?.locale || ''),
          category: String(item?.category || ''),
          apiType: String(item?.apiType || ''),
          apiBaseUrl: String(item?.apiBaseUrl || ''),
          capabilities: Array.isArray(item?.capabilities) ? item.capabilities : [],
          formats: Array.isArray(item?.formats) ? item.formats : [],
          tags: Array.isArray(item?.tags) ? item.tags : [],
          routingHint: QCUMBER_PROVIDER_ROUTING_HINTS[String(item?.id || '')] || '',
          helperTools: ['listQCumberDatasets', 'getQCumberDatasetHelp']
        }));
        return {
          llmResult: {
            success: true,
            providers,
            details: providers.length
              ? `Found ${providers.length} data providers.`
              : 'No data providers found.'
          }
        };
      } catch (error: any) {
        return {
          llmResult: {
            success: false,
            providers: [],
            details: `Failed to list providers: ${String(error?.message || error || 'Unknown error')}`
          }
        };
      }
    }
  });
}

function createQueryQCumberDatasetTool(policyMode: QcumberQueryPolicy = 'auto') {
  const isTerritorialOnly = policyMode === 'territorial';
  const isSpatialOnly = policyMode === 'thematic_spatial';
  const description = isTerritorialOnly
    ? 'Query territorial/admin-unit datasets from q-cumber backend (boundaries/levels).'
    : isSpatialOnly
    ? 'Query thematic datasets from q-cumber backend. Prefer spatialBbox to constrain extent.'
    : 'Run a read-only query on q-cumber dataset and optionally load result into map.';
  const commonParameterShape = {
    providerId: OPTIONAL_NON_EMPTY_STRING_SCHEMA,
    datasetId: z
      .string()
      .trim()
      .min(1)
      .optional()
      .describe(
        'Exact datasetId from listQCumberDatasets(providerId). If omitted, the tool attempts deterministic auto-selection from provider catalog metadata.'
      ),
    filters: z.array(QCUMBER_FILTER_SCHEMA).max(32).optional(),
    orderBy: OPTIONAL_NON_EMPTY_STRING_SCHEMA,
    orderDirection: QCUMBER_ORDER_DIRECTION_SCHEMA,
    limit: z.number().int().min(1).max(100000).optional(),
    offset: z.number().int().min(0).optional(),
    loadToMap: z.boolean().optional(),
    showOnMap: z
      .boolean()
      .optional()
      .describe('When loadToMap=true, controls layer visibility. Default false (dataset loaded, layer hidden).')
  };
  const expectedAdminTypeSchema = QCUMBER_EXPECTED_ADMIN_TYPE_SCHEMA.describe(
    'Optional strict administrative guardrail. Enforces expected level (e.g. comune/municipality -> lv=9).'
  );
  const spatialBboxSchema = z
    .tuple([z.number(), z.number(), z.number(), z.number()])
    .optional()
    .describe('Optional bbox [minLon,minLat,maxLon,maxLat] in EPSG:4326 for backend prefilter.');
  const parameters = isTerritorialOnly
    ? z
        .object({
          ...commonParameterShape,
          expectedAdminType: expectedAdminTypeSchema
        })
        .strict()
    : isSpatialOnly
    ? z
        .object({
          ...commonParameterShape,
          spatialBbox: spatialBboxSchema
        })
        .strict()
    : z
        .object({
          ...commonParameterShape,
          spatialBbox: spatialBboxSchema,
          expectedAdminType: expectedAdminTypeSchema,
          inferPointsFromLatLon: z
            .boolean()
            .optional()
            .describe('Default false. If true and geometry is missing, build point geometry from lat/lon columns.')
        })
        .strict();

  return extendedTool({
    description,
    parameters,
    execute: async (args: any) => {
      const {
        providerId,
        datasetId,
        filters,
        orderBy,
        orderDirection,
        limit,
        offset,
        spatialBbox,
        loadToMap,
        showOnMap,
        expectedAdminType,
        inferPointsFromLatLon
      } = args || {};
      if (isInvalidProviderIdLiteral(providerId)) {
        const providerSelection = await resolveExistingQCumberProviderId(undefined, {forceRefresh: true});
        return {
          llmResult: {
            success: false,
            providerId: String(providerId || ''),
            requestedDatasetId: String(datasetId || '').trim() || undefined,
            details:
              `Invalid providerId "${String(providerId || '')}". ` +
              `Use an exact provider id from listQCumberProviders (e.g. ${providerSelection.availableProviderIds.join(', ') || 'local-assets-it'}).`
          }
        };
      }
      const requestedDatasetId = String(datasetId || '').trim();
      const requestedProviderId = String(providerId || '').trim();
      const normalizedExpectedAdminTypeInput = normalizeExpectedAdminType(expectedAdminType);
      const normalizedExpectedAdminType = isSpatialOnly ? null : normalizedExpectedAdminTypeInput;
      let queryPolicyAdjustmentsNote =
        isSpatialOnly && normalizedExpectedAdminTypeInput
          ? ` expectedAdminType "${normalizedExpectedAdminTypeInput}" ignored for thematic spatial query.`
          : '';
      const normalizedSpatialBbox = normalizeSpatialBboxInput(spatialBbox);
      let providerSelection = await resolveExistingQCumberProviderId(providerId);
      let resolvedProviderId = await resolvePreferredTerritorialProviderId(
        providerId,
        requestedDatasetId,
        providerSelection.providerId || resolveQCumberProviderId(providerId)
      );
      if (
        providerSelection.availableProviderIds.length &&
        resolvedProviderId &&
        !providerSelection.availableProviderIds.includes(resolvedProviderId)
      ) {
        resolvedProviderId = providerSelection.providerId || resolvedProviderId;
      }
      const resolveDatasetIdForProvider = async (providerIdCandidate: string) =>
        resolveQCumberDatasetIdForQuery({
          providerId: providerIdCandidate,
          rawDatasetId: datasetId,
          policyMode,
          normalizedExpectedAdminType,
          spatialBbox: normalizedSpatialBbox
        });
      let resolvedDatasetId = '';
      let datasetResolution: {
        datasetId: string;
        requestedDatasetId: string;
        requestedDatasetIdInvalid: boolean;
        availableDatasetIds: string[];
        autoSelected: boolean;
      } = {
        datasetId: '',
        requestedDatasetId: '',
        requestedDatasetIdInvalid: false,
        availableDatasetIds: [],
        autoSelected: false
      };
      let datasetResolutionError: unknown = null;
      try {
        datasetResolution = await resolveDatasetIdForProvider(resolvedProviderId);
        resolvedDatasetId = datasetResolution.datasetId;
      } catch (error) {
        datasetResolutionError = error;
      }
      const providerResolutionChanged =
        !!requestedProviderId &&
        !!resolvedProviderId &&
        requestedProviderId.toLowerCase() !== resolvedProviderId.toLowerCase();
      const providerAutoSelected = !requestedProviderId && !!resolvedProviderId;
      const providerResolutionNote = providerResolutionChanged
        ? ` Provider resolved from "${requestedProviderId}" to "${resolvedProviderId}".`
        : providerAutoSelected && providerSelection.autoSelected
        ? ` Provider auto-selected as "${resolvedProviderId}".`
        : '';
      if (datasetResolutionError) {
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId || resolveQCumberProviderId(providerId),
            requestedDatasetId: requestedDatasetId || undefined,
            details:
              `Failed to resolve provider/dataset before query: ${String(
                (datasetResolutionError as any)?.message || datasetResolutionError || 'Unknown error'
              )}.${providerResolutionNote}` +
              (providerSelection.availableProviderIds.length
                ? ` Available providers: ${providerSelection.availableProviderIds.join(', ')}.`
                : '')
          }
        };
      }
      if (datasetResolution.requestedDatasetIdInvalid) {
        const availableDatasets = formatAvailableDatasetIds(datasetResolution.availableDatasetIds);
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            requestedDatasetId: requestedDatasetId || undefined,
            details:
              `Invalid datasetId "${datasetResolution.requestedDatasetId}". ` +
              'Use an exact datasetId from listQCumberDatasets(providerId).' +
              (availableDatasets ? ` Available datasetIds: ${availableDatasets}.` : '') +
              providerResolutionNote
          }
        };
      }
      if (!resolvedDatasetId) {
        const availableDatasets = formatAvailableDatasetIds(datasetResolution.availableDatasetIds);
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            requestedDatasetId: requestedDatasetId || undefined,
            details:
              `Missing datasetId and unable to auto-select a unique dataset for provider "${resolvedProviderId}". ` +
              `Call listQCumberDatasets(providerId), then pass datasetId explicitly (optionally after getQCumberDatasetHelp).` +
              (availableDatasets ? ` Available datasetIds: ${availableDatasets}.` : '') +
              providerResolutionNote
          }
        };
      }
      let datasetCatalogItem: any | null = null;
      let datasetHelp: any | null = null;
      let datasetRouting = inferQcumberDatasetRouting({id: resolvedDatasetId});
      let datasetIsTerritorial = false;
      try {
        datasetCatalogItem = await getQcumberDatasetCatalogItem(resolvedProviderId, resolvedDatasetId);
      } catch {
        datasetCatalogItem = null;
      }
      datasetHelp = await getQcumberDatasetHelpCached(resolvedProviderId, resolvedDatasetId);
      datasetRouting = pickRoutingFromBackendOrInfer(datasetCatalogItem || {id: resolvedDatasetId}, datasetHelp);
      const hasBackendRouting = hasBackendRoutingMetadata(datasetCatalogItem, datasetHelp);
      const hasAdministrativeRoutingSignal = hasBackendRouting && typeof datasetRouting?.isAdministrative === 'boolean';
      if ((isTerritorialOnly || !!normalizedExpectedAdminType) && !hasAdministrativeRoutingSignal) {
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            routing: datasetRouting,
            details:
              `Cannot enforce territorial/admin routing for dataset "${resolvedDatasetId}" because backend routing metadata is missing or incomplete. ` +
              'Call getQCumberDatasetHelp(providerId,datasetId) and retry with a dataset that exposes routing.isAdministrative.'
          }
        };
      }
      datasetIsTerritorial = hasAdministrativeRoutingSignal ? Boolean(datasetRouting?.isAdministrative) : false;
      if (isTerritorialOnly && !datasetIsTerritorial) {
        const preferredRetryTool = getRoutingPreferredQueryTool(datasetRouting);
        const retryTool =
          preferredRetryTool && preferredRetryTool !== 'queryQCumberTerritorialUnits'
            ? preferredRetryTool
            : 'queryQCumberDataset';
        const retryFilters = (Array.isArray(filters) ? filters : []).filter(
          filterItem => !isLikelyAdminLevelFieldName((filterItem as any)?.field)
        );
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            routing: datasetRouting,
            retryWithTool: retryTool,
            retryWithArgs: {
              providerId: resolvedProviderId,
              datasetId: resolvedDatasetId,
              filters: retryFilters.length ? retryFilters : undefined,
              orderBy,
              orderDirection,
              limit,
              offset,
              spatialBbox: normalizedSpatialBbox,
              loadToMap,
              showOnMap
            },
            retryReason: 'non-administrative-dataset',
            details:
              'This tool is reserved for territorial/admin-unit datasets. ' +
              `Metadata suggests "${resolvedDatasetId}" should use ${String(
                getRoutingPreferredQueryTool(datasetRouting) || 'queryQCumberDataset'
              )}.`
          }
        };
      }
      const spatialRequired = isSpatialOnly || (policyMode === 'auto' && !datasetIsTerritorial);
      let effectiveSpatialBbox = normalizedSpatialBbox;
      if (spatialRequired && !effectiveSpatialBbox) {
        if (resolvedProviderId === 'local-assets-it') {
          effectiveSpatialBbox = [...ITALY_DEFAULT_SPATIAL_BBOX];
          queryPolicyAdjustmentsNote +=
            ' spatialBbox auto-set to Italy extent for local-assets-it thematic query.';
        } else {
          queryPolicyAdjustmentsNote += ' spatialBbox not provided; running thematic query without bbox prefilter.';
        }
      }
      const datasetResolutionChanged =
        !!requestedDatasetId &&
        !!resolvedDatasetId &&
        requestedDatasetId.toLowerCase() !== resolvedDatasetId.toLowerCase();
      const datasetAutoSelected = !requestedDatasetId && datasetResolution.autoSelected && !!resolvedDatasetId;
      const datasetResolutionNote = datasetResolutionChanged
        ? ` Dataset resolved from "${requestedDatasetId}" to "${resolvedDatasetId}".`
        : datasetAutoSelected
        ? ` Dataset auto-selected as "${resolvedDatasetId}" for provider "${resolvedProviderId}".`
        : '';
      const adminTypeToLevel = resolveAdminTypeToLevelMap(datasetCatalogItem, datasetHelp, datasetRouting);
      const explicitExpectedLv = resolveExpectedAdminLevel(normalizedExpectedAdminType, adminTypeToLevel);
      if (normalizedExpectedAdminType && explicitExpectedLv === null) {
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            routing: datasetRouting,
            details:
              `Cannot resolve expectedAdminType "${normalizedExpectedAdminType}" for dataset "${resolvedDatasetId}" ` +
              'because adminTypeToLevel metadata is missing/incomplete. Add ai.profile.adminWorkflows.adminTypeToLevel in provider descriptor or pass explicit lv filter.'
          }
        };
      }
      let expectedLv = explicitExpectedLv;
      let normalizedFilters = (filters || []).map((filterItem: any) => {
        const normalizedOp = normalizeQcumberFilterOp(filterItem?.op);
        return {
          ...filterItem,
          op: normalizedOp
        };
      });
      const firstInvalidFilter = normalizedFilters.find((filterItem: any) => {
        const op = String(filterItem?.op || 'eq').toLowerCase();
        return !QCUMBER_FILTER_OPS.has(op);
      });
      if (firstInvalidFilter) {
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            details: `Unsupported filter operator "${String(
              firstInvalidFilter?.op || ''
            )}". Use attribute operators only; spatial constraints must be passed via spatialBbox.`
          }
        };
      }
      const suspiciousFieldValueFilter = normalizedFilters.find((filterItem: any) => {
        const op = String(filterItem?.op || 'eq').toLowerCase();
        if (op === 'eq') {
          return isFilterValueLikelyFieldName(filterItem?.field, filterItem?.value);
        }
        if (op === 'in') {
          const values = Array.isArray(filterItem?.values)
            ? filterItem.values
            : Array.isArray(filterItem?.value)
            ? filterItem.value
            : [filterItem?.value];
          return values.some((candidate: unknown) => isFilterValueLikelyFieldName(filterItem?.field, candidate));
        }
        return false;
      });
      if (suspiciousFieldValueFilter) {
        const badValue = Array.isArray(suspiciousFieldValueFilter?.values)
          ? suspiciousFieldValueFilter.values.find((candidate: unknown) =>
              isFilterValueLikelyFieldName(suspiciousFieldValueFilter?.field, candidate)
            )
          : suspiciousFieldValueFilter?.value;
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            datasetId: resolvedDatasetId,
            routing: datasetRouting,
            details:
              `Suspicious filter value "${String(badValue || '')}" for field "${String(
                suspiciousFieldValueFilter?.field || ''
              )}". ` +
              'The value looks like a field name. Use the actual parent-id value from a returned row (e.g. "037126"), not the field label.'
          }
        };
      }
      const requestedLimit = Number.isFinite(Number(limit))
        ? Math.max(1, Math.min(100000, Number(limit)))
        : loadToMap === false
        ? 1000
        : 50000;
      const requestedOffset = Number.isFinite(Number(offset)) ? Number(offset) : 0;
      const hasExplicitLoadToMap = typeof loadToMap === 'boolean';
      const hasExplicitShowOnMap = typeof showOnMap === 'boolean';
      const hasRequestedOrderBy = String(orderBy || '').trim().length > 0;
      const isRankingLikeQuery = hasRequestedOrderBy && requestedLimit <= 200 && normalizedFilters.length <= 1;
      const preserveRankingLimit = isRankingLikeQuery && Number.isFinite(Number(limit));
      // Keep off-map ranking scans by default only for non-territorial/thematic lookups.
      // Territorial ranking flows are often followed by map transforms (area/styling), so prefer loadToMap.
      const preferOffMapRanking = isRankingLikeQuery && !isTerritorialOnly && !normalizedExpectedAdminType;
      const shouldLoadToMap = hasExplicitLoadToMap ? loadToMap !== false : !preferOffMapRanking;
      const shouldShowOnMap = shouldLoadToMap ? (hasExplicitShowOnMap ? showOnMap === true : false) : false;
      if (hasExplicitShowOnMap && !shouldLoadToMap && showOnMap === true) {
        queryPolicyAdjustmentsNote += ' showOnMap=true ignored because loadToMap=false.';
      }
      const municipalWorkflow = isLikelyMunicipalFilter(normalizedFilters);
      const resolvedLimit =
        shouldLoadToMap && municipalWorkflow && !preserveRankingLimit ? Math.max(requestedLimit, 50000) : requestedLimit;
      // Keep full source schema for q-cumber queries.
      // Do not pass `select` to backend: kepler/frontend decides field filtering later.
      const resolvedSelect: string[] = [];
      const rankingFieldCandidates = collectMetadataRankingFieldCandidates(
        datasetRouting,
        datasetHelp,
        datasetCatalogItem
      );
      let effectiveOrderBy = String(orderBy || '').trim();
      let rankingFieldPreFixNote = '';
      if (preserveRankingLimit && isGeometryLikeRankingField(effectiveOrderBy)) {
        const metadataFallback = pickBestMetadataRankingCandidate(rankingFieldCandidates);
        if (metadataFallback) {
          const requestedOrderBy = String(orderBy || '').trim();
          effectiveOrderBy = metadataFallback;
          rankingFieldPreFixNote =
            ` Requested orderBy "${requestedOrderBy || '_auto_'}" was replaced with "${metadataFallback}"` +
            ' because the original field was not comparable for ranking.';
        }
      }

      const baseBody = {
        providerId: resolvedProviderId,
        datasetId: String(resolvedDatasetId || '').trim(),
        select: resolvedSelect.length ? resolvedSelect : undefined,
        orderBy: effectiveOrderBy || undefined,
        orderDirection: orderDirection || 'asc'
      };

      if (!baseBody.datasetId) {
        return {
          llmResult: {
            success: false,
            details: 'Missing datasetId.'
          }
        };
      }

      const shouldInferPoints = inferPointsFromLatLon === true || QCUMBER_INCLUDE_LATLON_FALLBACK_FIELDS;
      const queryCacheKey = stableSerializeForCache({
        cacheVersion: 1,
        policyMode,
        providerId: resolvedProviderId,
        datasetId: baseBody.datasetId,
        filters: normalizedFilters,
        orderBy: baseBody.orderBy || null,
        orderDirection: baseBody.orderDirection || 'asc',
        limit: resolvedLimit,
        offset: requestedOffset,
        spatialBbox: effectiveSpatialBbox || null,
        expectedAdminType: normalizedExpectedAdminType || null,
        loadToMap: shouldLoadToMap,
        showOnMap: shouldShowOnMap,
        inferPointsFromLatLon: shouldInferPoints,
        rankingMode: preserveRankingLimit
      });
      const cachedSuccessfulQuery = getCachedSuccessfulQcumberQuery(queryCacheKey);
      if (cachedSuccessfulQuery?.llmResult?.success) {
        return {
          llmResult: {
            ...cachedSuccessfulQuery.llmResult,
            details: `${String(cachedSuccessfulQuery.llmResult?.details || '').trim()} Reused cached successful query result for identical arguments.`
          }
        };
      }

      try {
        let filterAutoRewriteNote = '';
        let spatialRetryNote = '';
        let querySpatialBbox = effectiveSpatialBbox;
        const parentIdRetryCandidates = collectParentIdRetryFieldCandidates(
          datasetRouting,
          datasetHelp,
          datasetCatalogItem,
          normalizedFilters
        );
        const queryWithLimit = async (limitValue: number, orderByOverride?: string, offsetValue?: number) => {
          const body = {
            ...baseBody,
            spatialBbox: querySpatialBbox,
            filters: normalizedFilters.length ? normalizedFilters : undefined,
            orderBy: String(orderByOverride || '').trim() || baseBody.orderBy,
            limit: Math.max(1, Math.min(100000, Number(limitValue))),
            offset: Number.isFinite(Number(offsetValue)) ? Number(offsetValue) : requestedOffset
          };
          try {
            return await requestQcumberJson('/datasets/query', {
              method: 'POST',
              body: JSON.stringify(body)
            });
          } catch (queryError) {
            const canRetryGenericParent =
              hasGenericParentIdFilter(normalizedFilters) &&
              isMissingFilterFieldError(queryError, 'parent_id') &&
              parentIdRetryCandidates.length > 0;
            if (!canRetryGenericParent) {
              throw queryError;
            }
            for (const retryFieldName of parentIdRetryCandidates) {
              const retryFilters = rewriteGenericParentIdFilters(normalizedFilters, retryFieldName);
              if (!retryFilters.length) continue;
              const retryBody = {
                ...body,
                filters: retryFilters
              };
              try {
                const retryPayload = await requestQcumberJson('/datasets/query', {
                  method: 'POST',
                  body: JSON.stringify(retryBody)
                });
                normalizedFilters = retryFilters;
                filterAutoRewriteNote =
                  ` Parent filter auto-resolved: "parent_id" -> "${retryFieldName}" using backend routing metadata.`;
                return retryPayload;
              } catch (retryError) {
                if (isMissingFilterFieldError(retryError, retryFieldName)) {
                  continue;
                }
                throw retryError;
              }
            }
            throw queryError;
          }
        };
        const runPagedQueryWindow = async () => {
          let payload = await queryWithLimit(resolvedLimit);
          let rows = Array.isArray(payload?.rows) ? payload.rows : [];
          let fields = Array.isArray(payload?.fields) ? payload.fields : [];
          let returnedCount = Number(payload?.returned || rows.length || 0);
          let totalMatched = Number(payload?.totalMatched || rows.length || 0);
          let paginationNote = '';
          if (shouldLoadToMap && !preserveRankingLimit && totalMatched > returnedCount && resolvedLimit < 100000) {
            const retryLimit = Math.max(resolvedLimit, Math.min(100000, totalMatched));
            payload = await queryWithLimit(retryLimit);
            rows = Array.isArray(payload?.rows) ? payload.rows : [];
            fields = Array.isArray(payload?.fields) ? payload.fields : [];
            returnedCount = Number(payload?.returned || rows.length || 0);
            totalMatched = Number(payload?.totalMatched || rows.length || 0);
          }
          const canAutoPageFullWindow =
            shouldLoadToMap && !preserveRankingLimit && requestedOffset <= 0 && totalMatched > returnedCount;
          if (canAutoPageFullWindow) {
            const mergedRows = Array.isArray(rows) ? rows.slice() : [];
            let nextOffset = Number(returnedCount || mergedRows.length || 0);
            const pageLimit = 100000;
            while (nextOffset < totalMatched) {
              const remaining = Math.max(0, totalMatched - nextOffset);
              if (remaining <= 0) break;
              const pagePayload = await queryWithLimit(Math.min(pageLimit, remaining), undefined, nextOffset);
              const pageRows = Array.isArray(pagePayload?.rows) ? pagePayload.rows : [];
              if (!pageRows.length) break;
              mergedRows.push(...pageRows);
              nextOffset += pageRows.length;
              if (pageRows.length < Math.min(pageLimit, remaining)) {
                break;
              }
            }
            rows = mergedRows;
            returnedCount = rows.length;
            if (payload && typeof payload === 'object') {
              payload = {
                ...payload,
                rows,
                returned: returnedCount
              };
            }
            paginationNote =
              returnedCount >= totalMatched
                ? ` Loaded all ${totalMatched} matched rows across paged windows.`
                : ` Loaded ${returnedCount}/${totalMatched} rows across paged windows.`;
          }
          return {payload, rows, fields, returnedCount, totalMatched, paginationNote};
        };

        let {payload, rows, fields, returnedCount, totalMatched, paginationNote} = await runPagedQueryWindow();
        const isAutoSpatialBbox = !normalizedSpatialBbox && Array.isArray(effectiveSpatialBbox);
        const canRetryWithoutSpatialBbox =
          isAutoSpatialBbox &&
          shouldLoadToMap &&
          !preserveRankingLimit &&
          spatialRequired &&
          !datasetIsTerritorial &&
          !normalizedFilters.length &&
          requestedOffset <= 0 &&
          totalMatched <= 0;
        if (canRetryWithoutSpatialBbox) {
          querySpatialBbox = undefined;
          try {
            const noBboxResult = await runPagedQueryWindow();
            if (noBboxResult.totalMatched > 0) {
              payload = noBboxResult.payload;
              rows = noBboxResult.rows;
              fields = noBboxResult.fields;
              returnedCount = noBboxResult.returnedCount;
              totalMatched = noBboxResult.totalMatched;
              paginationNote = noBboxResult.paginationNote;
              spatialRetryNote =
                ` Spatial prefilter returned zero rows; retried without spatialBbox and recovered ${totalMatched} rows.`;
            } else {
              querySpatialBbox = effectiveSpatialBbox;
            }
          } catch {
            querySpatialBbox = effectiveSpatialBbox;
          }
        }

        const normalizeLevelString = (value: unknown) => String(value ?? '').trim();
        const rowsMatchExpectedLevel = (inputRows: any[], levelFieldName: string, expectedLevel: number) => {
          const list = Array.isArray(inputRows) ? inputRows : [];
          return list.every((row: any) => {
            if (!row || typeof row !== 'object' || Array.isArray(row)) return true;
            return valuesEqualAdminLevel((row as any)[levelFieldName], expectedLevel);
          });
        };
        const filtersIncludeExpectedLevel = (inputFilters: any[], levelFieldName: string, expectedLevel: number) => {
          const list = Array.isArray(inputFilters) ? inputFilters : [];
          return list.some((filter: any) => {
            const field = String(filter?.field || '').trim();
            const op = String(filter?.op || 'eq').toLowerCase();
            if (field !== levelFieldName) return false;
            if (op === 'eq') {
              return valuesEqualAdminLevel(filter?.value, expectedLevel);
            }
            if (op === 'in') {
              const values = Array.isArray(filter?.values)
                ? filter.values
                : Array.isArray(filter?.value)
                ? filter.value
                : [filter?.value];
              return values.some((value: unknown) => valuesEqualAdminLevel(value, expectedLevel));
            }
            return false;
          });
        };

        let adminMetadata = inferQcumberAdminMetadata(fields, rows);
        const exactNameFilterValues = extractExactNameFilterValues(normalizedFilters);
        // Preserve explicit administrative intent (e.g. province -> lv=7).
        // Inference from sampled levels can be misleading on small/partial result sets.
        if (explicitExpectedLv === null) {
          expectedLv = inferExpectedAdminLevelFromAvailableLevels(
            normalizedExpectedAdminType,
            adminTypeToLevel,
            adminMetadata?.levelCounts,
            expectedLv
          );
        }

        const sampledLevels = Object.keys(adminMetadata?.levelCounts || {})
          .map(value => String(value || '').trim())
          .filter(Boolean);
        if (!normalizedExpectedAdminType && exactNameFilterValues.length > 0 && sampledLevels.length > 1) {
          return {
            llmResult: {
              success: false,
              providerId: resolvedProviderId,
              datasetId: baseBody.datasetId,
              clarificationRequired: true,
              clarificationQuestion:
                `Il toponimo ${exactNameFilterValues.join(', ')} esiste su più livelli amministrativi (${sampledLevels.join(', ')}). ` +
                'Vuoi provincia, comune, regione o stato?',
              clarificationOptions: ['province', 'municipality', 'region', 'country'],
              details:
                `Ambiguous administrative match for name filter (${exactNameFilterValues.join(', ')}). ` +
                `Matched multiple levels (${sampledLevels.join(', ')}). ` +
                'Retry with expectedAdminType (province/municipality/region/country) or add explicit lv filter.'
            }
          };
        }
        if (expectedLv !== null) {
          let levelFieldName = String(adminMetadata?.levelField || '').trim();
          if (!levelFieldName) {
            const strictLvBody = {
              ...baseBody,
              // Remove select for this internal guardrail retry to expose all fields.
              select: undefined,
              filters: [...normalizedFilters, {field: 'lv', op: 'eq', value: expectedLv}],
              limit: Math.max(1, Math.min(100000, Number(resolvedLimit))),
              offset: Number.isFinite(Number(offset)) ? Number(offset) : 0
            };
            payload = await requestQcumberJson('/datasets/query', {
              method: 'POST',
              body: JSON.stringify(strictLvBody)
            });
            rows = Array.isArray(payload?.rows) ? payload.rows : [];
            fields = Array.isArray(payload?.fields) ? payload.fields : [];
            returnedCount = Number(payload?.returned || rows.length || 0);
            totalMatched = Number(payload?.totalMatched || rows.length || 0);
            adminMetadata = inferQcumberAdminMetadata(fields, rows);
            levelFieldName = String(adminMetadata?.levelField || '').trim();
          }
          if (!levelFieldName) {
            return {
              llmResult: {
                success: false,
                providerId: resolvedProviderId,
                datasetId: baseBody.datasetId,
                details:
                  `Cannot enforce expected administrative type "${normalizedExpectedAdminType}" because no level field was found in dataset rows.`
              }
            };
          }

          const expectedLevelLabel = String(expectedLv);
          const availableLevels = Object.keys(adminMetadata?.levelCounts || {}).map(normalizeLevelString);
          const allRowsMatch = rowsMatchExpectedLevel(rows, levelFieldName, expectedLv);
          const alreadyConstrained = filtersIncludeExpectedLevel(normalizedFilters, levelFieldName, expectedLv);

          if (!allRowsMatch && !alreadyConstrained) {
            const strictFilters = [...normalizedFilters, {field: levelFieldName, op: 'eq', value: expectedLv}];
            const strictBody = {
              ...baseBody,
              filters: strictFilters,
              limit: Math.max(1, Math.min(100000, Number(resolvedLimit))),
              offset: Number.isFinite(Number(offset)) ? Number(offset) : 0
            };
            payload = await requestQcumberJson('/datasets/query', {
              method: 'POST',
              body: JSON.stringify(strictBody)
            });
            rows = Array.isArray(payload?.rows) ? payload.rows : [];
            fields = Array.isArray(payload?.fields) ? payload.fields : [];
            returnedCount = Number(payload?.returned || rows.length || 0);
            totalMatched = Number(payload?.totalMatched || rows.length || 0);
            adminMetadata = inferQcumberAdminMetadata(fields, rows);
          } else if (!allRowsMatch && alreadyConstrained) {
            return {
              llmResult: {
                success: false,
                providerId: resolvedProviderId,
                datasetId: baseBody.datasetId,
                details:
                  `Administrative level mismatch: expected ${normalizedExpectedAdminType} (lv=${expectedLevelLabel}), ` +
                  `but result rows include other levels in field "${levelFieldName}".`
              }
            };
          }

          const postRowsMatch = rowsMatchExpectedLevel(rows, levelFieldName, expectedLv);
          if (!postRowsMatch) {
            return {
              llmResult: {
                success: false,
                providerId: resolvedProviderId,
                datasetId: baseBody.datasetId,
                details:
                  `Administrative level mismatch after strict filtering: expected ${normalizedExpectedAdminType} (lv=${expectedLevelLabel}) on field "${levelFieldName}".`
              }
            };
          }

          if (rows.length === 0 && availableLevels.length && !availableLevels.includes(expectedLevelLabel)) {
            return {
              llmResult: {
                success: false,
                providerId: resolvedProviderId,
                datasetId: baseBody.datasetId,
                details:
                  `Expected administrative type "${normalizedExpectedAdminType}" (lv=${expectedLevelLabel}) not found. ` +
                  `Available sampled levels: ${availableLevels.join(', ')}.`
              }
            };
          }
        }

        const executionKey = `qcumber-query-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const hasUserFilters = normalizedFilters.length > 0;
        const consistency = verifyRowsAgainstFilters(rows, normalizedFilters);
        if (hasUserFilters && !consistency.ok) {
          return {
            llmResult: {
              success: false,
              providerId: resolvedProviderId,
              datasetId: baseBody.datasetId,
              details:
                `Query returned rows not consistent with requested filters (${consistency.failedFilter || 'filter mismatch'}). ` +
                'Result not loaded to map. Refine filters and retry.'
            }
          };
        }
        if (hasUserFilters && totalMatched <= 0) {
          return {
            llmResult: {
              success: true,
              providerId: resolvedProviderId,
              datasetId: baseBody.datasetId,
              returned: 0,
              totalMatched: 0,
              fields,
              h3Metadata: inferQcumberH3Metadata(fields, []),
              rows: [],
              details: `Query completed for dataset ${baseBody.datasetId} with zero matches. No dataset loaded to map.`
            }
          };
        }
        let rankingMetricField = String(effectiveOrderBy || '').trim();
        let rankingFieldAutoFixNote = '';
        if (preserveRankingLimit) {
          let rankingMetricValidation = validateRankingMetric(rows, fields, rankingMetricField);
          if (!rankingMetricValidation.ok) {
            const fallbackOrderBy = resolveFallbackRankingOrderBy(rows, fields, [
              ...rankingFieldCandidates,
              ...['population', 'area_m2', 'gid', 'id', 'name']
            ]);
            const requestedOrderBy = String(orderBy || '').trim();
            const canRetryWithFallback =
              !!fallbackOrderBy && fallbackOrderBy.toLowerCase() !== String(rankingMetricField || '').toLowerCase();
            if (canRetryWithFallback) {
              payload = await queryWithLimit(resolvedLimit, fallbackOrderBy);
              rows = Array.isArray(payload?.rows) ? payload.rows : [];
              fields = Array.isArray(payload?.fields) ? payload.fields : [];
              returnedCount = Number(payload?.returned || rows.length || 0);
              totalMatched = Number(payload?.totalMatched || rows.length || 0);
              rankingMetricValidation = validateRankingMetric(rows, fields, fallbackOrderBy);
              if (rankingMetricValidation.ok) {
                rankingMetricField = rankingMetricValidation.metricField;
                rankingFieldAutoFixNote =
                  ` Requested orderBy "${requestedOrderBy || '_auto_'}" was replaced with "${rankingMetricField}"` +
                  ' because the original field was not comparable for ranking.';
              }
            }
            if (!rankingMetricValidation.ok) {
              return {
                llmResult: {
                  success: false,
                  providerId: resolvedProviderId,
                  requestedDatasetId: requestedDatasetId || undefined,
                  datasetId: baseBody.datasetId,
                  routing: datasetRouting,
                  details:
                    `Ranking query guardrail: ${rankingMetricValidation.reason || 'invalid orderBy metric'}.` +
                    `${providerResolutionNote}${datasetResolutionNote}`
                }
              };
            }
          }
          rankingMetricField = rankingMetricValidation.metricField;
        }
        const h3Metadata = inferQcumberH3Metadata(fields, rows);

        const rawMapDataset = shouldLoadToMap ? rebuildQcumberMapDatasetRows(payload?.dataset, rows, fields) : null;
        const mapDataset =
          shouldLoadToMap && shouldInferPoints ? ensureGeojsonPointsFromLatLon(rawMapDataset) : rawMapDataset;
        const canRenderGeometryLayer = shouldLoadToMap ? datasetHasRenderableGeometry(mapDataset) : false;
        const forceTableOnlyForLargeThematicGeometry =
          canRenderGeometryLayer &&
          !datasetIsTerritorial &&
          returnedCount > QCUMBER_MAX_AUTO_LAYER_GEOMETRY_ROWS;
        const autoCreateLayers =
          shouldShowOnMap && canRenderGeometryLayer && !forceTableOnlyForLargeThematicGeometry;
        const autoLayerDisabledReason = !shouldShowOnMap
          ? 'showOnMap=false'
          : forceTableOnlyForLargeThematicGeometry
          ? 'large geometry policy'
          : 'no renderable geometry';
        const autoLayerPolicyNote = forceTableOnlyForLargeThematicGeometry
          ? ` Auto-layer disabled for large thematic geometry dataset (${returnedCount} rows > ${QCUMBER_MAX_AUTO_LAYER_GEOMETRY_ROWS}) to avoid UI freeze.`
          : !shouldShowOnMap
          ? ' Auto-layer disabled (showOnMap=false).'
          : '';
        const loadedIdentity = shouldLoadToMap
          ? deriveDatasetIdentity(mapDataset, executionKey)
          : {id: '', label: '', ref: ''};
        const previewRowsLimit = preserveRankingLimit ? Math.min(20, Math.max(1, requestedLimit)) : 8;
        const rankingSummary = preserveRankingLimit
          ? ` Ranking preserved with limit=${requestedLimit}, ordered by ${rankingMetricField || 'field'} ${orderDirection || 'asc'}.${rankingFieldPreFixNote}${rankingFieldAutoFixNote}`
          : '';
        const rankingPreview = preserveRankingLimit
          ? buildRankingPreviewDetails(rows, rankingMetricField, orderDirection, requestedLimit)
          : '';
        const offMapGuidance = shouldLoadToMap
          ? ''
          : ' Off-map query only: no dataset was created in current map state. Do not call waitForQMapDataset/countQMapRows; answer directly from returned rows. If you need map transforms (e.g. createDatasetWithGeometryArea), rerun the same query with loadToMap=true.';

        const queryResult = {
          llmResult: {
            success: true,
            providerId: resolvedProviderId,
            requestedDatasetId: requestedDatasetId || undefined,
            datasetId: baseBody.datasetId,
            loadedDatasetId: loadedIdentity.id || undefined,
            loadedDatasetName: loadedIdentity.label || undefined,
            loadedDatasetRef: loadedIdentity.ref || undefined,
            returned: returnedCount,
            totalMatched,
            fields,
            h3Metadata,
            adminMetadata,
            adminTypeToLevel,
            routing: datasetRouting,
            expectedAdminType: normalizedExpectedAdminType || undefined,
            rows: compactQcumberPreviewRows(rows, previewRowsLimit),
            loadedToMap: shouldLoadToMap,
            showOnMap: shouldLoadToMap ? shouldShowOnMap : false,
            details: shouldLoadToMap
              ? autoCreateLayers
                ? `Query completed and loaded dataset ${baseBody.datasetId} into map as "${loadedIdentity.label}" (${loadedIdentity.ref}).${providerResolutionNote}${datasetResolutionNote}${queryPolicyAdjustmentsNote}${filterAutoRewriteNote}${spatialRetryNote}${rankingSummary}${rankingPreview}${paginationNote}`
                : `Query completed and loaded dataset ${baseBody.datasetId} as table "${loadedIdentity.label}" (${loadedIdentity.ref}) without auto layer (${autoLayerDisabledReason}).${providerResolutionNote}${datasetResolutionNote}${queryPolicyAdjustmentsNote}${filterAutoRewriteNote}${spatialRetryNote}${rankingSummary}${rankingPreview}${paginationNote}${autoLayerPolicyNote}`
              : `Query completed for dataset ${baseBody.datasetId}.${providerResolutionNote}${datasetResolutionNote}${queryPolicyAdjustmentsNote}${filterAutoRewriteNote}${spatialRetryNote}${rankingSummary}${rankingPreview}${offMapGuidance}`
          },
          additionalData: shouldLoadToMap
            ? {
                dataset: mapDataset,
                autoCreateLayers,
                executionKey
              }
            : undefined
        };
        setCachedSuccessfulQcumberQuery(queryCacheKey, queryResult.llmResult);
        return queryResult;
      } catch (error: any) {
        return {
          llmResult: {
            success: false,
            providerId: resolvedProviderId,
            requestedDatasetId: requestedDatasetId || undefined,
            datasetId: baseBody.datasetId,
            routing: datasetRouting,
            details: `Query failed for dataset ${baseBody.datasetId}: ${String(
              error?.message || error || 'Unknown error'
            )}.${providerResolutionNote}${datasetResolutionNote}`
          }
        };
      }
    },
    component: QueryQCumberDatasetComponent
  });
}

export function LoadQMapCloudMapComponent({provider, loadParams}: {provider: string; loadParams: any}) {
  const dispatch = useDispatch();

  useEffect(() => {
    const providerInstance = createCloudStorageProvider(provider);
    dispatch(
      wrapTo(
        'map',
        loadCloudMap({
          provider: providerInstance as any,
          loadParams
        }) as any
      )
    );
  }, [dispatch, provider, loadParams]);

  return null;
}

export function QueryQCumberDatasetComponent({
  dataset,
  executionKey,
  autoCreateLayers
}: {
  dataset: any;
  executionKey?: string;
  autoCreateLayers?: boolean;
}) {
  const dispatch = useDispatch();

  useEffect(() => {
    if (!dataset) {
      return;
    }
    if (executionKey && EXECUTED_QCUMBER_QUERY_KEYS.has(executionKey)) {
      return;
    }
    if (executionKey) {
      rememberBoundedSetValue(EXECUTED_QCUMBER_QUERY_KEYS, executionKey, EXECUTED_QCUMBER_QUERY_KEYS_MAX_SIZE);
    }
    const mapDataset = withUniqueMapDatasetIdentity(dataset, executionKey);

    dispatch(
      wrapTo(
        'map',
        addDataToMap({
          datasets: mapDataset,
          options: {
            // Query results can change schema between calls; avoid reusing stale layer config.
            keepExistingConfig: false,
            centerMap: false,
            autoCreateLayers: autoCreateLayers !== false
          }
        }) as any
      )
    );
  }, [dispatch, dataset, executionKey, autoCreateLayers]);

  return null;
}

export function getQMapCloudTools(apiBaseUrl?: string) {
  const resolvedBase = (apiBaseUrl || DEFAULT_ASSISTANT_BASE).replace(/\/+$/, '');
  return {
    listQMapCloudMaps: createListQMapCloudMapsTool(resolvedBase),
    loadQMapCloudMap: createLoadQMapCloudMapTool(resolvedBase),
    listQCumberProviders: createListQCumberProvidersTool(),
    listQCumberDatasets: createListQCumberDatasetsTool(),
    getQCumberDatasetHelp: createGetQCumberDatasetHelpTool(),
    queryQCumberDataset: createQueryQCumberDatasetTool('auto'),
    queryQCumberTerritorialUnits: createQueryQCumberDatasetTool('territorial'),
    queryQCumberDatasetSpatial: createQueryQCumberDatasetTool('thematic_spatial')
  };
}
