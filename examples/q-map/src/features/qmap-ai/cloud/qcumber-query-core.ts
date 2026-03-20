/**
 * Core q-cumber query execution logic: HTTP dispatch, filter building, spatial bbox handling,
 * result normalization, ranking validation, admin-level enforcement, and auto-paging.
 */
import {
  qcumberListProviders,
  qcumberListDatasets,
  qcumberGetDatasetHelp,
  qcumberQuery
} from '../services/qcumber-api';
import {normalizeDatasetToken} from './qcumber-dataset-identity';
import {normalizeQcumberEnumToken, normalizeQcumberFilterOp, QCUMBER_FILTER_OPS} from './qcumber-schemas';
import {QCUMBER_INCLUDE_LATLON_FALLBACK_FIELDS} from './constants';
import type {CanonicalAdminType} from './qcumber-catalog';

/**
 * Route q-cumber requests through the q-assistant backend proxy.
 * Replaces the previous direct-to-q-cumber fetch with auth token handling.
 */
export async function requestQcumberJson(path: string, init: RequestInit = {}) {
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

export async function requestQcumberDatasetHelp(providerId: string, datasetId: string): Promise<any> {
  return requestQcumberJson(
    `/providers/${encodeURIComponent(String(providerId || '').trim())}/datasets/${encodeURIComponent(
      String(datasetId || '').trim()
    )}/help`
  );
}

// --- Filter helpers ---

export function isFilterValueLikelyFieldName(fieldName: unknown, value: unknown): boolean {
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

// --- Compact preview rows ---

export function compactQcumberPreviewRows(rows: any[], maxRows = 8): any[] {
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

// --- Ranking helpers ---

export function buildRankingPreviewDetails(
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

export function validateRankingMetric(rows: any[], fields: any[], orderBy: unknown): {
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

export function isGeometryLikeRankingField(fieldName: unknown): boolean {
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

export function collectMetadataRankingFieldCandidates(datasetRouting: any, datasetHelp: any, datasetCatalogItem: any): string[] {
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

  const metricProfile = normalizeMetricProfileInternal(datasetRouting?.metricProfile || datasetHelp?.routing?.metricProfile);
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

// Internal normalizeMetricProfile for use in ranking candidate collection (avoids circular import).
function normalizeMetricProfileInternal(metricProfile: any): any | null {
  if (!metricProfile || typeof metricProfile !== 'object') return null;
  return metricProfile;
}

export function pickBestMetadataRankingCandidate(candidates: string[]): string {
  const normalized = (Array.isArray(candidates) ? candidates : [])
    .map(value => String(value || '').trim())
    .filter(Boolean);
  if (!normalized.length) return '';
  const nonId = normalized.find(value => !isGeometryLikeRankingField(value) && !isLikelyIdentifierRankingField(value));
  if (nonId) return nonId;
  return normalized.find(value => !isGeometryLikeRankingField(value)) || '';
}

export function resolveFallbackRankingOrderBy(
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

// --- H3 and admin metadata inference ---

export function inferQcumberH3Metadata(fields: any[], rows: any[]) {
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

export function inferQcumberAdminMetadata(fields: any[], rows: any[]) {
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

// --- Field token normalization ---

export function normalizeFieldToken(value: string): string {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

// --- Name-like and admin-level field detection ---

export function isNameLikeField(fieldName: unknown): boolean {
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

export function extractExactNameFilterValues(filters: any[]): string[] {
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

// --- Admin level comparison ---

export function valuesEqualAdminLevel(raw: unknown, expected: number): boolean {
  const num = Number(raw);
  if (Number.isFinite(num)) return Math.trunc(num) === expected;
  return String(raw || '').trim() === String(expected);
}

// --- Filter/row matching ---

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

export function isLikelyMunicipalFilter(filters: any[]): boolean {
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

// --- Parent ID filter helpers ---

function isGenericParentIdFieldName(fieldName: unknown): boolean {
  const normalized = normalizeFieldToken(String(fieldName || ''));
  return normalized === 'parent id' || normalized === 'parentid';
}

export function hasGenericParentIdFilter(filters: any[]): boolean {
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

export function collectParentIdRetryFieldCandidates(
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

export function rewriteGenericParentIdFilters(filters: any[], targetFieldName: string): any[] {
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

export function isMissingFilterFieldError(error: unknown, fieldName: string): boolean {
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

export function isLikelyAdminLevelFieldName(fieldName: unknown): boolean {
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

export function verifyRowsAgainstFilters(rows: any[], filters: any[]): {ok: boolean; failedFilter?: string} {
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

export function datasetHasRenderableGeometry(datasetPayload: any): boolean {
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

// --- Locale number parsing ---

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

// --- Spatial bbox ---

export function normalizeSpatialBboxInput(value: unknown): number[] | undefined {
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

// --- Geometry field enrichment ---

export function withGeometryFieldsForMapLoad(selectFields: string[]): string[] {
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

export function withAdminLevelFieldsForValidation(selectFields: string[]): string[] {
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

export function ensureGeojsonPointsFromLatLon(datasetPayload: any): any {
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
