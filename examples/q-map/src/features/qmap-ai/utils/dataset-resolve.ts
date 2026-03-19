import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {getPolygonsFromGeometry, parseGeoJsonLike, type PolygonCoords} from '../../../geo';
import {
  h3CellToPolygonFeature as h3CellToPolygonFeatureCore,
  normalizeH3Key as normalizeH3KeyCore
} from '../../../workers/h3-geometry-utils';
import {createCloudStorageProvider} from '../cloud-tools';
import {getToolResultSummary} from '../services/execution-tracking';

export function normalizeDatasetLookupToken(value: unknown): string {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/^id:\s*/i, '')
    .replace(/^[\"']+|[\"']+$/g, '')
    .replace(/\.(geojson|json|csv|parquet)$/gi, '')
    .replace(/[\s-]+/g, '_')
    .replace(/_+/g, '_')
    .trim();
}

export function normalizeCanonicalDatasetRef(value: unknown): string {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (/^id:/i.test(raw)) {
    const idValue = raw.replace(/^id:\s*/i, '').trim();
    return idValue ? `id:${idValue}` : '';
  }
  return '';
}

export function registerDatasetLineageAlias(lineage: Map<string, string>, alias: unknown, canonicalRef: string) {
  const key = normalizeDatasetLookupToken(alias);
  if (!key) return;
  lineage.set(key, canonicalRef);
}

export function syncDatasetLineageFromCurrentDatasets(lineage: Map<string, string>, datasets: Record<string, unknown>) {
  Object.values(datasets || {}).forEach((dataset: any) => {
    const datasetId = String(dataset?.id || '').trim();
    if (!datasetId) return;
    const canonicalRef = `id:${datasetId}`;
    registerDatasetLineageAlias(lineage, canonicalRef, canonicalRef);
    registerDatasetLineageAlias(lineage, datasetId, canonicalRef);
    registerDatasetLineageAlias(lineage, dataset?.label || '', canonicalRef);
  });
}

export function resolveCanonicalDatasetRefWithLineage(
  lineage: Map<string, string>,
  datasets: Record<string, unknown>,
  datasetCandidate: unknown
): string {
  const rawCandidate = String(datasetCandidate || '').trim();
  if (!rawCandidate) return '';
  const canonicalFromRaw = normalizeCanonicalDatasetRef(rawCandidate);
  const lineageKey = normalizeDatasetLookupToken(rawCandidate);
  if (lineageKey && lineage.has(lineageKey)) {
    return String(lineage.get(lineageKey) || '').trim();
  }
  const resolvedDataset = resolveDatasetByName(datasets || {}, rawCandidate);
  const resolvedDatasetId = String(resolvedDataset?.id || '').trim();
  if (resolvedDatasetId) {
    const canonicalRef = `id:${resolvedDatasetId}`;
    registerDatasetLineageAlias(lineage, rawCandidate, canonicalRef);
    registerDatasetLineageAlias(lineage, resolvedDataset?.label || '', canonicalRef);
    registerDatasetLineageAlias(lineage, resolvedDatasetId, canonicalRef);
    registerDatasetLineageAlias(lineage, canonicalRef, canonicalRef);
    return canonicalRef;
  }
  if (canonicalFromRaw) {
    registerDatasetLineageAlias(lineage, rawCandidate, canonicalFromRaw);
    registerDatasetLineageAlias(lineage, canonicalFromRaw, canonicalFromRaw);
    return canonicalFromRaw;
  }
  return '';
}

export function extractProducedDatasetRefsFromNormalizedResult(result: Record<string, unknown>): string[] {
  const summary = getToolResultSummary(result) || {};
  const summaryRefs = Array.isArray((summary as any)?.producedDatasetRefs)
    ? ((summary as any).producedDatasetRefs as unknown[]).map(value => String(value || '').trim())
    : [];
  const llmResult =
    result?.llmResult && typeof result.llmResult === 'object' && !Array.isArray(result.llmResult)
      ? (result.llmResult as Record<string, unknown>)
      : {};
  return dedupeNonEmpty([...summaryRefs, ...extractProducedDatasetRefs(result, llmResult)]);
}

export function updateDatasetLineageFromToolResult(
  lineage: Map<string, string>,
  datasets: Record<string, unknown>,
  normalizedResult: Record<string, unknown>
) {
  const producedRefs = extractProducedDatasetRefsFromNormalizedResult(normalizedResult);
  if (!producedRefs.length) return;

  const canonicalRefs = dedupeNonEmpty(
    producedRefs
      .map(ref => resolveCanonicalDatasetRefWithLineage(lineage, datasets, ref))
      .filter(ref => Boolean(ref) && /^id:/i.test(ref))
  );
  canonicalRefs.forEach(ref => registerDatasetLineageAlias(lineage, ref, ref));

  if (canonicalRefs.length === 1) {
    const canonicalRef = canonicalRefs[0];
    producedRefs.forEach(ref => registerDatasetLineageAlias(lineage, ref, canonicalRef));
  }
}

export function findDatasetCandidatesByName(datasets: any, datasetName: string): any[] {
  const rawNeedle = String(datasetName || '').trim();
  if (!rawNeedle) return [];
  const entries = Object.values(datasets || {}) as any[];
  if (!entries.length) return [];

  const needle = rawNeedle.toLowerCase();
  const idNeedle = needle.startsWith('id:') ? needle.slice(3).trim() : needle;
  const needleNorm = normalizeDatasetLookupToken(rawNeedle);

  const exact = entries.filter((d: any) => {
    const label = String(d?.label || '').toLowerCase();
    const id = String(d?.id || '').toLowerCase();
    return label === needle || id === needle || id === idNeedle;
  });
  if (exact.length) return exact;

  const normalizedExact = entries.filter((d: any) => {
    const labelNorm = normalizeDatasetLookupToken(d?.label || '');
    const idNorm = normalizeDatasetLookupToken(d?.id || '');
    return Boolean(needleNorm) && (labelNorm === needleNorm || idNorm === needleNorm);
  });
  if (normalizedExact.length) return normalizedExact;

  const normalizedStartsWith = entries.filter((d: any) => {
    const labelNorm = normalizeDatasetLookupToken(d?.label || '');
    return Boolean(needleNorm) && labelNorm.startsWith(needleNorm);
  });
  if (normalizedStartsWith.length) return normalizedStartsWith;

  const normalizedContains = entries.filter((d: any) => {
    const labelNorm = normalizeDatasetLookupToken(d?.label || '');
    return Boolean(needleNorm) && labelNorm.includes(needleNorm);
  });
  if (normalizedContains.length) return normalizedContains;

  return [];
}

export function normalizeToolDetails(value: unknown): string {
  if (typeof value !== 'string') return '';
  const text = value.trim();
  return text;
}

export function normalizeMessageList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value
      .map(item => normalizeToolDetails(item))
      .filter(Boolean);
  }
  const single = normalizeToolDetails(value);
  return single ? [single] : [];
}

export function toCanonicalDatasetRef(value: unknown, asId = false): string {
  const raw = normalizeToolDetails(value);
  if (!raw) return '';
  if (/^id:/i.test(raw)) return `id:${raw.replace(/^id:\s*/i, '').trim()}`;
  if (asId) return `id:${raw}`;
  return raw;
}

export function dedupeNonEmpty(values: string[]): string[] {
  return Array.from(
    new Set(
      values
        .map(value => normalizeToolDetails(value))
        .filter(Boolean)
    )
  );
}

export function extractProducedDatasetRefs(base: Record<string, unknown>, llmResult: Record<string, unknown>): string[] {
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

// ─── Section C: color utilities ───────────────────────────────────────────────

export const SAFE_COLOR_RANGE = {
  name: 'qmap.safeColorRange',
  type: 'custom',
  category: 'Custom',
  colors: ['#f8fafc', '#cbd5e1', '#94a3b8', '#64748b', '#475569', '#334155']
};

export function getQMapProvider(rawProvider?: string) {
  return createCloudStorageProvider(rawProvider);
}

export function resolveDatasetByName(datasets: any, datasetName: string) {
  const candidates = findDatasetCandidatesByName(datasets, datasetName);
  return candidates.length ? (candidates[0] as any) : null;
}

export function extractPolygonsFromGeoJsonLike(value: unknown): PolygonCoords[] {
  const parsed = parseGeoJsonLike(value);
  if (!parsed) return [];
  if (parsed?.type === 'FeatureCollection' && Array.isArray(parsed.features)) {
    const out: PolygonCoords[] = [];
    parsed.features.forEach((feature: any) => {
      const geometry = feature?.geometry || null;
      const polygons = getPolygonsFromGeometry(geometry);
      polygons.forEach(polygon => out.push(polygon));
    });
    return out;
  }
  const geometry = parsed?.type === 'Feature' ? parsed.geometry : parsed;
  return getPolygonsFromGeometry(geometry);
}

export function toComparable(value: unknown): unknown {
  if (typeof value === 'string') {
    return value.toLowerCase();
  }
  return value;
}

export function evaluateFilter(operator: string, left: unknown, right: unknown): boolean {
  const op = String(operator || 'eq').toLowerCase();
  const l = toComparable(left);
  const r = toComparable(right);

  if (op === 'eq') return l === r;
  if (op === 'neq') return l !== r;
  if (op === 'gt') return Number(left) > Number(right);
  if (op === 'gte') return Number(left) >= Number(right);
  if (op === 'lt') return Number(left) < Number(right);
  if (op === 'lte') return Number(left) <= Number(right);
  if (op === 'contains') return String(left ?? '').toLowerCase().includes(String(right ?? '').toLowerCase());
  if (op === 'startswith') return String(left ?? '').toLowerCase().startsWith(String(right ?? '').toLowerCase());
  if (op === 'endswith') return String(left ?? '').toLowerCase().endsWith(String(right ?? '').toLowerCase());
  if (op === 'in') {
    const values = Array.isArray(right) ? right : [right];
    return values.map(toComparable).includes(l);
  }
  return l === r;
}

export function filterTargetsDataset(filter: any, datasetId: string): boolean {
  if (Array.isArray(filter?.dataId)) {
    return filter.dataId.includes(datasetId);
  }
  return String(filter?.dataId || '') === String(datasetId || '');
}
export function resolveGeojsonFieldName(dataset: any, requestedFieldName?: string | null): string | null {
  if (requestedFieldName) {
    const resolved = resolveDatasetFieldName(dataset, requestedFieldName);
    if (resolved) return resolved;
  }
  const typed = (dataset?.fields || []).find((f: any) => String(f?.type || '').toLowerCase() === 'geojson');
  return typed?.name ? String(typed.name) : null;
}

export function getFilteredDatasetIndexes(dataset: any, visState: any, useActiveFilters: boolean): number[] {
  const baseIdx = getDatasetIndexes(dataset);
  if (!useActiveFilters) return baseIdx;
  const filters = (visState?.filters || []).filter((f: any) => filterTargetsDataset(f, dataset.id));
  if (!filters.length) return baseIdx;
  return baseIdx.filter((rowIdx: number) => {
    return filters.every((filter: any) => {
      const rawFieldName = Array.isArray(filter?.name) ? filter.name[0] : filter?.name;
      if (!rawFieldName) return true;
      const resolvedFilterField = resolveDatasetFieldName(dataset, String(rawFieldName));
      if (!resolvedFilterField) return true;
      const rowValue = dataset.getValue(resolvedFilterField, rowIdx);
      const filterValue = filter?.value;
      if (Array.isArray(filterValue) && filterValue.length === 2 && filter?.type !== 'multiSelect') {
        const minV = filterValue[0];
        const maxV = filterValue[1];
        return Number(rowValue) >= Number(minV) && Number(rowValue) <= Number(maxV);
      }
      if (Array.isArray(filterValue)) {
        return filterValue.map(toComparable).includes(toComparable(rowValue));
      }
      return toComparable(rowValue) === toComparable(filterValue);
    });
  });
}

export function resolveDatasetFieldName(dataset: any, requestedFieldName: string): string | null {
  const fields = Array.isArray(dataset?.fields) ? dataset.fields : [];
  const requested = String(requestedFieldName || '').trim();
  if (!requested) return null;
  const requestedLower = requested.toLowerCase();
  const requestedNorm = requestedLower.replace(/[^a-z0-9]/g, '');

  const exact = fields.find((f: any) => String(f?.name || '').toLowerCase() === requestedLower);
  if (exact?.name) return String(exact.name);

  const bySuffix = fields.find((f: any) => {
    const n = String(f?.name || '').toLowerCase();
    return n.endsWith(`__${requestedLower}`) || n.endsWith(`_${requestedLower}`);
  });
  if (bySuffix?.name) return String(bySuffix.name);

  const byNormalized = fields.find((f: any) => {
    const n = String(f?.name || '').toLowerCase().replace(/[^a-z0-9]/g, '');
    return n === requestedNorm || n.endsWith(requestedNorm);
  });
  if (byNormalized?.name) return String(byNormalized.name);

  if (requestedLower === 'name') {
    const nameLike = fields.find((f: any) => /(^|[_-])name($|[_-])/.test(String(f?.name || '').toLowerCase()));
    if (nameLike?.name) return String(nameLike.name);
  }

  return null;
}

export function resolveH3FieldName(dataset: any, requestedFieldName?: string | null): string | null {
  if (requestedFieldName) {
    const resolved = resolveDatasetFieldName(dataset, requestedFieldName);
    if (resolved) {
      return resolved;
    }
  }
  const fields = Array.isArray(dataset?.fields) ? dataset.fields : [];
  const typed = fields.find((f: any) => String(f?.type || '').toLowerCase() === 'h3');
  if (typed?.name) return String(typed.name);
  return (
    resolveDatasetFieldName(dataset, 'h3_id') ||
    resolveDatasetFieldName(dataset, 'h3__id') ||
    null
  );
}

export function normalizeH3Key(value: unknown): string {
  return normalizeH3KeyCore(value);
}

export function h3CellToPolygonFeature(h3IdRaw: unknown): any | null {
  return h3CellToPolygonFeatureCore(h3IdRaw);
}

export function getDatasetIndexes(dataset: any): number[] {
  return Array.isArray(dataset?.allIndexes)
    ? dataset.allIndexes
    : Array.from({length: Number(dataset?.length || 0)}, (_, i) => i);
}

export function getDatasetFieldNames(dataset: any, limit = 256): string[] {
  return (dataset?.fields || [])
    .map((f: any) => String(f?.name || '').trim())
    .filter(Boolean)
    .slice(0, Math.max(1, Number(limit || 256)));
}
