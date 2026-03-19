import {
  difference as turfDifference,
  featureCollection as turfFeatureCollection,
  intersect as turfIntersect
} from '@turf/turf';
import proj4 from 'proj4';
import {getPolygonsFromGeometry, parseGeoJsonLike, type PolygonCoords} from '../../../geo';
import {type ClipMetricsFeatureInput} from '../../../workers/clip-metrics';
import {
  resolveDatasetFieldName,
  getDatasetIndexes,
  getFilteredDatasetIndexes,
  filterTargetsDataset,
  toComparable,
  h3CellToPolygonFeature
} from './dataset-resolve';

// ─── Section A: async chunking utilities ─────────────────────────────────────

const _QMAP_UNBOUNDED_MAX_FEATURES = Number.MAX_SAFE_INTEGER;
const _QMAP_DEFAULT_CHUNK_SIZE = 250;

export async function yieldToMainThread() {
  await new Promise(resolve => setTimeout(resolve, 0));
}

export function resolveOptionalFeatureCap(value: unknown): number {
  const numericValue = Number(value);
  if (Number.isFinite(numericValue) && numericValue > 0) {
    return Math.max(1, Math.floor(numericValue));
  }
  return _QMAP_UNBOUNDED_MAX_FEATURES;
}

export async function filterIndexesChunked(
  indexes: number[],
  predicate: (rowIdx: number) => boolean,
  chunkSize = _QMAP_DEFAULT_CHUNK_SIZE
): Promise<number[]> {
  const out: number[] = [];
  for (let i = 0; i < indexes.length; i += 1) {
    const rowIdx = indexes[i];
    if (predicate(rowIdx)) out.push(rowIdx);
    if (i > 0 && i % chunkSize === 0) {
      await yieldToMainThread();
    }
  }
  return out;
}

export async function mapIndexesChunked<T>(
  indexes: number[],
  mapper: (rowIdx: number) => T,
  chunkSize = _QMAP_DEFAULT_CHUNK_SIZE
): Promise<T[]> {
  const out: T[] = [];
  for (let i = 0; i < indexes.length; i += 1) {
    out.push(mapper(indexes[i]));
    if (i > 0 && i % chunkSize === 0) {
      await yieldToMainThread();
    }
  }
  return out;
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
export function normalizeThresholds(values?: number[]): number[] | null {
  if (!Array.isArray(values) || values.length < 1) return null;
  const out = Array.from(new Set(values.map(v => Number(v)).filter(v => Number.isFinite(v)))).sort((a, b) => a - b);
  return out.length ? out : null;
}

export function updateBoundsFromGeometry(
  geometry: any,
  bounds: {minLng: number; minLat: number; maxLng: number; maxLat: number}
) {
  if (!geometry || !Array.isArray(geometry.coordinates)) return;
  const stack: any[] = [geometry.coordinates];
  while (stack.length) {
    const node = stack.pop();
    if (!Array.isArray(node) || !node.length) continue;
    if (typeof node[0] === 'number' && typeof node[1] === 'number') {
      const lng = Number(node[0]);
      const lat = Number(node[1]);
      if (!Number.isFinite(lng) || !Number.isFinite(lat)) continue;
      bounds.minLng = Math.min(bounds.minLng, lng);
      bounds.maxLng = Math.max(bounds.maxLng, lng);
      bounds.minLat = Math.min(bounds.minLat, lat);
      bounds.maxLat = Math.max(bounds.maxLat, lat);
      continue;
    }
    for (const child of node) {
      stack.push(child);
    }
  }
}

export function hasValidBounds(bounds: {minLng: number; minLat: number; maxLng: number; maxLat: number}): boolean {
  return (
    Number.isFinite(bounds.minLng) &&
    Number.isFinite(bounds.minLat) &&
    Number.isFinite(bounds.maxLng) &&
    Number.isFinite(bounds.maxLat)
  );
}

export function boundsOverlap(
  a: {minLng: number; minLat: number; maxLng: number; maxLat: number},
  b: {minLng: number; minLat: number; maxLng: number; maxLat: number}
): boolean {
  return !(a.maxLng < b.minLng || a.minLng > b.maxLng || a.maxLat < b.minLat || a.minLat > b.maxLat);
}

export type LngLat = [number, number];

export function toTurfPolygonFeature(input: any): any | null {
  if (!input) return null;
  const feature = input?.type === 'Feature' ? input : {type: 'Feature', properties: {}, geometry: input};
  const geometryType = String(feature?.geometry?.type || '');
  if (geometryType === 'Polygon' || geometryType === 'MultiPolygon') {
    return feature;
  }
  return null;
}

export function toTurfFeature(input: any): any | null {
  if (!input) return null;
  const feature = input?.type === 'Feature' ? input : {type: 'Feature', properties: {}, geometry: input};
  if (!feature?.geometry?.type) return null;
  return feature;
}

export function isPolygonLikeFeature(feature: any): boolean {
  const g = String(feature?.geometry?.type || '');
  return g === 'Polygon' || g === 'MultiPolygon';
}
export async function collectDatasetFeaturesForGeometryOps(
  dataset: any,
  visState: any,
  options: {
    geometryField?: string | null;
    h3Field?: string | null;
    useActiveFilters?: boolean;
    maxFeatures?: number;
    includeRowProperties?: boolean;
  }
): Promise<
  Array<{
    rowIdx: number;
    feature: any;
    bbox: [number, number, number, number] | null;
    rowProperties?: Record<string, unknown>;
  }>
> {
  const geometryField = String(options?.geometryField || '').trim() || null;
  const h3Field = String(options?.h3Field || '').trim() || null;
  const useActiveFilters = options?.useActiveFilters !== false;
  const includeRowProperties = options?.includeRowProperties === true;
  const maxFeatures = resolveOptionalFeatureCap(options?.maxFeatures);
  const idx = getFilteredDatasetIndexes(dataset, visState, useActiveFilters).slice(0, maxFeatures);
  const loopChunk = Math.max(50, Math.min(200, _QMAP_DEFAULT_CHUNK_SIZE));
  const rows = await mapIndexesChunked(
    idx,
    (rowIdx: number) => {
      const rawGeometry = geometryField
        ? parseGeoJsonLike(dataset.getValue(geometryField, rowIdx))
        : h3CellToPolygonFeature(dataset.getValue(String(h3Field || ''), rowIdx));
      const feature = toTurfFeature(rawGeometry);
      if (!feature) return null;
      const rowProperties: Record<string, unknown> | undefined = includeRowProperties
        ? (dataset?.fields || []).reduce((acc: Record<string, unknown>, field: any) => {
            const name = String(field?.name || '').trim();
            if (!name) return acc;
            acc[name] = dataset.getValue(name, rowIdx);
            return acc;
          }, {})
        : undefined;
      return {
        rowIdx,
        feature,
        bbox: geometryToBbox((feature as any)?.geometry),
        rowProperties
      };
    },
    loopChunk
  );

  return rows.filter(Boolean) as Array<{
    rowIdx: number;
    feature: any;
    bbox: [number, number, number, number] | null;
    rowProperties?: Record<string, unknown>;
  }>;
}
export function turfIntersectSafe(a: any, b: any): any | null {
  try {
    return turfIntersect(turfFeatureCollection([a, b]) as any) as any;
  } catch {
    try {
      return (turfIntersect as any)(a, b) as any;
    } catch {
      return null;
    }
  }
}

export function turfDifferenceSafe(a: any, b: any): any | null {
  try {
    return (turfDifference as any)(a, b) as any;
  } catch {
    try {
      return (turfDifference as any)(turfFeatureCollection([a, b])) as any;
    } catch {
      return null;
    }
  }
}

export type ClipFeatureDiagnosticsInput = ClipMetricsFeatureInput;

export function parseCoordinateValue(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'string') return null;
  const raw = value.trim();
  if (!raw) return null;
  const normalized = raw.includes(',') ? raw.replace(/\./g, '').replace(',', '.') : raw;
  const num = Number(normalized);
  return Number.isFinite(num) ? num : null;
}

export function reprojectCoordinateArray(
  value: any,
  project: (xy: [number, number]) => [number, number]
): any {
  if (!Array.isArray(value)) return value;
  if (value.length >= 2 && typeof value[0] === 'number' && typeof value[1] === 'number') {
    const x = Number(value[0]);
    const y = Number(value[1]);
    if (!Number.isFinite(x) || !Number.isFinite(y)) return value;
    const out = project([x, y]);
    if (!Array.isArray(out) || out.length < 2) return value;
    const rest = value.slice(2);
    return [out[0], out[1], ...rest];
  }
  return value.map(child => reprojectCoordinateArray(child, project));
}

export function reprojectGeoJsonLike(input: any, sourceCrs: string, targetCrs: string): any {
  if (!input) return null;
  const project = proj4(sourceCrs, targetCrs).forward as (xy: [number, number]) => [number, number];

  const transformGeometry = (geometry: any): any => {
    if (!geometry || typeof geometry !== 'object') return geometry;
    if (geometry.type === 'GeometryCollection' && Array.isArray(geometry.geometries)) {
      return {
        ...geometry,
        geometries: geometry.geometries.map((g: any) => transformGeometry(g))
      };
    }
    if (!Array.isArray(geometry.coordinates)) return geometry;
    return {
      ...geometry,
      coordinates: reprojectCoordinateArray(geometry.coordinates, project)
    };
  };

  if (input?.type === 'FeatureCollection' && Array.isArray(input?.features)) {
    return {
      ...input,
      features: input.features.map((feature: any) => {
        if (!feature || typeof feature !== 'object') return feature;
        return {
          ...feature,
          geometry: transformGeometry(feature.geometry)
        };
      })
    };
  }

  if (input?.type === 'Feature') {
    return {
      ...input,
      geometry: transformGeometry(input.geometry)
    };
  }

  return transformGeometry(input);
}

export function collectLonLatPairs(value: any, out: Array<[number, number]>) {
  if (!Array.isArray(value)) return;
  if (
    value.length >= 2 &&
    Number.isFinite(Number(value[0])) &&
    Number.isFinite(Number(value[1]))
  ) {
    out.push([Number(value[0]), Number(value[1])]);
    return;
  }
  value.forEach(item => collectLonLatPairs(item, out));
}

export function geometryToBbox(geometry: any): [number, number, number, number] | null {
  if (!geometry || typeof geometry !== 'object') return null;
  const coords = (geometry as any).coordinates;
  const pairs: Array<[number, number]> = [];
  collectLonLatPairs(coords, pairs);
  if (!pairs.length) return null;
  const minLon = Math.min(...pairs.map(pair => pair[0]));
  const minLat = Math.min(...pairs.map(pair => pair[1]));
  const maxLon = Math.max(...pairs.map(pair => pair[0]));
  const maxLat = Math.max(...pairs.map(pair => pair[1]));
  if (![minLon, minLat, maxLon, maxLat].every(Number.isFinite)) return null;
  return [minLon, minLat, maxLon, maxLat];
}

export function geometryBboxOverlap(
  a: [number, number, number, number],
  b: [number, number, number, number]
): boolean {
  return !(a[2] < b[0] || a[0] > b[2] || a[3] < b[1] || a[1] > b[3]);
}

