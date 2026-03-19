import {
  area as turfArea,
  booleanIntersects as turfBooleanIntersects,
  booleanPointInPolygon as turfBooleanPointInPolygon,
  centroid as turfCentroid,
  featureCollection as turfFeatureCollection,
  intersect as turfIntersect
} from '@turf/turf';

import {h3CellToPolygonFeature, normalizeH3Key} from './h3-geometry-utils';

type RequestMessage = {
  id: string;
  type: 'zonalStatsByAdmin';
  payload: {
    weightMode: 'intersects' | 'centroid' | 'area_weighted';
    includeValue: boolean;
    adminRows: Array<{rowIdx: number; geometry?: unknown; h3Id?: unknown}>;
    valueRows: Array<{rowIdx: number; geometry?: unknown; h3Id?: unknown; value: number | null}>;
  };
};

type ResultMessage = {
  id: string;
  type: 'result';
  payload: {
    statsByRow: Array<{
      rowIdx: number;
      count: number;
      sum: number;
      denom: number;
      min: number | null;
      max: number | null;
    }>;
  };
};

type ErrorMessage = {
  id: string;
  type: 'error';
  error: string;
};

type ProgressMessage = {
  id: string;
  type: 'progress';
  payload: {processed: number; total: number};
};

type BBox = [number, number, number, number];

type ValueFeature = {
  feature: any;
  value: number;
  bbox: BBox | null;
  centroid: any;
};

function parseGeoJsonLike(raw: unknown): any | null {
  if (raw === null || raw === undefined) return null;
  if (typeof raw === 'object') return raw;
  if (typeof raw !== 'string') return null;
  const text = raw.trim();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function toTurfPolygonFeature(input: any): any | null {
  if (!input) return null;
  const feature = input?.type === 'Feature' ? input : {type: 'Feature', properties: {}, geometry: input};
  const geometryType = String(feature?.geometry?.type || '');
  if (geometryType === 'Polygon' || geometryType === 'MultiPolygon') {
    return feature;
  }
  return null;
}

function toTurfFeature(input: any): any | null {
  if (!input) return null;
  const feature = input?.type === 'Feature' ? input : {type: 'Feature', properties: {}, geometry: input};
  if (!feature?.geometry?.type) return null;
  return feature;
}

function isPolygonLikeFeature(feature: any): boolean {
  const g = String(feature?.geometry?.type || '');
  return g === 'Polygon' || g === 'MultiPolygon';
}

function collectLonLatPairs(value: any, out: Array<[number, number]>) {
  if (!Array.isArray(value)) return;
  if (value.length >= 2 && Number.isFinite(Number(value[0])) && Number.isFinite(Number(value[1]))) {
    out.push([Number(value[0]), Number(value[1])]);
    return;
  }
  value.forEach(item => collectLonLatPairs(item, out));
}

function geometryToBbox(geometry: any): BBox | null {
  if (!geometry || typeof geometry !== 'object') return null;
  const coords = geometry.coordinates;
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

function geometryBboxOverlap(a: BBox, b: BBox): boolean {
  return !(a[2] < b[0] || a[0] > b[2] || a[3] < b[1] || a[1] > b[3]);
}

function turfIntersectSafe(a: any, b: any): any | null {
  try {
    const inter = turfIntersect(turfFeatureCollection([a, b]) as any);
    if (inter) return inter;
  } catch {
    // fallback for older/newer Turf signatures
  }
  try {
    const inter = (turfIntersect as any)(a, b);
    return inter || null;
  } catch {
    return null;
  }
}

self.onmessage = (event: MessageEvent<RequestMessage>) => {
  const message = event.data;
  if (!message || message.type !== 'zonalStatsByAdmin') return;
  const {id, payload} = message;

  try {
    const weightMode = payload?.weightMode || 'area_weighted';
    if (!['intersects', 'centroid', 'area_weighted'].includes(weightMode)) {
      const err: ErrorMessage = {id, type: 'error', error: 'Invalid weightMode'};
      self.postMessage(err);
      return;
    }

    const includeValue = payload?.includeValue === true;
    const adminRows = Array.isArray(payload?.adminRows) ? payload.adminRows : [];
    const valueRows = Array.isArray(payload?.valueRows) ? payload.valueRows : [];

    const valueFeatures: ValueFeature[] = [];
    for (let i = 0; i < valueRows.length; i += 1) {
      const valueRow = valueRows[i] || {};
      const geometryRaw =
        valueRow.geometry !== null && valueRow.geometry !== undefined
          ? parseGeoJsonLike(valueRow.geometry)
          : h3CellToPolygonFeature(normalizeH3Key(valueRow.h3Id));
      const feature = toTurfFeature(geometryRaw);
      if (!feature) continue;
      const centroid = turfCentroid(feature as any);
      const rawValue = Number(valueRow.value);
      valueFeatures.push({
        feature,
        value: Number.isFinite(rawValue) ? rawValue : NaN,
        bbox: geometryToBbox((feature as any)?.geometry),
        centroid
      });
    }
    if (!valueFeatures.length) {
      const out: ResultMessage = {id, type: 'result', payload: {statsByRow: []}};
      self.postMessage(out);
      return;
    }

    const statsByRow: Array<{
      rowIdx: number;
      count: number;
      sum: number;
      denom: number;
      min: number | null;
      max: number | null;
    }> = [];

    const total = adminRows.length;
    for (let i = 0; i < adminRows.length; i += 1) {
      const adminRow = adminRows[i] || {};
      const rowIdx = Number(adminRow.rowIdx);
      if (!Number.isFinite(rowIdx)) {
        continue;
      }
      const adminGeometryRaw =
        adminRow.geometry !== null && adminRow.geometry !== undefined
          ? parseGeoJsonLike(adminRow.geometry)
          : h3CellToPolygonFeature(normalizeH3Key(adminRow.h3Id));
      const adminFeature = toTurfPolygonFeature(adminGeometryRaw);
      if (!adminFeature) {
        if (i % 20 === 0 || i === total - 1) {
          const progress: ProgressMessage = {id, type: 'progress', payload: {processed: i + 1, total}};
          self.postMessage(progress);
        }
        continue;
      }

      const adminBbox = geometryToBbox((adminFeature as any)?.geometry);
      const candidates = adminBbox
        ? valueFeatures.filter(item => !item.bbox || geometryBboxOverlap(adminBbox, item.bbox))
        : valueFeatures;

      let sum = 0;
      let denom = 0;
      let min: number | null = null;
      let max: number | null = null;
      let count = 0;

      for (let j = 0; j < candidates.length; j += 1) {
        const item = candidates[j];
        let matched = false;
        let weight = 1;
        try {
          if (weightMode === 'centroid') {
            matched = turfBooleanPointInPolygon(item.centroid, adminFeature);
          } else if (weightMode === 'intersects') {
            matched = turfBooleanIntersects(item.feature, adminFeature);
          } else if (isPolygonLikeFeature(item.feature)) {
            const inter = turfIntersectSafe(adminFeature, item.feature);
            if (inter) {
              const interArea = turfArea(inter as any);
              const totalArea = Math.max(1e-12, turfArea(item.feature as any));
              weight = Math.max(0, interArea / totalArea);
              matched = weight > 0;
            }
          } else {
            matched = turfBooleanPointInPolygon(item.centroid, adminFeature);
          }
        } catch {
          matched = false;
        }
        if (!matched) continue;

        count += 1;
        if (includeValue && Number.isFinite(item.value)) {
          const v = Number(item.value);
          const weighted = v * Math.max(0, weight);
          sum += weighted;
          denom += Math.max(0, weight);
          min = min === null ? v : Math.min(min, v);
          max = max === null ? v : Math.max(max, v);
        }
      }

      statsByRow.push({rowIdx, count, sum, denom, min, max});
      if (i % 20 === 0 || i === total - 1) {
        const progress: ProgressMessage = {id, type: 'progress', payload: {processed: i + 1, total}};
        self.postMessage(progress);
      }
    }

    const out: ResultMessage = {
      id,
      type: 'result',
      payload: {statsByRow}
    };
    self.postMessage(out);
  } catch (error) {
    const err: ErrorMessage = {
      id,
      type: 'error',
      error: error instanceof Error ? error.message : String(error)
    };
    self.postMessage(err);
  }
};
