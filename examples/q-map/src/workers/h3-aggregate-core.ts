import {
  latLngToCell,
  cellToBoundary,
  polygonToCells,
  polygonToCellsExperimental,
  POLYGON_TO_CELLS_FLAGS
} from 'h3-js-v4';
import {
  getPolygonsFromGeometry,
  parseGeoJsonLike,
  polygonAreaAbs,
  polygonCentroid,
  polygonIntersectionAreaWithHex,
  PolygonCoords,
  LngLat
} from '../geo/geometry';

export type H3AggregateWeightMode = 'intersects' | 'centroid' | 'area_weighted';

export type H3AggregateRow = {
  h3Id?: string | null;
  geometry?: unknown;
  value: number | null;
  distinctValue?: unknown;
  groupValues?: Record<string, unknown>;
};

export type H3AggregateBucket = {
  h3Id: string;
  count: number;
  countWeighted: number;
  sum: number;
  avgNumerator: number;
  avgDenominator: number;
  min: number | null;
  max: number | null;
  distinctCount: number;
  groupValues: Record<string, unknown>;
};

type H3AggregateInternalBucket = H3AggregateBucket & {
  distinctValues: Set<string>;
};

type AggregateCoreOptions = {
  rows: H3AggregateRow[];
  resolution: number;
  weightMode: H3AggregateWeightMode;
  groupFieldNames: string[];
  onProgress?: (progress: {processed: number; total: number}) => void;
  cooperativeYieldEvery?: number;
};

function getV4OverlapFlag(): string {
  const flags = POLYGON_TO_CELLS_FLAGS as Record<string, unknown> | undefined;
  const candidate =
    (typeof flags?.containmentOverlapping === 'string' && flags.containmentOverlapping) ||
    (typeof flags?.containmentOverlappingBbox === 'string' && flags.containmentOverlappingBbox) ||
    (typeof flags?.containmentCenter === 'string' && flags.containmentCenter) ||
    null;
  return candidate || 'containmentOverlapping';
}

export function getIntersectingH3IdsForPolygons(polygons: PolygonCoords[], resolution: number): string[] {
  const ids = new Set<string>();
  const overlapFlag = getV4OverlapFlag();

  polygons.forEach(polygonCoords => {
    let cells: string[] = [];
    if (typeof polygonToCellsExperimental === 'function') {
      const out = polygonToCellsExperimental(polygonCoords, resolution, overlapFlag, true);
      if (Array.isArray(out)) {
        cells = out.map(String);
      }
    }
    if (!cells.length && typeof polygonToCells === 'function') {
      const out = polygonToCells(polygonCoords, resolution, true);
      if (Array.isArray(out)) {
        cells = out.map(String);
      }
    }
    cells.forEach(cellId => ids.add(cellId));
  });

  return Array.from(ids);
}

export function extractPolygonsFromRaw(raw: unknown): PolygonCoords[] {
  const parsed = parseGeoJsonLike(raw);
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

function safeGroupValues(
  input: Record<string, unknown> | undefined,
  groupFieldNames: string[]
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  groupFieldNames.forEach(fieldName => {
    out[fieldName] = input?.[fieldName] ?? null;
  });
  return out;
}

function makeGroupKey(
  h3Id: string,
  groupValues: Record<string, unknown>,
  groupFieldNames: string[]
): string {
  if (!groupFieldNames.length) return h3Id;
  const values = groupFieldNames.map(fieldName => groupValues[fieldName] ?? null);
  return `${h3Id}::${JSON.stringify(values)}`;
}

function pushAggregate(
  buckets: Map<string, H3AggregateInternalBucket>,
  h3Id: string,
  weight: number,
  value: number | null,
  distinctValue: unknown,
  groupValues: Record<string, unknown>,
  groupFieldNames: string[],
  weightMode: H3AggregateWeightMode
) {
  const key = makeGroupKey(h3Id, groupValues, groupFieldNames);
  const prev = buckets.get(key) || {
    h3Id,
    count: 0,
    countWeighted: 0,
    sum: 0,
    avgNumerator: 0,
    avgDenominator: 0,
    min: null,
    max: null,
    distinctCount: 0,
    distinctValues: new Set<string>(),
    groupValues
  };
  prev.count += 1;
  prev.countWeighted += Math.max(0, weight);
  if (value !== null && Number.isFinite(value)) {
    const weightedValue = weightMode === 'area_weighted' ? value * weight : value;
    prev.sum += weightedValue;
    prev.avgNumerator += weightedValue;
    prev.avgDenominator += weightMode === 'area_weighted' ? weight : 1;
    prev.min = prev.min === null ? value : Math.min(prev.min, value);
    prev.max = prev.max === null ? value : Math.max(prev.max, value);
  }
  if (distinctValue !== null && distinctValue !== undefined) {
    prev.distinctValues.add(String(distinctValue));
    prev.distinctCount = prev.distinctValues.size;
  }
  buckets.set(key, prev);
}

export async function aggregateGeometriesToH3Rows({
  rows,
  resolution,
  weightMode,
  groupFieldNames,
  onProgress,
  cooperativeYieldEvery = 0
}: AggregateCoreOptions): Promise<{cells: H3AggregateBucket[]}> {
  if (!Number.isFinite(resolution) || resolution < 0) {
    throw new Error('Invalid resolution');
  }
  if (!['intersects', 'centroid', 'area_weighted'].includes(weightMode)) {
    throw new Error('Invalid weightMode');
  }

  const resolvedRows = Array.isArray(rows) ? rows : [];
  const resolvedGroupFieldNames = Array.isArray(groupFieldNames)
    ? groupFieldNames.map(v => String(v || '')).filter(Boolean)
    : [];
  const buckets = new Map<string, H3AggregateInternalBucket>();
  const total = resolvedRows.length;

  for (let idx = 0; idx < resolvedRows.length; idx += 1) {
    const row = resolvedRows[idx];
    const value = row?.value === null || row?.value === undefined ? null : Number(row.value);
    const safeValue = value !== null && value !== undefined && Number.isFinite(Number(value)) ? Number(value) : null;
    const distinctValue = row?.distinctValue;
    const groupValues = safeGroupValues(row?.groupValues, resolvedGroupFieldNames);
    const directH3Id = String(row?.h3Id || '').trim();

    if (directH3Id) {
      pushAggregate(
        buckets,
        directH3Id,
        1,
        safeValue,
        distinctValue,
        groupValues,
        resolvedGroupFieldNames,
        'intersects'
      );
    } else {
      const polygons = extractPolygonsFromRaw(row?.geometry);
      if (polygons.length) {
        if (weightMode === 'centroid') {
          const touched = new Set<string>();
          polygons.forEach(poly => {
            const centroid = polygonCentroid(poly);
            if (!centroid) return;
            touched.add(latLngToCell(centroid[1], centroid[0], resolution));
          });
          touched.forEach(h3Id => {
            pushAggregate(
              buckets,
              h3Id,
              1,
              safeValue,
              distinctValue,
              groupValues,
              resolvedGroupFieldNames,
              'centroid'
            );
          });
        } else if (weightMode === 'intersects') {
          const ids = getIntersectingH3IdsForPolygons(polygons, resolution);
          ids.forEach(h3Id => {
            pushAggregate(
              buckets,
              h3Id,
              1,
              safeValue,
              distinctValue,
              groupValues,
              resolvedGroupFieldNames,
              'intersects'
            );
          });
        } else {
          const totalArea = polygons.reduce((acc, poly) => acc + polygonAreaAbs(poly), 0);
          if (totalArea > 0) {
            const weightedByCell = new Map<string, number>();
            polygons.forEach(poly => {
              const polyArea = polygonAreaAbs(poly);
              if (!(polyArea > 0)) return;
              const candidates = getIntersectingH3IdsForPolygons([poly], resolution);
              candidates.forEach(h3Id => {
                const hexRing = cellToBoundary(h3Id, true) as LngLat[];
                const interArea = polygonIntersectionAreaWithHex(poly, hexRing);
                if (interArea <= 0) return;
                weightedByCell.set(h3Id, (weightedByCell.get(h3Id) || 0) + interArea / totalArea);
              });
            });
            weightedByCell.forEach((weight, h3Id) => {
              if (weight <= 0) return;
              pushAggregate(
                buckets,
                h3Id,
                weight,
                safeValue,
                distinctValue,
                groupValues,
                resolvedGroupFieldNames,
                'area_weighted'
              );
            });
          }
        }
      }
    }

    if (idx % 50 === 0 || idx === total - 1) {
      onProgress?.({processed: idx + 1, total});
    }

    if (cooperativeYieldEvery > 0 && idx > 0 && idx % cooperativeYieldEvery === 0) {
      await new Promise(resolve => setTimeout(resolve, 0));
    }
  }

  return {
    cells: Array.from(buckets.values()).map((bucket: H3AggregateInternalBucket) => ({
      h3Id: bucket.h3Id,
      count: bucket.count,
      countWeighted: bucket.countWeighted,
      sum: bucket.sum,
      avgNumerator: bucket.avgNumerator,
      avgDenominator: bucket.avgDenominator,
      min: bucket.min,
      max: bucket.max,
      distinctCount: bucket.distinctCount,
      groupValues: bucket.groupValues
    }))
  };
}
