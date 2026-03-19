import {h3CellToPolygonFeature, normalizeH3Key} from './h3-geometry-utils';
import {computeClipMetricsForFeature, type ClipMetricsFeatureInput, type ClipMode} from './clip-metrics';

type RequestMessage = {
  id: string;
  type: 'clipRowsByGeometry';
  payload: {
    mode: ClipMode;
    includeMetrics: boolean;
    includeDistinctCounts: boolean;
    includeValueCountFields: boolean;
    sourceRows: Array<{rowIdx: number; geometry?: unknown; h3Id?: unknown}>;
    clipRows: Array<{geometry: unknown; properties?: Record<string, unknown>}>;
  };
};

type ResultMessage = {
  id: string;
  type: 'result';
  payload: {
    matchedRows: number[];
    metricsByRow: Array<{
      rowIdx: number;
      matchCount: number;
      intersectionAreaM2: number;
      intersectionPct: number;
      distinctValueCounts: Record<string, number>;
      propertyValueMatchCounts: Record<string, Record<string, number>>;
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

type PreparedClipRow = {
  feature: any;
  properties: Record<string, unknown>;
  bbox: BBox | null;
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

function boundsOverlap(a: BBox, b: BBox): boolean {
  return !(a[2] < b[0] || a[0] > b[2] || a[3] < b[1] || a[1] > b[3]);
}

self.onmessage = (event: MessageEvent<RequestMessage>) => {
  const message = event.data;
  if (!message || message.type !== 'clipRowsByGeometry') return;
  const {id, payload} = message;

  try {
    const mode = payload?.mode || 'intersects';
    if (!['intersects', 'centroid', 'within'].includes(mode)) {
      const err: ErrorMessage = {id, type: 'error', error: 'Invalid clip mode'};
      self.postMessage(err);
      return;
    }

    const includeMetrics = payload?.includeMetrics === true;
    const includeDistinctCounts = payload?.includeDistinctCounts === true;
    const includeValueCountFields = payload?.includeValueCountFields === true;
    const sourceRows = Array.isArray(payload?.sourceRows) ? payload.sourceRows : [];
    const clipRows = Array.isArray(payload?.clipRows) ? payload.clipRows : [];

    const preparedClipRows: PreparedClipRow[] = [];
    clipRows.forEach(rawClipRow => {
      const parsed = parseGeoJsonLike(rawClipRow?.geometry);
      const feature = toTurfPolygonFeature(parsed);
      if (!feature) return;
      preparedClipRows.push({
        feature,
        properties: rawClipRow?.properties || {},
        bbox: geometryToBbox(feature?.geometry)
      });
    });

    const matchedRows: number[] = [];
    const metricsByRow: Array<{
      rowIdx: number;
      matchCount: number;
      intersectionAreaM2: number;
      intersectionPct: number;
      distinctValueCounts: Record<string, number>;
      propertyValueMatchCounts: Record<string, Record<string, number>>;
    }> = [];
    const total = sourceRows.length;

    for (let i = 0; i < sourceRows.length; i += 1) {
      const sourceRow = sourceRows[i] || {};
      const rowIdx = Number(sourceRow.rowIdx);
      if (!Number.isFinite(rowIdx)) {
        if (i % 200 === 0 || i === total - 1) {
          const progress: ProgressMessage = {
            id,
            type: 'progress',
            payload: {processed: i + 1, total}
          };
          self.postMessage(progress);
        }
        continue;
      }

      let sourceFeature: any | null = null;
      if (sourceRow.geometry !== null && sourceRow.geometry !== undefined) {
        const parsed = parseGeoJsonLike(sourceRow.geometry);
        sourceFeature = toTurfPolygonFeature(parsed);
      } else if (sourceRow.h3Id !== null && sourceRow.h3Id !== undefined) {
        sourceFeature = h3CellToPolygonFeature(normalizeH3Key(sourceRow.h3Id));
      }

      if (!sourceFeature || !preparedClipRows.length) {
        if (i % 200 === 0 || i === total - 1) {
          const progress: ProgressMessage = {
            id,
            type: 'progress',
            payload: {processed: i + 1, total}
          };
          self.postMessage(progress);
        }
        continue;
      }

      const sourceBbox = geometryToBbox(sourceFeature?.geometry);
      const candidateClipRows = sourceBbox
        ? preparedClipRows.filter(clipRow => !clipRow.bbox || boundsOverlap(sourceBbox, clipRow.bbox))
        : preparedClipRows;

      if (candidateClipRows.length) {
        const metrics = computeClipMetricsForFeature(
          sourceFeature,
          candidateClipRows as ClipMetricsFeatureInput[],
          {
            mode,
            includeAreaMetrics: includeMetrics,
            includeDistinctCounts,
            includeValueCountFields
          }
        );
        if (metrics.matchCount > 0) {
          matchedRows.push(rowIdx);
          if (includeMetrics || includeDistinctCounts || includeValueCountFields) {
            metricsByRow.push({rowIdx, ...metrics});
          }
        }
      }

      if (i % 200 === 0 || i === total - 1) {
        const progress: ProgressMessage = {
          id,
          type: 'progress',
          payload: {processed: i + 1, total}
        };
        self.postMessage(progress);
      }
    }

    const result: ResultMessage = {
      id,
      type: 'result',
      payload: {matchedRows, metricsByRow}
    };
    self.postMessage(result);
  } catch (error) {
    const err: ErrorMessage = {
      id,
      type: 'error',
      error: error instanceof Error ? error.message : String(error)
    };
    self.postMessage(err);
  }
};
