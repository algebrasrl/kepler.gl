import {
  aggregateGeometriesToH3Rows,
  extractPolygonsFromRaw,
  getIntersectingH3IdsForPolygons,
  H3AggregateBucket,
  H3AggregateRow,
  H3AggregateWeightMode
} from './h3-aggregate-core';

type RequestMessage =
  | {
      id: string;
      type: 'tessellateGeometries';
      payload: {resolution: number; geometries: unknown[]};
    }
  | {
      id: string;
      type: 'aggregateGeometriesToH3';
      payload: {
        resolution: number;
        weightMode: H3AggregateWeightMode;
        groupFieldNames: string[];
        rows: H3AggregateRow[];
      };
    };

type ResultMessage = {
  id: string;
  type: 'result';
  payload:
    | {ids: string[]}
    | {
        cells: H3AggregateBucket[];
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

self.onmessage = (event: MessageEvent<RequestMessage>) => {
  const message = event.data;
  if (!message) {
    return;
  }

  if (message.type === 'tessellateGeometries') {
    const {id, payload} = message;
    const resolution = Number(payload?.resolution);
    const geometries = Array.isArray(payload?.geometries) ? payload.geometries : [];

    if (!Number.isFinite(resolution) || resolution < 0) {
      const err: ErrorMessage = {id, type: 'error', error: 'Invalid resolution'};
      self.postMessage(err);
      return;
    }

    try {
      const ids = new Set<string>();
      const total = geometries.length;
      geometries.forEach((raw, idx) => {
        const polygons = extractPolygonsFromRaw(raw);
        if (polygons.length) {
          const rowIds = getIntersectingH3IdsForPolygons(polygons, resolution);
          rowIds.forEach(cellId => ids.add(cellId));
        }
        if (idx % 50 === 0 || idx === total - 1) {
          const progress: ProgressMessage = {
            id,
            type: 'progress',
            payload: {processed: idx + 1, total}
          };
          self.postMessage(progress);
        }
      });

      const out: ResultMessage = {
        id,
        type: 'result',
        payload: {ids: Array.from(ids)}
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
    return;
  }

  if (message.type === 'aggregateGeometriesToH3') {
    const {id, payload} = message;
    const resolution = Number(payload?.resolution);
    const weightMode = payload?.weightMode || 'area_weighted';
    const groupFieldNames = Array.isArray(payload?.groupFieldNames)
      ? payload.groupFieldNames.map(v => String(v || '')).filter(Boolean)
      : [];
    const rows = Array.isArray(payload?.rows) ? payload.rows : [];

    if (!Number.isFinite(resolution) || resolution < 0) {
      const err: ErrorMessage = {id, type: 'error', error: 'Invalid resolution'};
      self.postMessage(err);
      return;
    }
    if (!['intersects', 'centroid', 'area_weighted'].includes(weightMode)) {
      const err: ErrorMessage = {id, type: 'error', error: 'Invalid weightMode'};
      self.postMessage(err);
      return;
    }

    (async () => {
      try {
        const result = await aggregateGeometriesToH3Rows({
          rows,
          resolution,
          weightMode,
          groupFieldNames,
          onProgress: progress => {
            const msg: ProgressMessage = {id, type: 'progress', payload: progress};
            self.postMessage(msg);
          }
        });
        const out: ResultMessage = {
          id,
          type: 'result',
          payload: {cells: result.cells}
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
    })();
    return;
  }

  const err: ErrorMessage = {
    id: (message as any).id || 'unknown',
    type: 'error',
    error: `Unsupported H3 job type: ${(message as any).type || 'unknown'}`
  };
  self.postMessage(err);
};
