import proj4 from 'proj4';

type RequestMessage = {
  id: string;
  type: 'reprojectRows';
  payload: {
    rows: Array<Record<string, unknown>>;
    sourceCrs: string;
    targetCrs: string;
    geometryField: string | null;
    outputGeometryField: string;
    latitudeField: string | null;
    longitudeField: string | null;
    outputLatitudeField: string;
    outputLongitudeField: string;
  };
};

type ResultMessage = {
  id: string;
  type: 'result';
  payload: {
    rows: Array<Record<string, unknown>>;
    transformedGeometryRows: number;
    transformedCoordinateRows: number;
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

function parseCoordinateValue(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'string') return null;
  const raw = value.trim();
  if (!raw) return null;
  const normalized = raw.includes(',') ? raw.replace(/\./g, '').replace(',', '.') : raw;
  const num = Number(normalized);
  return Number.isFinite(num) ? num : null;
}

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

function reprojectCoordinateArray(
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

function reprojectGeoJsonLike(input: any, project: (xy: [number, number]) => [number, number]): any {
  if (!input) return null;
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
      features: input.features.map((feature: any) =>
        feature && typeof feature === 'object'
          ? {
              ...feature,
              geometry: transformGeometry(feature.geometry)
            }
          : feature
      )
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

self.onmessage = (event: MessageEvent<RequestMessage>) => {
  const message = event.data;
  if (!message || message.type !== 'reprojectRows') return;

  const {id, payload} = message;
  try {
    const project = proj4(payload.sourceCrs, payload.targetCrs).forward as (xy: [number, number]) => [number, number];
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    const total = rows.length;
    let transformedGeometryRows = 0;
    let transformedCoordinateRows = 0;

    const outRows = rows.map((row, idx) => {
      const out = {...row};

      if (payload.geometryField) {
        const parsed = parseGeoJsonLike(out[payload.geometryField]);
        if (parsed) {
          const transformed = reprojectGeoJsonLike(parsed, project);
          if (transformed) {
            out[payload.outputGeometryField] = transformed;
            transformedGeometryRows += 1;
          }
        }
      }

      if (payload.latitudeField && payload.longitudeField) {
        const lat = parseCoordinateValue(out[payload.latitudeField]);
        const lon = parseCoordinateValue(out[payload.longitudeField]);
        if (lat !== null && lon !== null) {
          try {
            const [x2, y2] = project([lon, lat]);
            if (Number.isFinite(x2) && Number.isFinite(y2)) {
              out[payload.outputLongitudeField] = x2;
              out[payload.outputLatitudeField] = y2;
              transformedCoordinateRows += 1;
            }
          } catch {
            // ignore broken coordinates
          }
        }
      }

      if (idx % 500 === 0 || idx === total - 1) {
        const progress: ProgressMessage = {
          id,
          type: 'progress',
          payload: {processed: idx + 1, total}
        };
        self.postMessage(progress);
      }

      return out;
    });

    const result: ResultMessage = {
      id,
      type: 'result',
      payload: {
        rows: outRows,
        transformedGeometryRows,
        transformedCoordinateRows
      }
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

