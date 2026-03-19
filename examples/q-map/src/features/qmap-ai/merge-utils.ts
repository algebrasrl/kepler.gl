import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import proj4 from 'proj4';
import {parseGeoJsonLike} from '../../geo';
import {resolveDatasetFieldName, resolveGeojsonFieldName} from './dataset-utils';

export function normalizeFieldToken(value: unknown): string {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

export function normalizeFieldValueToken(value: unknown): string {
  const raw = String(value ?? '').trim();
  if (!raw) return 'empty';
  const normalized = raw
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
  const out = normalized || 'value';
  return out.slice(0, 48);
}

export type MergeFieldDefinition = {
  name: string;
  type: string;
  key: string;
  source?: 'datasetName' | 'datasetId' | 'derivedPointGeojson';
};

export type MergeSchemaConflict = {
  fieldKey: string;
  chosenType: string;
  conflictingType: string;
  conflictingFieldName: string;
  datasetId: string;
  datasetName: string;
};

export type MergeFieldBuildResult = {
  fields: MergeFieldDefinition[];
  schemaConflicts: MergeSchemaConflict[];
};

export type MergeGeometryMode = 'auto' | 'preserve_only' | 'derive_from_latlon' | 'none';

export type MergeGeometryReadiness = {
  hasGeometryField: boolean;
  hasPointPair: boolean;
  geometryFieldName: string | null;
  pointFields: {latField: string | null; lonField: string | null};
};

export function scoreMergeFieldType(typeName: string): number {
  const normalized = String(typeName || '').trim().toLowerCase();
  if (!normalized) return 0;
  if (normalized === 'geojson' || normalized.includes('geometry')) return 100;
  if (normalized === 'h3') return 95;
  if (normalized.includes('timestamp') || normalized.includes('datetime')) return 90;
  if (normalized === 'date') return 85;
  if (
    normalized === 'real' ||
    normalized === 'float' ||
    normalized === 'double' ||
    normalized === 'number' ||
    normalized.includes('decimal')
  )
    return 75;
  if (normalized === 'integer' || normalized === 'int' || normalized.includes('long')) return 70;
  if (normalized === 'boolean' || normalized === 'bool') return 60;
  if (normalized === 'string' || normalized === 'text') return 50;
  return 10;
}

export function mergeFieldType(currentType: string, incomingType: string): string {
  const currentScore = scoreMergeFieldType(currentType);
  const incomingScore = scoreMergeFieldType(incomingType);
  if (incomingScore > currentScore) return incomingType;
  return currentType || incomingType || String(ALL_FIELD_TYPES.string || 'string');
}

export function ensureUniqueMergeFieldName(baseName: string, usedKeys: Set<string>): {name: string; key: string} {
  const seed = String(baseName || 'field').trim() || 'field';
  let candidate = seed;
  let key = normalizeFieldToken(candidate);
  let counter = 2;
  while (!key || usedKeys.has(key)) {
    candidate = `${seed}_${counter}`;
    key = normalizeFieldToken(candidate);
    counter += 1;
  }
  usedKeys.add(key);
  return {name: candidate, key};
}

export function buildMergeFieldDefinitions(
  datasets: any[],
  options?: {includeSourceDatasetField?: boolean; includeSourceDatasetIdField?: boolean}
): MergeFieldBuildResult {
  const mergedFields: MergeFieldDefinition[] = [];
  const byKey = new Map<string, MergeFieldDefinition>();
  const usedKeys = new Set<string>();
  const schemaConflicts: MergeSchemaConflict[] = [];

  (datasets || []).forEach((dataset: any) => {
    const datasetFields = Array.isArray(dataset?.fields) ? dataset.fields : [];
    datasetFields.forEach((field: any) => {
      const fieldName = String(field?.name || '').trim();
      if (!fieldName) return;
      const key = normalizeFieldToken(fieldName);
      if (!key) return;
      const incomingType = String(field?.type || ALL_FIELD_TYPES.string || 'string');
      const existing = byKey.get(key);
      if (!existing) {
        const definition: MergeFieldDefinition = {
          name: fieldName,
          type: incomingType,
          key
        };
        byKey.set(key, definition);
        mergedFields.push(definition);
        usedKeys.add(key);
        return;
      }
      if (String(existing.type || '').toLowerCase() !== String(incomingType || '').toLowerCase()) {
        schemaConflicts.push({
          fieldKey: key,
          chosenType: mergeFieldType(existing.type, incomingType),
          conflictingType: incomingType,
          conflictingFieldName: fieldName,
          datasetId: String(dataset?.id || ''),
          datasetName: String(dataset?.label || dataset?.id || '')
        });
      }
      existing.type = mergeFieldType(existing.type, incomingType);
    });
  });

  if (options?.includeSourceDatasetField !== false) {
    const sourceDatasetField = ensureUniqueMergeFieldName('source_dataset', usedKeys);
    mergedFields.push({
      name: sourceDatasetField.name,
      type: String(ALL_FIELD_TYPES.string || 'string'),
      key: sourceDatasetField.key,
      source: 'datasetName'
    });
  }

  if (options?.includeSourceDatasetIdField === true) {
    const sourceDatasetIdField = ensureUniqueMergeFieldName('source_dataset_id', usedKeys);
    mergedFields.push({
      name: sourceDatasetIdField.name,
      type: String(ALL_FIELD_TYPES.string || 'string'),
      key: sourceDatasetIdField.key,
      source: 'datasetId'
    });
  }

  return {fields: mergedFields, schemaConflicts};
}

export function scorePointFieldName(fieldName: string, family: 'lat' | 'lon'): number {
  const normalized = String(fieldName || '').trim().toLowerCase();
  const compact = normalized.replace(/[^a-z0-9]/g, '');
  if (!normalized) return 0;
  if (family === 'lat') {
    if (normalized === 'lat') return 120;
    if (normalized === 'latitude') return 115;
    if (normalized === 'latitudine') return 114;
    if (normalized === 'lat_wgs84') return 110;
    if (compact === 'latwgs84') return 109;
    if (compact === 'latitudinewgs84') return 108;
    if (compact === 'ycoord' || compact === 'coordy') return 102;
    if (normalized === 'y') return 85;
    if (/(^|[_-])lat($|[_-])/.test(normalized)) return 95;
    if (/(^|[_-])latitudine($|[_-])/.test(normalized)) return 94;
    if (normalized.includes('latitude')) return 90;
    if (normalized.includes('latitudine')) return 89;
    return 0;
  }
  if (normalized === 'lon' || normalized === 'lng') return 120;
  if (normalized === 'longitude') return 115;
  if (normalized === 'longitudine') return 114;
  if (normalized === 'lon_wgs84' || normalized === 'lng_wgs84') return 110;
  if (compact === 'lonwgs84' || compact === 'lngwgs84') return 109;
  if (compact === 'longitudinewgs84') return 108;
  if (compact === 'xcoord' || compact === 'coordx') return 102;
  if (normalized === 'x') return 85;
  if (/(^|[_-])(lon|lng)($|[_-])/.test(normalized)) return 95;
  if (/(^|[_-])longitudine($|[_-])/.test(normalized)) return 94;
  if (normalized.includes('longitude')) return 90;
  if (normalized.includes('longitudine')) return 89;
  return 0;
}

export function resolveDatasetPointFieldPair(
  dataset: any,
  requestedLatFieldName?: string | null,
  requestedLonFieldName?: string | null
): {latField: string | null; lonField: string | null} {
  const explicitLat = requestedLatFieldName ? resolveDatasetFieldName(dataset, requestedLatFieldName) : null;
  const explicitLon = requestedLonFieldName ? resolveDatasetFieldName(dataset, requestedLonFieldName) : null;
  if (explicitLat && explicitLon) {
    return {latField: explicitLat, lonField: explicitLon};
  }

  const candidatePairs: Array<{lat: string; lon: string}> = [
    {lat: 'lat', lon: 'lon'},
    {lat: 'lat', lon: 'lng'},
    {lat: 'latitude', lon: 'longitude'},
    {lat: 'latitudine', lon: 'longitudine'},
    {lat: 'lat_wgs84', lon: 'lon_wgs84'},
    {lat: 'latitudine_wgs84', lon: 'longitudine_wgs84'},
    {lat: 'coord_y', lon: 'coord_x'},
    {lat: 'y', lon: 'x'}
  ];
  for (const pair of candidatePairs) {
    const lat = resolveDatasetFieldName(dataset, pair.lat);
    const lon = resolveDatasetFieldName(dataset, pair.lon);
    if (lat && lon) {
      return {
        latField: explicitLat || lat,
        lonField: explicitLon || lon
      };
    }
  }

  const fields: string[] = (Array.isArray(dataset?.fields) ? dataset.fields : [])
    .map((field: any) => String(field?.name || '').trim())
    .filter(Boolean);

  const pickBest = (family: 'lat' | 'lon'): string | null => {
    let bestField: string | null = null;
    let bestScore = -1;
    fields.forEach(fieldName => {
      const score = scorePointFieldName(fieldName, family);
      if (score > bestScore) {
        bestScore = score;
        bestField = fieldName;
      }
    });
    return bestScore > 0 ? bestField : null;
  };

  return {
    latField: explicitLat || pickBest('lat'),
    lonField: explicitLon || pickBest('lon')
  };
}

export function normalizeMergeGeometryMode(value: unknown): MergeGeometryMode {
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  if (normalized === 'preserve_only') return 'preserve_only';
  if (normalized === 'derive_from_latlon') return 'derive_from_latlon';
  if (normalized === 'none') return 'none';
  return 'auto';
}

export function normalizeCrsCode(value: unknown): string {
  const raw = String(value || '').trim();
  return raw || 'EPSG:4326';
}

export function isGeojsonMergeFieldDefinition(field: MergeFieldDefinition | null | undefined): boolean {
  if (!field) return false;
  const key = String(field?.key || '')
    .trim()
    .toLowerCase();
  const type = String(field?.type || '')
    .trim()
    .toLowerCase();
  return type === 'geojson' || key === 'geojson' || key.endsWith('geojson');
}

export function normalizeGeojsonCellValue(rawValue: unknown): any | null {
  const parsed = parseGeoJsonLike(rawValue);
  if (!parsed || typeof parsed !== 'object') return null;
  if (parsed?.type === 'FeatureCollection') {
    const firstFeature = Array.isArray(parsed?.features) ? parsed.features[0] : null;
    if (!firstFeature?.geometry?.type) return null;
    return {
      type: 'Feature',
      properties: firstFeature?.properties || {},
      geometry: firstFeature.geometry
    };
  }
  if (parsed?.type === 'Feature') {
    if (!parsed?.geometry?.type) return null;
    return {
      type: 'Feature',
      properties: parsed?.properties || {},
      geometry: parsed.geometry
    };
  }
  if (parsed?.type) {
    return {
      type: 'Feature',
      properties: {},
      geometry: parsed
    };
  }
  return null;
}

export function convertPointToWgs84(lon: number, lat: number, sourceCrs: string): [number, number] | null {
  const source = normalizeCrsCode(sourceCrs);
  if (source.toUpperCase() === 'EPSG:4326') {
    return [lon, lat];
  }
  try {
    const transformed = proj4(source, 'EPSG:4326', [lon, lat]) as [number, number];
    if (!Array.isArray(transformed) || transformed.length < 2) return null;
    const outLon = Number(transformed[0]);
    const outLat = Number(transformed[1]);
    if (!Number.isFinite(outLon) || !Number.isFinite(outLat)) return null;
    return [outLon, outLat];
  } catch {
    return null;
  }
}

export function getMergeDatasetGeometryReadiness(
  dataset: any,
  requestedLatFieldName?: string | null,
  requestedLonFieldName?: string | null
): MergeGeometryReadiness {
  const geometryField = resolveGeojsonFieldName(dataset, null);
  const pointFields = resolveDatasetPointFieldPair(dataset, requestedLatFieldName, requestedLonFieldName);
  return {
    hasGeometryField: Boolean(geometryField),
    hasPointPair: Boolean(pointFields.latField && pointFields.lonField),
    geometryFieldName: geometryField,
    pointFields
  };
}
