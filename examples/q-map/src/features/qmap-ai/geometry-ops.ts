import {
  area as turfArea,
  bboxPolygon as turfBboxPolygon,
  buffer as turfBuffer,
  cleanCoords as turfCleanCoords,
  difference as turfDifference,
  featureCollection as turfFeatureCollection,
  intersect as turfIntersect,
  lineSplit as turfLineSplit,
  pointOnFeature as turfPointOnFeature,
  polygonToLine as turfPolygonToLine,
  polygonize as turfPolygonize,
  simplify as turfSimplify,
  union as turfUnion,
  booleanPointInPolygon as turfBooleanPointInPolygon
} from '@turf/turf';

export type QMapFeatureLike = any;

function asFeature(input: any): any | null {
  if (!input) return null;
  if (input?.type === 'Feature' && input?.geometry?.type) return input;
  if (input?.type && input?.coordinates) {
    return {type: 'Feature', properties: {}, geometry: input};
  }
  return null;
}

function cloneFeature(input: any): any {
  return JSON.parse(JSON.stringify(input));
}

function safeUnionPair(a: any, b: any): any | null {
  if (!a || !b) return null;
  try {
    return turfUnion(turfFeatureCollection([a, b]) as any) as any;
  } catch {
    try {
      return (turfUnion as any)(a, b) as any;
    } catch {
      return null;
    }
  }
}

function safeIntersectPair(a: any, b: any): any | null {
  if (!a || !b) return null;
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

function safeDifferencePair(a: any, b: any): any | null {
  if (!a || !b) return null;
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

function explodeFeatureToPolygonFeatures(feature: any): any[] {
  const asFeat = asFeature(feature);
  if (!asFeat?.geometry?.type) return [];
  const type = String(asFeat.geometry.type);
  if (type === 'Polygon') {
    return [asFeat];
  }
  if (type === 'MultiPolygon') {
    const coords = Array.isArray(asFeat.geometry.coordinates) ? asFeat.geometry.coordinates : [];
    return coords
      .filter((polygon: any) => Array.isArray(polygon) && polygon.length > 0)
      .map((polygon: any) => ({
        type: 'Feature',
        properties: {...(asFeat.properties || {})},
        geometry: {type: 'Polygon', coordinates: polygon}
      }));
  }
  return [];
}

function normalizeGroupValue(value: unknown): string {
  if (value === null || value === undefined || value === '') return '__null__';
  return String(value);
}

export function buildBboxFeature(
  minLon: number,
  minLat: number,
  maxLon: number,
  maxLat: number,
  properties?: Record<string, unknown>
): any {
  const bbox = [Number(minLon), Number(minLat), Number(maxLon), Number(maxLat)] as [number, number, number, number];
  const poly = turfBboxPolygon(bbox as any) as any;
  return {
    type: 'Feature',
    properties: {
      ...(properties || {}),
      minLon: bbox[0],
      minLat: bbox[1],
      maxLon: bbox[2],
      maxLat: bbox[3]
    },
    geometry: poly?.geometry || poly
  };
}

export function unionFeatures(features: QMapFeatureLike[]): any | null {
  const valid = (features || []).map(asFeature).filter(Boolean);
  if (!valid.length) return null;
  let acc = cloneFeature(valid[0]);
  for (let i = 1; i < valid.length; i += 1) {
    const next = safeUnionPair(acc, valid[i]);
    if (next) {
      acc = next;
    }
  }
  return asFeature(acc);
}

export function intersectFeatureSets(left: QMapFeatureLike[], right: QMapFeatureLike[]): any | null {
  const leftUnion = unionFeatures(left);
  const rightUnion = unionFeatures(right);
  if (!leftUnion || !rightUnion) return null;
  const inter = safeIntersectPair(leftUnion, rightUnion);
  return asFeature(inter);
}

export function symmetricDifferenceFeatureSets(left: QMapFeatureLike[], right: QMapFeatureLike[]): any | null {
  const leftUnion = unionFeatures(left);
  const rightUnion = unionFeatures(right);
  if (!leftUnion || !rightUnion) return null;
  const leftMinusRight = safeDifferencePair(leftUnion, rightUnion);
  const rightMinusLeft = safeDifferencePair(rightUnion, leftUnion);
  const out = unionFeatures([leftMinusRight, rightMinusLeft].filter(Boolean));
  return asFeature(out);
}

export function dissolveFeaturesByProperty(
  features: Array<{feature: QMapFeatureLike; properties?: Record<string, unknown>}>,
  propertyName?: string
): Array<{feature: any; groupValue: string; featureCount: number}> {
  const grouped = new Map<string, any[]>();
  (features || []).forEach(entry => {
    const feature = asFeature(entry?.feature);
    if (!feature) return;
    const groupValue = propertyName ? normalizeGroupValue(entry?.properties?.[propertyName]) : '__all__';
    const bucket = grouped.get(groupValue) || [];
    bucket.push(feature);
    grouped.set(groupValue, bucket);
  });

  const rows: Array<{feature: any; groupValue: string; featureCount: number}> = [];
  grouped.forEach((bucket, groupValue) => {
    const dissolved = unionFeatures(bucket);
    if (!dissolved) return;
    rows.push({feature: dissolved, groupValue, featureCount: bucket.length});
  });
  return rows;
}

export function simplifyAndCleanFeatures(
  features: QMapFeatureLike[],
  tolerance = 0.0005,
  minAreaM2 = 0
): any[] {
  const out: any[] = [];
  const safeTolerance = Math.max(0, Number(tolerance) || 0);
  const safeMinArea = Math.max(0, Number(minAreaM2) || 0);
  (features || []).forEach(input => {
    const feature = asFeature(input);
    if (!feature) return;
    let current = cloneFeature(feature);
    try {
      current = turfCleanCoords(current as any) as any;
    } catch {
      // ignore clean failures
    }
    try {
      if (safeTolerance > 0) {
        current = turfSimplify(current as any, {tolerance: safeTolerance, highQuality: true}) as any;
      }
    } catch {
      // ignore simplify failures
    }

    const exploded = explodeFeatureToPolygonFeatures(current);
    if (exploded.length) {
      exploded.forEach(poly => {
        let area = 0;
        try {
          area = Number(turfArea(poly as any)) || 0;
        } catch {
          area = 0;
        }
        if (area >= safeMinArea) {
          out.push(poly);
        }
      });
      return;
    }

    out.push(current);
  });
  return out;
}

export function splitPolygonFeatureByLine(
  polygonFeatureRaw: QMapFeatureLike,
  lineFeatureRaw: QMapFeatureLike,
  lineBufferMeters = 0.5
): any[] {
  const polygonFeature = asFeature(polygonFeatureRaw);
  const lineFeature = asFeature(lineFeatureRaw);
  if (!polygonFeature || !lineFeature) return [];
  const polygonType = String(polygonFeature?.geometry?.type || '');
  const lineType = String(lineFeature?.geometry?.type || '');
  if (!['Polygon', 'MultiPolygon'].includes(polygonType)) return [];
  if (!['LineString', 'MultiLineString'].includes(lineType)) return [];

  try {
    const boundary = turfPolygonToLine(polygonFeature as any) as any;
    const splitLines = turfLineSplit(boundary as any, lineFeature as any) as any;
    const polygons = turfPolygonize(splitLines as any) as any;
    const polygonFeatures = Array.isArray(polygons?.features) ? polygons.features : [];
    const inSource = polygonFeatures.filter((candidate: any) => {
      try {
        const marker = turfPointOnFeature(candidate as any) as any;
        return turfBooleanPointInPolygon(marker as any, polygonFeature as any);
      } catch {
        return false;
      }
    });
    if (inSource.length >= 2) {
      return inSource.map((f: any) => asFeature(f)).filter(Boolean);
    }
  } catch {
    // fallback below
  }

  try {
    const buffered = turfBuffer(lineFeature as any, Math.max(0.1, Number(lineBufferMeters) || 0.5), {
      units: 'meters'
    }) as any;
    const diff = safeDifferencePair(polygonFeature, buffered);
    const parts = explodeFeatureToPolygonFeatures(diff);
    if (parts.length >= 2) return parts;
  } catch {
    // ignore
  }

  return [polygonFeature];
}

export function eraseFeatureByMasks(sourceFeatureRaw: QMapFeatureLike, masks: QMapFeatureLike[]): any | null {
  let current = asFeature(sourceFeatureRaw);
  if (!current) return null;
  const validMasks = (masks || []).map(asFeature).filter(Boolean);
  for (let i = 0; i < validMasks.length; i += 1) {
    const next = safeDifferencePair(current, validMasks[i]);
    if (next) {
      current = next;
    }
  }
  return asFeature(current);
}

export function featureAreaM2(featureRaw: QMapFeatureLike): number {
  const feature = asFeature(featureRaw);
  if (!feature) return 0;
  try {
    return Number(turfArea(feature as any)) || 0;
  } catch {
    return 0;
  }
}
