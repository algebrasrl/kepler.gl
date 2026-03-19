import {area as turfArea, featureCollection as turfFeatureCollection, union as turfUnion} from '@turf/turf';

function asPolygonFeature(geometry: any): any | null {
  if (!geometry?.type) {
    return null;
  }
  const type = String(geometry.type);
  if (type !== 'Polygon' && type !== 'MultiPolygon') {
    return null;
  }
  if (!Array.isArray(geometry.coordinates) || !geometry.coordinates.length) {
    return null;
  }
  return {
    type: 'Feature',
    properties: {},
    geometry
  };
}

function cloneFeature(input: any): any {
  return JSON.parse(JSON.stringify(input));
}

function safeUnionPair(left: any, right: any): any | null {
  if (!left || !right) {
    return null;
  }
  try {
    return turfUnion(turfFeatureCollection([left, right]) as any) as any;
  } catch {
    try {
      return (turfUnion as any)(left, right) as any;
    } catch {
      return null;
    }
  }
}

function safeFeatureAreaM2(feature: any): number {
  if (!feature) {
    return 0;
  }
  try {
    return Math.max(0, Number(turfArea(feature as any)) || 0);
  } catch {
    return 0;
  }
}

export function computePolygonGeometriesAreaM2(geometries: any[]): number {
  const features = (geometries || []).map(asPolygonFeature).filter(Boolean);
  if (!features.length) {
    return 0;
  }

  let merged = cloneFeature(features[0]);
  let canUseUnionArea = true;

  for (let index = 1; index < features.length; index += 1) {
    const unioned = safeUnionPair(merged, features[index]);
    if (!unioned) {
      canUseUnionArea = false;
      break;
    }
    merged = unioned;
  }

  if (canUseUnionArea) {
    return safeFeatureAreaM2(merged);
  }

  // Fail closed on topology issues by summing individual polygon areas.
  return features.reduce((sum: number, feature: any) => sum + safeFeatureAreaM2(feature), 0);
}

export function evaluateGeotokenTessellationArea(geometries: any[], maxAreaKm2: number) {
  const areaM2 = computePolygonGeometriesAreaM2(geometries);
  const maxAreaM2 = Math.max(0, Number(maxAreaKm2) || 0) * 1_000_000;
  return {
    areaM2,
    areaKm2: areaM2 / 1_000_000,
    maxAreaKm2: Math.max(0, Number(maxAreaKm2) || 0),
    exceedsLimit: maxAreaM2 > 0 && areaM2 > maxAreaM2
  };
}
