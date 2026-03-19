import turfArea from '@turf/area';
import turfBooleanIntersects from '@turf/boolean-intersects';
import turfBooleanPointInPolygon from '@turf/boolean-point-in-polygon';
import turfBooleanWithin from '@turf/boolean-within';
import turfCentroid from '@turf/centroid';
import turfDifference from '@turf/difference';
import {featureCollection as turfFeatureCollection} from '@turf/helpers';
import turfIntersect from '@turf/intersect';
import {computeClipPropertyCountsFromPropertyRows} from './clip-counts';

export type ClipMode = 'intersects' | 'centroid' | 'within';

export type ClipMetricsFeatureInput = {
  feature: any;
  properties?: Record<string, unknown>;
};

export type ClipMetrics = {
  matchCount: number;
  intersectionAreaM2: number;
  intersectionPct: number;
  distinctValueCounts: Record<string, number>;
  propertyValueMatchCounts: Record<string, Record<string, number>>;
};

type ClipMetricsOptions = {
  mode: ClipMode;
  includeAreaMetrics: boolean;
  includeDistinctCounts: boolean;
  includeValueCountFields: boolean;
};

function turfIntersectSafe(a: any, b: any): any | null {
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

function turfDifferenceSafe(a: any, b: any): any | null {
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

export function matchesClipPredicate(
  sourceFeature: any,
  clipFeature: any,
  mode: ClipMode,
  sourceCentroid?: any | null
): boolean {
  try {
    if (mode === 'centroid') {
      const centroid = sourceCentroid || turfCentroid(sourceFeature);
      return turfBooleanPointInPolygon(centroid, clipFeature);
    }
    if (mode === 'within') {
      return turfBooleanWithin(sourceFeature, clipFeature);
    }
    return turfBooleanIntersects(sourceFeature, clipFeature);
  } catch {
    return false;
  }
}

export function computeClipMetricsForFeature(
  sourceFeature: any,
  clipFeatures: ClipMetricsFeatureInput[],
  options: ClipMetricsOptions
): ClipMetrics {
  if (!sourceFeature || !Array.isArray(clipFeatures) || !clipFeatures.length) {
    return {
      matchCount: 0,
      intersectionAreaM2: 0,
      intersectionPct: 0,
      distinctValueCounts: {},
      propertyValueMatchCounts: {}
    };
  }

  const mode = options?.mode || 'intersects';
  const includeAreaMetrics = options?.includeAreaMetrics === true;
  const includeDistinctCounts = options?.includeDistinctCounts === true;
  const includeValueCountFields = options?.includeValueCountFields === true;
  const sourceCentroid = mode === 'centroid' ? turfCentroid(sourceFeature) : null;
  const matchedClipFeatures: ClipMetricsFeatureInput[] = [];
  for (const clipFeatureRow of clipFeatures) {
    const clipFeature = clipFeatureRow?.feature;
    if (!clipFeature) continue;
    if (matchesClipPredicate(sourceFeature, clipFeature, mode, sourceCentroid)) {
      matchedClipFeatures.push(clipFeatureRow);
    }
  }

  const matchCount = matchedClipFeatures.length;
  if (!matchCount) {
    return {
      matchCount: 0,
      intersectionAreaM2: 0,
      intersectionPct: 0,
      distinctValueCounts: {},
      propertyValueMatchCounts: {}
    };
  }

  const {distinctValueCounts, propertyValueMatchCounts} = computeClipPropertyCountsFromPropertyRows(
    matchedClipFeatures.map(entry => entry?.properties || {}),
    {includeDistinctCounts, includeValueCountFields}
  );

  if (!includeAreaMetrics) {
    return {matchCount, intersectionAreaM2: 0, intersectionPct: 0, distinctValueCounts, propertyValueMatchCounts};
  }

  const sourceAreaM2 = Math.max(0, Number(turfArea(sourceFeature as any)) || 0);
  if (!(sourceAreaM2 > 0)) {
    return {matchCount, intersectionAreaM2: 0, intersectionPct: 0, distinctValueCounts, propertyValueMatchCounts};
  }

  let remaining: any = sourceFeature;
  let coveredAreaM2 = 0;
  for (const clipFeatureEntry of matchedClipFeatures) {
    const clipFeature = clipFeatureEntry?.feature;
    if (!clipFeature) continue;
    if (!remaining) break;
    const inter = turfIntersectSafe(remaining, clipFeature);
    if (!inter) continue;
    const interArea = Math.max(0, Number(turfArea(inter as any)) || 0);
    if (interArea > 0) {
      coveredAreaM2 += interArea;
    }
    const nextRemaining = turfDifferenceSafe(remaining, clipFeature);
    remaining = nextRemaining || null;
  }
  coveredAreaM2 = Math.min(sourceAreaM2, Math.max(0, coveredAreaM2));

  return {
    matchCount,
    intersectionAreaM2: Number(coveredAreaM2.toFixed(2)),
    intersectionPct: Number(((coveredAreaM2 / sourceAreaM2) * 100).toFixed(2)),
    distinctValueCounts,
    propertyValueMatchCounts
  };
}
