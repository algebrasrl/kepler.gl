import {
  booleanContains as turfBooleanContains,
  booleanIntersects as turfBooleanIntersects,
  booleanTouches as turfBooleanTouches,
  booleanWithin as turfBooleanWithin,
  buffer as turfBuffer,
  centroid as turfCentroid,
  difference as turfDifference,
  distance as turfDistance,
  featureCollection as turfFeatureCollection,
  intersect as turfIntersect
} from '@turf/turf';

// ── Message types ──

type SpatialJoinPayload = {
  predicate: 'intersects' | 'within' | 'contains' | 'touches';
  aggregations: Array<'count' | 'sum' | 'avg' | 'min' | 'max'>;
  leftFeatures: Array<{
    properties: Record<string, unknown>;
    geometry: unknown;
  }>;
  rightFeatures: Array<{
    geometry: unknown;
    value: number;
    pickedFields: Record<string, unknown>;
    bbox: [number, number, number, number] | null;
  }>;
  includeRightFields: string[];
};

type OverlayDiffPayload = {
  includeIntersection: boolean;
  includeADifference: boolean;
  includeBDifference: boolean;
  aFeatures: Array<{
    rowIdx: number;
    geometry: unknown;
    bbox: [number, number, number, number] | null;
  }>;
  bFeatures: Array<{
    rowIdx: number;
    geometry: unknown;
    bbox: [number, number, number, number] | null;
  }>;
};

type BufferSummarizePayload = {
  radiusKm: number;
  aggregation: 'count' | 'sum' | 'avg';
  outputFieldName: string;
  sourceFeatures: Array<{
    properties: Record<string, unknown>;
    geometry: unknown;
  }>;
  targetFeatures: Array<{
    geometry: unknown;
    value: number;
    bbox: [number, number, number, number] | null;
  }>;
};

type AdjacencyGraphPayload = {
  predicate: 'touches' | 'intersects';
  features: Array<{
    nodeId: string;
    geometry: unknown;
    bbox: [number, number, number, number] | null;
  }>;
};

type NearestFeatureJoinPayload = {
  sourceFeatures: Array<{
    properties: Record<string, unknown>;
    geometry: unknown;
  }>;
  targetFeatures: Array<{
    geometry: unknown;
    picked: unknown;
  }>;
  k: number;
  maxDistanceKm: number | null;
  includeTargetField: string | null;
};

type CoverageQualityReportPayload = {
  predicate: 'intersects' | 'within' | 'contains' | 'touches';
  leftFeatures: Array<{
    geometry: unknown;
  }>;
  rightFeatures: Array<{
    geometry: unknown;
    value: unknown;
    bbox: [number, number, number, number] | null;
  }>;
  hasValueField: boolean;
};

type RequestMessage =
  | {id: string; type: 'spatialJoinByPredicate'; payload: SpatialJoinPayload}
  | {id: string; type: 'overlayDifference'; payload: OverlayDiffPayload}
  | {id: string; type: 'bufferAndSummarize'; payload: BufferSummarizePayload}
  | {id: string; type: 'adjacencyGraph'; payload: AdjacencyGraphPayload}
  | {id: string; type: 'nearestFeatureJoin'; payload: NearestFeatureJoinPayload}
  | {id: string; type: 'coverageQualityReport'; payload: CoverageQualityReportPayload};

type ResultMessage = {
  id: string;
  type: 'result';
  payload: unknown;
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

// ── Shared utilities (from zonal-ops.worker) ──

function parseGeoJsonLike(raw: unknown): any | null {
  if (raw === null || raw === undefined) return null;
  if (typeof raw === 'object') return raw;
  if (typeof raw !== 'string') return null;
  const text = (raw as string).trim();
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

function turfDifferenceSafe(a: any, b: any): any | null {
  try {
    const diff = turfDifference(turfFeatureCollection([a, b]) as any);
    if (diff) return diff;
  } catch {
    // fallback for older/newer Turf signatures
  }
  try {
    const diff = (turfDifference as any)(a, b);
    return diff || null;
  } catch {
    return null;
  }
}

function sendProgress(id: string, processed: number, total: number) {
  const msg: ProgressMessage = {id, type: 'progress', payload: {processed, total}};
  self.postMessage(msg);
}

// ── Handlers ──

function handleSpatialJoin(id: string, payload: SpatialJoinPayload) {
  const predicate = payload.predicate || 'intersects';
  const opSet = new Set(payload.aggregations?.length ? payload.aggregations : ['count']);
  const leftFeatures = Array.isArray(payload.leftFeatures) ? payload.leftFeatures : [];
  const rightFeatures = Array.isArray(payload.rightFeatures) ? payload.rightFeatures : [];

  // Parse right features and compute bbox
  const rightParsed: Array<{feature: any; value: number; pickedFields: Record<string, unknown>; bbox: BBox | null}> = [];
  for (let i = 0; i < rightFeatures.length; i += 1) {
    const rf = rightFeatures[i];
    const feature = toTurfFeature(parseGeoJsonLike(rf.geometry));
    if (!feature) continue;
    rightParsed.push({
      feature,
      value: Number(rf.value),
      pickedFields: rf.pickedFields || {},
      bbox: rf.bbox || geometryToBbox((feature as any)?.geometry)
    });
  }

  const rows: Array<Record<string, unknown>> = [];
  const total = leftFeatures.length;
  const includeRightFields = Array.isArray(payload.includeRightFields) ? payload.includeRightFields : [];

  for (let i = 0; i < leftFeatures.length; i += 1) {
    const lf = leftFeatures[i];
    const leftFeature = toTurfFeature(parseGeoJsonLike(lf.geometry));
    if (!leftFeature) {
      if (i % 50 === 0) sendProgress(id, i + 1, total);
      continue;
    }
    const leftBbox = geometryToBbox((leftFeature as any)?.geometry);
    const candidates = leftBbox
      ? rightParsed.filter(item => !item.bbox || boundsOverlap(leftBbox, item.bbox))
      : rightParsed;

    let matchCount = 0;
    let sum = 0;
    let numericCount = 0;
    let min: number | null = null;
    let max: number | null = null;
    let singlePicked: Record<string, unknown> | null = null;

    for (let j = 0; j < candidates.length; j += 1) {
      const item = candidates[j];
      let matched = false;
      try {
        if (predicate === 'within') matched = turfBooleanWithin(leftFeature, item.feature);
        else if (predicate === 'contains') matched = turfBooleanContains(leftFeature, item.feature);
        else if (predicate === 'touches') matched = turfBooleanTouches(leftFeature, item.feature);
        else matched = turfBooleanIntersects(leftFeature, item.feature);
      } catch {
        matched = false;
      }
      if (!matched) continue;
      matchCount += 1;
      if (matchCount === 1) {
        singlePicked = item.pickedFields || null;
      } else {
        singlePicked = null;
      }
      if (Number.isFinite(item.value)) {
        const v = Number(item.value);
        sum += v;
        numericCount += 1;
        min = min === null ? v : Math.min(min, v);
        max = max === null ? v : Math.max(max, v);
      }
    }

    const base: Record<string, unknown> = {...(lf.properties || {})};
    base.join_count = matchCount;
    if (opSet.has('sum')) base.join_sum = sum;
    if (opSet.has('avg')) base.join_avg = numericCount > 0 ? sum / numericCount : null;
    if (opSet.has('min')) base.join_min = min;
    if (opSet.has('max')) base.join_max = max;
    if (singlePicked && includeRightFields.length) {
      Object.entries(singlePicked).forEach(([k, v]) => {
        base[`right_${k}`] = v;
      });
    }
    rows.push(base);

    if (i % 50 === 0) sendProgress(id, i + 1, total);
  }

  const result: ResultMessage = {id, type: 'result', payload: {rows}};
  self.postMessage(result);
}

function handleOverlayDifference(id: string, payload: OverlayDiffPayload) {
  const includeIntersection = payload.includeIntersection !== false;
  const includeADifference = payload.includeADifference !== false;
  const includeBDifference = payload.includeBDifference === true;
  const rawA = Array.isArray(payload.aFeatures) ? payload.aFeatures : [];
  const rawB = Array.isArray(payload.bFeatures) ? payload.bFeatures : [];

  // Parse features
  const aFeatures: Array<{rowIdx: number; feature: any; bbox: BBox | null}> = [];
  for (let i = 0; i < rawA.length; i += 1) {
    const feature = toTurfPolygonFeature(parseGeoJsonLike(rawA[i].geometry));
    if (!feature) continue;
    aFeatures.push({
      rowIdx: rawA[i].rowIdx,
      feature,
      bbox: rawA[i].bbox || geometryToBbox((feature as any)?.geometry)
    });
  }
  const bFeatures: Array<{rowIdx: number; feature: any; bbox: BBox | null}> = [];
  for (let i = 0; i < rawB.length; i += 1) {
    const feature = toTurfPolygonFeature(parseGeoJsonLike(rawB[i].geometry));
    if (!feature) continue;
    bFeatures.push({
      rowIdx: rawB[i].rowIdx,
      feature,
      bbox: rawB[i].bbox || geometryToBbox((feature as any)?.geometry)
    });
  }

  const total = aFeatures.length + (includeBDifference ? bFeatures.length : 0);
  let processed = 0;
  const out: Array<{_geojson: unknown; overlay_type: string; a_row?: number; b_row?: number}> = [];

  for (let i = 0; i < aFeatures.length; i += 1) {
    const af = aFeatures[i];
    const sourceBbox = af.bbox;
    const candidateB = sourceBbox
      ? bFeatures.filter(bf => !bf.bbox || boundsOverlap(sourceBbox, bf.bbox))
      : bFeatures;

    if (includeIntersection) {
      for (let j = 0; j < candidateB.length; j += 1) {
        const bf = candidateB[j];
        const inter = turfIntersectSafe(af.feature, bf.feature);
        if (inter) {
          out.push({
            _geojson: inter.geometry || inter,
            overlay_type: 'intersection',
            a_row: af.rowIdx,
            b_row: bf.rowIdx
          });
        }
      }
    }
    if (includeADifference) {
      let diffFeature = af.feature;
      for (let j = 0; j < candidateB.length; j += 1) {
        const bf = candidateB[j];
        const next = turfDifferenceSafe(diffFeature, bf.feature);
        if (next) diffFeature = next;
      }
      out.push({_geojson: diffFeature.geometry || diffFeature, overlay_type: 'a_minus_b', a_row: af.rowIdx});
    }

    processed += 1;
    if (processed % 50 === 0) sendProgress(id, processed, total);
  }

  if (includeBDifference) {
    for (let i = 0; i < bFeatures.length; i += 1) {
      const bf = bFeatures[i];
      const sourceBbox = bf.bbox;
      const candidateA = sourceBbox
        ? aFeatures.filter(af => !af.bbox || boundsOverlap(sourceBbox, af.bbox))
        : aFeatures;
      let diffFeature = bf.feature;
      for (let j = 0; j < candidateA.length; j += 1) {
        const af = candidateA[j];
        const next = turfDifferenceSafe(diffFeature, af.feature);
        if (next) diffFeature = next;
      }
      out.push({_geojson: diffFeature.geometry || diffFeature, overlay_type: 'b_minus_a', b_row: bf.rowIdx});

      processed += 1;
      if (processed % 50 === 0) sendProgress(id, processed, total);
    }
  }

  const result: ResultMessage = {id, type: 'result', payload: {rows: out}};
  self.postMessage(result);
}

function handleBufferAndSummarize(id: string, payload: BufferSummarizePayload) {
  const radiusKm = Number(payload.radiusKm);
  const aggregation = payload.aggregation || 'count';
  const outputFieldName = payload.outputFieldName || 'buffer_metric';
  const sourceFeatures = Array.isArray(payload.sourceFeatures) ? payload.sourceFeatures : [];
  const rawTargets = Array.isArray(payload.targetFeatures) ? payload.targetFeatures : [];

  // Parse target features
  const targetFeatures: Array<{feature: any; value: number; bbox: BBox | null}> = [];
  for (let i = 0; i < rawTargets.length; i += 1) {
    const feature = toTurfFeature(parseGeoJsonLike(rawTargets[i].geometry));
    if (!feature) continue;
    targetFeatures.push({
      feature,
      value: Number(rawTargets[i].value),
      bbox: rawTargets[i].bbox || geometryToBbox((feature as any)?.geometry)
    });
  }

  const rows: Array<Record<string, unknown>> = [];
  const total = sourceFeatures.length;

  for (let i = 0; i < sourceFeatures.length; i += 1) {
    const sf = sourceFeatures[i];
    const srcFeature = toTurfFeature(parseGeoJsonLike(sf.geometry));
    if (!srcFeature) {
      if (i % 50 === 0) sendProgress(id, i + 1, total);
      continue;
    }

    let buffered: any = null;
    try {
      buffered = turfBuffer(srcFeature as any, radiusKm, {units: 'kilometers'});
    } catch {
      buffered = null;
    }
    if (!buffered) {
      if (i % 50 === 0) sendProgress(id, i + 1, total);
      continue;
    }

    const bufferedBbox = geometryToBbox((buffered as any)?.geometry);
    const candidateTargets = bufferedBbox
      ? targetFeatures.filter(item => !item.bbox || boundsOverlap(bufferedBbox, item.bbox))
      : targetFeatures;

    const nums: number[] = [];
    let matches = 0;
    for (let j = 0; j < candidateTargets.length; j += 1) {
      const tf = candidateTargets[j];
      let intersects = false;
      try {
        intersects = turfBooleanIntersects(buffered, tf.feature);
      } catch {
        intersects = false;
      }
      if (!intersects) continue;
      matches += 1;
      if (Number.isFinite(tf.value)) {
        nums.push(Number(tf.value));
      }
    }

    const metric =
      aggregation === 'sum'
        ? nums.reduce((a, v) => a + v, 0)
        : aggregation === 'avg'
        ? nums.length
          ? nums.reduce((a, v) => a + v, 0) / nums.length
          : null
        : matches;

    const row: Record<string, unknown> = {...(sf.properties || {})};
    row[outputFieldName] = metric;
    rows.push(row);

    if (i % 50 === 0) sendProgress(id, i + 1, total);
  }

  const result: ResultMessage = {id, type: 'result', payload: {rows}};
  self.postMessage(result);
}

function handleAdjacencyGraph(id: string, payload: AdjacencyGraphPayload) {
  const adjacencyPredicate = payload.predicate === 'intersects' ? 'intersects' : 'touches';
  const rawFeatures = Array.isArray(payload.features) ? payload.features : [];

  // Parse features
  const features: Array<{nodeId: string; feature: any; bbox: BBox | null}> = [];
  for (let i = 0; i < rawFeatures.length; i += 1) {
    const feature = toTurfPolygonFeature(parseGeoJsonLike(rawFeatures[i].geometry));
    if (!feature) continue;
    features.push({
      nodeId: rawFeatures[i].nodeId,
      feature,
      bbox: rawFeatures[i].bbox || geometryToBbox((feature as any)?.geometry)
    });
  }

  const edges: Array<{source_id: string; target_id: string; predicate: string}> = [];
  const total = features.length;

  for (let i = 0; i < features.length; i += 1) {
    const leftNode = features[i];
    for (let j = i + 1; j < features.length; j += 1) {
      const rightNode = features[j];
      if (leftNode.bbox && rightNode.bbox && !boundsOverlap(leftNode.bbox, rightNode.bbox)) {
        continue;
      }
      let matched = false;
      try {
        matched =
          adjacencyPredicate === 'intersects'
            ? turfBooleanIntersects(leftNode.feature, rightNode.feature)
            : turfBooleanTouches(leftNode.feature, rightNode.feature);
      } catch {
        matched = false;
      }
      if (matched) {
        edges.push({
          source_id: leftNode.nodeId,
          target_id: rightNode.nodeId,
          predicate: adjacencyPredicate
        });
      }
    }

    if (i % 50 === 0) sendProgress(id, i + 1, total);
  }

  const result: ResultMessage = {id, type: 'result', payload: {edges}};
  self.postMessage(result);
}

function handleNearestFeatureJoin(id: string, payload: NearestFeatureJoinPayload) {
  const rawSources = Array.isArray(payload.sourceFeatures) ? payload.sourceFeatures : [];
  const rawTargets = Array.isArray(payload.targetFeatures) ? payload.targetFeatures : [];
  const topK = Math.max(1, Number(payload.k || 1));
  const maxDistanceKm = payload.maxDistanceKm !== null && payload.maxDistanceKm !== undefined && Number.isFinite(Number(payload.maxDistanceKm)) ? Number(payload.maxDistanceKm) : null;
  const includeTargetField = payload.includeTargetField || null;

  // Parse target features and compute centroids
  const targets: Array<{centroid: any; lonLat: [number, number]; picked: unknown}> = [];
  for (let i = 0; i < rawTargets.length; i += 1) {
    const feature = toTurfFeature(parseGeoJsonLike(rawTargets[i].geometry));
    if (!feature) continue;
    const centroid = turfCentroid(feature as any);
    const coords = (centroid as any)?.geometry?.coordinates;
    if (!Array.isArray(coords) || coords.length < 2) continue;
    targets.push({centroid, lonLat: [Number(coords[0]), Number(coords[1])], picked: rawTargets[i].picked});
  }

  const rows: Array<Record<string, unknown>> = [];
  const total = rawSources.length;

  for (let i = 0; i < rawSources.length; i += 1) {
    const sf = rawSources[i];
    const srcFeature = toTurfFeature(parseGeoJsonLike(sf.geometry));
    if (!srcFeature) {
      if (i % 50 === 0) sendProgress(id, i + 1, total);
      continue;
    }
    const srcCentroid = turfCentroid(srcFeature as any);
    const srcCoords = (srcCentroid as any)?.geometry?.coordinates;
    if (!Array.isArray(srcCoords) || srcCoords.length < 2) {
      if (i % 50 === 0) sendProgress(id, i + 1, total);
      continue;
    }
    const srcLonLat: [number, number] = [Number(srcCoords[0]), Number(srcCoords[1])];

    const nearest: Array<{distanceKm: number; picked: unknown}> = [];
    for (let j = 0; j < targets.length; j += 1) {
      const t = targets[j];
      // Cheap coarse filter before expensive turfDistance
      if (maxDistanceKm !== null) {
        const dx = srcLonLat[0] - t.lonLat[0];
        const dy = srcLonLat[1] - t.lonLat[1];
        const approxKm = Math.sqrt(dx * dx + dy * dy) * 111;
        if (approxKm > maxDistanceKm * 1.5) continue;
      }
      let distanceKm: number;
      try {
        distanceKm = turfDistance(srcCentroid as any, t.centroid as any, {units: 'kilometers'});
      } catch {
        continue;
      }
      if (maxDistanceKm !== null && distanceKm > maxDistanceKm) continue;
      nearest.push({distanceKm, picked: t.picked});
    }
    nearest.sort((a, b) => a.distanceKm - b.distanceKm);
    const top = nearest.slice(0, topK);

    const row: Record<string, unknown> = {...(sf.properties || {})};
    row.nearest_count = top.length;
    row.nearest_distance_km = top.length ? top[0].distanceKm : null;
    if (includeTargetField && top.length) {
      row[`nearest_${includeTargetField}`] = top[0].picked;
    }
    rows.push(row);

    if (i % 50 === 0) sendProgress(id, i + 1, total);
  }

  const result: ResultMessage = {id, type: 'result', payload: {rows}};
  self.postMessage(result);
}

function handleCoverageQualityReport(id: string, payload: CoverageQualityReportPayload) {
  const op = payload.predicate || 'intersects';
  const hasValueField = payload.hasValueField === true;
  const rawLeft = Array.isArray(payload.leftFeatures) ? payload.leftFeatures : [];
  const rawRight = Array.isArray(payload.rightFeatures) ? payload.rightFeatures : [];

  // Parse right features
  const rightFeatures: Array<{feature: any; value: unknown; bbox: BBox | null}> = [];
  for (let i = 0; i < rawRight.length; i += 1) {
    const feature = toTurfFeature(parseGeoJsonLike(rawRight[i].geometry));
    if (!feature) continue;
    rightFeatures.push({
      feature,
      value: rawRight[i].value,
      bbox: rawRight[i].bbox || geometryToBbox((feature as any)?.geometry)
    });
  }

  let matched = 0;
  let nullJoined = 0;
  const total = rawLeft.length;

  for (let i = 0; i < rawLeft.length; i += 1) {
    const leftFeature = toTurfFeature(parseGeoJsonLike(rawLeft[i].geometry));
    if (!leftFeature) {
      if (i % 50 === 0) sendProgress(id, i + 1, total);
      continue;
    }
    const leftBbox = geometryToBbox((leftFeature as any)?.geometry);

    let hasMatch = false;
    let hasAnyNonNull = false;
    for (let j = 0; j < rightFeatures.length; j += 1) {
      const candidate = rightFeatures[j];
      if (leftBbox && candidate.bbox && !boundsOverlap(leftBbox, candidate.bbox)) {
        continue;
      }
      let isMatch = false;
      try {
        if (op === 'within') isMatch = turfBooleanWithin(leftFeature, candidate.feature);
        else if (op === 'contains') isMatch = turfBooleanContains(leftFeature, candidate.feature);
        else if (op === 'touches') isMatch = turfBooleanTouches(leftFeature, candidate.feature);
        else isMatch = turfBooleanIntersects(leftFeature, candidate.feature);
      } catch {
        isMatch = false;
      }
      if (isMatch) {
        hasMatch = true;
        if (hasValueField && candidate.value !== null && candidate.value !== undefined && candidate.value !== '') {
          hasAnyNonNull = true;
        }
        if (!hasValueField || hasAnyNonNull) break;
      }
    }
    if (hasMatch) matched += 1;
    if (hasValueField && hasMatch && !hasAnyNonNull) nullJoined += 1;

    if (i % 50 === 0) sendProgress(id, i + 1, total);
  }

  const result: ResultMessage = {id, type: 'result', payload: {matched, nullJoined, total}};
  self.postMessage(result);
}

// ── Dispatcher ──

self.onmessage = (event: MessageEvent<RequestMessage>) => {
  const message = event.data;
  if (!message || !message.id || !message.type) return;
  const {id} = message;

  try {
    switch (message.type) {
      case 'spatialJoinByPredicate':
        handleSpatialJoin(id, message.payload);
        break;
      case 'overlayDifference':
        handleOverlayDifference(id, message.payload);
        break;
      case 'bufferAndSummarize':
        handleBufferAndSummarize(id, message.payload);
        break;
      case 'adjacencyGraph':
        handleAdjacencyGraph(id, message.payload);
        break;
      case 'nearestFeatureJoin':
        handleNearestFeatureJoin(id, message.payload);
        break;
      case 'coverageQualityReport':
        handleCoverageQualityReport(id, message.payload);
        break;
      default: {
        const err: ErrorMessage = {id, type: 'error', error: `Unknown spatial-ops job type: ${(message as any).type}`};
        self.postMessage(err);
      }
    }
  } catch (error) {
    const err: ErrorMessage = {
      id,
      type: 'error',
      error: error instanceof Error ? error.message : String(error)
    };
    self.postMessage(err);
  }
};
