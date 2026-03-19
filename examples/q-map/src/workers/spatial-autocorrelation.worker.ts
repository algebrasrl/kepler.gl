import {centroid as turfCentroid} from '@turf/turf';
import {gridDisk, isValidCell} from 'h3-js-v4';
import {h3CellToPolygonFeature, normalizeH3Key} from './h3-geometry-utils';

// ─── Types ────────────────────────────────────────────────────────────────────

export type LisaCluster = 'HH' | 'LL' | 'HL' | 'LH' | 'NS';

export type SpatialAutocorrelationRequest = {
  id: string;
  type: 'lisa';
  payload: {
    features: Array<{
      geometry?: unknown;
      h3Id?: unknown;
      value: number | null;
    }>;
    weightType: 'queen' | 'knn';
    k: number;
    permutations: number;
    significance: number;
  };
};

export type BivariateRequest = {
  id: string;
  type: 'bivariate';
  payload: {
    featuresA: Array<{geometry?: unknown; h3Id?: unknown; value: number | null}>;
    featuresB: Array<{value: number | null}>;
    weightType: 'queen' | 'knn';
    k: number;
    permutations: number;
    significance: number;
  };
};

export type HotspotRequest = {
  id: string;
  type: 'hotspot';
  payload: {
    features: Array<{geometry?: unknown; h3Id?: unknown; value: number | null}>;
    weightType: 'queen' | 'knn';
    k: number;
    significance: number;
  };
};

export type HotspotResult = {
  id: string;
  type: 'hotspot_result';
  payload: {
    localGiStar: number[];
    pValues: number[];
    clusters: Array<'HH' | 'LL' | 'NS'>;
  };
};

export type BivariateResult = {
  id: string;
  type: 'bivariate_result';
  payload: {
    pearsonR: number;
    globalBivariateI: number;
    zScore: number;
    pValue: number;
    localI: number[];
    pValues: number[];
    clusters: LisaCluster[];
    lagValuesB: number[];
  };
};

export type SpatialAutocorrelationResult = {
  id: string;
  type: 'result';
  payload: {
    globalMoransI: number;
    zScore: number;
    pValue: number;
    localI: number[];
    pValues: number[];
    clusters: LisaCluster[];
    lagValues: number[];
  };
};

export type SpatialAutocorrelationError = {
  id: string;
  type: 'error';
  error: string;
};

export type SpatialAutocorrelationProgress = {
  id: string;
  type: 'progress';
  payload: {processed: number; total: number; phase: string};
};

type BBox = [number, number, number, number];

// ─── Geometry helpers ─────────────────────────────────────────────────────────

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
  value.forEach((item: any) => collectLonLatPairs(item, out));
}

function geometryToBbox(geometry: any): BBox | null {
  if (!geometry || typeof geometry !== 'object') return null;
  const coords = geometry.coordinates;
  const pairs: Array<[number, number]> = [];
  collectLonLatPairs(coords, pairs);
  if (!pairs.length) return null;
  const minLon = Math.min(...pairs.map(p => p[0]));
  const minLat = Math.min(...pairs.map(p => p[1]));
  const maxLon = Math.max(...pairs.map(p => p[0]));
  const maxLat = Math.max(...pairs.map(p => p[1]));
  if (![minLon, minLat, maxLon, maxLat].every(Number.isFinite)) return null;
  return [minLon, minLat, maxLon, maxLat];
}

function bboxOverlap(a: BBox, b: BBox): boolean {
  return !(a[2] < b[0] || a[0] > b[2] || a[3] < b[1] || a[1] > b[3]);
}

function bboxTouches(a: BBox, b: BBox): boolean {
  // Two bboxes touch or overlap when their expanded versions overlap
  const eps = 1e-8;
  return !(
    a[2] + eps < b[0] ||
    a[0] - eps > b[2] ||
    a[3] + eps < b[1] ||
    a[1] - eps > b[3]
  );
}

function getCentroidLonLat(feature: any): [number, number] | null {
  try {
    const c = turfCentroid(feature as any);
    const coords = (c as any)?.geometry?.coordinates;
    if (Array.isArray(coords) && coords.length >= 2 && Number.isFinite(Number(coords[0])) && Number.isFinite(Number(coords[1]))) {
      return [Number(coords[0]), Number(coords[1])];
    }
  } catch {
    // fall through
  }
  return null;
}

// ─── Contiguity tests ─────────────────────────────────────────────────────────

function polygonCoordPairs(geometry: any): Array<[number, number]> {
  const pairs: Array<[number, number]> = [];
  collectLonLatPairs(geometry?.coordinates, pairs);
  return pairs;
}

/**
 * Lightweight queen-contiguity test: two features are queen-adjacent when their
 * bounding boxes touch/overlap AND they share at least one vertex within a small
 * epsilon (shared vertex = queen adjacency).  This is fast (no turf import needed
 * in the worker) and correct for well-formed polygons.
 */
function queenAdjacent(
  pairsA: Array<[number, number]>,
  bboxA: BBox,
  pairsB: Array<[number, number]>,
  bboxB: BBox
): boolean {
  if (!bboxTouches(bboxA, bboxB)) return false;
  const eps = 1e-7;
  // Build a coarse lookup from A's vertices
  const setA = new Set(pairsA.map(([x, y]) => `${Math.round(x / eps)},${Math.round(y / eps)}`));
  for (const [x, y] of pairsB) {
    const key = `${Math.round(x / eps)},${Math.round(y / eps)}`;
    if (setA.has(key)) return true;
  }
  return false;
}

// ─── Spatial weights construction ─────────────────────────────────────────────

type FeatureEntry = {
  lonLat: [number, number] | null;
  bbox: BBox | null;
  pairs: Array<[number, number]>;
  h3Id: string | null;
  isH3: boolean;
};

function buildQueenWeights(entries: FeatureEntry[]): number[][] {
  const n = entries.length;
  const W: number[][] = Array.from({length: n}, () => Array(n).fill(0));
  for (let i = 0; i < n; i += 1) {
    const ei = entries[i];
    for (let j = i + 1; j < n; j += 1) {
      const ej = entries[j];
      let adjacent = false;
      if (ei.isH3 && ej.isH3 && ei.h3Id && ej.h3Id) {
        // H3 ring adjacency: disk of radius 1 around i includes j
        try {
          const disk = gridDisk(ei.h3Id, 1);
          adjacent = disk.includes(ej.h3Id);
        } catch {
          adjacent = false;
        }
      } else if (ei.bbox && ej.bbox && ei.pairs.length > 0 && ej.pairs.length > 0) {
        adjacent = queenAdjacent(ei.pairs, ei.bbox, ej.pairs, ej.bbox);
      }
      if (adjacent) {
        W[i][j] = 1;
        W[j][i] = 1;
      }
    }
  }
  return W;
}

/**
 * Haversine distance in km between two lon/lat points.
 * Used for KNN weight construction to avoid planar-distance distortion
 * at regional/national scales where Euclidean degrees are not uniform.
 */
function haversineKm(lonA: number, latA: number, lonB: number, latB: number): number {
  const R = 6371;
  const dLat = (latB - latA) * (Math.PI / 180);
  const dLon = (lonB - lonA) * (Math.PI / 180);
  const lat1 = latA * (Math.PI / 180);
  const lat2 = latB * (Math.PI / 180);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function buildKnnWeights(entries: FeatureEntry[], k: number): number[][] {
  const n = entries.length;
  const W: number[][] = Array.from({length: n}, () => Array(n).fill(0));
  const safeK = Math.min(Math.max(1, k), n - 1);
  for (let i = 0; i < n; i += 1) {
    const li = entries[i].lonLat;
    if (!li) continue;
    const dists: Array<{j: number; d: number}> = [];
    for (let j = 0; j < n; j += 1) {
      if (j === i) continue;
      const lj = entries[j].lonLat;
      if (!lj) continue;
      dists.push({j, d: haversineKm(li[0], li[1], lj[0], lj[1])});
    }
    dists.sort((a, b) => a.d - b.d);
    for (let r = 0; r < safeK && r < dists.length; r += 1) {
      W[i][dists[r].j] = 1;
      W[dists[r].j][i] = 1;
    }
  }
  return W;
}

function rowStandardize(W: number[][]): number[][] {
  const n = W.length;
  return W.map((row, i) => {
    const rowSum = row.reduce((acc, v) => acc + v, 0);
    if (rowSum === 0) return Array(n).fill(0);
    return row.map(v => v / rowSum);
  });
}

// ─── LISA computation ─────────────────────────────────────────────────────────

function computeGlobalMoransI(z: number[], Wstd: number[][]): {I: number; EI: number; VarI: number; zScore: number} {
  const n = z.length;
  const S0 = Wstd.reduce((acc, row) => acc + row.reduce((a, v) => a + v, 0), 0);
  const zz = z.reduce((acc, v) => acc + v * v, 0); // z'z (already standardized so = n)

  let wzSum = 0;
  for (let i = 0; i < n; i += 1) {
    let wz = 0;
    for (let j = 0; j < n; j += 1) {
      wz += Wstd[i][j] * z[j];
    }
    wzSum += z[i] * wz;
  }

  const I = S0 > 0 && zz > 0 ? (n / S0) * (wzSum / zz) : 0;

  // Analytical moments under randomization assumption
  const EI = -1 / (n - 1);
  const m2 = zz / n;
  const m4 = z.reduce((acc, v) => acc + v * v * v * v, 0) / n;
  const b2 = m4 / (m2 * m2);
  const S1 = 0.5 * Wstd.reduce((acc, row, i) => acc + row.reduce((a, v, j) => a + (v + Wstd[j][i]) ** 2, 0), 0);
  const S2 = Wstd.reduce((acc, row, i) => {
    const ri = row.reduce((a, v) => a + v, 0);
    const ci = Wstd.reduce((a, r) => a + r[i], 0);
    return acc + (ri + ci) ** 2;
  }, 0);
  const n2 = n * n;
  const A = n * ((n2 - 3 * n + 3) * S1 - n * S2 + 3 * S0 * S0);
  const B = b2 * ((n2 - n) * S1 - 2 * n * S2 + 6 * S0 * S0);
  const C = (n - 1) * (n - 2) * (n - 3) * S0 * S0;
  const VarI = C > 0 ? (A - B) / C - EI * EI : 0;
  const zScore = VarI > 0 ? (I - EI) / Math.sqrt(VarI) : 0;

  return {I, EI, VarI, zScore};
}

function computeLocalI(z: number[], Wstd: number[][]): {localI: number[]; lagZ: number[]} {
  const n = z.length;
  const localI: number[] = new Array(n).fill(0);
  const lagZ: number[] = new Array(n).fill(0);
  for (let i = 0; i < n; i += 1) {
    let lz = 0;
    for (let j = 0; j < n; j += 1) {
      lz += Wstd[i][j] * z[j];
    }
    lagZ[i] = lz;
    localI[i] = z[i] * lz;
  }
  return {localI, lagZ};
}

function fisherYatesShuffle(arr: number[], rng: () => number): void {
  for (let i = arr.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rng() * (i + 1));
    const tmp = arr[i];
    arr[i] = arr[j];
    arr[j] = tmp;
  }
}

// Simple LCG for deterministic fast pseudo-random
function makeLCG(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (Math.imul(1664525, s) + 1013904223) >>> 0;
    return s / 4294967296;
  };
}

async function computePseudoPValues(
  z: number[],
  Wstd: number[][],
  observedLocalI: number[],
  permutations: number,
  id: string,
  postProgress: (phase: string, processed: number, total: number) => void
): Promise<number[]> {
  const n = z.length;
  const exceedanceCounts: number[] = new Array(n).fill(0);
  const perm = [...z];
  const rng = makeLCG(0xdeadbeef);
  const yieldEvery = 500;

  for (let p = 0; p < permutations; p += 1) {
    fisherYatesShuffle(perm, rng);
    for (let i = 0; i < n; i += 1) {
      let lz = 0;
      for (let j = 0; j < n; j += 1) {
        lz += Wstd[i][j] * perm[j];
      }
      const permI = perm[i] * lz;
      if (Math.abs(permI) >= Math.abs(observedLocalI[i])) {
        exceedanceCounts[i] += 1;
      }
    }
    if (p > 0 && p % yieldEvery === 0) {
      postProgress('permutation', p, permutations);
      await new Promise(r => setTimeout(r, 0));
    }
  }

  return exceedanceCounts.map(count => (count + 1) / (permutations + 1));
}

function assignCluster(zi: number, lagZi: number, pValue: number, significance: number): LisaCluster {
  if (pValue >= significance) return 'NS';
  if (zi > 0 && lagZi > 0) return 'HH';
  if (zi < 0 && lagZi < 0) return 'LL';
  if (zi > 0 && lagZi < 0) return 'HL';
  if (zi < 0 && lagZi > 0) return 'LH';
  return 'NS';
}

// ─── Shared helpers ───────────────────────────────────────────────────────────

async function buildFeatureEntries(
  features: Array<{geometry?: unknown; h3Id?: unknown; value: number | null}>,
  postProgress: (phase: string, processed: number, total: number) => void
): Promise<{entries: FeatureEntry[]; rawValues: number[]}> {
  const entries: FeatureEntry[] = [];
  const rawValues: number[] = [];
  for (let i = 0; i < features.length; i += 1) {
    const f = features[i];
    const h3Raw = f.h3Id;
    const h3Str = h3Raw ? String(normalizeH3Key(h3Raw) || '').trim() : '';
    const isH3 = Boolean(h3Str && isValidCell(h3Str));

    let feature: any = null;
    let lonLat: [number, number] | null = null;
    let bbox: BBox | null = null;
    let pairs: Array<[number, number]> = [];

    if (isH3) {
      feature = h3CellToPolygonFeature(h3Str);
    } else if (f.geometry) {
      const raw = parseGeoJsonLike(f.geometry);
      feature = toTurfFeature(raw);
    }

    if (feature) {
      lonLat = getCentroidLonLat(feature);
      bbox = geometryToBbox(feature?.geometry);
      pairs = polygonCoordPairs(feature?.geometry);
    }

    entries.push({lonLat, bbox, pairs, h3Id: isH3 ? h3Str : null, isH3});
    rawValues.push(f.value !== null && f.value !== undefined && Number.isFinite(Number(f.value)) ? Number(f.value) : NaN);

    if (i > 0 && i % 200 === 0) {
      postProgress('geometry', i, features.length);
      await new Promise(r => setTimeout(r, 0));
    }
  }
  return {entries, rawValues};
}

function buildSpatialWeights(entries: FeatureEntry[], weightType: 'queen' | 'knn', k: number): number[][] {
  return weightType === 'knn'
    ? buildKnnWeights(entries, Math.max(1, k || 5))
    : buildQueenWeights(entries);
}

function standardizeValues(rawValues: number[], validMask: boolean[]): number[] {
  const validValues = rawValues.filter((_, i) => validMask[i]);
  const mean = validValues.reduce((a, v) => a + v, 0) / Math.max(1, validValues.length);
  const variance =
    validValues.reduce((a, v) => a + (v - mean) * (v - mean), 0) / Math.max(1, validValues.length);
  const std = Math.sqrt(variance) || 1;
  return rawValues.map((v, i) => (validMask[i] ? (v - mean) / std : 0));
}

// ─── Getis-Ord Gi* helpers ────────────────────────────────────────────────────

function normalCDF(z: number): number {
  const t = 1 / (1 + 0.2316419 * Math.abs(z));
  const poly = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
  const cdf = 1 - (1 / Math.sqrt(2 * Math.PI)) * Math.exp(-0.5 * z * z) * poly;
  return z >= 0 ? cdf : 1 - cdf;
}

function normalPValue(z: number): number {
  return 2 * (1 - normalCDF(Math.abs(z)));
}

// ─── Message handler ──────────────────────────────────────────────────────────

self.onmessage = async (event: MessageEvent<SpatialAutocorrelationRequest | BivariateRequest | HotspotRequest>) => {
  const message = event.data;
  if (!message) return;

  const {id} = message;

  const postProgress = (phase: string, processed: number, total: number) => {
    const progress: SpatialAutocorrelationProgress = {id, type: 'progress', payload: {processed, total, phase}};
    self.postMessage(progress);
  };

  if (message.type === 'lisa') {
    try {
      const {features, weightType, k, permutations, significance} = message.payload;
      if (!Array.isArray(features) || features.length < 3) {
        const err: SpatialAutocorrelationError = {id, type: 'error', error: 'Need at least 3 features for LISA.'};
        self.postMessage(err);
        return;
      }

      postProgress('geometry', 0, features.length);

      const {entries, rawValues} = await buildFeatureEntries(features, postProgress);
      const validMask: boolean[] = rawValues.map(v => Number.isFinite(v));
      const n = entries.length;

      postProgress('weights', 0, n);

      const Wstd = rowStandardize(buildSpatialWeights(entries, weightType, k));

      postProgress('weights', n, n);

      const z = standardizeValues(rawValues, validMask);

      // Global Moran's I
      const {I, zScore} = computeGlobalMoransI(z, Wstd);

      // Permutation test for global p-value
      const perm = [...z];
      const rng = makeLCG(0xfeedface);
      let globalExceedance = 0;
      const globalPerms = Math.min(permutations, 199);
      for (let p = 0; p < globalPerms; p += 1) {
        fisherYatesShuffle(perm, rng);
        const zz = perm.reduce((acc, v) => acc + v * v, 0);
        const S0 = Wstd.reduce((acc, row) => acc + row.reduce((a, v) => a + v, 0), 0);
        let wzSum = 0;
        for (let i = 0; i < n; i += 1) {
          let wz = 0;
          for (let j = 0; j < n; j += 1) wz += Wstd[i][j] * perm[j];
          wzSum += perm[i] * wz;
        }
        const permI = S0 > 0 && zz > 0 ? (n / S0) * (wzSum / zz) : 0;
        if (Math.abs(permI) >= Math.abs(I)) globalExceedance += 1;
      }
      const globalPValue = (globalExceedance + 1) / (globalPerms + 1);

      // Local LISA
      const {localI, lagZ} = computeLocalI(z, Wstd);

      postProgress('permutation', 0, permutations);

      const localPValues = await computePseudoPValues(z, Wstd, localI, permutations, id, postProgress);

      postProgress('permutation', permutations, permutations);

      const clusters: LisaCluster[] = localI.map((_, i) =>
        validMask[i] ? assignCluster(z[i], lagZ[i], localPValues[i], significance) : 'NS'
      );

      const result: SpatialAutocorrelationResult = {
        id,
        type: 'result',
        payload: {
          globalMoransI: I,
          zScore,
          pValue: globalPValue,
          localI,
          pValues: localPValues,
          clusters,
          lagValues: lagZ
        }
      };
      self.postMessage(result);
    } catch (error) {
      const err: SpatialAutocorrelationError = {
        id,
        type: 'error',
        error: error instanceof Error ? error.message : String(error)
      };
      self.postMessage(err);
    }
    return;
  }

  if (message.type === 'bivariate') {
    try {
      const {featuresA, featuresB, weightType, k, permutations, significance} = message.payload;
      if (!Array.isArray(featuresA) || featuresA.length < 3) {
        const err: SpatialAutocorrelationError = {id, type: 'error', error: 'Need at least 3 features for bivariate LISA.'};
        self.postMessage(err);
        return;
      }

      postProgress('geometry', 0, featuresA.length);

      const {entries, rawValues: rawA} = await buildFeatureEntries(featuresA, postProgress);
      const rawB: number[] = featuresB.map(f => (f.value !== null && f.value !== undefined && Number.isFinite(Number(f.value)) ? Number(f.value) : NaN));

      const validMask: boolean[] = rawA.map((v, i) => Number.isFinite(v) && Number.isFinite(rawB[i]));
      const n = entries.length;

      const zA = standardizeValues(rawA, validMask);
      const zB = standardizeValues(rawB, validMask);

      // Pearson r
      const pearsonR = zA.reduce((acc, z, i) => acc + z * zB[i], 0) / Math.max(1, n);

      postProgress('weights', 0, n);

      const Wstd = rowStandardize(buildSpatialWeights(entries, weightType, k));

      postProgress('weights', n, n);

      const S0 = Wstd.reduce((acc, row) => acc + row.reduce((a, v) => a + v, 0), 0);
      const zzA = zA.reduce((acc, v) => acc + v * v, 0);

      // Lag of zB
      const lagB: number[] = new Array(n).fill(0);
      for (let i = 0; i < n; i += 1) {
        for (let j = 0; j < n; j += 1) lagB[i] += Wstd[i][j] * zB[j];
      }

      // Global bivariate Moran's I
      let bivWzSum = 0;
      for (let i = 0; i < n; i += 1) bivWzSum += zA[i] * lagB[i];
      const globalBivariateI = S0 > 0 && zzA > 0 ? (n / S0) * (bivWzSum / zzA) : 0;

      // Global p-value via permutation of zB
      const permB = [...zB];
      const rng = makeLCG(0xdeadbeef);
      let globalExceedance = 0;
      const globalPerms = Math.min(permutations, 199);
      for (let p = 0; p < globalPerms; p += 1) {
        fisherYatesShuffle(permB, rng);
        let wzSum = 0;
        for (let i = 0; i < n; i += 1) {
          let wz = 0;
          for (let j = 0; j < n; j += 1) wz += Wstd[i][j] * permB[j];
          wzSum += zA[i] * wz;
        }
        const permI = S0 > 0 && zzA > 0 ? (n / S0) * (wzSum / zzA) : 0;
        if (Math.abs(permI) >= Math.abs(globalBivariateI)) globalExceedance += 1;
      }
      const globalPValue = (globalExceedance + 1) / (globalPerms + 1);
      const globalZScore = globalPValue < 0.05 ? (globalBivariateI > 0 ? 1.96 : -1.96) : 0;

      // Local bivariate I: l_i = zA_i * lag(zB)_i
      const localI: number[] = zA.map((z, i) => z * lagB[i]);

      postProgress('permutation', 0, permutations);

      // Pseudo p-values via permutation of zB (zA and W fixed)
      const exceedanceCounts: number[] = new Array(n).fill(0);
      const permB2 = [...zB];
      const rng2 = makeLCG(0xcafebabe);
      const yieldEvery = 500;
      for (let p = 0; p < permutations; p += 1) {
        fisherYatesShuffle(permB2, rng2);
        for (let i = 0; i < n; i += 1) {
          let wz = 0;
          for (let j = 0; j < n; j += 1) wz += Wstd[i][j] * permB2[j];
          const permLI = zA[i] * wz;
          if (Math.abs(permLI) >= Math.abs(localI[i])) exceedanceCounts[i] += 1;
        }
        if (p > 0 && p % yieldEvery === 0) {
          postProgress('permutation', p, permutations);
          await new Promise(r => setTimeout(r, 0));
        }
      }
      const localPValues = exceedanceCounts.map(count => (count + 1) / (permutations + 1));

      postProgress('permutation', permutations, permutations);

      const clusters: LisaCluster[] = localI.map((_, i) =>
        validMask[i] ? assignCluster(zA[i], lagB[i], localPValues[i], significance) : 'NS'
      );

      const result: BivariateResult = {
        id,
        type: 'bivariate_result',
        payload: {
          pearsonR,
          globalBivariateI,
          zScore: globalZScore,
          pValue: globalPValue,
          localI,
          pValues: localPValues,
          clusters,
          lagValuesB: lagB
        }
      };
      self.postMessage(result);
    } catch (error) {
      const err: SpatialAutocorrelationError = {
        id,
        type: 'error',
        error: error instanceof Error ? error.message : String(error)
      };
      self.postMessage(err);
    }
    return;
  }

  if (message.type === 'hotspot') {
    try {
      const {features, weightType, k, significance} = message.payload;
      if (!Array.isArray(features) || features.length < 3) {
        const err: SpatialAutocorrelationError = {id, type: 'error', error: 'Need at least 3 features for Gi* hotspot analysis.'};
        self.postMessage(err);
        return;
      }

      postProgress('geometry', 0, features.length);

      const {entries, rawValues} = await buildFeatureEntries(features, postProgress);
      const validMask: boolean[] = rawValues.map(v => Number.isFinite(v));
      const n = entries.length;

      // Build raw binary weights (no row-standardization for Gi*)
      postProgress('weights', 0, n);
      const rawWeights = buildSpatialWeights(entries, weightType, Math.max(1, k || 5));

      // Add self-weight (Gi* includes focal unit)
      for (let i = 0; i < n; i += 1) {
        rawWeights[i][i] = 1;
      }

      postProgress('weights', n, n);

      // xVals: use 0 for invalid features (they won't be classified as HH/LL)
      const xVals = rawValues.map((v, i) => (validMask[i] ? v : 0));

      // Compute X̄ and S over valid values only
      const validVals = rawValues.filter((_, i) => validMask[i]);
      const validN = validVals.length;
      const xBar = validN > 0 ? validVals.reduce((a, v) => a + v, 0) / validN : 0;
      const xSqMean = validN > 0 ? validVals.reduce((a, v) => a + v * v, 0) / validN : 0;
      const S = Math.sqrt(Math.max(0, xSqMean - xBar * xBar));

      const localGiStar: number[] = new Array(n).fill(0);
      const pValues: number[] = new Array(n).fill(1);
      const clusters: Array<'HH' | 'LL' | 'NS'> = new Array(n).fill('NS');

      for (let i = 0; i < n; i += 1) {
        const row = rawWeights[i];
        let weightedSum = 0;
        let W_i = 0;
        let W_i_sq = 0;
        for (let j = 0; j < n; j += 1) {
          const w = row[j];
          weightedSum += w * xVals[j];
          W_i += w;
          W_i_sq += w * w;
        }

        const numerator = weightedSum - xBar * W_i;
        const denomSq = S * S * ((validN * W_i_sq - W_i * W_i) / Math.max(1, validN - 1));
        const denom = denomSq > 0 ? Math.sqrt(denomSq) : 0;

        const gi = denom > 0 ? numerator / denom : 0;
        localGiStar[i] = gi;
        pValues[i] = normalPValue(gi);

        if (!validMask[i]) {
          clusters[i] = 'NS';
        } else if (pValues[i] < significance && gi > 0) {
          clusters[i] = 'HH';
        } else if (pValues[i] < significance && gi < 0) {
          clusters[i] = 'LL';
        } else {
          clusters[i] = 'NS';
        }
      }

      const result: HotspotResult = {
        id,
        type: 'hotspot_result',
        payload: {localGiStar, pValues, clusters}
      };
      self.postMessage(result);
    } catch (error) {
      const err: SpatialAutocorrelationError = {
        id,
        type: 'error',
        error: error instanceof Error ? error.message : String(error)
      };
      self.postMessage(err);
    }
    return;
  }
};
