export type PolygonCoords = number[][][];
export type LngLat = [number, number];

export function getPolygonsFromGeometry(geometry: any): PolygonCoords[] {
  if (!geometry || !geometry.type) return [];
  if (geometry.type === 'Polygon' && Array.isArray(geometry.coordinates)) {
    return [geometry.coordinates as PolygonCoords];
  }
  if (geometry.type === 'MultiPolygon' && Array.isArray(geometry.coordinates)) {
    return geometry.coordinates as PolygonCoords[];
  }
  return [];
}

export function parseGeoJsonLike(value: unknown): any | null {
  if (!value) return null;
  if (typeof value === 'object') return value;
  if (typeof value === 'string') {
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }
  return null;
}

export function pointInRing(point: LngLat, ring: number[][]): boolean {
  const [x, y] = point;
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0];
    const yi = ring[i][1];
    const xj = ring[j][0];
    const yj = ring[j][1];
    const intersect = yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / ((yj - yi) || 1e-12) + xi;
    if (intersect) inside = !inside;
  }
  return inside;
}

export function pointInPolygon(point: LngLat, polygon: PolygonCoords): boolean {
  const outer = polygon[0];
  if (!outer || !pointInRing(point, outer)) return false;
  for (let i = 1; i < polygon.length; i += 1) {
    if (pointInRing(point, polygon[i])) return false;
  }
  return true;
}

export function orient(a: LngLat, b: LngLat, c: LngLat) {
  return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0]);
}

export function onSegment(a: LngLat, b: LngLat, c: LngLat) {
  return (
    Math.min(a[0], b[0]) <= c[0] &&
    c[0] <= Math.max(a[0], b[0]) &&
    Math.min(a[1], b[1]) <= c[1] &&
    c[1] <= Math.max(a[1], b[1])
  );
}

export function segmentsIntersect(a1: LngLat, a2: LngLat, b1: LngLat, b2: LngLat) {
  const o1 = orient(a1, a2, b1);
  const o2 = orient(a1, a2, b2);
  const o3 = orient(b1, b2, a1);
  const o4 = orient(b1, b2, a2);
  if ((o1 > 0) !== (o2 > 0) && (o3 > 0) !== (o4 > 0)) return true;
  if (o1 === 0 && onSegment(a1, a2, b1)) return true;
  if (o2 === 0 && onSegment(a1, a2, b2)) return true;
  if (o3 === 0 && onSegment(b1, b2, a1)) return true;
  if (o4 === 0 && onSegment(b1, b2, a2)) return true;
  return false;
}

export function polygonIntersectsPolygon(a: PolygonCoords, b: PolygonCoords): boolean {
  const aOuter = a[0] || [];
  const bOuter = b[0] || [];
  if (!aOuter.length || !bOuter.length) return false;
  if (aOuter.some(p => pointInPolygon([p[0], p[1]], b))) return true;
  if (bOuter.some(p => pointInPolygon([p[0], p[1]], a))) return true;

  for (let ai = 0; ai < a.length; ai += 1) {
    const aRing = a[ai] || [];
    for (let bi = 0; bi < b.length; bi += 1) {
      const bRing = b[bi] || [];
      for (let i = 0; i < aRing.length - 1; i += 1) {
        const a1: LngLat = [aRing[i][0], aRing[i][1]];
        const a2: LngLat = [aRing[i + 1][0], aRing[i + 1][1]];
        for (let j = 0; j < bRing.length - 1; j += 1) {
          const b1: LngLat = [bRing[j][0], bRing[j][1]];
          const b2: LngLat = [bRing[j + 1][0], bRing[j + 1][1]];
          if (segmentsIntersect(a1, a2, b1, b2)) return true;
        }
      }
    }
  }
  return false;
}

function closeRing(points: LngLat[]): LngLat[] {
  if (!points.length) return points;
  const first = points[0];
  const last = points[points.length - 1];
  if (first[0] === last[0] && first[1] === last[1]) return points;
  return [...points, first];
}

export function toLngLatRing(points: number[][]): LngLat[] {
  return points
    .filter(p => Array.isArray(p) && Number.isFinite(Number(p[0])) && Number.isFinite(Number(p[1])))
    .map(p => [Number(p[0]), Number(p[1])] as LngLat);
}

export function ringArea(ring: LngLat[]): number {
  if (!Array.isArray(ring) || ring.length < 3) return 0;
  let sum = 0;
  for (let i = 0; i < ring.length; i += 1) {
    const [x1, y1] = ring[i];
    const [x2, y2] = ring[(i + 1) % ring.length];
    sum += x1 * y2 - x2 * y1;
  }
  return sum / 2;
}

export function polygonAreaAbs(polygon: PolygonCoords): number {
  if (!Array.isArray(polygon) || !polygon.length) return 0;
  const outer = Math.abs(ringArea(toLngLatRing(polygon[0] || [])));
  const holes = polygon
    .slice(1)
    .reduce((acc, hole) => acc + Math.abs(ringArea(toLngLatRing(hole || []))), 0);
  return Math.max(0, outer - holes);
}

function lineIntersection(a: LngLat, b: LngLat, c: LngLat, d: LngLat): LngLat {
  const [x1, y1] = a;
  const [x2, y2] = b;
  const [x3, y3] = c;
  const [x4, y4] = d;
  const den = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
  if (Math.abs(den) < 1e-12) return b;
  const px =
    ((x1 * y2 - y1 * x2) * (x3 - x4) - (x1 - x2) * (x3 * y4 - y3 * x4)) / den;
  const py =
    ((x1 * y2 - y1 * x2) * (y3 - y4) - (y1 - y2) * (x3 * y4 - y3 * x4)) / den;
  return [px, py];
}

function clipPolygonWithConvex(subject: LngLat[], clipRingRaw: LngLat[]): LngLat[] {
  const clipRing = closeRing(clipRingRaw);
  if (!subject.length || clipRing.length < 3) return [];
  let output = subject.slice();
  const clipArea = ringArea(clipRing);
  const isInside = (p: LngLat, a: LngLat, b: LngLat) => {
    const cross = orient(a, b, p);
    return clipArea >= 0 ? cross >= -1e-12 : cross <= 1e-12;
  };

  for (let i = 0; i < clipRing.length - 1; i += 1) {
    const cp1 = clipRing[i];
    const cp2 = clipRing[i + 1];
    const input = output.slice();
    output = [];
    if (!input.length) break;
    let s = input[input.length - 1];
    for (const e of input) {
      const eInside = isInside(e, cp1, cp2);
      const sInside = isInside(s, cp1, cp2);
      if (eInside) {
        if (!sInside) {
          output.push(lineIntersection(s, e, cp1, cp2));
        }
        output.push(e);
      } else if (sInside) {
        output.push(lineIntersection(s, e, cp1, cp2));
      }
      s = e;
    }
  }
  return output;
}

export function polygonIntersectionAreaWithHex(polygon: PolygonCoords, hexRing: LngLat[]): number {
  if (!polygon.length) return 0;
  const clipRing = closeRing(hexRing);
  if (clipRing.length < 4) return 0;

  const outer = toLngLatRing(polygon[0] || []);
  const clippedOuter = clipPolygonWithConvex(outer, clipRing);
  const outerArea = Math.abs(ringArea(clippedOuter));
  if (outerArea <= 0) return 0;

  const holesArea = polygon.slice(1).reduce((acc, holeRaw) => {
    const hole = toLngLatRing(holeRaw || []);
    const clippedHole = clipPolygonWithConvex(hole, clipRing);
    return acc + Math.abs(ringArea(clippedHole));
  }, 0);

  return Math.max(0, outerArea - holesArea);
}

export function polygonCentroid(polygon: PolygonCoords): LngLat | null {
  const ring = toLngLatRing(polygon?.[0] || []);
  if (ring.length < 3) return null;
  let cx = 0;
  let cy = 0;
  let a = 0;
  for (let i = 0; i < ring.length; i += 1) {
    const [x1, y1] = ring[i];
    const [x2, y2] = ring[(i + 1) % ring.length];
    const f = x1 * y2 - x2 * y1;
    a += f;
    cx += (x1 + x2) * f;
    cy += (y1 + y2) * f;
  }
  if (Math.abs(a) < 1e-12) return ring[0] || null;
  const denom = 3 * a;
  return [cx / denom, cy / denom];
}
