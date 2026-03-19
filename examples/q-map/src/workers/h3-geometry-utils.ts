import {cellToBoundary, isValidCell} from 'h3-js-v4';

export function normalizeH3Key(value: unknown): string {
  return String(value ?? '')
    .trim()
    .toLowerCase();
}

export function h3CellToPolygonFeature(h3IdRaw: unknown): any | null {
  const h3Id = normalizeH3Key(h3IdRaw);
  if (!h3Id) return null;
  if (!isValidCell(h3Id)) return null;
  try {
    const boundary = cellToBoundary(h3Id, true) as Array<[number, number]>;
    if (!Array.isArray(boundary) || boundary.length < 3) return null;
    const ring = boundary
      .map(pair => [Number(pair?.[0]), Number(pair?.[1])])
      .filter((pair: number[]) => Number.isFinite(pair[0]) && Number.isFinite(pair[1]));
    if (ring.length < 3) return null;
    const first = ring[0];
    const last = ring[ring.length - 1];
    if (!last || first[0] !== last[0] || first[1] !== last[1]) {
      ring.push([first[0], first[1]]);
    }
    return {
      type: 'Feature',
      properties: {h3_id: h3Id},
      geometry: {type: 'Polygon', coordinates: [ring]}
    };
  } catch {
    return null;
  }
}

export type AdjacencyPredicate = 'touches' | 'intersects';
export type AdjacencyNode = {nodeId: string; feature: any};
export type AdjacencyMatchFn = (
  leftFeature: any,
  rightFeature: any,
  predicate: AdjacencyPredicate
) => boolean;

export function buildAdjacencyEdges(
  nodes: AdjacencyNode[],
  predicate: AdjacencyPredicate,
  matchFn: AdjacencyMatchFn
): Array<{source_id: string; target_id: string; predicate: AdjacencyPredicate}> {
  const safePredicate: AdjacencyPredicate = predicate === 'intersects' ? 'intersects' : 'touches';
  const edges: Array<{source_id: string; target_id: string; predicate: AdjacencyPredicate}> = [];
  for (let i = 0; i < nodes.length; i += 1) {
    for (let j = i + 1; j < nodes.length; j += 1) {
      const a = nodes[i];
      const b = nodes[j];
      let ok = false;
      try {
        ok = Boolean(matchFn(a.feature, b.feature, safePredicate));
      } catch {
        ok = false;
      }
      if (ok) {
        edges.push({source_id: a.nodeId, target_id: b.nodeId, predicate: safePredicate});
      }
    }
  }
  return edges;
}
