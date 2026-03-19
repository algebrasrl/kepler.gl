import assert from 'node:assert/strict';
import test from 'node:test';
import {latLngToCell} from 'h3-js-v4';
import {
  buildAdjacencyEdges,
  h3CellToPolygonFeature,
  normalizeH3Key
} from '../../../src/workers/h3-geometry-utils';

test('normalizeH3Key trims and lowercases values', () => {
  assert.equal(normalizeH3Key(' 8A2A1072B59FFFF '), '8a2a1072b59ffff');
  assert.equal(normalizeH3Key(null), '');
});

test('h3CellToPolygonFeature builds a closed polygon feature from a valid h3 id', () => {
  const h3Id = latLngToCell(45.4384, 12.3271, 6);
  const feature = h3CellToPolygonFeature(h3Id);
  assert.ok(feature);
  assert.equal(feature?.type, 'Feature');
  assert.equal(feature?.geometry?.type, 'Polygon');
  const ring = feature?.geometry?.coordinates?.[0];
  assert.ok(Array.isArray(ring));
  assert.ok(ring.length >= 4);
  const first = ring[0];
  const last = ring[ring.length - 1];
  assert.deepEqual(first, last, 'ring must be closed');
});

test('h3CellToPolygonFeature returns null for invalid h3 id', () => {
  const feature = h3CellToPolygonFeature('not-a-valid-h3-cell');
  assert.equal(feature, null);
});

function squareFeature(minX: number, minY: number, maxX: number, maxY: number): any {
  return {
    type: 'Feature',
    geometry: {
      type: 'Polygon',
      coordinates: [
        [
          [minX, minY],
          [maxX, minY],
          [maxX, maxY],
          [minX, maxY],
          [minX, minY]
        ]
      ]
    },
    properties: {}
  };
}

function bounds(feature: any): {minX: number; minY: number; maxX: number; maxY: number} {
  const ring = feature?.geometry?.coordinates?.[0] || [];
  const xs = ring.map((p: number[]) => Number(p[0]));
  const ys = ring.map((p: number[]) => Number(p[1]));
  return {
    minX: Math.min(...xs),
    minY: Math.min(...ys),
    maxX: Math.max(...xs),
    maxY: Math.max(...ys)
  };
}

function matchRectangles(a: any, b: any, predicate: 'touches' | 'intersects'): boolean {
  const aa = bounds(a);
  const bb = bounds(b);
  const overlapX = Math.max(aa.minX, bb.minX) <= Math.min(aa.maxX, bb.maxX);
  const overlapY = Math.max(aa.minY, bb.minY) <= Math.min(aa.maxY, bb.maxY);
  if (!overlapX || !overlapY) return false;
  if (predicate === 'intersects') return true;
  const shareVerticalEdge =
    (aa.maxX === bb.minX || bb.maxX === aa.minX) &&
    Math.max(aa.minY, bb.minY) <= Math.min(aa.maxY, bb.maxY);
  const shareHorizontalEdge =
    (aa.maxY === bb.minY || bb.maxY === aa.minY) &&
    Math.max(aa.minX, bb.minX) <= Math.min(aa.maxX, bb.maxX);
  return shareVerticalEdge || shareHorizontalEdge;
}

test('buildAdjacencyEdges returns touching edges', () => {
  const nodes = [
    {nodeId: 'a', feature: squareFeature(0, 0, 1, 1)},
    {nodeId: 'b', feature: squareFeature(1, 0, 2, 1)}, // touches a
    {nodeId: 'c', feature: squareFeature(3, 0, 4, 1)} // disjoint
  ];
  const edges = buildAdjacencyEdges(nodes, 'touches', matchRectangles);
  assert.deepEqual(edges, [{source_id: 'a', target_id: 'b', predicate: 'touches'}]);
});

test('buildAdjacencyEdges returns intersecting edges', () => {
  const nodes = [
    {nodeId: 'a', feature: squareFeature(0, 0, 2, 2)},
    {nodeId: 'b', feature: squareFeature(1, 1, 3, 3)}, // intersects a
    {nodeId: 'c', feature: squareFeature(4, 4, 5, 5)} // disjoint
  ];
  const edges = buildAdjacencyEdges(nodes, 'intersects', matchRectangles);
  assert.deepEqual(edges, [{source_id: 'a', target_id: 'b', predicate: 'intersects'}]);
});

test('buildAdjacencyEdges falls back to touches for unknown predicates', () => {
  const nodes = [
    {nodeId: 'a', feature: squareFeature(0, 0, 1, 1)},
    {nodeId: 'b', feature: squareFeature(1, 0, 2, 1)}
  ];
  const edges = buildAdjacencyEdges(nodes, 'invalid' as any, matchRectangles);
  assert.deepEqual(edges, [{source_id: 'a', target_id: 'b', predicate: 'touches'}]);
});

test('buildAdjacencyEdges ignores matcher errors and still processes valid pairs', () => {
  const nodes = [
    {nodeId: 'a', feature: squareFeature(0, 0, 1, 1)},
    {nodeId: 'b', feature: squareFeature(1, 0, 2, 1)},
    {nodeId: 'c', feature: squareFeature(5, 5, 6, 6)}
  ];
  const edges = buildAdjacencyEdges(nodes, 'touches', (left, right, predicate) => {
    const leftBounds = bounds(left);
    const rightBounds = bounds(right);
    if (leftBounds.minX === 0 && rightBounds.minX === 5) {
      throw new Error('synthetic matcher failure');
    }
    return matchRectangles(left, right, predicate);
  });
  assert.deepEqual(edges, [{source_id: 'a', target_id: 'b', predicate: 'touches'}]);
});
