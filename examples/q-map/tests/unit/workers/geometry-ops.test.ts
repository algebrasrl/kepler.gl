import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildBboxFeature,
  dissolveFeaturesByProperty,
  eraseFeatureByMasks,
  featureAreaM2,
  intersectFeatureSets,
  simplifyAndCleanFeatures,
  splitPolygonFeatureByLine,
  symmetricDifferenceFeatureSets,
  unionFeatures
} from '../../../src/features/qmap-ai/geometry-ops';

function square(minX: number, minY: number, maxX: number, maxY: number) {
  return {
    type: 'Feature',
    properties: {},
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
    }
  } as any;
}

test('buildBboxFeature returns a polygon with expected bounds', () => {
  const out = buildBboxFeature(9.0, 45.0, 9.3, 45.2, {name: 'milan'});
  assert.equal(out?.type, 'Feature');
  assert.equal(out?.geometry?.type, 'Polygon');
  assert.equal(out?.properties?.minLon, 9.0);
  assert.equal(out?.properties?.maxLat, 45.2);
  const ring = out?.geometry?.coordinates?.[0] || [];
  assert.deepEqual(ring[0], [9.0, 45.0]);
  assert.deepEqual(ring[2], [9.3, 45.2]);
});

test('union/intersection/symmetricDifference produce coherent areas', () => {
  const a = square(0, 0, 2, 2);
  const b = square(1, 0, 3, 2);
  const areaA = featureAreaM2(a);
  const areaB = featureAreaM2(b);

  const union = unionFeatures([a, b]);
  const intersection = intersectFeatureSets([a], [b]);
  const symdiff = symmetricDifferenceFeatureSets([a], [b]);

  assert.ok(union);
  assert.ok(intersection);
  assert.ok(symdiff);

  const areaUnion = featureAreaM2(union);
  const areaIntersection = featureAreaM2(intersection);
  const areaSymDiff = featureAreaM2(symdiff);

  assert.ok(areaUnion > areaA);
  assert.ok(areaIntersection > 0);
  assert.ok(Math.abs((areaA + areaB) - (areaUnion + areaIntersection)) < 1e-3);
  assert.ok(Math.abs(areaSymDiff - (areaUnion - areaIntersection)) < 1e-3);
});

test('unionFeatures ignores invalid entries and keeps valid geometry', () => {
  const out = unionFeatures([null as any, {foo: 'bar'} as any, square(0, 0, 1, 1)]);
  assert.ok(out);
  assert.equal(out?.geometry?.type, 'Polygon');
  assert.ok(featureAreaM2(out) > 0);
});

test('intersectFeatureSets returns null when feature sets are disjoint', () => {
  const a = square(0, 0, 1, 1);
  const b = square(10, 10, 11, 11);
  const intersection = intersectFeatureSets([a], [b]);
  assert.equal(intersection, null);
});

test('dissolveFeaturesByProperty groups and merges by property value', () => {
  const rows = dissolveFeaturesByProperty(
    [
      {feature: square(0, 0, 1, 1), properties: {grp: 'A'}},
      {feature: square(1, 0, 2, 1), properties: {grp: 'A'}},
      {feature: square(10, 10, 11, 11), properties: {grp: 'B'}}
    ],
    'grp'
  );

  assert.equal(rows.length, 2);
  const groupA = rows.find(item => item.groupValue === 'A');
  const groupB = rows.find(item => item.groupValue === 'B');
  assert.ok(groupA);
  assert.ok(groupB);
  assert.equal(groupA?.featureCount, 2);
  assert.equal(groupB?.featureCount, 1);
});

test('simplifyAndCleanFeatures can remove slivers with minAreaM2', () => {
  const tiny = square(0, 0, 0.00001, 0.00001);
  const big = square(0, 0, 0.01, 0.01);
  const out = simplifyAndCleanFeatures([tiny, big], 0.0001, featureAreaM2(big) * 0.5);
  assert.ok(out.length >= 1);
  assert.equal(out.length, 1);
  assert.ok(featureAreaM2(out[0]) > 0);
});

test('simplifyAndCleanFeatures explodes multipolygon parts and filters by area threshold', () => {
  const multiPolygon = {
    type: 'Feature',
    properties: {},
    geometry: {
      type: 'MultiPolygon',
      coordinates: [
        [
          [
            [0, 0],
            [0.01, 0],
            [0.01, 0.01],
            [0, 0.01],
            [0, 0]
          ]
        ],
        [
          [
            [1, 1],
            [1.00001, 1],
            [1.00001, 1.00001],
            [1, 1.00001],
            [1, 1]
          ]
        ]
      ]
    }
  } as any;
  const out = simplifyAndCleanFeatures([multiPolygon], 0, 1000);
  assert.equal(out.length, 1);
  assert.equal(out[0]?.geometry?.type, 'Polygon');
});

test('splitPolygonFeatureByLine splits a polygon into multiple parts', () => {
  const polygon = square(0, 0, 10, 10);
  const line = {
    type: 'Feature',
    properties: {},
    geometry: {type: 'LineString', coordinates: [[5, -2], [5, 12]]}
  } as any;

  const parts = splitPolygonFeatureByLine(polygon, line, 0.5);
  assert.ok(parts.length >= 2);

  const totalArea = parts.reduce((acc, feature) => acc + featureAreaM2(feature), 0);
  const polyArea = featureAreaM2(polygon);
  assert.ok(totalArea > 0);
  assert.ok(totalArea <= polyArea * 1.01);
});

test('splitPolygonFeatureByLine returns source polygon when no split is possible', () => {
  const polygon = square(0, 0, 10, 10);
  const line = {
    type: 'Feature',
    properties: {},
    geometry: {type: 'LineString', coordinates: [[20, 20], [25, 25]]}
  } as any;
  const parts = splitPolygonFeatureByLine(polygon, line, 0.5);
  assert.equal(parts.length, 1);
  assert.ok(Math.abs(featureAreaM2(parts[0]) - featureAreaM2(polygon)) < 1e-3);
});

test('eraseFeatureByMasks subtracts mask geometry from source', () => {
  const source = square(0, 0, 10, 10);
  const mask = square(4, 4, 8, 8);
  const erased = eraseFeatureByMasks(source, [mask]);
  assert.ok(erased);
  const sourceArea = featureAreaM2(source);
  const erasedArea = featureAreaM2(erased);
  assert.ok(erasedArea < sourceArea);
  assert.ok(erasedArea > 0);
});

test('eraseFeatureByMasks applies multiple masks cumulatively', () => {
  const source = square(0, 0, 10, 10);
  const maskA = square(1, 1, 3, 3);
  const maskB = square(7, 7, 9, 9);
  const erased = eraseFeatureByMasks(source, [maskA, maskB]);
  assert.ok(erased);
  const sourceArea = featureAreaM2(source);
  const erasedArea = featureAreaM2(erased);
  assert.ok(erasedArea < sourceArea);
  assert.ok(erasedArea > 0);
});
