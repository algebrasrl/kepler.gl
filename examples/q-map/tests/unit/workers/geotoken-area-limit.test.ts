import assert from 'node:assert/strict';
import test from 'node:test';

import {computePolygonGeometriesAreaM2, evaluateGeotokenTessellationArea} from '../../../src/geo';

function rectangleGeometry(minLon: number, minLat: number, maxLon: number, maxLat: number) {
  return {
    type: 'Polygon',
    coordinates: [
      [
        [minLon, minLat],
        [maxLon, minLat],
        [maxLon, maxLat],
        [minLon, maxLat],
        [minLon, minLat]
      ]
    ]
  };
}

test('geotoken area guard accepts polygons under 100 km2', () => {
  const result = evaluateGeotokenTessellationArea([rectangleGeometry(0, 0, 0.05, 0.05)], 100);

  assert.equal(result.exceedsLimit, false);
  assert.ok(result.areaKm2 > 0);
  assert.ok(result.areaKm2 < 100);
});

test('geotoken area guard rejects polygons over 100 km2', () => {
  const result = evaluateGeotokenTessellationArea([rectangleGeometry(0, 0, 0.1, 0.1)], 100);

  assert.equal(result.exceedsLimit, true);
  assert.ok(result.areaKm2 > 100);
});

test('geotoken area guard uses union area for overlapping polygons', () => {
  const first = rectangleGeometry(0, 0, 0.06, 0.06);
  const second = rectangleGeometry(0.03, 0.03, 0.09, 0.09);

  const unionAreaM2 = computePolygonGeometriesAreaM2([first, second]);
  const summedAreaM2 =
    computePolygonGeometriesAreaM2([first]) + computePolygonGeometriesAreaM2([second]);

  assert.ok(unionAreaM2 > 0);
  assert.ok(unionAreaM2 < summedAreaM2);
});
