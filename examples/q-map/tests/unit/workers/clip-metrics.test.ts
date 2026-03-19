import assert from 'node:assert/strict';
import test from 'node:test';
import {computeClipMetricsForFeature} from '../../../src/workers/clip-metrics';

function squareAround(lon: number, lat: number, delta = 0.05) {
  return {
    type: 'Feature',
    properties: {},
    geometry: {
      type: 'Polygon',
      coordinates: [
        [
          [lon - delta, lat - delta],
          [lon + delta, lat - delta],
          [lon + delta, lat + delta],
          [lon - delta, lat + delta],
          [lon - delta, lat - delta]
        ]
      ]
    }
  };
}

test('computeClipMetricsForFeature returns distinct and per-value counts', () => {
  const metrics = computeClipMetricsForFeature(
    squareAround(12.5, 45.5, 0.3),
    [
      {feature: squareAround(12.5, 45.5, 0.1), properties: {admin_name: 'veneto', code: 'A'}},
      {feature: squareAround(12.55, 45.5, 0.1), properties: {admin_name: 'treviso', code: 'A'}}
    ],
    {
      mode: 'intersects',
      includeAreaMetrics: true,
      includeDistinctCounts: true,
      includeValueCountFields: true
    }
  );

  assert.equal(metrics.matchCount, 2);
  assert.equal(metrics.distinctValueCounts.admin_name, 2);
  assert.equal(metrics.distinctValueCounts.code, 1);
  assert.equal(metrics.propertyValueMatchCounts.code.A, 2);
  assert.ok(Number(metrics.intersectionAreaM2) > 0);
  assert.ok(Number(metrics.intersectionPct) > 0);
});

test('computeClipMetricsForFeature can skip area metrics', () => {
  const metrics = computeClipMetricsForFeature(
    squareAround(12.5, 45.5, 0.3),
    [{feature: squareAround(12.5, 45.5, 0.1), properties: {admin_name: 'veneto'}}],
    {
      mode: 'intersects',
      includeAreaMetrics: false,
      includeDistinctCounts: true,
      includeValueCountFields: false
    }
  );

  assert.equal(metrics.matchCount, 1);
  assert.equal(metrics.intersectionAreaM2, 0);
  assert.equal(metrics.intersectionPct, 0);
  assert.equal(metrics.distinctValueCounts.admin_name, 1);
});

test('computeClipMetricsForFeature preserves centroid vs within semantics', () => {
  const source = squareAround(12.5, 45.5, 0.3);
  const clip = [{feature: squareAround(12.5, 45.5, 0.1), properties: {name: 'inner'}}];

  const centroidMetrics = computeClipMetricsForFeature(source, clip, {
    mode: 'centroid',
    includeAreaMetrics: false,
    includeDistinctCounts: false,
    includeValueCountFields: false
  });
  const withinMetrics = computeClipMetricsForFeature(source, clip, {
    mode: 'within',
    includeAreaMetrics: false,
    includeDistinctCounts: false,
    includeValueCountFields: false
  });

  assert.equal(centroidMetrics.matchCount, 1);
  assert.equal(withinMetrics.matchCount, 0);
});
