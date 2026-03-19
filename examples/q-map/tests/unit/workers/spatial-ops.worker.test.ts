import assert from 'node:assert/strict';
import test from 'node:test';

type PostedMessage = {id: string; type: string; payload?: any; error?: string};

function setupWorkerHarness() {
  const messages: PostedMessage[] = [];
  const globalSelf = (globalThis as any).self;
  const selfRef: any =
    globalSelf && typeof globalSelf === 'object'
      ? globalSelf
      : {
          onmessage: null
        };
  selfRef.postMessage = (message: PostedMessage) => {
    messages.push(message);
  };
  (globalThis as any).self = selfRef;
  return {selfRef, messages};
}

async function waitForMessage(
  messages: PostedMessage[],
  type: string,
  id: string,
  timeoutMs = 5000
): Promise<PostedMessage | undefined> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const found = messages.find(message => message.type === type && message.id === id);
    if (found) return found;
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  return messages.find(message => message.type === type && message.id === id);
}

function squareAround(lon: number, lat: number, delta = 0.05) {
  return {
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
  };
}

function pointAt(lon: number, lat: number) {
  return {type: 'Point', coordinates: [lon, lat]};
}

// ── spatialJoinByPredicate ──

test('spatialJoinByPredicate — overlapping polygons match', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'sj1',
      type: 'spatialJoinByPredicate',
      payload: {
        predicate: 'intersects',
        aggregations: ['count', 'sum'],
        leftFeatures: [
          {properties: {name: 'left1'}, geometry: squareAround(12.5, 45.5, 0.1)}
        ],
        rightFeatures: [
          {geometry: squareAround(12.5, 45.5, 0.05), value: 7, pickedFields: {}, bbox: null},
          {geometry: squareAround(20.0, 40.0, 0.05), value: 3, pickedFields: {}, bbox: null}
        ],
        includeRightFields: []
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'sj1');
  assert.ok(result, 'expected result');
  const rows = result?.payload?.rows;
  assert.ok(Array.isArray(rows));
  assert.equal(rows.length, 1);
  assert.equal(rows[0].name, 'left1');
  assert.equal(rows[0].join_count, 1);
  assert.equal(rows[0].join_sum, 7);
});

test('spatialJoinByPredicate — non-overlapping polygons give 0 match', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'sj2',
      type: 'spatialJoinByPredicate',
      payload: {
        predicate: 'intersects',
        aggregations: ['count'],
        leftFeatures: [
          {properties: {name: 'left1'}, geometry: squareAround(0.0, 0.0, 0.01)}
        ],
        rightFeatures: [
          {geometry: squareAround(50.0, 50.0, 0.01), value: 5, pickedFields: {}, bbox: null}
        ],
        includeRightFields: []
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'sj2');
  assert.ok(result, 'expected result');
  const rows = result?.payload?.rows;
  assert.ok(Array.isArray(rows));
  assert.equal(rows.length, 1);
  assert.equal(rows[0].join_count, 0);
});

// ── overlayDifference ──

test('overlayDifference — overlapping squares produce intersection + differences', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'od1',
      type: 'overlayDifference',
      payload: {
        includeIntersection: true,
        includeADifference: true,
        includeBDifference: true,
        aFeatures: [{rowIdx: 0, geometry: squareAround(12.5, 45.5, 0.1), bbox: null}],
        bFeatures: [{rowIdx: 1, geometry: squareAround(12.55, 45.5, 0.1), bbox: null}]
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'od1');
  assert.ok(result, 'expected result');
  const rows = result?.payload?.rows;
  assert.ok(Array.isArray(rows));
  const types = rows.map((r: any) => r.overlay_type);
  assert.ok(types.includes('intersection'), 'expected intersection');
  assert.ok(types.includes('a_minus_b'), 'expected a_minus_b');
  assert.ok(types.includes('b_minus_a'), 'expected b_minus_a');
});

test('overlayDifference — only intersection flag active', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'od2',
      type: 'overlayDifference',
      payload: {
        includeIntersection: true,
        includeADifference: false,
        includeBDifference: false,
        aFeatures: [{rowIdx: 0, geometry: squareAround(12.5, 45.5, 0.1), bbox: null}],
        bFeatures: [{rowIdx: 1, geometry: squareAround(12.55, 45.5, 0.1), bbox: null}]
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'od2');
  assert.ok(result, 'expected result');
  const rows = result?.payload?.rows;
  assert.ok(Array.isArray(rows));
  assert.ok(rows.length >= 1);
  assert.ok(rows.every((r: any) => r.overlay_type === 'intersection'));
});

// ── bufferAndSummarize ──

test('bufferAndSummarize — point with buffer, target inside', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'bs1',
      type: 'bufferAndSummarize',
      payload: {
        radiusKm: 50,
        aggregation: 'count',
        outputFieldName: 'nearby_count',
        sourceFeatures: [
          {properties: {id: 'src1'}, geometry: pointAt(12.5, 45.5)}
        ],
        targetFeatures: [
          {geometry: squareAround(12.5, 45.5, 0.01), value: 10, bbox: null}
        ]
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'bs1');
  assert.ok(result, 'expected result');
  const rows = result?.payload?.rows;
  assert.ok(Array.isArray(rows));
  assert.equal(rows.length, 1);
  assert.equal(rows[0].id, 'src1');
  assert.equal(rows[0].nearby_count, 1);
});

test('bufferAndSummarize — target outside buffer', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'bs2',
      type: 'bufferAndSummarize',
      payload: {
        radiusKm: 1,
        aggregation: 'count',
        outputFieldName: 'nearby_count',
        sourceFeatures: [
          {properties: {id: 'src1'}, geometry: pointAt(0.0, 0.0)}
        ],
        targetFeatures: [
          {geometry: squareAround(50.0, 50.0, 0.01), value: 10, bbox: null}
        ]
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'bs2');
  assert.ok(result, 'expected result');
  const rows = result?.payload?.rows;
  assert.ok(Array.isArray(rows));
  assert.equal(rows.length, 1);
  assert.equal(rows[0].nearby_count, 0);
});

// ── adjacencyGraph ──

test('adjacencyGraph — 3 adjacent squares produce edges', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  // Three squares sharing edges
  selfRef.onmessage({
    data: {
      id: 'ag1',
      type: 'adjacencyGraph',
      payload: {
        predicate: 'intersects',
        features: [
          {nodeId: 'A', geometry: squareAround(12.0, 45.0, 0.05), bbox: null},
          {nodeId: 'B', geometry: squareAround(12.1, 45.0, 0.05), bbox: null},
          {nodeId: 'C', geometry: squareAround(12.2, 45.0, 0.05), bbox: null}
        ]
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'ag1');
  assert.ok(result, 'expected result');
  const edges = result?.payload?.edges;
  assert.ok(Array.isArray(edges));
  // A-B overlap, B-C overlap; A-C do not overlap (0.05 + 0.05 = 0.1 gap between 12.0 and 12.2 centers)
  assert.ok(edges.length >= 2, `expected at least 2 edges, got ${edges.length}`);
});

test('adjacencyGraph — disjoint polygons produce 0 edges', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'ag2',
      type: 'adjacencyGraph',
      payload: {
        predicate: 'touches',
        features: [
          {nodeId: 'X', geometry: squareAround(0.0, 0.0, 0.01), bbox: null},
          {nodeId: 'Y', geometry: squareAround(50.0, 50.0, 0.01), bbox: null}
        ]
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'ag2');
  assert.ok(result, 'expected result');
  const edges = result?.payload?.edges;
  assert.ok(Array.isArray(edges));
  assert.equal(edges.length, 0);
});

// ── nearestFeatureJoin ──

test('nearestFeatureJoin — finds single nearest target', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'nfj1',
      type: 'nearestFeatureJoin',
      payload: {
        sourceFeatures: [
          {properties: {name: 'src1'}, geometry: pointAt(12.5, 45.5)}
        ],
        targetFeatures: [
          {geometry: pointAt(12.501, 45.501), picked: 'city_a'},
          {geometry: pointAt(20.0, 40.0), picked: 'city_b'}
        ],
        k: 1,
        maxDistanceKm: null,
        includeTargetField: 'city'
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'nfj1');
  assert.ok(result, 'expected result');
  const rows = result?.payload?.rows;
  assert.ok(Array.isArray(rows));
  assert.equal(rows.length, 1);
  assert.equal(rows[0].name, 'src1');
  assert.equal(rows[0].nearest_count, 1);
  assert.ok(Number(rows[0].nearest_distance_km) < 1, 'nearest should be very close');
  assert.equal(rows[0].nearest_city, 'city_a');
});

test('nearestFeatureJoin — respects maxDistanceKm filter', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'nfj2',
      type: 'nearestFeatureJoin',
      payload: {
        sourceFeatures: [
          {properties: {id: 'a'}, geometry: pointAt(0.0, 0.0)}
        ],
        targetFeatures: [
          {geometry: pointAt(50.0, 50.0), picked: null}
        ],
        k: 1,
        maxDistanceKm: 10,
        includeTargetField: null
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'nfj2');
  assert.ok(result, 'expected result');
  const rows = result?.payload?.rows;
  assert.ok(Array.isArray(rows));
  assert.equal(rows.length, 1);
  assert.equal(rows[0].nearest_count, 0);
  assert.equal(rows[0].nearest_distance_km, null);
});

test('nearestFeatureJoin — k=3 returns top 3', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'nfj3',
      type: 'nearestFeatureJoin',
      payload: {
        sourceFeatures: [
          {properties: {id: 's'}, geometry: pointAt(10.0, 45.0)}
        ],
        targetFeatures: [
          {geometry: pointAt(10.001, 45.001), picked: null},
          {geometry: pointAt(10.01, 45.01), picked: null},
          {geometry: pointAt(10.1, 45.1), picked: null},
          {geometry: pointAt(10.5, 45.5), picked: null},
          {geometry: pointAt(15.0, 50.0), picked: null}
        ],
        k: 3,
        maxDistanceKm: null,
        includeTargetField: null
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'nfj3');
  assert.ok(result, 'expected result');
  const rows = result?.payload?.rows;
  assert.equal(rows.length, 1);
  assert.equal(rows[0].nearest_count, 3);
  assert.ok(Number(rows[0].nearest_distance_km) < 1);
});

// ── coverageQualityReport ──

test('coverageQualityReport — overlapping features counted as matched', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'cqr1',
      type: 'coverageQualityReport',
      payload: {
        predicate: 'intersects',
        leftFeatures: [
          {geometry: squareAround(12.5, 45.5, 0.05)},
          {geometry: squareAround(0.0, 0.0, 0.01)}
        ],
        rightFeatures: [
          {geometry: squareAround(12.5, 45.5, 0.03), value: 42, bbox: null}
        ],
        hasValueField: false
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'cqr1');
  assert.ok(result, 'expected result');
  assert.equal(result?.payload?.matched, 1);
  assert.equal(result?.payload?.nullJoined, 0);
  assert.equal(result?.payload?.total, 2);
});

test('coverageQualityReport — non-overlapping produces 0 matched', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'cqr2',
      type: 'coverageQualityReport',
      payload: {
        predicate: 'intersects',
        leftFeatures: [{geometry: squareAround(0.0, 0.0, 0.01)}],
        rightFeatures: [{geometry: squareAround(50.0, 50.0, 0.01), value: null, bbox: null}],
        hasValueField: false
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'cqr2');
  assert.ok(result, 'expected result');
  assert.equal(result?.payload?.matched, 0);
  assert.equal(result?.payload?.total, 1);
});

test('coverageQualityReport — tracks null-value joins', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'cqr3',
      type: 'coverageQualityReport',
      payload: {
        predicate: 'intersects',
        leftFeatures: [{geometry: squareAround(12.5, 45.5, 0.05)}],
        rightFeatures: [{geometry: squareAround(12.5, 45.5, 0.03), value: null, bbox: null}],
        hasValueField: true
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'cqr3');
  assert.ok(result, 'expected result');
  assert.equal(result?.payload?.matched, 1);
  assert.equal(result?.payload?.nullJoined, 1);
});

test('coverageQualityReport — non-null value does not increment nullJoined', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'cqr4',
      type: 'coverageQualityReport',
      payload: {
        predicate: 'intersects',
        leftFeatures: [{geometry: squareAround(12.5, 45.5, 0.05)}],
        rightFeatures: [{geometry: squareAround(12.5, 45.5, 0.03), value: 42, bbox: null}],
        hasValueField: true
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'cqr4');
  assert.ok(result, 'expected result');
  assert.equal(result?.payload?.matched, 1);
  assert.equal(result?.payload?.nullJoined, 0);
});

// ── Error handling ──

test('unknown message type returns error', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'err1',
      type: 'invalidJobType',
      payload: {}
    }
  });

  const error = await waitForMessage(messages, 'error', 'err1');
  assert.ok(error, 'expected error message');
  assert.ok(String(error?.error || '').includes('Unknown spatial-ops job type'));
});

// ── Progress ──

test('progress messages are emitted', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/spatial-ops.worker');

  // Create enough features to trigger progress at i % 50 === 0 (i=0)
  selfRef.onmessage({
    data: {
      id: 'prog1',
      type: 'spatialJoinByPredicate',
      payload: {
        predicate: 'intersects',
        aggregations: ['count'],
        leftFeatures: [
          {properties: {id: '1'}, geometry: squareAround(12.5, 45.5, 0.01)}
        ],
        rightFeatures: [
          {geometry: squareAround(12.5, 45.5, 0.005), value: 1, pickedFields: {}, bbox: null}
        ],
        includeRightFields: []
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'prog1');
  assert.ok(result, 'expected result');
  const progressMessages = messages.filter(m => m.type === 'progress' && m.id === 'prog1');
  assert.ok(progressMessages.length >= 1, 'expected at least one progress message');
  assert.ok(Number(progressMessages[0]?.payload?.total) >= 1);
});
