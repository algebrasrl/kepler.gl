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
  timeoutMs = 2000
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

test('clip worker clips geometry rows and emits metrics/distinct counts', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/clip-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'clip1',
      type: 'clipRowsByGeometry',
      payload: {
        mode: 'intersects',
        includeMetrics: true,
        includeDistinctCounts: true,
        includeValueCountFields: true,
        clipRows: [
          {
            geometry: squareAround(12.5, 45.5, 0.2),
            properties: {admin_name: 'veneto'}
          }
        ],
        sourceRows: [
          {rowIdx: 10, geometry: squareAround(12.5, 45.5, 0.05)},
          {rowIdx: 11, geometry: squareAround(15.0, 44.0, 0.05)}
        ]
      }
    }
  });

  const progress = await waitForMessage(messages, 'progress', 'clip1');
  const result = await waitForMessage(messages, 'result', 'clip1');
  assert.ok(progress, 'expected progress message');
  assert.ok(result, 'expected result message');
  assert.deepEqual(result?.payload?.matchedRows, [10]);
  assert.equal(result?.payload?.metricsByRow?.length, 1);
  assert.equal(result?.payload?.metricsByRow?.[0]?.rowIdx, 10);
  assert.equal(result?.payload?.metricsByRow?.[0]?.matchCount, 1);
  assert.equal(result?.payload?.metricsByRow?.[0]?.distinctValueCounts?.admin_name, 1);
  assert.equal(result?.payload?.metricsByRow?.[0]?.propertyValueMatchCounts?.admin_name?.veneto, 1);
  assert.ok(Number(result?.payload?.metricsByRow?.[0]?.intersectionAreaM2) > 0);
});

test('clip worker supports h3 source rows', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/clip-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'clip2',
      type: 'clipRowsByGeometry',
      payload: {
        mode: 'intersects',
        includeMetrics: false,
        includeDistinctCounts: false,
        includeValueCountFields: false,
        clipRows: [
          {
            geometry: {
              type: 'Polygon',
              coordinates: [
                [
                  [-180, -90],
                  [180, -90],
                  [180, 90],
                  [-180, 90],
                  [-180, -90]
                ]
              ]
            }
          }
        ],
        sourceRows: [{rowIdx: 7, h3Id: '86283082fffffff'}]
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'clip2');
  assert.ok(result, 'expected result message');
  assert.deepEqual(result?.payload?.matchedRows, [7]);
  assert.equal(Array.isArray(result?.payload?.metricsByRow), true);
});
