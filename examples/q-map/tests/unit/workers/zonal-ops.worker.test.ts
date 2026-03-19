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

test('zonal worker computes stats for polygon rows', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/zonal-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'z1',
      type: 'zonalStatsByAdmin',
      payload: {
        weightMode: 'intersects',
        includeValue: true,
        adminRows: [{rowIdx: 10, geometry: squareAround(12.5, 45.5, 0.25)}],
        valueRows: [
          {rowIdx: 1, geometry: squareAround(12.5, 45.5, 0.05), value: 7},
          {rowIdx: 2, geometry: squareAround(15.0, 44.0, 0.05), value: 3}
        ]
      }
    }
  });

  const progress = await waitForMessage(messages, 'progress', 'z1');
  const result = await waitForMessage(messages, 'result', 'z1');
  assert.ok(progress, 'expected progress message');
  assert.ok(result, 'expected result message');
  assert.equal(result?.id, 'z1');
  const stats = result?.payload?.statsByRow;
  assert.ok(Array.isArray(stats));
  assert.equal(stats.length, 1);
  assert.equal(stats[0].rowIdx, 10);
  assert.equal(stats[0].count, 1);
  assert.equal(stats[0].sum, 7);
  assert.equal(stats[0].denom, 1);
  assert.equal(stats[0].min, 7);
  assert.equal(stats[0].max, 7);
});

test('zonal worker computes area_weighted sum for polygon rows', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/zonal-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'z1w',
      type: 'zonalStatsByAdmin',
      payload: {
        weightMode: 'area_weighted',
        includeValue: true,
        adminRows: [{rowIdx: 11, geometry: squareAround(12.5, 45.5, 0.25)}],
        valueRows: [
          {rowIdx: 3, geometry: squareAround(12.5, 45.5, 0.05), value: 12},
          {rowIdx: 4, geometry: squareAround(16.0, 41.5, 0.05), value: 9}
        ]
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'z1w');
  assert.ok(result, 'expected result message');
  const stats = result?.payload?.statsByRow;
  assert.ok(Array.isArray(stats));
  assert.equal(stats.length, 1);
  assert.equal(stats[0].rowIdx, 11);
  assert.equal(stats[0].count, 1);
  assert.ok(Number(stats[0].sum) > 0);
  assert.equal(stats[0].sum, 12);
  assert.equal(stats[0].denom, 1);
});

test('zonal worker supports H3 rows', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/zonal-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'z2',
      type: 'zonalStatsByAdmin',
      payload: {
        weightMode: 'intersects',
        includeValue: true,
        adminRows: [{rowIdx: 20, h3Id: '86283082fffffff'}],
        valueRows: [
          {rowIdx: 5, h3Id: '86283082fffffff', value: 11},
          {rowIdx: 6, h3Id: '862830807ffffff', value: 9}
        ]
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'z2');
  assert.ok(result, 'expected result message');
  const stats = result?.payload?.statsByRow;
  assert.ok(Array.isArray(stats));
  assert.equal(stats.length, 1);
  assert.equal(stats[0].rowIdx, 20);
  assert.ok(Number(stats[0].count) >= 1);
  assert.ok(Number(stats[0].sum) >= 11);
});

test('zonal worker validates weightMode', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/zonal-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'z3',
      type: 'zonalStatsByAdmin',
      payload: {
        weightMode: 'bad-mode',
        includeValue: true,
        adminRows: [],
        valueRows: []
      }
    }
  });

  const error = await waitForMessage(messages, 'error', 'z3');
  assert.ok(error, 'expected error message');
  assert.match(String(error?.error || ''), /Invalid weightMode/i);
});
