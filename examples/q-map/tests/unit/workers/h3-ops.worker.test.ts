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

function rectangleGeometry(): any {
  return {
    type: 'Polygon',
    coordinates: [
      [
        [12.0, 45.0],
        [12.3, 45.0],
        [12.3, 45.2],
        [12.0, 45.2],
        [12.0, 45.0]
      ]
    ]
  };
}

async function waitForMessage(
  messages: PostedMessage[],
  type: string,
  id: string,
  timeoutMs = 1000
): Promise<PostedMessage | undefined> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const found = messages.find(message => message.type === type && message.id === id);
    if (found) return found;
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  return messages.find(message => message.type === type && message.id === id);
}

test('h3 worker tessellates polygon geometries and emits result', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/h3-ops.worker');

  selfRef.onmessage({
    data: {
      id: 't1',
      type: 'tessellateGeometries',
      payload: {resolution: 6, geometries: [rectangleGeometry()]}
    }
  });

  const progress = await waitForMessage(messages, 'progress', 't1');
  const result = await waitForMessage(messages, 'result', 't1');
  assert.ok(progress, 'expected progress message');
  assert.ok(result, 'expected result message');
  assert.equal(result?.id, 't1');
  assert.ok(Array.isArray(result?.payload?.ids));
  assert.ok((result?.payload?.ids?.length || 0) > 0);
});

test('h3 worker aggregates direct h3 rows by group fields', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/h3-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'a1',
      type: 'aggregateGeometriesToH3',
      payload: {
        resolution: 6,
        weightMode: 'intersects',
        groupFieldNames: ['class'],
        rows: [
          {
            h3Id: '86283082fffffff',
            value: 10,
            groupValues: {class: 'A'}
          },
          {
            h3Id: '86283082fffffff',
            value: 20,
            groupValues: {class: 'A'}
          },
          {
            h3Id: '86283082fffffff',
            value: 7,
            groupValues: {class: 'B'}
          }
        ]
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'a1');
  assert.ok(result);
  const cells = result?.payload?.cells;
  assert.ok(Array.isArray(cells));
  assert.equal(cells.length, 2);

  const classA = cells.find((c: any) => c.groupValues?.class === 'A');
  const classB = cells.find((c: any) => c.groupValues?.class === 'B');
  assert.ok(classA);
  assert.ok(classB);
  assert.equal(classA.sum, 30);
  assert.equal(classA.count, 2);
  assert.equal(classA.min, 10);
  assert.equal(classA.max, 20);
  assert.equal(classB.sum, 7);
});

test('h3 worker computes distinctCount per h3 bucket', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/h3-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'a2',
      type: 'aggregateGeometriesToH3',
      payload: {
        resolution: 6,
        weightMode: 'intersects',
        groupFieldNames: [],
        rows: [
          {h3Id: '86283082fffffff', value: null, distinctValue: '311'},
          {h3Id: '86283082fffffff', value: null, distinctValue: '311'},
          {h3Id: '86283082fffffff', value: null, distinctValue: '312'}
        ]
      }
    }
  });

  const result = await waitForMessage(messages, 'result', 'a2');
  assert.ok(result);
  const cells = result?.payload?.cells;
  assert.ok(Array.isArray(cells));
  assert.equal(cells.length, 1);
  assert.equal(cells[0].count, 3);
  assert.equal(cells[0].distinctCount, 2);
});

test('h3 worker returns validation error for invalid resolution', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/h3-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'err1',
      type: 'tessellateGeometries',
      payload: {resolution: -1, geometries: [rectangleGeometry()]}
    }
  });

  const error = await waitForMessage(messages, 'error', 'err1');
  assert.ok(error);
  assert.equal(error?.id, 'err1');
  assert.match(String(error?.error || ''), /Invalid resolution/i);
});

test('h3 worker returns validation error for invalid weightMode', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/h3-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'err2',
      type: 'aggregateGeometriesToH3',
      payload: {
        resolution: 6,
        weightMode: 'unsupported_mode',
        groupFieldNames: [],
        rows: []
      }
    }
  });

  const error = await waitForMessage(messages, 'error', 'err2');
  assert.ok(error);
  assert.match(String(error?.error || ''), /Invalid weightMode/i);
});

test('h3 worker returns error for unsupported job type', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/h3-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'err3',
      type: 'unknownJobType',
      payload: {}
    }
  });

  const error = await waitForMessage(messages, 'error', 'err3');
  assert.ok(error);
  assert.match(String(error?.error || ''), /Unsupported H3 job type/i);
});

test('h3 worker tessellates empty geometry list to empty ids', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/h3-ops.worker');

  selfRef.onmessage({
    data: {
      id: 't-empty',
      type: 'tessellateGeometries',
      payload: {resolution: 6, geometries: []}
    }
  });

  const result = await waitForMessage(messages, 'result', 't-empty');
  assert.ok(result);
  assert.deepEqual(result?.payload?.ids, []);
});
