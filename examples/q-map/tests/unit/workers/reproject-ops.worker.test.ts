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

test('reproject worker transforms point geometry and lat/lon fields', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/reproject-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'r1',
      type: 'reprojectRows',
      payload: {
        rows: [
          {
            id: 1,
            latitude: 45.4384,
            longitude: 12.3271,
            geom: {
              type: 'Point',
              coordinates: [12.3271, 45.4384]
            }
          }
        ],
        sourceCrs: 'EPSG:4326',
        targetCrs: 'EPSG:3857',
        geometryField: 'geom',
        outputGeometryField: 'geom_3857',
        latitudeField: 'latitude',
        longitudeField: 'longitude',
        outputLatitudeField: 'lat_3857',
        outputLongitudeField: 'lon_3857'
      }
    }
  });

  const progress = await waitForMessage(messages, 'progress', 'r1');
  const result = await waitForMessage(messages, 'result', 'r1');

  assert.ok(progress, 'expected progress message');
  assert.ok(result, 'expected result message');
  assert.equal(result?.id, 'r1');
  assert.equal(result?.payload?.transformedGeometryRows, 1);
  assert.equal(result?.payload?.transformedCoordinateRows, 1);

  const row = result?.payload?.rows?.[0];
  assert.ok(row);
  assert.equal(typeof row.lon_3857, 'number');
  assert.equal(typeof row.lat_3857, 'number');
  assert.ok(Math.abs(row.lon_3857 - 1372252) < 2500);
  assert.ok(Math.abs(row.lat_3857 - 5690068) < 2500);
  assert.equal(row.geom_3857?.type, 'Point');
  assert.equal(row.geom_3857?.coordinates?.length, 2);
});

test('reproject worker emits error on invalid CRS', async () => {
  const {selfRef, messages} = setupWorkerHarness();
  await import('../../../src/workers/reproject-ops.worker');

  selfRef.onmessage({
    data: {
      id: 'r2',
      type: 'reprojectRows',
      payload: {
        rows: [{latitude: 45, longitude: 12}],
        sourceCrs: 'EPSG:NOT_REAL',
        targetCrs: 'EPSG:3857',
        geometryField: null,
        outputGeometryField: 'geom_out',
        latitudeField: 'latitude',
        longitudeField: 'longitude',
        outputLatitudeField: 'lat_out',
        outputLongitudeField: 'lon_out'
      }
    }
  });

  const error = await waitForMessage(messages, 'error', 'r2');
  assert.ok(error);
  assert.equal(error?.id, 'r2');
  assert.ok(String(error?.error || '').length > 0);
});
