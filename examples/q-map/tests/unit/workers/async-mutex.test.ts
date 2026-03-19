import assert from 'node:assert/strict';
import test from 'node:test';
import {AsyncMutex} from '../../../src/features/qmap-ai/middleware/cache';

test('acquire + release: no deadlock on sequential use', async () => {
  const mutex = new AsyncMutex();
  const release1 = await mutex.acquire();
  assert.equal(mutex.held, true);
  release1();
  assert.equal(mutex.held, false);
  const release2 = await mutex.acquire();
  assert.equal(mutex.held, true);
  release2();
  assert.equal(mutex.held, false);
});

test('FIFO: 3 concurrent acquires resolve in order', async () => {
  const mutex = new AsyncMutex();
  const order: number[] = [];

  const release1 = await mutex.acquire();

  const p2 = mutex.acquire().then(release => {
    order.push(2);
    release();
  });
  const p3 = mutex.acquire().then(release => {
    order.push(3);
    release();
  });

  assert.equal(mutex.queueDepth, 2);
  order.push(1);
  release1();

  await Promise.all([p2, p3]);
  assert.deepEqual(order, [1, 2, 3]);
});

test('queueDepth increments and decrements correctly', async () => {
  const mutex = new AsyncMutex();
  assert.equal(mutex.queueDepth, 0);

  const release1 = await mutex.acquire();
  assert.equal(mutex.queueDepth, 0); // acquired, not queued

  const p2 = mutex.acquire();
  const p3 = mutex.acquire();
  assert.equal(mutex.queueDepth, 2);

  release1();
  const release2 = await p2;
  assert.equal(mutex.queueDepth, 1);

  release2();
  const release3 = await p3;
  assert.equal(mutex.queueDepth, 0);

  release3();
  assert.equal(mutex.held, false);
});

test('held is false initially', () => {
  const mutex = new AsyncMutex();
  assert.equal(mutex.held, false);
  assert.equal(mutex.queueDepth, 0);
});
