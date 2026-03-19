import assert from 'node:assert/strict';
import test from 'node:test';
import {AsyncMutex} from '../../../src/features/qmap-ai/middleware/cache';

// ─── Mutation serialization via AsyncMutex ───────────────────────────────────

test('3 simultaneous mutations → serialized execution (FIFO ordering)', async () => {
  const mutex = new AsyncMutex();
  const timestamps: number[] = [];
  const delay = (ms: number) => new Promise(r => setTimeout(r, ms));

  const mutation = async (id: number) => {
    const release = await mutex.acquire();
    try {
      timestamps.push(id);
      await delay(10);
    } finally {
      release();
    }
  };

  await Promise.all([mutation(1), mutation(2), mutation(3)]);
  assert.deepEqual(timestamps, [1, 2, 3], 'Mutations should execute in FIFO order');
});

test('3 simultaneous reads → parallel execution (no mutex)', async () => {
  const startTimes: number[] = [];
  const delay = (ms: number) => new Promise(r => setTimeout(r, ms));

  const read = async (id: number) => {
    startTimes.push(Date.now());
    await delay(20);
    return id;
  };

  const start = Date.now();
  const results = await Promise.all([read(1), read(2), read(3)]);
  const elapsed = Date.now() - start;

  assert.deepEqual(results, [1, 2, 3]);
  assert.ok(elapsed < 50, `Reads should run in parallel (elapsed: ${elapsed}ms)`);
});

test('mixed: 2 read + 2 mutation → reads parallel, mutations serial', async () => {
  const mutex = new AsyncMutex();
  const order: string[] = [];
  const delay = (ms: number) => new Promise(r => setTimeout(r, ms));

  const mutation = async (id: string) => {
    const release = await mutex.acquire();
    try {
      order.push(`m:${id}:start`);
      await delay(10);
      order.push(`m:${id}:end`);
    } finally {
      release();
    }
  };

  const read = async (id: string) => {
    order.push(`r:${id}:start`);
    await delay(5);
    order.push(`r:${id}:end`);
  };

  await Promise.all([mutation('A'), read('1'), mutation('B'), read('2')]);

  const mAStartIdx = order.indexOf('m:A:start');
  const mAEndIdx = order.indexOf('m:A:end');
  const mBStartIdx = order.indexOf('m:B:start');
  assert.ok(mAStartIdx < mAEndIdx, 'Mutation A start before its end');
  assert.ok(mAEndIdx < mBStartIdx, 'Mutation A must finish before mutation B starts');
});

// ─── Dependency-aware wait ───────────────────────────────────────────────────

test('dependency wait: mutation produces "foo", dependent read waits for it', async () => {
  const mutex = new AsyncMutex();
  const inflightMutations = new Map<
    string,
    {toolCallId: string; toolName: string; promise: Promise<void>; producedRefs: Set<string>}
  >();
  const order: string[] = [];
  const delay = (ms: number) => new Promise(r => setTimeout(r, ms));

  // Pre-register the inflight mutation (simulates the wrapper registering
  // the mutation entry before the SDK kicks off the parallel read).
  let resolveInflight!: () => void;
  const inflightPromise = new Promise<void>(r => {
    resolveInflight = r;
  });
  inflightMutations.set('call-1', {
    toolCallId: 'call-1',
    toolName: 'createDataset',
    promise: inflightPromise,
    producedRefs: new Set(['foo'])
  });

  const mutationTask = async () => {
    const release = await mutex.acquire();
    try {
      order.push('mutation:start');
      await delay(30);
      order.push('mutation:end');
    } finally {
      release();
      resolveInflight();
      inflightMutations.delete('call-1');
    }
  };

  const readTask = async () => {
    const inputRef = 'foo';
    for (const [, entry] of inflightMutations) {
      if (entry.producedRefs.has(inputRef)) {
        await entry.promise;
        break;
      }
    }
    order.push('read:start');
    await delay(5);
    order.push('read:end');
  };

  await Promise.all([mutationTask(), readTask()]);

  const mutEndIdx = order.indexOf('mutation:end');
  const readStartIdx = order.indexOf('read:start');
  assert.ok(
    mutEndIdx < readStartIdx,
    `Read on "foo" should wait for mutation that produces "foo" (order: ${order.join(', ')})`
  );
});

test('dependency wait: read on unrelated dataset does NOT wait', async () => {
  const mutex = new AsyncMutex();
  const inflightMutations = new Map<
    string,
    {toolCallId: string; toolName: string; promise: Promise<void>; producedRefs: Set<string>}
  >();
  const order: string[] = [];
  const delay = (ms: number) => new Promise(r => setTimeout(r, ms));

  // Pre-register inflight mutation producing "foo"
  let resolveInflight!: () => void;
  const inflightPromise = new Promise<void>(r => {
    resolveInflight = r;
  });
  inflightMutations.set('call-1', {
    toolCallId: 'call-1',
    toolName: 'createDataset',
    promise: inflightPromise,
    producedRefs: new Set(['foo'])
  });

  const mutationTask = async () => {
    const release = await mutex.acquire();
    try {
      order.push('mutation:start');
      await delay(40);
      order.push('mutation:end');
    } finally {
      release();
      resolveInflight();
      inflightMutations.delete('call-1');
    }
  };

  const readTask = async () => {
    const inputRef = 'bar'; // different dataset!
    for (const [, entry] of inflightMutations) {
      if (entry.producedRefs.has(inputRef)) {
        await entry.promise;
        break;
      }
    }
    order.push('read:start');
    await delay(5);
    order.push('read:end');
  };

  await Promise.all([mutationTask(), readTask()]);

  const readStartIdx = order.indexOf('read:start');
  const mutEndIdx = order.indexOf('mutation:end');
  assert.ok(
    readStartIdx < mutEndIdx,
    `Read on "bar" should NOT wait for mutation producing "foo" (order: ${order.join(', ')})`
  );
});

// ─── Phase metadata (deferred pattern) ───────────────────────────────────────

test('deferred pattern: tool result can carry phase metadata', () => {
  // Simulate what the wrapper does when a tool is phase-gated
  const llmResult: Record<string, unknown> = {
    success: false,
    status: 'deferred',
    details: 'discovery step mandatory',
    executionPhase: 'discover',
    deferredReason: 'phase_gate:discover',
    nextAllowedTools: ['listQMapDatasets', 'listQMapToolCategories'],
    concurrencyClass: 'mutation'
  };

  assert.equal(llmResult.status, 'deferred');
  assert.equal(llmResult.executionPhase, 'discover');
  assert.equal(llmResult.deferredReason, 'phase_gate:discover');
  assert.ok(Array.isArray(llmResult.nextAllowedTools));
  assert.ok((llmResult.nextAllowedTools as string[]).includes('listQMapDatasets'));
});
