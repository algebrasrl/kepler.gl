import type {ClipMode} from './clip-metrics';

type ClipSourceRowPayload = {
  rowIdx: number;
  geometry?: unknown;
  h3Id?: unknown;
};

type ClipBoundaryRowPayload = {
  geometry: unknown;
  properties?: Record<string, unknown>;
};

type ClipRowsPayload = {
  mode: ClipMode;
  includeMetrics: boolean;
  includeDistinctCounts: boolean;
  includeValueCountFields: boolean;
  sourceRows: ClipSourceRowPayload[];
  clipRows: ClipBoundaryRowPayload[];
};

type ClipRowsResult = {
  matchedRows: number[];
  metricsByRow: Array<{
    rowIdx: number;
    matchCount: number;
    intersectionAreaM2: number;
    intersectionPct: number;
    distinctValueCounts: Record<string, number>;
    propertyValueMatchCounts: Record<string, Record<string, number>>;
  }>;
};

type WorkerResponse =
  | {id: string; type: 'result'; payload: ClipRowsResult}
  | {id: string; type: 'error'; error: string}
  | {id: string; type: 'progress'; payload: {processed: number; total: number}};

type RunClipRowsOptions = {
  payload: ClipRowsPayload;
  timeoutMs?: number;
  signal?: AbortSignal;
  onProgress?: (progress: {processed: number; total: number}) => void;
};

function makeAbortError(): Error {
  const err = new Error('The operation was aborted.');
  err.name = 'AbortError';
  return err;
}

export function runClipRowsJob({
  payload,
  timeoutMs = 300000,
  signal,
  onProgress
}: RunClipRowsOptions): Promise<ClipRowsResult> {
  return new Promise((resolve, reject) => {
    const requestId = `clip:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;
    const worker = new Worker(new URL('./clip-ops.worker.ts', import.meta.url), {type: 'module'});

    let finished = false;
    const complete = (cb: () => void) => {
      if (finished) return;
      finished = true;
      worker.terminate();
      cb();
    };

    const timeout = setTimeout(() => {
      complete(() => reject(new Error(`Clip job timed out after ${timeoutMs}ms`)));
    }, Math.max(1000, Number(timeoutMs)));

    const onAbort = () => {
      complete(() => reject(makeAbortError()));
    };
    if (signal) {
      if (signal.aborted) {
        clearTimeout(timeout);
        worker.terminate();
        reject(makeAbortError());
        return;
      }
      signal.addEventListener('abort', onAbort, {once: true});
    }

    worker.onmessage = event => {
      const message = event.data as WorkerResponse;
      if (!message || message.id !== requestId) return;
      if (message.type === 'progress') {
        onProgress?.(message.payload);
        return;
      }

      clearTimeout(timeout);
      if (signal) signal.removeEventListener('abort', onAbort);

      if (message.type === 'error') {
        complete(() => reject(new Error(message.error || 'Clip worker failed')));
        return;
      }

      complete(() => resolve(message.payload));
    };

    worker.onerror = event => {
      clearTimeout(timeout);
      if (signal) signal.removeEventListener('abort', onAbort);
      complete(() => reject(new Error(event?.message || 'Clip worker crashed')));
    };

    worker.onmessageerror = () => {
      clearTimeout(timeout);
      if (signal) signal.removeEventListener('abort', onAbort);
      complete(() => reject(new Error('Clip worker message error')));
    };

    worker.postMessage({
      id: requestId,
      type: 'clipRowsByGeometry',
      payload
    });
  });
}
