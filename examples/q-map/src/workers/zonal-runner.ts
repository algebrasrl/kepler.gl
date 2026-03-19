type ZonalAdminRowPayload = {
  rowIdx: number;
  geometry?: unknown;
  h3Id?: unknown;
};

type ZonalValueRowPayload = {
  rowIdx: number;
  geometry?: unknown;
  h3Id?: unknown;
  value: number | null;
};

type ZonalPayload = {
  weightMode: 'intersects' | 'centroid' | 'area_weighted';
  includeValue: boolean;
  adminRows: ZonalAdminRowPayload[];
  valueRows: ZonalValueRowPayload[];
};

type ZonalResult = {
  statsByRow: Array<{
    rowIdx: number;
    count: number;
    sum: number;
    denom: number;
    min: number | null;
    max: number | null;
  }>;
};

type WorkerResponse =
  | {id: string; type: 'result'; payload: ZonalResult}
  | {id: string; type: 'error'; error: string}
  | {id: string; type: 'progress'; payload: {processed: number; total: number}};

type RunZonalOptions = {
  payload: ZonalPayload;
  timeoutMs?: number;
  signal?: AbortSignal;
  onProgress?: (progress: {processed: number; total: number}) => void;
};

function makeAbortError(): Error {
  const err = new Error('The operation was aborted.');
  err.name = 'AbortError';
  return err;
}

export function runZonalStatsJob({
  payload,
  timeoutMs = 300000,
  signal,
  onProgress
}: RunZonalOptions): Promise<ZonalResult> {
  return new Promise((resolve, reject) => {
    const requestId = `zonal:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;
    const worker = new Worker(new URL('./zonal-ops.worker.ts', import.meta.url), {type: 'module'});

    let finished = false;
    const complete = (cb: () => void) => {
      if (finished) return;
      finished = true;
      worker.terminate();
      cb();
    };

    const timeout = setTimeout(() => {
      complete(() => reject(new Error(`Zonal job timed out after ${timeoutMs}ms`)));
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
        complete(() => reject(new Error(message.error || 'Zonal worker failed')));
        return;
      }

      complete(() => resolve(message.payload));
    };

    worker.onerror = event => {
      clearTimeout(timeout);
      if (signal) signal.removeEventListener('abort', onAbort);
      complete(() => reject(new Error(event?.message || 'Zonal worker crashed')));
    };

    worker.onmessageerror = () => {
      clearTimeout(timeout);
      if (signal) signal.removeEventListener('abort', onAbort);
      complete(() => reject(new Error('Zonal worker message error')));
    };

    worker.postMessage({
      id: requestId,
      type: 'zonalStatsByAdmin',
      payload
    });
  });
}
