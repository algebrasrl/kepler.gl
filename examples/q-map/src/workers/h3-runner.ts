type H3JobName = 'tessellateGeometries' | 'aggregateGeometriesToH3';

type TessellatePayload = {
  resolution: number;
  geometries: unknown[];
};

type AggregateRow = {
  h3Id?: string | null;
  geometry?: unknown;
  value: number | null;
  distinctValue?: unknown;
  groupValues?: Record<string, unknown>;
};

type AggregatePayload = {
  resolution: number;
  weightMode: 'intersects' | 'centroid' | 'area_weighted';
  groupFieldNames: string[];
  rows: AggregateRow[];
};

type H3JobPayloadMap = {
  tessellateGeometries: TessellatePayload;
  aggregateGeometriesToH3: AggregatePayload;
};

type H3JobResultMap = {
  tessellateGeometries: {ids: string[]};
  aggregateGeometriesToH3: {
    cells: Array<{
      h3Id: string;
      count: number;
      countWeighted: number;
      sum: number;
      avgNumerator: number;
      avgDenominator: number;
      min: number | null;
      max: number | null;
      distinctCount: number;
      groupValues: Record<string, unknown>;
    }>;
  };
};

type WorkerResponse =
  | {id: string; type: 'result'; payload: unknown}
  | {id: string; type: 'error'; error: string}
  | {id: string; type: 'progress'; payload: {processed: number; total: number}};

type RunH3JobOptions<T extends H3JobName> = {
  name: T;
  payload: H3JobPayloadMap[T];
  timeoutMs?: number;
  signal?: AbortSignal;
  onProgress?: (progress: {processed: number; total: number}) => void;
};

function makeAbortError(): Error {
  const err = new Error('The operation was aborted.');
  err.name = 'AbortError';
  return err;
}

export function runH3Job<T extends H3JobName>({
  name,
  payload,
  timeoutMs = 120000,
  signal,
  onProgress
}: RunH3JobOptions<T>): Promise<H3JobResultMap[T]> {
  return new Promise((resolve, reject) => {
    const requestId = `${name}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;
    const worker = new Worker(new URL('./h3-ops.worker.ts', import.meta.url), {type: 'module'});

    let finished = false;
    const complete = (cb: () => void) => {
      if (finished) return;
      finished = true;
      worker.terminate();
      cb();
    };

    const timeout = setTimeout(() => {
      complete(() => reject(new Error(`H3 job "${name}" timed out after ${timeoutMs}ms`)));
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
      if (signal) {
        signal.removeEventListener('abort', onAbort);
      }

      if (message.type === 'error') {
        complete(() => reject(new Error(message.error || `H3 job "${name}" failed`)));
        return;
      }

      complete(() => resolve(message.payload as H3JobResultMap[T]));
    };

    worker.onerror = event => {
      clearTimeout(timeout);
      if (signal) {
        signal.removeEventListener('abort', onAbort);
      }
      complete(() =>
        reject(new Error(event?.message || `H3 worker failed while running "${name}"`))
      );
    };

    worker.onmessageerror = () => {
      clearTimeout(timeout);
      if (signal) {
        signal.removeEventListener('abort', onAbort);
      }
      complete(() => reject(new Error(`H3 worker message error for "${name}"`)));
    };

    worker.postMessage({
      id: requestId,
      type: name,
      payload
    });
  });
}
