type ReprojectRowsPayload = {
  rows: Array<Record<string, unknown>>;
  sourceCrs: string;
  targetCrs: string;
  geometryField: string | null;
  outputGeometryField: string;
  latitudeField: string | null;
  longitudeField: string | null;
  outputLatitudeField: string;
  outputLongitudeField: string;
};

type ReprojectRowsResult = {
  rows: Array<Record<string, unknown>>;
  transformedGeometryRows: number;
  transformedCoordinateRows: number;
};

type WorkerResponse =
  | {id: string; type: 'result'; payload: ReprojectRowsResult}
  | {id: string; type: 'error'; error: string}
  | {id: string; type: 'progress'; payload: {processed: number; total: number}};

type RunReprojectOptions = {
  payload: ReprojectRowsPayload;
  timeoutMs?: number;
  signal?: AbortSignal;
  onProgress?: (progress: {processed: number; total: number}) => void;
};

function makeAbortError(): Error {
  const err = new Error('The operation was aborted.');
  err.name = 'AbortError';
  return err;
}

export function runReprojectJob({
  payload,
  timeoutMs = 180000,
  signal,
  onProgress
}: RunReprojectOptions): Promise<ReprojectRowsResult> {
  return new Promise((resolve, reject) => {
    const requestId = `reproject:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;
    const worker = new Worker(new URL('./reproject-ops.worker.ts', import.meta.url), {type: 'module'});

    let finished = false;
    const complete = (cb: () => void) => {
      if (finished) return;
      finished = true;
      worker.terminate();
      cb();
    };

    const timeout = setTimeout(() => {
      complete(() => reject(new Error(`Reproject job timed out after ${timeoutMs}ms`)));
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
        complete(() => reject(new Error(message.error || 'Reproject worker failed')));
        return;
      }

      complete(() => resolve(message.payload));
    };

    worker.onerror = event => {
      clearTimeout(timeout);
      if (signal) signal.removeEventListener('abort', onAbort);
      complete(() => reject(new Error(event?.message || 'Reproject worker crashed')));
    };

    worker.onmessageerror = () => {
      clearTimeout(timeout);
      if (signal) signal.removeEventListener('abort', onAbort);
      complete(() => reject(new Error('Reproject worker message error')));
    };

    worker.postMessage({
      id: requestId,
      type: 'reprojectRows',
      payload
    });
  });
}

