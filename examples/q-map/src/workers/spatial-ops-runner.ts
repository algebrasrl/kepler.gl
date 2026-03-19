type SpatialJoinPayload = {
  predicate: 'intersects' | 'within' | 'contains' | 'touches';
  aggregations: Array<'count' | 'sum' | 'avg' | 'min' | 'max'>;
  leftFeatures: Array<{
    properties: Record<string, unknown>;
    geometry: unknown;
  }>;
  rightFeatures: Array<{
    geometry: unknown;
    value: number;
    pickedFields: Record<string, unknown>;
    bbox: [number, number, number, number] | null;
  }>;
  includeRightFields: string[];
};

type OverlayDiffPayload = {
  includeIntersection: boolean;
  includeADifference: boolean;
  includeBDifference: boolean;
  aFeatures: Array<{
    rowIdx: number;
    geometry: unknown;
    bbox: [number, number, number, number] | null;
  }>;
  bFeatures: Array<{
    rowIdx: number;
    geometry: unknown;
    bbox: [number, number, number, number] | null;
  }>;
};

type BufferSummarizePayload = {
  radiusKm: number;
  aggregation: 'count' | 'sum' | 'avg';
  outputFieldName: string;
  sourceFeatures: Array<{
    properties: Record<string, unknown>;
    geometry: unknown;
  }>;
  targetFeatures: Array<{
    geometry: unknown;
    value: number;
    bbox: [number, number, number, number] | null;
  }>;
};

type AdjacencyGraphPayload = {
  predicate: 'touches' | 'intersects';
  features: Array<{
    nodeId: string;
    geometry: unknown;
    bbox: [number, number, number, number] | null;
  }>;
};

type NearestFeatureJoinPayload = {
  sourceFeatures: Array<{
    properties: Record<string, unknown>;
    geometry: unknown;
  }>;
  targetFeatures: Array<{
    geometry: unknown;
    picked: unknown;
  }>;
  k: number;
  maxDistanceKm: number | null;
  includeTargetField: string | null;
};

type CoverageQualityReportPayload = {
  predicate: 'intersects' | 'within' | 'contains' | 'touches';
  leftFeatures: Array<{
    geometry: unknown;
  }>;
  rightFeatures: Array<{
    geometry: unknown;
    value: unknown;
    bbox: [number, number, number, number] | null;
  }>;
  hasValueField: boolean;
};

type SpatialOpsJobName = 'spatialJoinByPredicate' | 'overlayDifference' | 'bufferAndSummarize' | 'adjacencyGraph' | 'nearestFeatureJoin' | 'coverageQualityReport';

type SpatialOpsPayloadMap = {
  spatialJoinByPredicate: SpatialJoinPayload;
  overlayDifference: OverlayDiffPayload;
  bufferAndSummarize: BufferSummarizePayload;
  adjacencyGraph: AdjacencyGraphPayload;
  nearestFeatureJoin: NearestFeatureJoinPayload;
  coverageQualityReport: CoverageQualityReportPayload;
};

type SpatialOpsResultMap = {
  spatialJoinByPredicate: {rows: Array<Record<string, unknown>>};
  overlayDifference: {rows: Array<{_geojson: unknown; overlay_type: string; a_row?: number; b_row?: number}>};
  bufferAndSummarize: {rows: Array<Record<string, unknown>>};
  adjacencyGraph: {edges: Array<{source_id: string; target_id: string; predicate: string}>};
  nearestFeatureJoin: {rows: Array<Record<string, unknown>>};
  coverageQualityReport: {matched: number; nullJoined: number; total: number};
};

type WorkerResponse =
  | {id: string; type: 'result'; payload: unknown}
  | {id: string; type: 'error'; error: string}
  | {id: string; type: 'progress'; payload: {processed: number; total: number}};

type RunSpatialOpsOptions<T extends SpatialOpsJobName> = {
  name: T;
  payload: SpatialOpsPayloadMap[T];
  timeoutMs?: number;
  signal?: AbortSignal;
  onProgress?: (progress: {processed: number; total: number}) => void;
};

function makeAbortError(): Error {
  const err = new Error('The operation was aborted.');
  err.name = 'AbortError';
  return err;
}

export function computeSpatialOpsTimeout(jobName: string, pairEstimate: number): number {
  const perPairMs: Record<string, number> = {
    spatialJoinByPredicate: 0.02,
    overlayDifference: 0.05,
    bufferAndSummarize: 0.03,
    adjacencyGraph: 0.02,
    nearestFeatureJoin: 0.01,
    coverageQualityReport: 0.02
  };
  return Math.min(900_000, Math.max(60_000, 30_000 + pairEstimate * (perPairMs[jobName] || 0.03)));
}

export function runSpatialOpsJob<T extends SpatialOpsJobName>({
  name,
  payload,
  timeoutMs,
  signal,
  onProgress
}: RunSpatialOpsOptions<T>): Promise<SpatialOpsResultMap[T]> {
  return new Promise((resolve, reject) => {
    const requestId = `spatial-ops:${name}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;
    const worker = new Worker(new URL('./spatial-ops.worker.ts', import.meta.url), {type: 'module'});
    const effectiveTimeout = Math.max(1000, Number(timeoutMs || 300000));

    let finished = false;
    const complete = (cb: () => void) => {
      if (finished) return;
      finished = true;
      worker.terminate();
      cb();
    };

    const timeout = setTimeout(() => {
      complete(() => reject(new Error(`Spatial-ops job "${name}" timed out after ${effectiveTimeout}ms`)));
    }, effectiveTimeout);

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
        complete(() => reject(new Error(message.error || `Spatial-ops job "${name}" failed`)));
        return;
      }

      complete(() => resolve(message.payload as SpatialOpsResultMap[T]));
    };

    worker.onerror = event => {
      clearTimeout(timeout);
      if (signal) signal.removeEventListener('abort', onAbort);
      complete(() => reject(new Error(event?.message || `Spatial-ops worker failed while running "${name}"`)));
    };

    worker.onmessageerror = () => {
      clearTimeout(timeout);
      if (signal) signal.removeEventListener('abort', onAbort);
      complete(() => reject(new Error(`Spatial-ops worker message error for "${name}"`)));
    };

    worker.postMessage({
      id: requestId,
      type: name,
      payload
    });
  });
}
