import React, {useEffect} from 'react';
import {layerConfigChange, setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {extendedTool} from '../tool-shim';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapLayers, selectQMapVisState} from '../../../state/qmap-selectors';
import {useToolExecution} from './use-tool-execution';
import type {LisaCluster, BivariateResult, HotspotResult} from '../../../workers/spatial-autocorrelation.worker';

import type {QMapToolContext} from '../context/tool-context';

// ─── LISA colour preset ───────────────────────────────────────────────────────

const LISA_COLORS: Record<LisaCluster, string> = {
  HH: '#d73027',
  LL: '#4575b4',
  HL: '#fc8d59',
  LH: '#91bfdb',
  NS: '#cccccc'
};

// Ordered category list used for building the custom color range
const LISA_CATEGORY_ORDER: LisaCluster[] = ['HH', 'HL', 'LH', 'LL', 'NS'];

// ─── Worker runner ────────────────────────────────────────────────────────────

type WorkerPayload = {
  features: Array<{geometry?: unknown; h3Id?: unknown; value: number | null}>;
  weightType: 'queen' | 'knn';
  k: number;
  permutations: number;
  significance: number;
};

type WorkerResult = {
  globalMoransI: number;
  zScore: number;
  pValue: number;
  localI: number[];
  pValues: number[];
  clusters: LisaCluster[];
  lagValues: number[];
};

function runSpatialAutocorrelationJob(payload: WorkerPayload): Promise<WorkerResult> {
  return new Promise((resolve, reject) => {
    let worker: Worker | null = null;
    try {
      worker = new Worker(
        new URL('../../../workers/spatial-autocorrelation.worker.ts', import.meta.url),
        {type: 'module'}
      );
    } catch {
      reject(new Error('Failed to instantiate spatial-autocorrelation worker.'));
      return;
    }
    const jobId = `lisa-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    worker.onmessage = (event: MessageEvent) => {
      const msg = event.data;
      if (!msg || msg.id !== jobId) return;
      if (msg.type === 'result') {
        worker!.terminate();
        resolve(msg.payload as WorkerResult);
      } else if (msg.type === 'error') {
        worker!.terminate();
        reject(new Error(String(msg.error || 'Worker error')));
      }
      // progress messages are informational only
    };
    worker.onerror = (err) => {
      worker!.terminate();
      reject(new Error(err.message || 'Worker error'));
    };
    worker.postMessage({id: jobId, type: 'lisa', payload});
  });
}

// Fallback: run LISA synchronously in the main thread when Web Workers are unavailable.
// Imports the worker module dynamically — this is only reached in environments where
// Worker construction fails (e.g., certain test runners).
async function runSpatialAutocorrelationFallback(payload: WorkerPayload): Promise<WorkerResult> {
  // Dynamic import of the worker module so the bundler still finds the file
  const mod = await import('../../../workers/spatial-autocorrelation.worker?worker&inline');
  if (mod && typeof (mod as any).default === 'function') {
    // Vite inline-worker path: instantiate via the exported class
    const WorkerClass = (mod as any).default;
    return new Promise((resolve, reject) => {
      const w = new WorkerClass();
      const jobId = `lisa-fallback-${Date.now()}`;
      w.onmessage = (event: MessageEvent) => {
        const msg = event.data;
        if (!msg || msg.id !== jobId) return;
        if (msg.type === 'result') { w.terminate(); resolve(msg.payload as WorkerResult); }
        else if (msg.type === 'error') { w.terminate(); reject(new Error(String(msg.error || 'Worker error'))); }
      };
      w.onerror = (err: any) => { w.terminate(); reject(new Error(err.message || 'Worker error')); };
      w.postMessage({id: jobId, type: 'lisa', payload});
    });
  }
  throw new Error('Spatial autocorrelation computation unavailable: worker module not found.');
}

async function runLisaJob(payload: WorkerPayload): Promise<WorkerResult> {
  try {
    return await runSpatialAutocorrelationJob(payload);
  } catch {
    return runSpatialAutocorrelationFallback(payload);
  }
}

// ─── Bivariate worker runner ──────────────────────────────────────────────────

type BivariatePayload = {
  featuresA: Array<{geometry?: unknown; h3Id?: unknown; value: number | null}>;
  featuresB: Array<{value: number | null}>;
  weightType: 'queen' | 'knn';
  k: number;
  permutations: number;
  significance: number;
};

function runBivariateJob(payload: BivariatePayload): Promise<BivariateResult['payload']> {
  return new Promise((resolve, reject) => {
    let worker: Worker | null = null;
    try {
      worker = new Worker(
        new URL('../../../workers/spatial-autocorrelation.worker.ts', import.meta.url),
        {type: 'module'}
      );
    } catch {
      reject(new Error('Failed to instantiate spatial-autocorrelation worker.'));
      return;
    }
    const jobId = `bivariate-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    worker.onmessage = (event: MessageEvent) => {
      const msg = event.data;
      if (!msg || msg.id !== jobId) return;
      if (msg.type === 'bivariate_result') {
        worker!.terminate();
        resolve(msg.payload as BivariateResult['payload']);
      } else if (msg.type === 'error') {
        worker!.terminate();
        reject(new Error(String(msg.error || 'Worker error')));
      }
    };
    worker.onerror = (err) => {
      worker!.terminate();
      reject(new Error(err.message || 'Worker error'));
    };
    worker.postMessage({id: jobId, type: 'bivariate', payload});
  });
}

async function runBivariateFallback(payload: BivariatePayload): Promise<BivariateResult['payload']> {
  const mod = await import('../../../workers/spatial-autocorrelation.worker?worker&inline');
  if (mod && typeof (mod as any).default === 'function') {
    const WorkerClass = (mod as any).default;
    return new Promise((resolve, reject) => {
      const w = new WorkerClass();
      const jobId = `bivariate-fallback-${Date.now()}`;
      w.onmessage = (event: MessageEvent) => {
        const msg = event.data;
        if (!msg || msg.id !== jobId) return;
        if (msg.type === 'bivariate_result') { w.terminate(); resolve(msg.payload as BivariateResult['payload']); }
        else if (msg.type === 'error') { w.terminate(); reject(new Error(String(msg.error || 'Worker error'))); }
      };
      w.onerror = (err: any) => { w.terminate(); reject(new Error(err.message || 'Worker error')); };
      w.postMessage({id: jobId, type: 'bivariate', payload});
    });
  }
  throw new Error('Bivariate correlation computation unavailable: worker module not found.');
}

async function runBivariateJobWithFallback(payload: BivariatePayload): Promise<BivariateResult['payload']> {
  try {
    return await runBivariateJob(payload);
  } catch {
    return runBivariateFallback(payload);
  }
}

// ─── Tool factory ─────────────────────────────────────────────────────────────

export function createComputeQMapSpatialAutocorrelationTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveGeojsonFieldName,
    resolveH3FieldName,
    resolveDatasetFieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    resolveOptionalFeatureCap,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    shouldUseLoadingIndicator,
    getFilteredDatasetIndexes,
    mapIndexesChunked,
    parseGeoJsonLike,
    h3CellToPolygonFeature,
    toTurfFeature,
    turfCentroid,
    yieldToMainThread,
    upsertDerivedDatasetRows,
    ensureColorRange,
    resolveStyleTargetLayer,
    findDatasetForLayer
  } = ctx;

  return extendedTool({
    description:
      'Compute spatial autocorrelation (Global Moran\'s I + Local LISA) for a numeric field on a loaded dataset. ' +
      'Adds lisa_cluster (HH/LL/HL/LH/NS), lisa_local_i, and lisa_p_value columns to a derived dataset, ' +
      'and applies a 5-color categorical LISA styling preset to the layer.',
    parameters: z.object({
      datasetName: z.string().describe('Dataset to analyse (name or id)'),
      valueField: z.string().describe('Numeric field for autocorrelation (e.g. population, density)'),
      weightType: z
        .enum(['queen', 'knn'])
        .optional()
        .describe('Spatial weights: queen contiguity for polygons, knn for points/H3. Default: auto-detect.'),
      k: z.number().min(1).max(20).optional().describe('Number of nearest neighbours for knn weights. Default 5.'),
      permutations: z.number().min(99).max(9999).optional().describe('Permutation count for pseudo p-values. Default 499.'),
      significance: z
        .number()
        .min(0.001)
        .max(0.2)
        .optional()
        .describe('Significance threshold for cluster classification. Default 0.05.'),
      newDatasetName: z.string().optional().describe('Output dataset name. Default: <dataset>_lisa'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Auto-create layer for output dataset and apply LISA colour preset. Default true.')
    }),
    execute: async ({datasetName, valueField, weightType, k, permutations, significance, newDatasetName, showOnMap}) => {
      const vis = getCurrentVisState();
      const dataset = resolveDatasetByName(vis?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }

      const resolvedValueField = resolveDatasetFieldName(dataset, valueField);
      if (!resolvedValueField) {
        return {llmResult: {success: false, details: `Field "${valueField}" not found in dataset "${datasetName}".`}};
      }

      const geomField = resolveGeojsonFieldName(dataset, null);
      const h3Field = !geomField ? resolveH3FieldName(dataset, null) : null;
      if (!geomField && !h3Field) {
        return {
          llmResult: {
            success: false,
            details: 'Dataset must have a GeoJSON geometry field or H3 field for spatial autocorrelation.'
          }
        };
      }

      // Determine weight type when not specified
      const effectiveWeightType: 'queen' | 'knn' =
        weightType === 'queen'
          ? 'queen'
          : weightType === 'knn'
          ? 'knn'
          : h3Field
          ? 'queen' // H3 uses grid-disk adjacency (queen-like)
          : geomField
          ? 'queen'
          : 'knn';

      const outName = String(newDatasetName || '').trim() || `${dataset.label || dataset.id}_lisa`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        vis?.datasets || {},
        outName,
        'qmap_lisa'
      );

      const fieldCatalog = Array.from(
        new Set([
          ...((dataset.fields || []).map((f: any) => String(f?.name || '')).filter(Boolean) as string[]),
          'lisa_cluster',
          'lisa_local_i',
          'lisa_p_value'
        ])
      );

      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          fieldCatalog,
          numericFields: ['lisa_local_i', 'lisa_p_value'],
          styleableFields: ['lisa_cluster'],
          defaultStyleField: 'lisa_cluster',
          details:
            `Computing LISA spatial autocorrelation on field "${resolvedValueField}" ` +
            `(weights: ${effectiveWeightType}, permutations: ${permutations || 499}).` +
            `${showOnMap !== false ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('spatial-autocorrelation'),
          datasetId: dataset.id,
          valueField: resolvedValueField,
          geometryField: geomField || null,
          h3Field: h3Field || null,
          weightType: effectiveWeightType,
          k: Math.max(1, Math.min(20, Number(k || 5))),
          permutations: Math.max(99, Math.min(9999, Number(permutations || 499))),
          significance: Number.isFinite(Number(significance)) ? Number(significance) : 0.05,
          maxFeatures: resolveOptionalFeatureCap(undefined),
          showOnMap: showOnMap !== false,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function ComputeQMapSpatialAutocorrelationComponent({
      executionKey,
      datasetId,
      valueField,
      geometryField,
      h3Field,
      weightType,
      k,
      permutations,
      significance,
      maxFeatures,
      showOnMap,
      newDatasetName
    }: {
      executionKey?: string;
      datasetId: string;
      valueField: string;
      geometryField: string | null;
      h3Field: string | null;
      weightType: 'queen' | 'knn';
      k: number;
      permutations: number;
      significance: number;
      maxFeatures: number;
      showOnMap: boolean;
      newDatasetName: string;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const localLayers = useSelector(selectQMapLayers) as any[];
      const localDatasets = useSelector(selectQMapDatasets) as Record<string, any>;
      const cancelledRef = React.useRef(false);
      const {shouldSkip, complete} = useToolExecution({
        executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });

      useEffect(() => {
        return () => {
          cancelledRef.current = true;
        };
      }, []);

      useEffect(() => {
        if (shouldSkip()) return;
        const datasets = localVisState?.datasets || {};
        const dataset = datasets[datasetId];
        if (!dataset) return;
        complete();

        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }

        (async () => {
          try {
            const idx = getFilteredDatasetIndexes(dataset, localVisState, true).slice(0, maxFeatures);
            if (!idx.length) return;

            // Build feature list for worker
            const featuresRaw = await mapIndexesChunked(
              idx,
              (rowIdx: number) => {
                const geomRaw = geometryField
                  ? parseGeoJsonLike(dataset.getValue(geometryField, rowIdx))
                  : h3Field
                  ? h3CellToPolygonFeature(dataset.getValue(String(h3Field), rowIdx))
                  : null;
                const feature = geomRaw ? toTurfFeature(geomRaw) : null;
                const value = dataset.getValue(valueField, rowIdx);
                const numericValue = value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value)) ? Number(value) : null;
                return {
                  geometry: feature ? (feature as any)?.geometry ?? null : null,
                  h3Id: h3Field ? dataset.getValue(String(h3Field), rowIdx) : null,
                  value: numericValue,
                  rowIdx
                };
              },
              250
            );

            if (cancelledRef.current) return;

            type ValidFeature = {geometry: unknown; h3Id: unknown; value: number | null; rowIdx: number};
            const validFeatures = (featuresRaw as Array<ValidFeature | null>).filter(
              (f): f is ValidFeature => f !== null
            );
            if (validFeatures.length < 3) return;

            // Run LISA in worker
            const workerResult = await runLisaJob({
              features: validFeatures.map((f: ValidFeature) => ({
                geometry: f.geometry ?? undefined,
                h3Id: f.h3Id ?? undefined,
                value: f.value
              })),
              weightType,
              k,
              permutations,
              significance
            });

            if (cancelledRef.current) return;

            // Build output rows with original fields + LISA columns
            await yieldToMainThread();

            const outRows: Array<Record<string, unknown>> = [];
            for (let fi = 0; fi < validFeatures.length; fi += 1) {
              const rowIdx = validFeatures[fi].rowIdx;
              const row: Record<string, unknown> = {};
              (dataset.fields || []).forEach((f: any) => {
                row[f.name] = dataset.getValue(f.name, rowIdx);
              });
              row.lisa_cluster = workerResult.clusters[fi] ?? 'NS';
              row.lisa_local_i = workerResult.localI[fi] ?? null;
              row.lisa_p_value = workerResult.pValues[fi] ?? null;
              outRows.push(row);
              if (fi > 0 && fi % 500 === 0) {
                await yieldToMainThread();
                if (cancelledRef.current) return;
              }
            }

            if (cancelledRef.current) return;
            if (!outRows.length) return;

            upsertDerivedDatasetRows(
              localDispatch,
              datasets,
              newDatasetName,
              outRows,
              'qmap_lisa',
              showOnMap
            );

            // Apply 5-color LISA styling if showOnMap is true
            // We need to wait a tick for the dataset to be registered then find the layer
            if (showOnMap) {
              await yieldToMainThread();
              if (cancelledRef.current) return;

              const currentDatasets = localVisState?.datasets || {};
              const outputDataset =
                Object.values(currentDatasets).find(
                  (d: any) => String(d?.label || '').toLowerCase() === String(newDatasetName).toLowerCase()
                ) as any || null;

              if (outputDataset?.id) {
                const target = resolveStyleTargetLayer(localLayers || [], outputDataset, undefined);
                const layer = target?.layer;
                if (layer?.id) {
                  const lisaField = (outputDataset.fields || []).find(
                    (f: any) => String(f?.name || '') === 'lisa_cluster'
                  );
                  if (lisaField) {
                    const colorRange = ensureColorRange({
                      name: 'qmap.lisa5',
                      type: 'custom',
                      category: 'Custom',
                      colors: LISA_CATEGORY_ORDER.map(cat => LISA_COLORS[cat])
                    });
                    const nextConfig: any = {
                      colorField: lisaField,
                      colorScale: 'custom',
                      visConfig: {
                        ...(layer.config?.visConfig || {}),
                        colorRange
                      }
                    };
                    try {
                      localDispatch(wrapTo('map', layerConfigChange(layer, nextConfig)));
                    } catch {
                      // swallow styling errors
                    }
                  }
                }
              }
            }

            // Compute cluster counts for summary
            const clusterCounts: Record<string, number> = {HH: 0, LL: 0, HL: 0, LH: 0, NS: 0};
            workerResult.clusters.forEach(c => {
              clusterCounts[c] = (clusterCounts[c] || 0) + 1;
            });

            // Dispatch a no-op to force component re-render with result (store in ref for potential future use)
            void {
              globalMoransI: workerResult.globalMoransI,
              zScore: workerResult.zScore,
              pValue: workerResult.pValue,
              clusterCounts
            };
          } finally {
            if (useLoadingIndicator) {
              localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
            }
          }
        })();
      }, [
        localDispatch,
        localVisState,
        localLayers,
        localDatasets,
        executionKey,
        datasetId,
        valueField,
        geometryField,
        h3Field,
        weightType,
        k,
        permutations,
        significance,
        maxFeatures,
        showOnMap,
        newDatasetName,
        shouldSkip,
        complete
      ]);

      return null;
    }
  });
}

// ─── Bivariate Correlation Tool ───────────────────────────────────────────────

export function createComputeQMapBivariateCorrelationTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveGeojsonFieldName,
    resolveH3FieldName,
    resolveDatasetFieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    resolveOptionalFeatureCap,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    shouldUseLoadingIndicator,
    getFilteredDatasetIndexes,
    mapIndexesChunked,
    parseGeoJsonLike,
    h3CellToPolygonFeature,
    toTurfFeature,
    yieldToMainThread,
    upsertDerivedDatasetRows,
    ensureColorRange,
    resolveStyleTargetLayer,
    findDatasetForLayer
  } = ctx;

  return extendedTool({
    description:
      'Compute bivariate spatial correlation between two numeric fields: Pearson r (global linear correlation), ' +
      'Global Bivariate Moran\'s I (spatial autocorrelation of fieldA with lagged fieldB), ' +
      'and Local Bivariate LISA clusters (HH/HL/LH/LL/NS). ' +
      'Adds bivariate_cluster, bivariate_local_i, and bivariate_p_value columns and applies 5-color LISA styling.',
    parameters: z.object({
      datasetName: z.string().describe('Dataset to analyse'),
      fieldA: z.string().describe('Focal variable (X): used as the reference field'),
      fieldB: z.string().describe('Lagged variable (Y): spatially lagged in the bivariate statistic'),
      weightType: z.enum(['queen', 'knn']).optional().describe('Spatial weights type. Default: auto-detect.'),
      k: z.number().min(1).max(20).optional().describe('Nearest neighbours for knn weights. Default 5.'),
      permutations: z.number().min(99).max(9999).optional().describe('Permutations for pseudo p-values. Default 499.'),
      significance: z.number().min(0.001).max(0.2).optional().describe('Significance threshold. Default 0.05.'),
      newDatasetName: z.string().optional().describe('Output dataset name. Default: <dataset>_bivariate'),
      showOnMap: z.boolean().optional().describe('Create layer and apply LISA colour preset. Default true.')
    }),
    execute: async ({datasetName, fieldA, fieldB, weightType, k, permutations, significance, newDatasetName, showOnMap}) => {
      const vis = getCurrentVisState();
      const dataset = resolveDatasetByName(vis?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }

      const resolvedFieldA = resolveDatasetFieldName(dataset, fieldA);
      if (!resolvedFieldA) {
        return {llmResult: {success: false, details: `Field "${fieldA}" not found in dataset "${datasetName}".`}};
      }
      const resolvedFieldB = resolveDatasetFieldName(dataset, fieldB);
      if (!resolvedFieldB) {
        return {llmResult: {success: false, details: `Field "${fieldB}" not found in dataset "${datasetName}".`}};
      }

      const geomField = resolveGeojsonFieldName(dataset, null);
      const h3Field = !geomField ? resolveH3FieldName(dataset, null) : null;
      if (!geomField && !h3Field) {
        return {
          llmResult: {
            success: false,
            details: 'Dataset must have a GeoJSON geometry field or H3 field for bivariate LISA.'
          }
        };
      }

      const effectiveWeightType: 'queen' | 'knn' =
        weightType === 'queen' ? 'queen' : weightType === 'knn' ? 'knn' : 'queen';

      const outName = String(newDatasetName || '').trim() || `${dataset.label || dataset.id}_bivariate`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        vis?.datasets || {},
        outName,
        'qmap_bivariate'
      );

      const fieldCatalog = Array.from(
        new Set([
          ...((dataset.fields || []).map((f: any) => String(f?.name || '')).filter(Boolean) as string[]),
          'bivariate_cluster',
          'bivariate_local_i',
          'bivariate_p_value'
        ])
      );

      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          fieldCatalog,
          numericFields: ['bivariate_local_i', 'bivariate_p_value'],
          styleableFields: ['bivariate_cluster'],
          defaultStyleField: 'bivariate_cluster',
          details:
            `Computing bivariate correlation between "${resolvedFieldA}" (focal) and "${resolvedFieldB}" (lagged) ` +
            `(weights: ${effectiveWeightType}, permutations: ${permutations || 499}).`
        },
        additionalData: {
          executionKey: makeExecutionKey('bivariate-correlation'),
          datasetId: dataset.id,
          fieldA: resolvedFieldA,
          fieldB: resolvedFieldB,
          geometryField: geomField || null,
          h3Field: h3Field || null,
          weightType: effectiveWeightType,
          k: Math.max(1, Math.min(20, Number(k || 5))),
          permutations: Math.max(99, Math.min(9999, Number(permutations || 499))),
          significance: Number.isFinite(Number(significance)) ? Number(significance) : 0.05,
          maxFeatures: resolveOptionalFeatureCap(undefined),
          showOnMap: showOnMap !== false,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function ComputeQMapBivariateCorrelationComponent({
      executionKey,
      datasetId,
      fieldA,
      fieldB,
      geometryField,
      h3Field,
      weightType,
      k,
      permutations,
      significance,
      maxFeatures,
      showOnMap,
      newDatasetName
    }: {
      executionKey?: string;
      datasetId: string;
      fieldA: string;
      fieldB: string;
      geometryField: string | null;
      h3Field: string | null;
      weightType: 'queen' | 'knn';
      k: number;
      permutations: number;
      significance: number;
      maxFeatures: number;
      showOnMap: boolean;
      newDatasetName: string;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const localLayers = useSelector(selectQMapLayers) as any[];
      const localDatasets = useSelector(selectQMapDatasets) as Record<string, any>;
      const cancelledRef = React.useRef(false);
      const {shouldSkip, complete} = useToolExecution({
        executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });

      useEffect(() => {
        return () => {
          cancelledRef.current = true;
        };
      }, []);

      useEffect(() => {
        if (shouldSkip()) return;
        const datasets = localVisState?.datasets || {};
        const dataset = datasets[datasetId];
        if (!dataset) return;
        complete();

        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }

        (async () => {
          try {
            const idx = getFilteredDatasetIndexes(dataset, localVisState, true).slice(0, maxFeatures);
            if (!idx.length) return;

            const featuresRaw = await mapIndexesChunked(
              idx,
              (rowIdx: number) => {
                const geomRaw = geometryField
                  ? parseGeoJsonLike(dataset.getValue(geometryField, rowIdx))
                  : h3Field
                  ? h3CellToPolygonFeature(dataset.getValue(String(h3Field), rowIdx))
                  : null;
                const feature = geomRaw ? toTurfFeature(geomRaw) : null;
                const valA = dataset.getValue(fieldA, rowIdx);
                const valB = dataset.getValue(fieldB, rowIdx);
                return {
                  geometry: feature ? (feature as any)?.geometry ?? null : null,
                  h3Id: h3Field ? dataset.getValue(String(h3Field), rowIdx) : null,
                  valueA: valA !== null && valA !== undefined && valA !== '' && Number.isFinite(Number(valA)) ? Number(valA) : null,
                  valueB: valB !== null && valB !== undefined && valB !== '' && Number.isFinite(Number(valB)) ? Number(valB) : null,
                  rowIdx
                };
              },
              250
            );

            if (cancelledRef.current) return;

            type ValidFeature = {
              geometry: unknown; h3Id: unknown;
              valueA: number | null; valueB: number | null; rowIdx: number
            };
            const validFeatures = (featuresRaw as Array<ValidFeature | null>).filter(
              (f): f is ValidFeature => f !== null
            );
            if (validFeatures.length < 3) return;

            const workerResult = await runBivariateJobWithFallback({
              featuresA: validFeatures.map(f => ({
                geometry: f.geometry ?? undefined,
                h3Id: f.h3Id ?? undefined,
                value: f.valueA
              })),
              featuresB: validFeatures.map(f => ({value: f.valueB})),
              weightType,
              k,
              permutations,
              significance
            });

            if (cancelledRef.current) return;

            await yieldToMainThread();

            const outRows: Array<Record<string, unknown>> = [];
            for (let fi = 0; fi < validFeatures.length; fi += 1) {
              const rowIdx = validFeatures[fi].rowIdx;
              const row: Record<string, unknown> = {};
              (dataset.fields || []).forEach((f: any) => {
                row[f.name] = dataset.getValue(f.name, rowIdx);
              });
              row.bivariate_cluster = workerResult.clusters[fi] ?? 'NS';
              row.bivariate_local_i = workerResult.localI[fi] ?? null;
              row.bivariate_p_value = workerResult.pValues[fi] ?? null;
              outRows.push(row);
              if (fi > 0 && fi % 500 === 0) {
                await yieldToMainThread();
                if (cancelledRef.current) return;
              }
            }

            if (cancelledRef.current) return;
            if (!outRows.length) return;

            upsertDerivedDatasetRows(
              localDispatch,
              datasets,
              newDatasetName,
              outRows,
              'qmap_bivariate',
              showOnMap
            );

            if (showOnMap) {
              await yieldToMainThread();
              if (cancelledRef.current) return;

              const currentDatasets = localVisState?.datasets || {};
              const outputDataset =
                Object.values(currentDatasets).find(
                  (d: any) => String(d?.label || '').toLowerCase() === String(newDatasetName).toLowerCase()
                ) as any || null;

              if (outputDataset?.id) {
                const target = resolveStyleTargetLayer(localLayers || [], outputDataset, undefined);
                const layer = target?.layer;
                if (layer?.id) {
                  const clusterField = (outputDataset.fields || []).find(
                    (f: any) => String(f?.name || '') === 'bivariate_cluster'
                  );
                  if (clusterField) {
                    const colorRange = ensureColorRange({
                      name: 'qmap.lisa5',
                      type: 'custom',
                      category: 'Custom',
                      colors: LISA_CATEGORY_ORDER.map(cat => LISA_COLORS[cat])
                    });
                    const nextConfig: any = {
                      colorField: clusterField,
                      colorScale: 'custom',
                      visConfig: {
                        ...(layer.config?.visConfig || {}),
                        colorRange
                      }
                    };
                    try {
                      localDispatch(wrapTo('map', layerConfigChange(layer, nextConfig)));
                    } catch {
                      // swallow styling errors
                    }
                  }
                }
              }
            }
          } finally {
            if (useLoadingIndicator) {
              localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
            }
          }
        })();
      }, [
        localDispatch,
        localVisState,
        localLayers,
        localDatasets,
        executionKey,
        datasetId,
        fieldA,
        fieldB,
        geometryField,
        h3Field,
        weightType,
        k,
        permutations,
        significance,
        maxFeatures,
        showOnMap,
        newDatasetName,
        shouldSkip,
        complete
      ]);

      return null;
    }
  });
}

// ─── Hotspot (Getis-Ord Gi*) worker runner ────────────────────────────────────

type HotspotPayload = {
  features: Array<{geometry?: unknown; h3Id?: unknown; value: number | null}>;
  weightType: 'queen' | 'knn';
  k: number;
  significance: number;
};

function runHotspotJob(payload: HotspotPayload): Promise<HotspotResult['payload']> {
  return new Promise((resolve, reject) => {
    let worker: Worker | null = null;
    try {
      worker = new Worker(
        new URL('../../../workers/spatial-autocorrelation.worker.ts', import.meta.url),
        {type: 'module'}
      );
    } catch {
      reject(new Error('Failed to instantiate spatial-autocorrelation worker.'));
      return;
    }
    const jobId = `hotspot-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    worker.onmessage = (event: MessageEvent) => {
      const msg = event.data;
      if (!msg || msg.id !== jobId) return;
      if (msg.type === 'hotspot_result') {
        worker!.terminate();
        resolve(msg.payload as HotspotResult['payload']);
      } else if (msg.type === 'error') {
        worker!.terminate();
        reject(new Error(String(msg.error || 'Worker error')));
      }
    };
    worker.onerror = (err) => {
      worker!.terminate();
      reject(new Error(err.message || 'Worker error'));
    };
    worker.postMessage({id: jobId, type: 'hotspot', payload});
  });
}

async function runHotspotFallback(payload: HotspotPayload): Promise<HotspotResult['payload']> {
  const mod = await import('../../../workers/spatial-autocorrelation.worker?worker&inline');
  if (mod && typeof (mod as any).default === 'function') {
    const WorkerClass = (mod as any).default;
    return new Promise((resolve, reject) => {
      const w = new WorkerClass();
      const jobId = `hotspot-fallback-${Date.now()}`;
      w.onmessage = (event: MessageEvent) => {
        const msg = event.data;
        if (!msg || msg.id !== jobId) return;
        if (msg.type === 'hotspot_result') { w.terminate(); resolve(msg.payload as HotspotResult['payload']); }
        else if (msg.type === 'error') { w.terminate(); reject(new Error(String(msg.error || 'Worker error'))); }
      };
      w.onerror = (err: any) => { w.terminate(); reject(new Error(err.message || 'Worker error')); };
      w.postMessage({id: jobId, type: 'hotspot', payload});
    });
  }
  throw new Error('Hotspot analysis computation unavailable: worker module not found.');
}

async function runHotspotJobWithFallback(payload: HotspotPayload): Promise<HotspotResult['payload']> {
  try {
    return await runHotspotJob(payload);
  } catch {
    return runHotspotFallback(payload);
  }
}

// ─── Hotspot Analysis Tool ────────────────────────────────────────────────────

export function createComputeQMapHotspotAnalysisTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveGeojsonFieldName,
    resolveH3FieldName,
    resolveDatasetFieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    resolveOptionalFeatureCap,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    shouldUseLoadingIndicator,
    getFilteredDatasetIndexes,
    mapIndexesChunked,
    parseGeoJsonLike,
    h3CellToPolygonFeature,
    toTurfFeature,
    yieldToMainThread,
    upsertDerivedDatasetRows,
    ensureColorRange,
    resolveStyleTargetLayer,
    findDatasetForLayer
  } = ctx;

  return extendedTool({
    description:
      'Compute Getis-Ord Gi* hotspot analysis for a numeric field on a loaded dataset. ' +
      'Identifies statistically significant spatial clusters of high values (HH hotspots) and low values (LL coldspots). ' +
      'Adds hotspot_z (Gi* z-score), hotspot_p (analytical p-value), and hotspot_class (HH/LL/NS) columns to a derived dataset ' +
      'and applies the 5-color LISA styling preset. ' +
      "Unlike LISA (Moran's I), Gi* measures if values in the neighbourhood of i (including i itself) are significantly above or below the global mean.",
    parameters: z.object({
      datasetName: z.string().describe('Dataset to analyse (name or id)'),
      valueField: z.string().describe('Numeric field for hotspot analysis (e.g. population, pm25)'),
      weightType: z
        .enum(['queen', 'knn'])
        .optional()
        .describe('Spatial weights: queen contiguity for polygons/H3, knn for points. Default: queen.'),
      k: z.number().min(1).max(20).optional().describe('Nearest neighbours for knn weights. Default 5.'),
      significance: z
        .number()
        .min(0.001)
        .max(0.2)
        .optional()
        .describe('Significance threshold for HH/LL classification. Default 0.05.'),
      newDatasetName: z.string().optional().describe('Output dataset name. Default: <dataset>_hotspot'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Auto-create layer and apply LISA colour preset. Default true.')
    }),
    execute: async ({datasetName, valueField, weightType, k, significance, newDatasetName, showOnMap}) => {
      const vis = getCurrentVisState();
      const dataset = resolveDatasetByName(vis?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }

      const resolvedValueField = resolveDatasetFieldName(dataset, valueField);
      if (!resolvedValueField) {
        return {
          llmResult: {
            success: false,
            details: `Field "${valueField}" not found in dataset "${datasetName}".`
          }
        };
      }

      const geomField = resolveGeojsonFieldName(dataset, null);
      const h3Field = !geomField ? resolveH3FieldName(dataset, null) : null;
      if (!geomField && !h3Field) {
        return {
          llmResult: {
            success: false,
            details: 'Dataset must have a GeoJSON geometry field or H3 field for hotspot analysis.'
          }
        };
      }

      const effectiveWeightType: 'queen' | 'knn' = weightType === 'knn' ? 'knn' : 'queen';

      const outName = String(newDatasetName || '').trim() || `${dataset.label || dataset.id}_hotspot`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        vis?.datasets || {},
        outName,
        'qmap_hotspot'
      );

      const fieldCatalog = Array.from(
        new Set([
          ...((dataset.fields || []).map((f: any) => String(f?.name || '')).filter(Boolean) as string[]),
          'hotspot_z',
          'hotspot_p',
          'hotspot_class'
        ])
      );

      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          fieldCatalog,
          numericFields: ['hotspot_z', 'hotspot_p'],
          styleableFields: ['hotspot_class'],
          defaultStyleField: 'hotspot_class',
          details:
            `Computing Getis-Ord Gi* hotspot analysis on field "${resolvedValueField}" ` +
            `(weights: ${effectiveWeightType}, significance: ${significance || 0.05}).` +
            `${showOnMap !== false ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('hotspot-analysis'),
          datasetId: dataset.id,
          valueField: resolvedValueField,
          geometryField: geomField || null,
          h3Field: h3Field || null,
          weightType: effectiveWeightType,
          k: Math.max(1, Math.min(20, Number(k || 5))),
          significance: Number.isFinite(Number(significance)) ? Number(significance) : 0.05,
          maxFeatures: resolveOptionalFeatureCap(undefined),
          showOnMap: showOnMap !== false,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function ComputeQMapHotspotAnalysisComponent({
      executionKey,
      datasetId,
      valueField,
      geometryField,
      h3Field,
      weightType,
      k,
      significance,
      maxFeatures,
      showOnMap,
      newDatasetName
    }: {
      executionKey?: string;
      datasetId: string;
      valueField: string;
      geometryField: string | null;
      h3Field: string | null;
      weightType: 'queen' | 'knn';
      k: number;
      significance: number;
      maxFeatures: number;
      showOnMap: boolean;
      newDatasetName: string;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const localLayers = useSelector(selectQMapLayers) as any[];
      const localDatasets = useSelector(selectQMapDatasets) as Record<string, any>;
      const cancelledRef = React.useRef(false);
      const {shouldSkip, complete} = useToolExecution({
        executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });

      useEffect(() => {
        return () => {
          cancelledRef.current = true;
        };
      }, []);

      useEffect(() => {
        if (shouldSkip()) return;
        const datasets = localVisState?.datasets || {};
        const dataset = datasets[datasetId];
        if (!dataset) return;
        complete();

        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }

        (async () => {
          try {
            const idx = getFilteredDatasetIndexes(dataset, localVisState, true).slice(0, maxFeatures);
            if (!idx.length) return;

            const featuresRaw = await mapIndexesChunked(
              idx,
              (rowIdx: number) => {
                const geomRaw = geometryField
                  ? parseGeoJsonLike(dataset.getValue(geometryField, rowIdx))
                  : h3Field
                  ? h3CellToPolygonFeature(dataset.getValue(String(h3Field), rowIdx))
                  : null;
                const feature = geomRaw ? toTurfFeature(geomRaw) : null;
                const value = dataset.getValue(valueField, rowIdx);
                const numericValue = value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value)) ? Number(value) : null;
                return {
                  geometry: feature ? (feature as any)?.geometry ?? null : null,
                  h3Id: h3Field ? dataset.getValue(String(h3Field), rowIdx) : null,
                  value: numericValue,
                  rowIdx
                };
              },
              250
            );

            if (cancelledRef.current) return;

            type ValidFeature = {geometry: unknown; h3Id: unknown; value: number | null; rowIdx: number};
            const validFeatures = (featuresRaw as Array<ValidFeature | null>).filter(
              (f): f is ValidFeature => f !== null
            );
            if (validFeatures.length < 3) return;

            const workerResult = await runHotspotJobWithFallback({
              features: validFeatures.map((f: ValidFeature) => ({
                geometry: f.geometry ?? undefined,
                h3Id: f.h3Id ?? undefined,
                value: f.value
              })),
              weightType,
              k,
              significance
            });

            if (cancelledRef.current) return;

            await yieldToMainThread();

            const outRows: Array<Record<string, unknown>> = [];
            for (let fi = 0; fi < validFeatures.length; fi += 1) {
              const rowIdx = validFeatures[fi].rowIdx;
              const row: Record<string, unknown> = {};
              (dataset.fields || []).forEach((f: any) => {
                row[f.name] = dataset.getValue(f.name, rowIdx);
              });
              row.hotspot_z = workerResult.localGiStar[fi] ?? null;
              row.hotspot_p = workerResult.pValues[fi] ?? null;
              row.hotspot_class = workerResult.clusters[fi] ?? 'NS';
              outRows.push(row);
              if (fi > 0 && fi % 500 === 0) {
                await yieldToMainThread();
                if (cancelledRef.current) return;
              }
            }

            if (cancelledRef.current) return;
            if (!outRows.length) return;

            upsertDerivedDatasetRows(
              localDispatch,
              datasets,
              newDatasetName,
              outRows,
              'qmap_hotspot',
              showOnMap
            );

            if (showOnMap) {
              await yieldToMainThread();
              if (cancelledRef.current) return;

              const currentDatasets = localVisState?.datasets || {};
              const outputDataset =
                (Object.values(currentDatasets).find(
                  (d: any) => String(d?.label || '').toLowerCase() === String(newDatasetName).toLowerCase()
                ) as any) || null;

              if (outputDataset?.id) {
                const target = resolveStyleTargetLayer(localLayers || [], outputDataset, undefined);
                const layer = target?.layer;
                if (layer?.id) {
                  const hotspotField = (outputDataset.fields || []).find(
                    (f: any) => String(f?.name || '') === 'hotspot_class'
                  );
                  if (hotspotField) {
                    const colorRange = ensureColorRange({
                      name: 'qmap.lisa5',
                      type: 'custom',
                      category: 'Custom',
                      colors: LISA_CATEGORY_ORDER.map(cat => LISA_COLORS[cat])
                    });
                    const nextConfig: any = {
                      colorField: hotspotField,
                      colorScale: 'custom',
                      visConfig: {
                        ...(layer.config?.visConfig || {}),
                        colorRange
                      }
                    };
                    try {
                      localDispatch(wrapTo('map', layerConfigChange(layer, nextConfig)));
                    } catch {
                      // swallow styling errors
                    }
                  }
                }
              }
            }
          } finally {
            if (useLoadingIndicator) {
              localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
            }
          }
        })();
      }, [
        localDispatch,
        localVisState,
        localLayers,
        localDatasets,
        executionKey,
        datasetId,
        valueField,
        geometryField,
        h3Field,
        weightType,
        k,
        significance,
        maxFeatures,
        showOnMap,
        newDatasetName,
        shouldSkip,
        complete
      ]);

      return null;
    }
  });
}
