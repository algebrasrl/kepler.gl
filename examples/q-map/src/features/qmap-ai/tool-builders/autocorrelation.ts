import React, {useEffect} from 'react';
import {layerConfigChange, setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapLayers, selectQMapVisState} from '../../../state/qmap-selectors';
import {useToolExecution} from './use-tool-execution';
import type {LisaCluster} from '../../../workers/spatial-autocorrelation.worker';

import type {QMapToolContext} from '../context/tool-context';

// ─── LISA colour preset (shared with bivariate/hotspot) ───────────────────────

const LISA_COLORS: Record<LisaCluster, string> = {
  HH: '#d73027',
  LL: '#4575b4',
  HL: '#fc8d59',
  LH: '#91bfdb',
  NS: '#cccccc'
};

const LISA_CATEGORY_ORDER: LisaCluster[] = ['HH', 'HL', 'LH', 'LL', 'NS'];

export {LISA_COLORS, LISA_CATEGORY_ORDER};

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
    };
    worker.onerror = (err) => {
      worker!.terminate();
      reject(new Error(err.message || 'Worker error'));
    };
    worker.postMessage({id: jobId, type: 'lisa', payload});
  });
}

async function runSpatialAutocorrelationFallback(payload: WorkerPayload): Promise<WorkerResult> {
  const mod = await import('../../../workers/spatial-autocorrelation.worker?worker&inline');
  if (mod && typeof (mod as any).default === 'function') {
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

  return {
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
    execute: async ({datasetName, valueField, weightType, k, permutations, significance, newDatasetName, showOnMap}: any) => {
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
  };
}
