import React, {useEffect} from 'react';
import {layerConfigChange, setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapLayers, selectQMapVisState} from '../../../state/qmap-selectors';
import {useToolExecution} from './use-tool-execution';
import type {BivariateResult} from '../../../workers/spatial-autocorrelation.worker';

import type {QMapToolContext} from '../context/tool-context';
import {LISA_COLORS, LISA_CATEGORY_ORDER} from './autocorrelation';


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

// --- Tool factory ---

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

  return {
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
    execute: async ({datasetName, fieldA, fieldB, weightType, k, permutations, significance, newDatasetName, showOnMap}: any) => {
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
  };
}
