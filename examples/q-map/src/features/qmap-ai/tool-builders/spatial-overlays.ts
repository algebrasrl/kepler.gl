import React, {useEffect} from 'react';
import {setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {extendedTool} from '../tool-shim';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapVisState} from '../../../state/qmap-selectors';
import {runSpatialOpsJob, computeSpatialOpsTimeout} from '../../../workers/spatial-ops-runner';
import type {QMapToolContext} from '../context/tool-context';
import {useToolExecution} from './use-tool-execution';

export function createSpatialJoinByPredicateTool(ctx: QMapToolContext) {
  const {
    QMAP_SPATIAL_PREDICATE_SCHEMA,
    QMAP_AGGREGATION_REQUIRED_SCHEMA,
    QMAP_DEFAULT_CHUNK_SIZE,
    QMAP_AUTO_HIDE_SOURCE_LAYERS,
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
    geometryToBbox,
    upsertDerivedDatasetRows,
    hideLayersForDatasetIds
  } = ctx;

  return extendedTool({
    description:
      'Spatially join two geometry/H3 datasets by predicate and materialize a derived dataset with aggregated right-side metrics.',
    parameters: z.object({
      leftDatasetName: z.string().describe('Primary dataset (rows preserved)'),
      rightDatasetName: z.string().describe('Secondary dataset used for spatial matching'),
      leftGeometryField: z.string().optional(),
      rightGeometryField: z.string().optional(),
      predicate: QMAP_SPATIAL_PREDICATE_SCHEMA.describe('Default intersects'),
      rightValueField: z.string().optional().describe('Numeric right field for sum/avg/min/max'),
      aggregations: z.array(QMAP_AGGREGATION_REQUIRED_SCHEMA).optional().describe('Default ["count"]'),
      includeRightFields: z.array(z.string()).optional().describe('Optional right fields copied when single match'),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxLeftFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on left features. Unset = full matched coverage (no truncation).'),
      maxRightFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on right features. Unset = full matched coverage (no truncation).'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default false. Set true to auto-create a map layer for the output dataset.'),
      newDatasetName: z.string().optional()
    }),
    execute: async ({
      leftDatasetName,
      rightDatasetName,
      leftGeometryField,
      rightGeometryField,
      predicate,
      rightValueField,
      aggregations,
      includeRightFields,
      useActiveFilters,
      maxLeftFeatures,
      maxRightFeatures,
      showOnMap,
      newDatasetName
    }) => {
      const currentVisState = getCurrentVisState();
      const left = resolveDatasetByName(currentVisState?.datasets || {}, leftDatasetName);
      const right = resolveDatasetByName(currentVisState?.datasets || {}, rightDatasetName);
      if (!left?.id) return {llmResult: {success: false, details: `Left dataset "${leftDatasetName}" not found.`}};
      if (!right?.id) return {llmResult: {success: false, details: `Right dataset "${rightDatasetName}" not found.`}};
      const leftGeom = resolveGeojsonFieldName(left, leftGeometryField);
      const rightGeom = resolveGeojsonFieldName(right, rightGeometryField);
      const leftH3 = !leftGeom ? resolveH3FieldName(left, leftGeometryField || null) : null;
      const rightH3 = !rightGeom ? resolveH3FieldName(right, rightGeometryField || null) : null;
      if ((!leftGeom && !leftH3) || (!rightGeom && !rightH3)) {
        return {
          llmResult: {
            success: false,
            details: 'Both datasets must include either a geojson field or an H3 field (h3_id/h3__id).'
          }
        };
      }
      const resolvedRightValue = rightValueField ? resolveDatasetFieldName(right, rightValueField) : null;
      const target =
        String(newDatasetName || '').trim() || `${left.label || left.id}_join_${right.label || right.id}`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        currentVisState?.datasets || {},
        target,
        'qmap_spatial_join'
      );
      const normalizedAggregations = (aggregations?.length ? aggregations : ['count']) as Array<
        'count' | 'sum' | 'avg' | 'min' | 'max'
      >;
      const aggregationOutputs: Record<string, string> = {};
      if (normalizedAggregations.includes('count')) aggregationOutputs.count = 'join_count';
      if (normalizedAggregations.includes('sum') && resolvedRightValue) aggregationOutputs.sum = 'join_sum';
      if (normalizedAggregations.includes('avg') && resolvedRightValue) aggregationOutputs.avg = 'join_avg';
      if (normalizedAggregations.includes('min') && resolvedRightValue) aggregationOutputs.min = 'join_min';
      if (normalizedAggregations.includes('max') && resolvedRightValue) aggregationOutputs.max = 'join_max';
      const copiedRightFields = (includeRightFields || [])
        .map((field: any) => String(field || '').trim())
        .filter(Boolean)
        .map((field: any) => `right_${field}`);
      const fieldCatalog = Array.from(
        new Set([
          ...((left.fields || []).map((field: any) => String(field?.name || '').trim()).filter(Boolean) as string[]),
          ...Object.values(aggregationOutputs),
          ...copiedRightFields
        ])
      );
      const styleableFields = Object.values(aggregationOutputs);
      const defaultStyleField =
        aggregationOutputs.sum ||
        aggregationOutputs.count ||
        aggregationOutputs.avg ||
        aggregationOutputs.max ||
        aggregationOutputs.min ||
        '';
      const fieldAliases: Record<string, string> = {};
      if (resolvedRightValue) {
        const aliasBase = String(resolvedRightValue).trim();
        if (aliasBase) {
          if (aggregationOutputs.sum) {
            fieldAliases[`sum_${aliasBase}`] = aggregationOutputs.sum;
            fieldAliases[`${aliasBase}_sum`] = aggregationOutputs.sum;
          }
          if (aggregationOutputs.avg) {
            fieldAliases[`avg_${aliasBase}`] = aggregationOutputs.avg;
            fieldAliases[`${aliasBase}_avg`] = aggregationOutputs.avg;
          }
          if (aggregationOutputs.min) {
            fieldAliases[`min_${aliasBase}`] = aggregationOutputs.min;
            fieldAliases[`${aliasBase}_min`] = aggregationOutputs.min;
          }
          if (aggregationOutputs.max) {
            fieldAliases[`max_${aliasBase}`] = aggregationOutputs.max;
            fieldAliases[`${aliasBase}_max`] = aggregationOutputs.max;
          }
        }
      }
      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          fieldCatalog,
          numericFields: styleableFields,
          styleableFields,
          defaultStyleField,
          aggregationOutputs,
          fieldAliases,
          details:
            `Running spatial join ${predicate || 'intersects'}: "${left.label || left.id}" <- "${
              right.label || right.id
            }".` + `${showOnMap === true ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('spatial-join-by-predicate'),
          leftDatasetId: left.id,
          rightDatasetId: right.id,
          leftGeometryField: leftGeom || null,
          rightGeometryField: rightGeom || null,
          leftH3Field: leftH3 || null,
          rightH3Field: rightH3 || null,
          predicate: (predicate || 'intersects') as 'intersects' | 'within' | 'contains' | 'touches',
          rightValueField: resolvedRightValue,
          aggregations: normalizedAggregations,
          includeRightFields: Array.isArray(includeRightFields) ? includeRightFields : [],
          useActiveFilters: useActiveFilters !== false,
          maxLeftFeatures: resolveOptionalFeatureCap(maxLeftFeatures),
          maxRightFeatures: resolveOptionalFeatureCap(maxRightFeatures),
          showOnMap: showOnMap === true,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId,
          fieldCatalog,
          numericFields: styleableFields,
          styleableFields,
          defaultStyleField,
          aggregationOutputs,
          fieldAliases
        }
      };
    },
    component: function SpatialJoinByPredicateComponent({
      executionKey,
      leftDatasetId,
      rightDatasetId,
      leftGeometryField,
      rightGeometryField,
      leftH3Field,
      rightH3Field,
      predicate,
      rightValueField,
      aggregations,
      includeRightFields,
      useActiveFilters,
      maxLeftFeatures,
      maxRightFeatures,
      showOnMap,
      newDatasetName
    }: {
      executionKey?: string;
      leftDatasetId: string;
      rightDatasetId: string;
      leftGeometryField: string | null;
      rightGeometryField: string | null;
      leftH3Field: string | null;
      rightH3Field: string | null;
      predicate: 'intersects' | 'within' | 'contains' | 'touches';
      rightValueField: string | null;
      aggregations: Array<'count' | 'sum' | 'avg' | 'min' | 'max'>;
      includeRightFields: string[];
      useActiveFilters: boolean;
      maxLeftFeatures: number;
      maxRightFeatures: number;
      showOnMap: boolean;
      newDatasetName: string;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const cancelledRef = React.useRef(false);
      const activeAbortControllersRef = React.useRef<Set<AbortController>>(new Set());
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey});
      useEffect(() => {
        return () => {
          cancelledRef.current = true;
          activeAbortControllersRef.current.forEach(controller => {
            try { controller.abort(); } catch { /* ignore */ }
          });
          activeAbortControllersRef.current.clear();
        };
      }, []);
      useEffect(() => {
        if (shouldSkip()) return;
        const datasets = localVisState?.datasets || {};
        const left = datasets[leftDatasetId];
        const right = datasets[rightDatasetId];
        if (!left || !right) return;
        complete();

        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }

        (async () => {
          try {
            const runWithAbortSignal = async <T,>(runner: (signal: AbortSignal) => Promise<T>): Promise<T> => {
              const controller = new AbortController();
              activeAbortControllersRef.current.add(controller);
              try {
                return await runner(controller.signal);
              } finally {
                activeAbortControllersRef.current.delete(controller);
              }
            };

            const leftIdx = getFilteredDatasetIndexes(left, localVisState, useActiveFilters).slice(0, maxLeftFeatures);
            const rightIdx = getFilteredDatasetIndexes(right, localVisState, useActiveFilters).slice(0, maxRightFeatures);
            const resolvedRightFieldPairs = (includeRightFields || [])
              .map(requestedName => ({
                requestedName,
                resolvedName: resolveDatasetFieldName(right, requestedName)
              }))
              .filter((entry: {requestedName: string; resolvedName: string | null}) => Boolean(entry.resolvedName)) as Array<{
              requestedName: string;
              resolvedName: string;
            }>;

            // Prepare right features for worker (serializable)
            const rightFeaturesForWorker = await mapIndexesChunked(
              rightIdx,
              (rowIdx: number) => {
                const rightGeometryRaw = rightGeometryField
                  ? parseGeoJsonLike(right.getValue(rightGeometryField, rowIdx))
                  : h3CellToPolygonFeature(right.getValue(String(rightH3Field || ''), rowIdx));
                const feature = toTurfFeature(rightGeometryRaw);
                if (!feature) return null;
                const value = rightValueField ? Number(right.getValue(rightValueField, rowIdx)) : NaN;
                const pickedFields: Record<string, unknown> = {};
                resolvedRightFieldPairs.forEach(({requestedName, resolvedName}) => {
                  pickedFields[requestedName] = right.getValue(resolvedName, rowIdx);
                });
                return {
                  geometry: feature,
                  value,
                  pickedFields,
                  bbox: geometryToBbox((feature as any)?.geometry)
                };
              },
              Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
            );
            if (cancelledRef.current) return;
            const rightFeatures = rightFeaturesForWorker.filter(Boolean) as Array<{
              geometry: any;
              value: number;
              pickedFields: Record<string, unknown>;
              bbox: [number, number, number, number] | null;
            }>;
            if (!rightFeatures.length) return;

            // Prepare left features for worker (with all properties)
            const leftFeaturesForWorker = await mapIndexesChunked(
              leftIdx,
              (rowIdx: number) => {
                const leftGeometryRaw = leftGeometryField
                  ? parseGeoJsonLike(left.getValue(leftGeometryField, rowIdx))
                  : h3CellToPolygonFeature(left.getValue(String(leftH3Field || ''), rowIdx));
                const feature = toTurfFeature(leftGeometryRaw);
                if (!feature) return null;
                const properties: Record<string, unknown> = {};
                (left.fields || []).forEach((f: any) => {
                  properties[f.name] = left.getValue(f.name, rowIdx);
                });
                return {geometry: feature, properties};
              },
              Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
            );
            if (cancelledRef.current) return;
            const leftFeatures = leftFeaturesForWorker.filter(Boolean) as Array<{
              geometry: any;
              properties: Record<string, unknown>;
            }>;
            if (!leftFeatures.length) return;

            const pairEstimate = leftFeatures.length * rightFeatures.length;
            const timeoutMs = computeSpatialOpsTimeout('spatialJoinByPredicate', pairEstimate);

            const workerResult = await runWithAbortSignal(signal =>
              runSpatialOpsJob({
                name: 'spatialJoinByPredicate',
                payload: {
                  predicate,
                  aggregations: aggregations || ['count'],
                  leftFeatures,
                  rightFeatures,
                  includeRightFields: resolvedRightFieldPairs.map(p => p.requestedName)
                },
                timeoutMs,
                signal
              })
            );
            if (cancelledRef.current) return;
            if (!workerResult.rows.length) return;
            upsertDerivedDatasetRows(
              localDispatch,
              datasets,
              newDatasetName,
              workerResult.rows,
              'qmap_spatial_join',
              showOnMap
            );
            if (showOnMap && QMAP_AUTO_HIDE_SOURCE_LAYERS) {
              hideLayersForDatasetIds(localDispatch, localVisState?.layers || [], [left.id, right.id]);
            }
          } catch (error) {
            if ((error as Error)?.name === 'AbortError' && cancelledRef.current) return;
            throw error;
          } finally {
            if (useLoadingIndicator) {
              localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
            }
          }
        })();
      }, [
        localDispatch,
        localVisState,
        executionKey,
        leftDatasetId,
        rightDatasetId,
        leftGeometryField,
        rightGeometryField,
        leftH3Field,
        rightH3Field,
        predicate,
        rightValueField,
        aggregations,
        includeRightFields,
        useActiveFilters,
        maxLeftFeatures,
        maxRightFeatures,
        showOnMap,
        newDatasetName,
        shouldSkip,
        complete
      ]);
      return null;
    }
  });

}

export function createOverlayDifferenceTool(ctx: QMapToolContext) {
  const {
    QMAP_DEFAULT_CHUNK_SIZE,
    getCurrentVisState,
    resolveDatasetByName,
    resolveGeojsonFieldName,
    resolveH3FieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    resolveOptionalFeatureCap,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    shouldUseLoadingIndicator,
    getFilteredDatasetIndexes,
    mapIndexesChunked,
    parseGeoJsonLike,
    toTurfPolygonFeature,
    h3CellToPolygonFeature,
    geometryToBbox,
    upsertDerivedDatasetRows
  } = ctx;

  return extendedTool({
    description:
      'Overlay two polygon/H3 datasets and materialize intersection/difference geometries for gap analysis.',
    parameters: z.object({
      datasetAName: z.string(),
      datasetBName: z.string(),
      geometryFieldA: z.string().optional(),
      geometryFieldB: z.string().optional(),
      includeIntersection: z.boolean().optional().describe('Default true'),
      includeADifference: z.boolean().optional().describe('Default true'),
      includeBDifference: z.boolean().optional().describe('Default false'),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxFeaturesA: z
        .number()
        .optional()
        .describe('Optional explicit cap on dataset A features. Unset = full matched coverage (no truncation).'),
      maxFeaturesB: z
        .number()
        .optional()
        .describe('Optional explicit cap on dataset B features. Unset = full matched coverage (no truncation).'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default false. Set true to auto-create a map layer for the output dataset.'),
      newDatasetName: z.string().optional()
    }),
    execute: async ({
      datasetAName,
      datasetBName,
      geometryFieldA,
      geometryFieldB,
      includeIntersection,
      includeADifference,
      includeBDifference,
      useActiveFilters,
      maxFeaturesA,
      maxFeaturesB,
      showOnMap,
      newDatasetName
    }) => {
      const vis = getCurrentVisState();
      const a = resolveDatasetByName(vis?.datasets || {}, datasetAName);
      const b = resolveDatasetByName(vis?.datasets || {}, datasetBName);
      if (!a?.id) return {llmResult: {success: false, details: `Dataset A "${datasetAName}" not found.`}};
      if (!b?.id) return {llmResult: {success: false, details: `Dataset B "${datasetBName}" not found.`}};
      const aGeom = resolveGeojsonFieldName(a, geometryFieldA);
      const bGeom = resolveGeojsonFieldName(b, geometryFieldB);
      const aH3 = !aGeom ? resolveH3FieldName(a, geometryFieldA || null) : null;
      const bH3 = !bGeom ? resolveH3FieldName(b, geometryFieldB || null) : null;
      if ((!aGeom && !aH3) || (!bGeom && !bH3)) {
        return {
          llmResult: {
            success: false,
            details: 'Both datasets must include either a geojson field or an H3 field (h3_id/h3__id).'
          }
        };
      }
      const target = String(newDatasetName || '').trim() || `${a.label || a.id}_overlay_${b.label || b.id}`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        vis?.datasets || {},
        target,
        'qmap_overlay_diff'
      );
      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          details:
            `Overlay difference between "${a.label || a.id}" and "${b.label || b.id}".` +
            `${showOnMap === true ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('overlay-difference'),
          datasetAId: a.id,
          datasetBId: b.id,
          geometryFieldA: aGeom || null,
          geometryFieldB: bGeom || null,
          h3FieldA: aH3 || null,
          h3FieldB: bH3 || null,
          includeIntersection: includeIntersection !== false,
          includeADifference: includeADifference !== false,
          includeBDifference: includeBDifference === true,
          useActiveFilters: useActiveFilters !== false,
          maxFeaturesA: resolveOptionalFeatureCap(maxFeaturesA),
          maxFeaturesB: resolveOptionalFeatureCap(maxFeaturesB),
          showOnMap: showOnMap === true,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function OverlayDifferenceComponent({
      executionKey,
      datasetAId,
      datasetBId,
      geometryFieldA,
      geometryFieldB,
      h3FieldA,
      h3FieldB,
      includeIntersection,
      includeADifference,
      includeBDifference,
      useActiveFilters,
      maxFeaturesA,
      maxFeaturesB,
      showOnMap,
      newDatasetName
    }: any) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const cancelledRef = React.useRef(false);
      const activeAbortControllersRef = React.useRef<Set<AbortController>>(new Set());
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey});
      useEffect(() => {
        return () => {
          cancelledRef.current = true;
          activeAbortControllersRef.current.forEach(controller => {
            try { controller.abort(); } catch { /* ignore */ }
          });
          activeAbortControllersRef.current.clear();
        };
      }, []);
      useEffect(() => {
        if (shouldSkip()) return;
        const datasets = localVisState?.datasets || {};
        const a = datasets[datasetAId];
        const b = datasets[datasetBId];
        if (!a || !b) return;
        complete();

        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }

        (async () => {
          try {
            const runWithAbortSignal = async <T,>(runner: (signal: AbortSignal) => Promise<T>): Promise<T> => {
              const controller = new AbortController();
              activeAbortControllersRef.current.add(controller);
              try {
                return await runner(controller.signal);
              } finally {
                activeAbortControllersRef.current.delete(controller);
              }
            };

            const aIdx = getFilteredDatasetIndexes(a, localVisState, useActiveFilters).slice(0, maxFeaturesA);
            const bIdx = getFilteredDatasetIndexes(b, localVisState, useActiveFilters).slice(0, maxFeaturesB);
            const aFeaturesRaw = await mapIndexesChunked(
              aIdx,
              (rowIdx: number) => {
                const feature = toTurfPolygonFeature(
                  geometryFieldA
                    ? parseGeoJsonLike(a.getValue(geometryFieldA, rowIdx))
                    : h3CellToPolygonFeature(a.getValue(String(h3FieldA || ''), rowIdx))
                );
                if (!feature) return null;
                return {rowIdx, geometry: feature, bbox: geometryToBbox((feature as any)?.geometry)};
              },
              Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
            );
            if (cancelledRef.current) return;
            const bFeaturesRaw = await mapIndexesChunked(
              bIdx,
              (rowIdx: number) => {
                const feature = toTurfPolygonFeature(
                  geometryFieldB
                    ? parseGeoJsonLike(b.getValue(geometryFieldB, rowIdx))
                    : h3CellToPolygonFeature(b.getValue(String(h3FieldB || ''), rowIdx))
                );
                if (!feature) return null;
                return {rowIdx, geometry: feature, bbox: geometryToBbox((feature as any)?.geometry)};
              },
              Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
            );
            if (cancelledRef.current) return;

            const aFeatures = aFeaturesRaw.filter(Boolean) as Array<{
              rowIdx: number;
              geometry: any;
              bbox: [number, number, number, number] | null;
            }>;
            const bFeatures = bFeaturesRaw.filter(Boolean) as Array<{
              rowIdx: number;
              geometry: any;
              bbox: [number, number, number, number] | null;
            }>;
            if (!aFeatures.length || !bFeatures.length) return;

            const pairEstimate = aFeatures.length * bFeatures.length;
            const timeoutMs = computeSpatialOpsTimeout('overlayDifference', pairEstimate);

            const workerResult = await runWithAbortSignal(signal =>
              runSpatialOpsJob({
                name: 'overlayDifference',
                payload: {
                  includeIntersection,
                  includeADifference,
                  includeBDifference,
                  aFeatures,
                  bFeatures
                },
                timeoutMs,
                signal
              })
            );
            if (cancelledRef.current) return;

            const cleaned = workerResult.rows.filter(
              row =>
                row?._geojson &&
                (row._geojson as any)?.type &&
                !['GeometryCollection'].includes(String((row._geojson as any).type))
            );
            if (!cleaned.length) return;
            upsertDerivedDatasetRows(
              localDispatch,
              datasets,
              newDatasetName,
              cleaned,
              'qmap_overlay_diff',
              showOnMap
            );
          } catch (error) {
            if ((error as Error)?.name === 'AbortError' && cancelledRef.current) return;
            throw error;
          } finally {
            if (useLoadingIndicator) {
              localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
            }
          }
        })();
      }, [
        localDispatch,
        localVisState,
        executionKey,
        datasetAId,
        datasetBId,
        geometryFieldA,
        geometryFieldB,
        h3FieldA,
        h3FieldB,
        includeIntersection,
        includeADifference,
        includeBDifference,
        useActiveFilters,
        maxFeaturesA,
        maxFeaturesB,
        showOnMap,
        newDatasetName,
        shouldSkip,
        complete
      ]);
      return null;
    }
  });

}
