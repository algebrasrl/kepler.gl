import React, {useEffect} from 'react';
import {setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapVisState} from '../../../state/qmap-selectors';
import {runSpatialOpsJob, computeSpatialOpsTimeout} from '../../../workers/spatial-ops-runner';
import {preprocessDualDatasetArgs} from '../tool-args-normalization';
import {useToolExecution} from './use-tool-execution';

import type {QMapToolContext} from '../context/tool-context';

export function createNearestFeatureJoinTool(ctx: QMapToolContext) {
  const {
    QMAP_DEFAULT_CHUNK_SIZE,
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
    turfDistance,
    yieldToMainThread,
    upsertDerivedDatasetRows
  } = ctx;

  return {
    description:
      'Join nearest feature attributes/distances from target dataset to source dataset (geojson or H3).',
    parameters: z.object({
      sourceDatasetName: z.string(),
      targetDatasetName: z.string(),
      sourceGeometryField: z.string().optional(),
      targetGeometryField: z.string().optional(),
      k: z.number().min(1).max(10).optional().describe('Default 1'),
      maxDistanceKm: z.number().positive().optional(),
      includeTargetField: z.string().optional(),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxSourceFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on source features. Unset = full matched coverage (no truncation).'),
      maxTargetFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on target features. Unset = full matched coverage (no truncation).'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default false. Set true to auto-create a map layer for the output dataset.'),
      newDatasetName: z.string().optional()
    }),
    execute: async ({
      sourceDatasetName,
      targetDatasetName,
      sourceGeometryField,
      targetGeometryField,
      k,
      maxDistanceKm,
      includeTargetField,
      useActiveFilters,
      maxSourceFeatures,
      maxTargetFeatures,
      showOnMap,
      newDatasetName
    }: any) => {
      const vis = getCurrentVisState();
      const source = resolveDatasetByName(vis?.datasets || {}, sourceDatasetName);
      const target = resolveDatasetByName(vis?.datasets || {}, targetDatasetName);
      if (!source?.id) return {llmResult: {success: false, details: `Source dataset "${sourceDatasetName}" not found.`}};
      if (!target?.id) return {llmResult: {success: false, details: `Target dataset "${targetDatasetName}" not found.`}};
      const sourceGeom = resolveGeojsonFieldName(source, sourceGeometryField);
      const targetGeom = resolveGeojsonFieldName(target, targetGeometryField);
      const sourceH3 = !sourceGeom ? resolveH3FieldName(source, sourceGeometryField || null) : null;
      const targetH3 = !targetGeom ? resolveH3FieldName(target, targetGeometryField || null) : null;
      if ((!sourceGeom && !sourceH3) || (!targetGeom && !targetH3)) {
        return {
          llmResult: {
            success: false,
            details: 'Both datasets must include either a geojson field or an H3 field (h3_id/h3__id).'
          }
        };
      }
      const includeFieldResolved = includeTargetField ? resolveDatasetFieldName(target, includeTargetField) : null;
      const outName =
        String(newDatasetName || '').trim() || `${source.label || source.id}_nearest_${target.label || target.id}`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        vis?.datasets || {},
        outName,
        'qmap_nearest_join'
      );
      const nearestTargetFieldName = includeFieldResolved ? `nearest_${includeFieldResolved}` : '';
      const fieldCatalog = Array.from(
        new Set(
          [
            ...((source.fields || []).map((field: any) => String(field?.name || '')).filter(Boolean) as string[]),
            'nearest_count',
            'nearest_distance_km',
            nearestTargetFieldName
          ].filter(Boolean)
        )
      );
      const numericFields = ['nearest_count', 'nearest_distance_km'];
      const fieldAliases: Record<string, string> = {
        count: 'nearest_count',
        nearest_total: 'nearest_count',
        distance_km: 'nearest_distance_km',
        nearest_distance: 'nearest_distance_km'
      };
      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          fieldCatalog,
          numericFields,
          styleableFields: numericFields,
          defaultStyleField: 'nearest_distance_km',
          fieldAliases,
          details:
            `Computing nearest-feature join (k=${k || 1}).` +
            `${showOnMap === true ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('nearest-feature-join'),
          sourceDatasetId: source.id,
          targetDatasetId: target.id,
          sourceGeometryField: sourceGeom || null,
          targetGeometryField: targetGeom || null,
          sourceH3Field: sourceH3 || null,
          targetH3Field: targetH3 || null,
          k: Math.max(1, Math.min(10, Number(k || 1))),
          maxDistanceKm: Number.isFinite(Number(maxDistanceKm)) ? Number(maxDistanceKm) : null,
          includeTargetField: includeFieldResolved,
          useActiveFilters: useActiveFilters !== false,
          maxSourceFeatures: resolveOptionalFeatureCap(maxSourceFeatures),
          maxTargetFeatures: resolveOptionalFeatureCap(maxTargetFeatures),
          showOnMap: showOnMap === true,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId,
          fieldAliases
        }
      };
    },
    component: function NearestFeatureJoinComponent(props: any) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const cancelledRef = React.useRef(false);
      const activeAbortControllersRef = React.useRef<Set<AbortController>>(new Set());
      const {shouldSkip, complete} = useToolExecution({
        executionKey: props.executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });
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
        const source = datasets[props.sourceDatasetId];
        const target = datasets[props.targetDatasetId];
        if (!source || !target) return;
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

            const targetIdx = getFilteredDatasetIndexes(target, localVisState, props.useActiveFilters).slice(
              0,
              props.maxTargetFeatures
            );
            const sourceIdx = getFilteredDatasetIndexes(source, localVisState, props.useActiveFilters).slice(
              0,
              props.maxSourceFeatures
            );

            // Prepare target features for worker (raw geometry + picked value)
            const targetFeaturesRaw = await mapIndexesChunked(
              targetIdx,
              (rowIdx: number) => {
                const targetGeometryRaw = props.targetGeometryField
                  ? parseGeoJsonLike(target.getValue(props.targetGeometryField, rowIdx))
                  : h3CellToPolygonFeature(target.getValue(String(props.targetH3Field || ''), rowIdx));
                const feature = toTurfFeature(targetGeometryRaw);
                if (!feature) return null;
                const picked = props.includeTargetField ? target.getValue(props.includeTargetField, rowIdx) : null;
                return {geometry: feature, picked};
              },
              Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
            );
            if (cancelledRef.current) return;
            const targetFeatures = targetFeaturesRaw.filter(Boolean) as Array<{geometry: any; picked: unknown}>;
            if (!targetFeatures.length) return;

            // Prepare source features for worker (with all properties)
            const sourceFeaturesRaw = await mapIndexesChunked(
              sourceIdx,
              (rowIdx: number) => {
                const sourceGeometryRaw = props.sourceGeometryField
                  ? parseGeoJsonLike(source.getValue(props.sourceGeometryField, rowIdx))
                  : h3CellToPolygonFeature(source.getValue(String(props.sourceH3Field || ''), rowIdx));
                const feature = toTurfFeature(sourceGeometryRaw);
                if (!feature) return null;
                const properties: Record<string, unknown> = {};
                (source.fields || []).forEach((f: any) => {
                  properties[f.name] = source.getValue(f.name, rowIdx);
                });
                return {geometry: feature, properties};
              },
              Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
            );
            if (cancelledRef.current) return;
            const sourceFeatures = sourceFeaturesRaw.filter(Boolean) as Array<{
              geometry: any;
              properties: Record<string, unknown>;
            }>;
            if (!sourceFeatures.length) return;

            const topK = Math.max(1, Number(props.k || 1));
            const maxDistance = Number.isFinite(Number(props.maxDistanceKm)) ? Number(props.maxDistanceKm) : null;
            const pairEstimate = sourceFeatures.length * targetFeatures.length;
            const timeoutMs = computeSpatialOpsTimeout('nearestFeatureJoin', pairEstimate);

            let outRows: Array<Record<string, unknown>> = [];
            let workerSucceeded = false;
            try {
              const workerResult = await runWithAbortSignal(signal =>
                runSpatialOpsJob({
                  name: 'nearestFeatureJoin',
                  payload: {
                    sourceFeatures,
                    targetFeatures,
                    k: topK,
                    maxDistanceKm: maxDistance,
                    includeTargetField: props.includeTargetField || null
                  },
                  timeoutMs,
                  signal
                })
              );
              outRows = workerResult.rows;
              workerSucceeded = true;
            } catch (error) {
              if ((error as Error)?.name === 'AbortError' && cancelledRef.current) return;
              // Fallback to local loop
            }

            if (!workerSucceeded) {
              const loopYieldEvery = Math.max(20, Math.floor(QMAP_DEFAULT_CHUNK_SIZE / 3));
              for (let i = 0; i < sourceIdx.length; i += 1) {
                if (cancelledRef.current) return;
                const rowIdx = sourceIdx[i];
                const sourceGeometryRaw = props.sourceGeometryField
                  ? parseGeoJsonLike(source.getValue(props.sourceGeometryField, rowIdx))
                  : h3CellToPolygonFeature(source.getValue(String(props.sourceH3Field || ''), rowIdx));
                const sourceFeature = toTurfFeature(sourceGeometryRaw);
                if (!sourceFeature) {
                  if (i > 0 && i % loopYieldEvery === 0) await yieldToMainThread();
                  continue;
                }
                const sourceCentroid = turfCentroid(sourceFeature as any);
                const sourceLonLat = (sourceCentroid as any)?.geometry?.coordinates as [number, number] | undefined;
                if (!Array.isArray(sourceLonLat) || sourceLonLat.length < 2) continue;

                const nearest: Array<{distanceKm: number; picked: unknown}> = [];
                for (let j = 0; j < targetFeatures.length; j += 1) {
                  const tf = targetFeatures[j];
                  const tCentroid = turfCentroid(tf.geometry as any);
                  const tLonLat = (tCentroid as any)?.geometry?.coordinates;
                  if (!Array.isArray(tLonLat) || tLonLat.length < 2) continue;
                  if (maxDistance !== null) {
                    const dx = Number(sourceLonLat[0]) - Number(tLonLat[0]);
                    const dy = Number(sourceLonLat[1]) - Number(tLonLat[1]);
                    const approxKm = Math.sqrt(dx * dx + dy * dy) * 111;
                    if (approxKm > maxDistance * 1.5) continue;
                  }
                  const distanceKm = turfDistance(sourceCentroid as any, tCentroid as any, {units: 'kilometers'});
                  if (maxDistance !== null && distanceKm > maxDistance) continue;
                  nearest.push({distanceKm, picked: tf.picked});
                  if (j > 0 && j % loopYieldEvery === 0) {
                    await yieldToMainThread();
                    if (cancelledRef.current) return;
                  }
                }
                nearest.sort((a, b) => a.distanceKm - b.distanceKm);
                const top = nearest.slice(0, topK);
                const row: Record<string, unknown> = {};
                (source.fields || []).forEach((f: any) => (row[f.name] = source.getValue(f.name, rowIdx)));
                row.nearest_count = top.length;
                row.nearest_distance_km = top.length ? top[0].distanceKm : null;
                if (props.includeTargetField && top.length) {
                  row[`nearest_${props.includeTargetField}`] = top[0].picked;
                }
                outRows.push(row);
                if (i > 0 && i % loopYieldEvery === 0) await yieldToMainThread();
              }
            }

            if (cancelledRef.current) return;
            if (!outRows.length) return;
            upsertDerivedDatasetRows(
              localDispatch,
              datasets,
              props.newDatasetName,
              outRows,
              'qmap_nearest_join',
              props.showOnMap
            );
          } finally {
            if (useLoadingIndicator) {
              localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
            }
          }
        })();
      }, [localDispatch, localVisState, props, shouldSkip, complete]);
      return null;
    }
  };
}

export function createAdjacencyGraphFromPolygonsTool(ctx: QMapToolContext) {
  const {
    QMAP_TOUCH_PREDICATE_SCHEMA,
    QMAP_DEFAULT_CHUNK_SIZE,
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
    toTurfPolygonFeature,
    geometryToBbox,
    geometryBboxOverlap,
    turfBooleanIntersects,
    turfBooleanTouches,
    yieldToMainThread,
    upsertDerivedDatasetRows
  } = ctx;

  return {
    description: 'Build adjacency graph (edges table) from polygon or H3 datasets.',
    parameters: z.object({
      datasetName: z.string(),
      geometryField: z.string().optional(),
      idField: z.string().optional().describe('Node id field (default row index)'),
      predicate: QMAP_TOUCH_PREDICATE_SCHEMA.describe('Default touches'),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on features. Unset = full matched coverage (no truncation).'),
      newDatasetName: z.string().optional().describe('Default <dataset>_adjacency')
    }),
    execute: async ({datasetName, geometryField, idField, predicate, useActiveFilters, maxFeatures, newDatasetName}: any) => {
      const vis = getCurrentVisState();
      const dataset = resolveDatasetByName(vis?.datasets || {}, datasetName);
      if (!dataset?.id) return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      const geom = resolveGeojsonFieldName(dataset, geometryField);
      const h3Field = !geom ? resolveH3FieldName(dataset, geometryField || null) : null;
      if (!geom && !h3Field) {
        return {
          llmResult: {
            success: false,
            details: 'Dataset must include either a geojson field or an H3 field (h3_id/h3__id).'
          }
        };
      }
      const idResolved = idField ? resolveDatasetFieldName(dataset, idField) : null;
      const targetName = String(newDatasetName || '').trim() || `${dataset.label || dataset.id}_adjacency`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        vis?.datasets || {},
        targetName,
        'qmap_adjacency'
      );
      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          details: `Building adjacency graph using ${predicate || 'touches'}.`
        },
        additionalData: {
          executionKey: makeExecutionKey('adjacency-graph'),
          datasetId: dataset.id,
          geometryField: geom || null,
          h3Field: h3Field || null,
          idField: idResolved,
          predicate: (predicate || 'touches') as 'touches' | 'intersects',
          useActiveFilters: useActiveFilters !== false,
          maxFeatures: resolveOptionalFeatureCap(maxFeatures),
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function AdjacencyGraphFromPolygonsComponent(props: any) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const cancelledRef = React.useRef(false);
      const activeAbortControllersRef = React.useRef<Set<AbortController>>(new Set());
      const {shouldSkip, complete} = useToolExecution({
        executionKey: props.executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });
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
        const dataset = localVisState?.datasets?.[props.datasetId];
        if (!dataset) return;
        complete();

        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }

        (async () => {
          try {
            const idx = getFilteredDatasetIndexes(dataset, localVisState, props.useActiveFilters).slice(0, props.maxFeatures);
            const featuresRaw = await mapIndexesChunked(
              idx,
              (rowIdx: number) => {
                const rawGeometry = props.geometryField
                  ? parseGeoJsonLike(dataset.getValue(props.geometryField, rowIdx))
                  : h3CellToPolygonFeature(dataset.getValue(String(props.h3Field || ''), rowIdx));
                const feature = toTurfPolygonFeature(rawGeometry);
                if (!feature) return null;
                const nodeId = props.idField ? dataset.getValue(props.idField, rowIdx) : rowIdx;
                return {
                  nodeId: String(nodeId),
                  feature,
                  bbox: geometryToBbox((feature as any)?.geometry)
                };
              },
              Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
            );
            if (cancelledRef.current) return;
            const features = featuresRaw.filter(Boolean) as Array<{
              nodeId: string;
              feature: any;
              bbox: [number, number, number, number] | null;
            }>;
            if (!features.length) return;

            const adjacencyPredicate: 'touches' | 'intersects' =
              props.predicate === 'intersects' ? 'intersects' : 'touches';

            // Try worker-first, fallback to local loop on failure
            let edges: Array<{source_id: string; target_id: string; predicate: string}> = [];
            let workerSucceeded = false;
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

              const pairEstimate = (features.length * (features.length - 1)) / 2;
              const timeoutMs = computeSpatialOpsTimeout('adjacencyGraph', pairEstimate);

              const workerResult = await runWithAbortSignal(signal =>
                runSpatialOpsJob({
                  name: 'adjacencyGraph',
                  payload: {
                    predicate: adjacencyPredicate,
                    features: features.map(f => ({
                      nodeId: f.nodeId,
                      geometry: f.feature,
                      bbox: f.bbox
                    }))
                  },
                  timeoutMs,
                  signal
                })
              );
              edges = workerResult.edges;
              workerSucceeded = true;
            } catch (error) {
              if ((error as Error)?.name === 'AbortError' && cancelledRef.current) return;
              // Fallback to local loop
            }

            if (!workerSucceeded) {
              const loopYieldEvery = Math.max(20, Math.floor(QMAP_DEFAULT_CHUNK_SIZE / 3));
              for (let i = 0; i < features.length; i += 1) {
                if (cancelledRef.current) return;
                const leftNode = features[i];
                for (let j = i + 1; j < features.length; j += 1) {
                  const rightNode = features[j];
                  if (leftNode.bbox && rightNode.bbox && !geometryBboxOverlap(leftNode.bbox, rightNode.bbox)) {
                    continue;
                  }
                  let matched = false;
                  try {
                    matched =
                      adjacencyPredicate === 'intersects'
                        ? turfBooleanIntersects(leftNode.feature, rightNode.feature)
                        : turfBooleanTouches(leftNode.feature, rightNode.feature);
                  } catch {
                    matched = false;
                  }
                  if (matched) {
                    edges.push({
                      source_id: leftNode.nodeId,
                      target_id: rightNode.nodeId,
                      predicate: adjacencyPredicate
                    });
                  }
                  if (j > 0 && j % loopYieldEvery === 0) {
                    await yieldToMainThread();
                    if (cancelledRef.current) return;
                  }
                }
                if (i > 0 && i % loopYieldEvery === 0) {
                  await yieldToMainThread();
                }
              }
            }

            if (cancelledRef.current) return;
            if (!edges.length) return;
            upsertDerivedDatasetRows(
              localDispatch,
              localVisState?.datasets || {},
              props.newDatasetName,
              edges,
              'qmap_adjacency',
              false
            );
          } finally {
            if (useLoadingIndicator) {
              localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
            }
          }
        })();
      }, [localDispatch, localVisState, props, shouldSkip, complete]);
      return null;
    }
  };
}

export function createCoverageQualityReportTool(ctx: QMapToolContext) {
  const {
    QMAP_SPATIAL_PREDICATE_SCHEMA,
    QMAP_DEFAULT_CHUNK_SIZE,
    getCurrentVisState,
    resolveDatasetByName,
    resolveGeojsonFieldName,
    resolveH3FieldName,
    resolveDatasetFieldName,
    resolveOptionalFeatureCap,
    getFilteredDatasetIndexes,
    mapIndexesChunked,
    parseGeoJsonLike,
    h3CellToPolygonFeature,
    toTurfFeature,
    geometryToBbox,
    geometryBboxOverlap,
    turfBooleanWithin,
    turfBooleanContains,
    turfBooleanTouches,
    turfBooleanIntersects,
    yieldToMainThread
  } = ctx;

  return {
    description:
      'Report coverage and data-quality diagnostics for spatial matching between two geometry/H3 datasets.',
    parameters: z.preprocess(preprocessDualDatasetArgs, z.object({
      leftDatasetName: z.string(),
      rightDatasetName: z.string(),
      leftGeometryField: z.string().optional(),
      rightGeometryField: z.string().optional(),
      predicate: QMAP_SPATIAL_PREDICATE_SCHEMA.describe('Default intersects'),
      rightValueField: z.string().optional(),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxLeftFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on left features. Unset = full matched coverage (no truncation).'),
      maxRightFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on right features. Unset = full matched coverage (no truncation).')
    })),
    execute: async ({
      leftDatasetName,
      rightDatasetName,
      leftGeometryField,
      rightGeometryField,
      predicate,
      rightValueField,
      useActiveFilters,
      maxLeftFeatures,
      maxRightFeatures
    }: any) => {
      const vis = getCurrentVisState();
      const left = resolveDatasetByName(vis?.datasets || {}, leftDatasetName);
      const right = resolveDatasetByName(vis?.datasets || {}, rightDatasetName);
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
      const rightValueResolved = rightValueField ? resolveDatasetFieldName(right, rightValueField) : null;
      const leftIdx = getFilteredDatasetIndexes(left, vis, useActiveFilters !== false).slice(
        0,
        resolveOptionalFeatureCap(maxLeftFeatures)
      );
      const rightIdx = getFilteredDatasetIndexes(right, vis, useActiveFilters !== false).slice(
        0,
        resolveOptionalFeatureCap(maxRightFeatures)
      );
      const rightFeaturesRaw = await mapIndexesChunked(
        rightIdx,
        (rowIdx: number) => {
          const rightGeometryRaw = rightGeom
            ? parseGeoJsonLike(right.getValue(rightGeom, rowIdx))
            : h3CellToPolygonFeature(right.getValue(String(rightH3 || ''), rowIdx));
          const feature = toTurfFeature(rightGeometryRaw);
          if (!feature) return null;
          const value = rightValueResolved ? right.getValue(rightValueResolved, rowIdx) : null;
          return {
            feature,
            value,
            bbox: geometryToBbox((feature as any)?.geometry)
          };
        },
        Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
      );
      const rightFeatures = rightFeaturesRaw.filter(Boolean) as Array<{
        feature: any;
        value: unknown;
        bbox: [number, number, number, number] | null;
      }>;
      if (!rightFeatures.length || !leftIdx.length) {
        return {llmResult: {success: false, details: 'Insufficient features for coverage report.'}};
      }
      const op = (predicate || 'intersects') as 'intersects' | 'within' | 'contains' | 'touches';

      // Prepare left features for worker
      const leftFeaturesForWorker = await mapIndexesChunked(
        leftIdx,
        (rowIdx: number) => {
          const leftGeometryRaw = leftGeom
            ? parseGeoJsonLike(left.getValue(leftGeom, rowIdx))
            : h3CellToPolygonFeature(left.getValue(String(leftH3 || ''), rowIdx));
          const feature = toTurfFeature(leftGeometryRaw);
          if (!feature) return null;
          return {geometry: feature};
        },
        Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
      );
      const leftFeatures = leftFeaturesForWorker.filter(Boolean) as Array<{geometry: any}>;

      let matched = 0;
      let nullJoined = 0;
      let total = leftIdx.length;
      let workerSucceeded = false;

      // Try worker-first
      try {
        const pairEstimate = leftFeatures.length * rightFeatures.length;
        const timeoutMs = computeSpatialOpsTimeout('coverageQualityReport', pairEstimate);
        const workerResult = await runSpatialOpsJob({
          name: 'coverageQualityReport',
          payload: {
            predicate: op,
            leftFeatures: leftFeatures.map(f => ({geometry: f.geometry})),
            rightFeatures: rightFeatures.map(f => ({
              geometry: f.feature,
              value: f.value,
              bbox: f.bbox
            })),
            hasValueField: !!rightValueResolved
          },
          timeoutMs
        });
        matched = workerResult.matched;
        nullJoined = workerResult.nullJoined;
        total = workerResult.total;
        workerSucceeded = true;
      } catch {
        // Fallback to local loop
      }

      if (!workerSucceeded) {
        const loopYieldEvery = Math.max(20, Math.floor(QMAP_DEFAULT_CHUNK_SIZE / 3));
        for (let i = 0; i < leftIdx.length; i += 1) {
          const rowIdx = leftIdx[i];
          const leftGeometryRaw = leftGeom
            ? parseGeoJsonLike(left.getValue(leftGeom, rowIdx))
            : h3CellToPolygonFeature(left.getValue(String(leftH3 || ''), rowIdx));
          const leftFeature = toTurfFeature(leftGeometryRaw);
          if (!leftFeature) {
            if (i > 0 && i % loopYieldEvery === 0) {
              await yieldToMainThread();
            }
            continue;
          }
          const leftBbox = geometryToBbox((leftFeature as any)?.geometry);
          let hasMatch = false;
          let hasAnyNonNull = false;
          for (let j = 0; j < rightFeatures.length; j += 1) {
            const candidate = rightFeatures[j];
            if (leftBbox && candidate.bbox && !geometryBboxOverlap(leftBbox, candidate.bbox)) {
              continue;
            }
            let isMatch = false;
            try {
              if (op === 'within') isMatch = turfBooleanWithin(leftFeature, candidate.feature);
              else if (op === 'contains') isMatch = turfBooleanContains(leftFeature, candidate.feature);
              else if (op === 'touches') isMatch = turfBooleanTouches(leftFeature, candidate.feature);
              else isMatch = turfBooleanIntersects(leftFeature, candidate.feature);
            } catch {
              isMatch = false;
            }
            if (isMatch) {
              hasMatch = true;
              if (rightValueResolved && candidate.value !== null && candidate.value !== undefined && candidate.value !== '') {
                hasAnyNonNull = true;
              }
              if (!rightValueResolved || hasAnyNonNull) break;
            }
            if (j > 0 && j % loopYieldEvery === 0) {
              await yieldToMainThread();
            }
          }
          if (hasMatch) matched += 1;
          if (rightValueResolved && hasMatch && !hasAnyNonNull) {
            nullJoined += 1;
          }
          if (i > 0 && i % loopYieldEvery === 0) {
            await yieldToMainThread();
          }
        }
        total = leftIdx.length;
      }
      const coveragePct = Number(((matched / Math.max(1, total)) * 100).toFixed(1));
      const nullPct = rightValueResolved
        ? Number(((nullJoined / Math.max(1, matched || 1)) * 100).toFixed(1))
        : 0;
      return {
        llmResult: {
          success: true,
          report: {
            predicate: op,
            leftSample: total,
            rightSample: rightFeatures.length,
            matchedLeft: matched,
            coveragePct,
            rightValueField: rightValueResolved || null,
            matchedRowsWithNullValuePct: rightValueResolved ? nullPct : null
          },
          details:
            `Coverage ${coveragePct}% (${matched}/${total})` +
            (rightValueResolved ? `, null-value rate on matched rows ${nullPct}%` : '')
        }
      };
    }
  };
}
