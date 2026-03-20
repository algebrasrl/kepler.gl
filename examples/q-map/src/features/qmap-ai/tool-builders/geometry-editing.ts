import React, {useEffect} from 'react';
import {setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapVisState} from '../../../state/qmap-selectors';
import {runSpatialOpsJob, computeSpatialOpsTimeout} from '../../../workers/spatial-ops-runner';
import {useToolExecution} from './use-tool-execution';

import type {QMapToolContext} from '../context/tool-context';
import {
  resolveGeometryDataset,
  collectFeaturesWithLoading,
  withLoadingIndicator,
  upsertAndHideSources
} from './geometry-tool-helpers';

export function createSimplifyQMapDatasetGeometryTool(ctx: QMapToolContext) {
  return {
    description:
      'Clean/simplify dataset geometries and optionally remove slivers by min area threshold (m2).',
    parameters: z.object({
      datasetName: z.string(),
      geometryField: z.string().optional(),
      tolerance: z.number().min(0).optional().describe('Simplification tolerance in decimal degrees (default 0.0005).'),
      minAreaM2: z.number().min(0).optional().describe('Drop polygon parts below this area threshold. Default 0.'),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxFeatures: z.number().optional(),
      showOnMap: z.boolean().optional().describe('Default false'),
      newDatasetName: z.string().optional()
    }),
    execute: async ({datasetName, geometryField, tolerance, minAreaM2, useActiveFilters, maxFeatures, showOnMap, newDatasetName}: any) => {
      const vis = ctx.getCurrentVisState();
      const res = resolveGeometryDataset(ctx, vis?.datasets || {}, datasetName, geometryField);
      if (!res.resolved) return (res as any).failResult;
      const {dataset: source} = res.resolved;

      return withLoadingIndicator(ctx, async () => {
        const features = await collectFeaturesWithLoading(ctx, res.resolved!, vis, {
          useActiveFilters, maxFeatures, includeRowProperties: true
        });
        if (!features.length) {
          return {llmResult: {success: false, details: `No valid geometries found in dataset "${source.label || source.id}" for cleanup.`}};
        }
        const rows: Array<Record<string, unknown>> = [];
        features.forEach((item: any) => {
          const cleanedParts = ctx.simplifyAndCleanFeatures([item.feature], Number(tolerance || 0.0005), Number(minAreaM2 || 0));
          cleanedParts.forEach((cleaned: any, partIdx: number) => {
            rows.push({
              ...(item.rowProperties || {}),
              _geojson: cleaned,
              source_row: item.rowIdx,
              clean_part: partIdx + 1,
              cleaned_area_m2: Number(ctx.featureAreaM2(cleaned).toFixed(2))
            });
          });
        });
        if (!rows.length) {
          return {llmResult: {success: false, details: 'Cleanup removed all geometries (check minAreaM2/tolerance).'}};
        }
        const targetName = String(newDatasetName || '').trim() || `${source.label || source.id}_cleaned`;
        upsertAndHideSources(ctx, vis, {
          targetName, rows, idPrefix: 'qmap_clean_geometry',
          showOnMap: showOnMap === true, hideSourceIds: [source.id]
        });
        return {llmResult: {success: true, dataset: targetName, rows: rows.length, details: `Geometry cleanup completed on "${source.label || source.id}" (${rows.length} output rows).`}};
      });
    }
  };
}

export function createSplitQMapPolygonByLineTool(ctx: QMapToolContext) {
  return {
    description: 'Split one polygon feature with one line feature and materialize split parts as a derived dataset.',
    parameters: z.object({
      polygonDatasetName: z.string(),
      lineDatasetName: z.string(),
      polygonGeometryField: z.string().optional(),
      lineGeometryField: z.string().optional(),
      polygonRowIndex: z.number().optional().describe('Optional explicit source row index in polygon dataset'),
      lineRowIndex: z.number().optional().describe('Optional explicit source row index in line dataset'),
      lineBufferMeters: z.number().positive().optional().describe('Fallback split corridor width; default 0.5m'),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      showOnMap: z.boolean().optional().describe('Default true'),
      newDatasetName: z.string().optional()
    }),
    execute: async ({polygonDatasetName, lineDatasetName, polygonGeometryField, lineGeometryField, polygonRowIndex, lineRowIndex, lineBufferMeters, useActiveFilters, showOnMap, newDatasetName}: any) => {
      const vis = ctx.getCurrentVisState();
      const datasets = vis?.datasets || {};
      const polyRes = resolveGeometryDataset(ctx, datasets, polygonDatasetName, polygonGeometryField, 'Polygon dataset');
      if (!polyRes.resolved) return (polyRes as any).failResult;
      const lineRes = resolveGeometryDataset(ctx, datasets, lineDatasetName, lineGeometryField, 'Line dataset');
      if (!lineRes.resolved) return (lineRes as any).failResult;

      return withLoadingIndicator(ctx, async () => {
        const polygonRows = await collectFeaturesWithLoading(ctx, polyRes.resolved!, vis, {useActiveFilters, includeRowProperties: true});
        const lineRows = await collectFeaturesWithLoading(ctx, lineRes.resolved!, vis, {useActiveFilters, includeRowProperties: true});
        const polygonCandidate =
          Number.isFinite(Number(polygonRowIndex))
            ? polygonRows.find((item: any) => item.rowIdx === Number(polygonRowIndex))
            : polygonRows.find((item: any) => ['Polygon', 'MultiPolygon'].includes(String(item?.feature?.geometry?.type || '')));
        const lineCandidate =
          Number.isFinite(Number(lineRowIndex))
            ? lineRows.find((item: any) => item.rowIdx === Number(lineRowIndex))
            : lineRows.find((item: any) => ['LineString', 'MultiLineString'].includes(String(item?.feature?.geometry?.type || '')));
        if (!polygonCandidate) return {llmResult: {success: false, details: 'No polygon candidate found for split operation.'}};
        if (!lineCandidate) return {llmResult: {success: false, details: 'No line candidate found for split operation.'}};
        const parts = ctx.splitPolygonFeatureByLine(polygonCandidate.feature, lineCandidate.feature, Number(lineBufferMeters || 0.5));
        if (!parts.length) return {llmResult: {success: false, details: 'Split produced no output geometries.'}};
        const targetName = String(newDatasetName || '').trim() || `${polyRes.resolved!.dataset.label || polyRes.resolved!.dataset.id}_split_parts`;
        const rows = parts.map((part: any, partIdx: number) => ({
          ...(polygonCandidate.rowProperties || {}),
          _geojson: part,
          split_part: partIdx + 1,
          split_area_m2: Number(ctx.featureAreaM2(part).toFixed(2))
        }));
        upsertAndHideSources(ctx, vis, {
          targetName, rows, idPrefix: 'qmap_split',
          showOnMap: showOnMap !== false,
          hideSourceIds: [polyRes.resolved!.dataset.id, lineRes.resolved!.dataset.id]
        });
        return {llmResult: {success: true, dataset: targetName, parts: rows.length, details: `Polygon split generated ${rows.length} part(s).`}};
      });
    }
  };
}

export function createEraseQMapDatasetByGeometryTool(ctx: QMapToolContext) {
  return {
    description: 'Erase/mask source dataset geometries using one mask dataset (difference operation per feature).',
    parameters: z.object({
      sourceDatasetName: z.string(),
      maskDatasetName: z.string(),
      sourceGeometryField: z.string().optional(),
      maskGeometryField: z.string().optional(),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxSourceFeatures: z.number().optional(),
      maxMaskFeatures: z.number().optional(),
      showOnMap: z.boolean().optional().describe('Default false'),
      newDatasetName: z.string().optional()
    }),
    execute: async ({sourceDatasetName, maskDatasetName, sourceGeometryField, maskGeometryField, useActiveFilters, maxSourceFeatures, maxMaskFeatures, showOnMap, newDatasetName}: any) => {
      const vis = ctx.getCurrentVisState();
      const datasets = vis?.datasets || {};
      const srcRes = resolveGeometryDataset(ctx, datasets, sourceDatasetName, sourceGeometryField, 'Source dataset');
      if (!srcRes.resolved) return (srcRes as any).failResult;
      const maskRes = resolveGeometryDataset(ctx, datasets, maskDatasetName, maskGeometryField, 'Mask dataset');
      if (!maskRes.resolved) return (maskRes as any).failResult;

      return withLoadingIndicator(ctx, async () => {
        const sourceRows = await collectFeaturesWithLoading(ctx, srcRes.resolved!, vis, {
          useActiveFilters, maxFeatures: maxSourceFeatures, includeRowProperties: true
        });
        const maskRows = await collectFeaturesWithLoading(ctx, maskRes.resolved!, vis, {
          useActiveFilters, maxFeatures: maxMaskFeatures
        });
        if (!sourceRows.length || !maskRows.length) {
          return {llmResult: {success: false, details: 'Insufficient source/mask geometries for erase operation.'}};
        }
        const outputRows: Array<Record<string, unknown>> = [];
        sourceRows.forEach((sourceRow: any) => {
          const candidateMasks = sourceRow.bbox
            ? maskRows.filter((maskRow: any) => !maskRow.bbox || ctx.geometryBboxOverlap(sourceRow.bbox as any, maskRow.bbox as any))
            : maskRows;
          if (!candidateMasks.length) return;
          const erased = ctx.eraseFeatureByMasks(sourceRow.feature, candidateMasks.map((item: any) => item.feature));
          if (!erased) return;
          outputRows.push({
            ...(sourceRow.rowProperties || {}),
            _geojson: erased,
            erased_area_m2: Number(ctx.featureAreaM2(erased).toFixed(2))
          });
        });
        if (!outputRows.length) return {llmResult: {success: false, details: 'Erase operation produced no output geometries.'}};
        const source = srcRes.resolved!.dataset;
        const mask = maskRes.resolved!.dataset;
        const targetName = String(newDatasetName || '').trim() || `${source.label || source.id}_erased_by_${mask.label || mask.id}`;
        upsertAndHideSources(ctx, vis, {
          targetName, rows: outputRows, idPrefix: 'qmap_erase',
          showOnMap: showOnMap === true, hideSourceIds: [source.id, mask.id]
        });
        return {llmResult: {success: true, dataset: targetName, rows: outputRows.length, details: `Erase operation completed with ${outputRows.length} output row(s).`}};
      });
    }
  };
}

export function createBufferAndSummarizeTool(ctx: QMapToolContext) {
  const {
    QMAP_AGGREGATION_BASIC_SCHEMA,
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
    geometryToBbox,
    upsertDerivedDatasetRows
  } = ctx;

  return {
    description:
      'Buffer source features (km) and summarize target features/values inside each buffer (geojson or H3).',
    parameters: z.object({
      sourceDatasetName: z.string(),
      targetDatasetName: z.string(),
      sourceGeometryField: z.string().optional(),
      targetGeometryField: z.string().optional(),
      radiusKm: z.number().positive(),
      targetValueField: z.string().optional(),
      aggregation: QMAP_AGGREGATION_BASIC_SCHEMA.describe('Default count'),
      outputFieldName: z.string().optional().describe('Default buffer_metric'),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxSourceFeatures: z.number().optional().describe('Optional explicit cap on source features.'),
      maxTargetFeatures: z.number().optional().describe('Optional explicit cap on target features.'),
      showOnMap: z.boolean().optional().describe('Default false.'),
      newDatasetName: z.string().optional()
    }),
    execute: async ({
      sourceDatasetName, targetDatasetName, sourceGeometryField, targetGeometryField,
      radiusKm, targetValueField, aggregation, outputFieldName,
      useActiveFilters, maxSourceFeatures, maxTargetFeatures, showOnMap, newDatasetName
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
        return {llmResult: {success: false, details: 'Both datasets must include either a geojson field or an H3 field (h3_id/h3__id).'}};
      }
      const resolvedTargetValue = targetValueField ? resolveDatasetFieldName(target, targetValueField) : null;
      const targetName = String(newDatasetName || '').trim() || `${source.label || source.id}_buffer_${Math.round(radiusKm)}km`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(vis?.datasets || {}, targetName, 'qmap_buffer_summary');
      const resolvedOutputFieldName = String(outputFieldName || '').trim() || 'buffer_metric';
      const fieldCatalog = Array.from(new Set([
        ...((source.fields || []).map((field: any) => String(field?.name || '').trim()).filter(Boolean) as string[]),
        resolvedOutputFieldName
      ]));
      const resolvedAggregation = (aggregation || 'count') as 'count' | 'sum' | 'avg';
      const aggregationOutputs: Record<string, string> = {[resolvedAggregation]: resolvedOutputFieldName};
      const fieldAliases: Record<string, string> = {};
      if (resolvedTargetValue) {
        const aliasBase = String(resolvedTargetValue).trim();
        fieldAliases[`${resolvedAggregation}_${aliasBase}`] = resolvedOutputFieldName;
        fieldAliases[`${aliasBase}_${resolvedAggregation}`] = resolvedOutputFieldName;
      }
      return {
        llmResult: {
          success: true, dataset: resolvedTargetLabel, datasetId: resolvedTargetDatasetId,
          outputFieldName: resolvedOutputFieldName, fieldCatalog,
          numericFields: [resolvedOutputFieldName], styleableFields: [resolvedOutputFieldName],
          defaultStyleField: resolvedOutputFieldName, aggregationOutputs, fieldAliases,
          details: `Buffering ${source.label || source.id} at ${radiusKm}km and summarizing target.` +
            `${showOnMap === true ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('buffer-and-summarize'),
          sourceDatasetId: source.id, targetDatasetId: target.id,
          sourceGeometryField: sourceGeom || null, targetGeometryField: targetGeom || null,
          sourceH3Field: sourceH3 || null, targetH3Field: targetH3 || null,
          radiusKm: Number(radiusKm), targetValueField: resolvedTargetValue,
          aggregation: resolvedAggregation, outputFieldName: resolvedOutputFieldName,
          fieldCatalog, numericFields: [resolvedOutputFieldName],
          styleableFields: [resolvedOutputFieldName], defaultStyleField: resolvedOutputFieldName,
          aggregationOutputs, fieldAliases,
          useActiveFilters: useActiveFilters !== false,
          maxSourceFeatures: resolveOptionalFeatureCap(maxSourceFeatures),
          maxTargetFeatures: resolveOptionalFeatureCap(maxTargetFeatures),
          showOnMap: showOnMap === true, newDatasetName: resolvedTargetLabel, newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function BufferAndSummarizeComponent(props: any) {
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
          activeAbortControllersRef.current.forEach(controller => { try { controller.abort(); } catch { /* ignore */ } });
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
        if (useLoadingIndicator) localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        (async () => {
          try {
            const runWithAbortSignal = async <T,>(runner: (signal: AbortSignal) => Promise<T>): Promise<T> => {
              const controller = new AbortController();
              activeAbortControllersRef.current.add(controller);
              try { return await runner(controller.signal); } finally { activeAbortControllersRef.current.delete(controller); }
            };
            const sourceIdx = getFilteredDatasetIndexes(source, localVisState, props.useActiveFilters).slice(0, props.maxSourceFeatures);
            const targetIdx = getFilteredDatasetIndexes(target, localVisState, props.useActiveFilters).slice(0, props.maxTargetFeatures);
            const targetFeaturesRaw = await mapIndexesChunked(targetIdx, (rowIdx: number) => {
              const targetGeometryRaw = props.targetGeometryField ? parseGeoJsonLike(target.getValue(props.targetGeometryField, rowIdx)) : h3CellToPolygonFeature(target.getValue(String(props.targetH3Field || ''), rowIdx));
              const feature = toTurfFeature(targetGeometryRaw);
              if (!feature) return null;
              const value = props.targetValueField ? Number(target.getValue(props.targetValueField, rowIdx)) : NaN;
              return {geometry: feature, value, bbox: geometryToBbox((feature as any)?.geometry)};
            }, Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE)));
            if (cancelledRef.current) return;
            const targetFeatures = targetFeaturesRaw.filter(Boolean) as Array<{geometry: any; value: number; bbox: [number, number, number, number] | null}>;
            if (!targetFeatures.length) return;
            const sourceFeaturesRaw = await mapIndexesChunked(sourceIdx, (rowIdx: number) => {
              const sourceGeometryRaw = props.sourceGeometryField ? parseGeoJsonLike(source.getValue(props.sourceGeometryField, rowIdx)) : h3CellToPolygonFeature(source.getValue(String(props.sourceH3Field || ''), rowIdx));
              const feature = toTurfFeature(sourceGeometryRaw);
              if (!feature) return null;
              const properties: Record<string, unknown> = {};
              (source.fields || []).forEach((f: any) => { properties[f.name] = source.getValue(f.name, rowIdx); });
              return {geometry: feature, properties};
            }, Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE)));
            if (cancelledRef.current) return;
            const sourceFeatures = sourceFeaturesRaw.filter(Boolean) as Array<{geometry: any; properties: Record<string, unknown>}>;
            if (!sourceFeatures.length) return;
            const pairEstimate = sourceFeatures.length * targetFeatures.length;
            const timeoutMs = computeSpatialOpsTimeout('bufferAndSummarize', pairEstimate);
            const workerResult = await runWithAbortSignal(signal => runSpatialOpsJob({
              name: 'bufferAndSummarize',
              payload: {radiusKm: Number(props.radiusKm), aggregation: props.aggregation || 'count', outputFieldName: props.outputFieldName || 'buffer_metric', sourceFeatures, targetFeatures},
              timeoutMs, signal
            }));
            if (cancelledRef.current) return;
            if (!workerResult.rows.length) return;
            upsertDerivedDatasetRows(localDispatch, datasets, props.newDatasetName, workerResult.rows, 'qmap_buffer_summary', props.showOnMap);
          } catch (error) {
            if ((error as Error)?.name === 'AbortError' && cancelledRef.current) return;
            throw error;
          } finally {
            if (useLoadingIndicator) localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
          }
        })();
      }, [localDispatch, localVisState, props, shouldSkip, complete]);
      return null;
    }
  };
}
