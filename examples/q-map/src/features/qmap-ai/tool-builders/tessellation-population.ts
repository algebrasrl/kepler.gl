import React, {useEffect} from 'react';
import {extendedTool} from '../tool-shim';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapVisState} from '../../../state/qmap-selectors';
import type {H3AggregateRow} from '../../../workers/h3-aggregate-core';
import {useToolExecution} from './use-tool-execution';

import type {QMapToolContext} from '../context/tool-context';

export function createTassellateSelectedGeometryTool(ctx: QMapToolContext) {
  const {
    DEFAULT_TASSELLATION_DATASET,
    getCurrentVisState,
    getPolygonsFromGeometry,
    getTassellationDatasetInfo,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    getIntersectingH3Ids,
    upsertTassellationDataset
  } = ctx;

  return extendedTool({
    description:
      'Tessellate currently selected geometry into H3 cells (intersection-based) and upsert dataset Tassellation.',
    parameters: z.object({
      resolution: z.number().min(4).max(11),
      targetDatasetName: z.string().optional(),
      appendToExisting: z.boolean().optional().describe('Default false: replace target dataset content')
    }),
    execute: async ({resolution, targetDatasetName, appendToExisting}) => {
      const selectedGeometry = getCurrentVisState()?.editor?.selectedFeature?.geometry;
      const polygons = getPolygonsFromGeometry(selectedGeometry);
      if (!polygons.length) {
        return {
          llmResult: {
            success: false,
            details: 'No selected Polygon/MultiPolygon geometry found.'
          }
        };
      }
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getTassellationDatasetInfo(
        String(targetDatasetName || DEFAULT_TASSELLATION_DATASET),
        getCurrentVisState()?.datasets || {}
      );
      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          details: `Tessellating selected geometry at H3 resolution ${resolution}.`
        },
        additionalData: {
          executionKey: makeExecutionKey('tassellate-selected-geometry'),
          resolution,
          targetDatasetName: resolvedTargetLabel,
          targetDatasetId: resolvedTargetDatasetId,
          appendToExisting: appendToExisting === true
        }
      };
    },
    component: function TassellateSelectedGeometryComponent({
      executionKey,
      resolution,
      targetDatasetName,
      targetDatasetId,
      appendToExisting
    }: {
      executionKey?: string;
      resolution: number;
      targetDatasetName: string;
      targetDatasetId: string;
      appendToExisting: boolean;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey});
      useEffect(() => {
        if (shouldSkip()) return;
        const geometry = localVisState?.editor?.selectedFeature?.geometry;
        const polygons = getPolygonsFromGeometry(geometry);
        if (!polygons.length) return;
        const ids = getIntersectingH3Ids(polygons, resolution);
        if (!ids.length) return;
        complete();
        upsertTassellationDataset(
          localDispatch,
          localVisState?.datasets,
          ids,
          resolution,
          targetDatasetName,
          appendToExisting
        );
      }, [localDispatch, localVisState, executionKey, resolution, targetDatasetName, targetDatasetId, appendToExisting, shouldSkip, complete]);
      return null;
    }
  });

}

export function createTassellateDatasetLayerTool(ctx: QMapToolContext) {
  const {
    DEFAULT_TASSELLATION_DATASET,
    getCurrentVisState,
    resolveDatasetByName,
    getDatasetIndexes,
    isLikelyLandCoverDataset,
    resolveLandCoverGroupByFields,
    resolveOptionalFeatureCap,
    filterTargetsDataset,
    getTassellationDatasetInfo,
    getDatasetInfoByLabel,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    shouldUseLoadingIndicator,
    wrapTo,
    setLoadingIndicator,
    filterIndexesChunked,
    toComparable,
    resolveDatasetFieldName,
    upsertIntermediateDataset,
    mapIndexesChunked,
    runH3Job,
    extractPolygonsFromGeoJsonLike,
    getIntersectingH3Ids,
    upsertTassellationDataset
  } = ctx;

  return extendedTool({
    description:
      'Tessellate geometries from a dataset/layer into H3 cells (intersection-based) and upsert dataset Tassellation.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      resolution: z.number().min(4).max(11),
      targetDatasetName: z.string().optional(),
      maxFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on features to tessellate. Unset = full matched coverage (no truncation).'),
      useActiveFilters: z.boolean().optional().describe('Default true: apply current dataset UI filters first'),
      appendToExisting: z.boolean().optional().describe('Default false: replace target dataset content'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default true: auto-create layer for tessellation dataset'),
      materializeFilteredDataset: z
        .boolean()
        .optional()
        .describe('Default true with active filters: create intermediate filtered dataset before tessellation')
    }),
    execute: async ({
      datasetName,
      resolution,
      targetDatasetName,
      maxFeatures,
      useActiveFilters,
      appendToExisting,
      showOnMap,
      materializeFilteredDataset
    }) => {
      const sourceDataset = resolveDatasetByName(getCurrentVisState()?.datasets || {}, datasetName);
      if (!sourceDataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found.`
          }
        };
      }
      const sourceRowCount = getDatasetIndexes(sourceDataset).length;
      const likelyLandCover = isLikelyLandCoverDataset(sourceDataset);
      const isLargeThematicCoverageDataset = likelyLandCover && sourceRowCount > 4000;
      if (isLargeThematicCoverageDataset) {
        const suggestedWeightMode: 'intersects' | 'area_weighted' =
          sourceRowCount > 12000 ? 'intersects' : 'area_weighted';
        const suggestedGroupByFields = resolveLandCoverGroupByFields(sourceDataset);
        const suggestedMaxFeatures =
          Number.isFinite(Number(maxFeatures)) && Number(maxFeatures) > 0
            ? Math.max(1, Math.floor(Number(maxFeatures)))
            : undefined;
        return {
          llmResult: {
            success: false,
            retryWithTool: 'aggregateDatasetToH3',
            retryWithArgs: {
              datasetName: sourceDataset.label || sourceDataset.id,
              resolution,
              operations: ['count'],
              groupByFields: suggestedGroupByFields.length ? suggestedGroupByFields : undefined,
              weightMode: suggestedWeightMode,
              targetDatasetName: targetDatasetName || undefined,
              maxFeatures: suggestedMaxFeatures,
              useActiveFilters: useActiveFilters !== false,
              showOnMap: showOnMap !== false
            },
            retryReason: 'thematic-coverage-large-dataset',
            details:
              `Dataset "${sourceDataset.label || sourceDataset.id}" looks like thematic land-cover with ${sourceRowCount} rows. ` +
              'Direct tessellation is heavy and often times out. Auto-routing to aggregateDatasetToH3 with class grouping is recommended.'
          }
        };
      }
      const geometryField =
        (sourceDataset.fields || []).find((f: any) => f?.type === 'geojson')?.name || '_geojson';
      const cap = resolveOptionalFeatureCap(maxFeatures);
      const explicitCap =
        Number.isFinite(Number(maxFeatures)) && Number(maxFeatures) > 0 ? Math.max(1, Math.floor(Number(maxFeatures))) : null;
      const applyFilters = useActiveFilters !== false;
      const activeFilters = applyFilters
        ? (getCurrentVisState()?.filters || []).filter((f: any) => filterTargetsDataset(f, sourceDataset.id))
        : [];
      const createIntermediate = materializeFilteredDataset ?? applyFilters;
      const intermediateName = `${sourceDataset.label || sourceDataset.id}_filtered_for_tassellation`;
      const shouldShowOnMap = showOnMap !== false;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getTassellationDatasetInfo(
        String(targetDatasetName || DEFAULT_TASSELLATION_DATASET),
        getCurrentVisState()?.datasets || {}
      );
      const {label: resolvedIntermediateLabel, datasetId: resolvedIntermediateDatasetId} = getDatasetInfoByLabel(
        getCurrentVisState()?.datasets || {},
        intermediateName,
        'qmap_intermediate'
      );

      return {
        llmResult: {
          success: true,
          details: `Tessellating dataset "${sourceDataset.label || sourceDataset.id}" using field "${geometryField}" at H3 resolution ${resolution} (${explicitCap ? `cap=${explicitCap}` : 'full matched coverage'}${applyFilters ? `, filters enabled (${activeFilters.length})` : ''}${createIntermediate ? `, intermediate dataset "${intermediateName}" added (no layer)` : ''}). Output dataset: "${resolvedTargetLabel}" (id: ${resolvedTargetDatasetId})${shouldShowOnMap ? '' : ' (no auto layer)'}.`,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          intermediateDataset: createIntermediate ? resolvedIntermediateLabel : null,
          intermediateDatasetId: createIntermediate ? resolvedIntermediateDatasetId : null
        },
        additionalData: {
          executionKey: makeExecutionKey('tassellate-dataset-layer'),
          datasetId: sourceDataset.id,
          geometryField,
          resolution,
          targetDatasetName: resolvedTargetLabel,
          targetDatasetId: resolvedTargetDatasetId,
          maxFeatures: cap,
          useActiveFilters: applyFilters,
          appendToExisting: appendToExisting === true,
          showOnMap: shouldShowOnMap,
          materializeFilteredDataset: createIntermediate,
          intermediateDatasetName: resolvedIntermediateLabel,
          intermediateDatasetId: resolvedIntermediateDatasetId
        }
      };
    },
    component: function TassellateDatasetLayerComponent({
      executionKey,
      datasetId,
      geometryField,
      resolution,
      targetDatasetName,
      maxFeatures,
      useActiveFilters,
      appendToExisting,
      showOnMap,
      materializeFilteredDataset,
      intermediateDatasetName
    }: {
      executionKey?: string;
      datasetId: string;
      geometryField: string;
      resolution: number;
      targetDatasetName: string;
      maxFeatures: number;
      useActiveFilters: boolean;
      appendToExisting: boolean;
      showOnMap: boolean;
      materializeFilteredDataset: boolean;
      intermediateDatasetName: string;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const cancelledRef = React.useRef(false);
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey});
      useEffect(() => {
        return () => {
          cancelledRef.current = true;
        };
      }, []);
      useEffect(() => {
        if (shouldSkip()) return;
        const sourceDataset = localVisState?.datasets?.[datasetId];
        if (!sourceDataset) return;
        // Mark as started before any dispatch/state mutation to avoid effect re-entry loops.
        complete();
        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }
        (async () => {
          let cappedIdxForFallback: number[] = [];
          try {
            const baseIdx = Array.isArray(sourceDataset.allIndexes)
              ? sourceDataset.allIndexes
              : Array.from({length: Number(sourceDataset.length || 0)}, (_, i) => i);

            const filters = useActiveFilters
              ? (localVisState?.filters || []).filter((f: any) => filterTargetsDataset(f, sourceDataset.id))
              : [];

            let matchedIdx = await filterIndexesChunked(baseIdx, (rowIdx: number) => {
              return filters.every((filter: any) => {
                const rawFieldName = Array.isArray(filter?.name) ? filter.name[0] : filter?.name;
                if (!rawFieldName) return true;
                const resolvedFilterField = resolveDatasetFieldName(sourceDataset, String(rawFieldName));
                if (!resolvedFilterField) return true;
                const rowValue = sourceDataset.getValue(resolvedFilterField, rowIdx);
                const filterValue = filter?.value;
                if (Array.isArray(filterValue) && filterValue.length === 2 && filter?.type !== 'multiSelect') {
                  const minV = filterValue[0];
                  const maxV = filterValue[1];
                  return Number(rowValue) >= Number(minV) && Number(rowValue) <= Number(maxV);
                }
                if (Array.isArray(filterValue)) {
                  return filterValue.map(toComparable).includes(toComparable(rowValue));
                }
                return toComparable(rowValue) === toComparable(filterValue);
              });
            });
            if (cancelledRef.current) return;
            if (useActiveFilters && matchedIdx.length === 0 && baseIdx.length > 0) {
              // Avoid empty tessellation due to stale/overly-restrictive filters.
              matchedIdx = baseIdx.slice();
            }

            const cappedIdx = matchedIdx.slice(0, resolveOptionalFeatureCap(maxFeatures));
            cappedIdxForFallback = cappedIdx;

            if (materializeFilteredDataset && cappedIdx.length > 0) {
              await upsertIntermediateDataset(
                localDispatch,
                localVisState?.datasets,
                sourceDataset,
                cappedIdx,
                intermediateDatasetName
              );
            }

            const rawGeometries = await mapIndexesChunked(
              cappedIdx,
              (rowIdx: number) => sourceDataset.getValue(geometryField, rowIdx)
            );
            if (cancelledRef.current) return;

            const h3TimeoutMs =
              typeof window !== 'undefined' && (window as any).__QMAP_E2E_TOOLS__ ? 15000 : 120000;
            const result = await runH3Job({
              name: 'tessellateGeometries',
              payload: {
                resolution,
                geometries: rawGeometries
              },
              timeoutMs: h3TimeoutMs
            });
            if (cancelledRef.current) return;
            let ids = Array.isArray(result?.ids) ? result.ids : [];
            if (!ids.length) {
              const fallbackSet = new Set<string>();
              rawGeometries.forEach((rawGeometry: unknown) => {
                const polygons = extractPolygonsFromGeoJsonLike(rawGeometry);
                if (!polygons.length) return;
                const rowIds = getIntersectingH3Ids(polygons, resolution);
                rowIds.forEach((id: string) => fallbackSet.add(id));
              });
              ids = Array.from(fallbackSet);
            }
            const existingTarget = resolveDatasetByName(localVisState?.datasets || {}, targetDatasetName);
            if (!ids.length) {
              if (!existingTarget?.id) {
                upsertTassellationDataset(
                  localDispatch,
                  localVisState?.datasets,
                  [],
                  resolution,
                  targetDatasetName,
                  appendToExisting,
                  showOnMap
                );
              }
              return;
            }
            upsertTassellationDataset(
              localDispatch,
              localVisState?.datasets,
              ids,
              resolution,
              targetDatasetName,
              appendToExisting,
              showOnMap
            );
          } catch (error) {
            if (cancelledRef.current) return;
            if ((error as Error)?.name === 'AbortError') return;
            // Worker fallback: run tessellation locally to avoid failed tool execution.
            console.error('TassellateDatasetLayer worker failed; fallback to local path:', error);
            const ids = new Set<string>();
            const rawGeometries = await mapIndexesChunked(
              cappedIdxForFallback,
              (rowIdx: number) => sourceDataset.getValue(geometryField, rowIdx)
            );
            rawGeometries.forEach((rawGeometry: unknown) => {
              const polygons = extractPolygonsFromGeoJsonLike(rawGeometry);
              if (!polygons.length) return;
              const rowIds = getIntersectingH3Ids(polygons, resolution);
              rowIds.forEach((id: string) => ids.add(id));
            });
            if (cancelledRef.current) return;
            if (!ids.size) {
              const existingTarget = resolveDatasetByName(localVisState?.datasets || {}, targetDatasetName);
              if (!existingTarget?.id) {
                upsertTassellationDataset(
                  localDispatch,
                  localVisState?.datasets,
                  [],
                  resolution,
                  targetDatasetName,
                  appendToExisting,
                  showOnMap
                );
              }
              return;
            }
            upsertTassellationDataset(
              localDispatch,
              localVisState?.datasets,
              Array.from(ids),
              resolution,
              targetDatasetName,
              appendToExisting,
              showOnMap
            );
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
        datasetId,
        geometryField,
        resolution,
        targetDatasetName,
        maxFeatures,
        useActiveFilters,
        appendToExisting,
        showOnMap,
        materializeFilteredDataset,
        intermediateDatasetName,
        shouldSkip,
        complete
      ]);
      return null;
    }
  });

}

export function createAggregateDatasetToH3Tool(ctx: QMapToolContext) {
  const {
    QMAP_AGGREGATION_WITH_DISTINCT_REQUIRED_SCHEMA,
    QMAP_WEIGHT_MODE_SCHEMA,
    QMAP_AUTO_HIDE_SOURCE_LAYERS,
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    resolveOptionalFeatureCap,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    shouldUseLoadingIndicator,
    wrapTo,
    setLoadingIndicator,
    filterTargetsDataset,
    filterIndexesChunked,
    toComparable,
    mapIndexesChunked,
    toWorkerSafeAggregateRows,
    runH3Job,
    aggregateGeometriesToH3Rows,
    hideLayersForDatasetIds,
    upsertH3AggregationDataset
  } = ctx;

  return extendedTool({
    description:
      'Aggregate dataset geometries into H3 cells with clipping mode and statistical metrics.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      resolution: z.number().min(4).max(11),
      valueField: z.string().optional().describe('Numeric field used for sum/avg/min/max'),
      operations: z
        .array(QMAP_AGGREGATION_WITH_DISTINCT_REQUIRED_SCHEMA)
        .min(1)
        .optional()
        .describe('Default ["count"]'),
      distinctField: z
        .string()
        .optional()
        .describe('Field used for distinct_count operation (e.g. code_18).'),
      groupByFields: z
        .array(z.string())
        .optional()
        .describe(
          'Optional categorical fields to preserve in long output (one row per h3_id + group combination), e.g. ["code_18","clc_name_it"].'
        ),
      weightMode: QMAP_WEIGHT_MODE_SCHEMA.describe('Default area_weighted'),
      targetDatasetName: z.string().optional(),
      maxFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on input features. Unset = full matched coverage (no truncation).'),
      useActiveFilters: z.boolean().optional(),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default true. Set false for intermediate technical datasets kept off-map.')
    }),
    execute: async ({
      datasetName,
      resolution,
      valueField,
      operations,
      distinctField,
      groupByFields,
      weightMode,
      targetDatasetName,
      maxFeatures,
      useActiveFilters,
      showOnMap
    }) => {
      const sourceDataset = resolveDatasetByName(getCurrentVisState()?.datasets || {}, datasetName);
      if (!sourceDataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }
      const geometryField = (sourceDataset.fields || []).find((f: any) => f?.type === 'geojson')?.name || null;
      const h3FieldName =
        (sourceDataset.fields || []).find((f: any) => f?.type === 'h3')?.name ||
        resolveDatasetFieldName(sourceDataset, 'h3_id') ||
        resolveDatasetFieldName(sourceDataset, 'h3__id');
      const sourceGeometryMode: 'geojson' | 'h3' = geometryField ? 'geojson' : 'h3';
      if (!geometryField && !h3FieldName) {
        return {
          llmResult: {
            success: false,
            details: 'Dataset must contain either a geojson field or an H3 field (h3_id/h3__id).'
          }
        };
      }
      const ops = (operations?.length ? operations : ['count']) as Array<
        'count' | 'distinct_count' | 'sum' | 'avg' | 'min' | 'max'
      >;
      const needValueField = ops.some(op => op !== 'count' && op !== 'distinct_count');
      const resolvedValueField = valueField ? resolveDatasetFieldName(sourceDataset, valueField) : null;
      const needDistinctField = ops.includes('distinct_count');
      const resolvedDistinctField = distinctField ? resolveDatasetFieldName(sourceDataset, distinctField) : null;
      const resolvedGroupByFields = Array.from(
        new Set(
          (groupByFields || [])
            .map((fieldName: any) => resolveDatasetFieldName(sourceDataset, String(fieldName)))
            .filter(Boolean)
        )
      ) as string[];
      if (needValueField && !resolvedValueField) {
        return {
          llmResult: {
            success: false,
            details: 'A valid numeric valueField is required for sum/avg/min/max.'
          }
        };
      }
      if (needDistinctField && !resolvedDistinctField) {
        return {
          llmResult: {
            success: false,
            details: 'A valid distinctField is required for distinct_count (for example "code_18").'
          }
        };
      }
      const currentDatasets = Object.values(getCurrentVisState()?.datasets || {}) as any[];
      const existingLabels = new Set(
        currentDatasets.map((d: any) => String(d?.label || '').trim().toLowerCase()).filter(Boolean)
      );
      const providedTarget = String(targetDatasetName || '').trim();
      const baseTarget = `${sourceDataset.label || sourceDataset.id}_h3_agg_r${resolution}`;
      let target = providedTarget || baseTarget;
      let mode: 'create' | 'update' = 'create';
      if (providedTarget) {
        mode = existingLabels.has(providedTarget.toLowerCase()) ? 'update' : 'create';
      } else {
        let version = 1;
        while (existingLabels.has(target.toLowerCase())) {
          version += 1;
          target = `${baseTarget}_v${version}`;
        }
        mode = 'create';
      }
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        currentDatasets,
        target,
        'qmap_h3_aggregation'
      );
      const effectiveMaxFeatures = resolveOptionalFeatureCap(maxFeatures);
      const explicitMaxFeatures =
        Number.isFinite(Number(maxFeatures)) && Number(maxFeatures) > 0
          ? Math.max(1, Math.floor(Number(maxFeatures)))
          : null;
      const outputMetricFields = [
        'count',
        'count_weighted',
        ...(ops.includes('distinct_count') ? ['distinct_count'] : []),
        ...(ops.includes('sum') ? ['sum'] : []),
        ...(ops.includes('avg') ? ['avg'] : []),
        ...(ops.includes('min') ? ['min'] : []),
        ...(ops.includes('max') ? ['max'] : [])
      ];
      const fieldCatalog = [
        'h3_id',
        'h3_resolution',
        ...resolvedGroupByFields,
        ...outputMetricFields,
        ...(resolvedValueField ? ['source_field'] : [])
      ];
      const defaultStyleField =
        (ops.includes('sum') && 'sum') ||
        (ops.includes('avg') && 'avg') ||
        (ops.includes('count') && 'count') ||
        (ops.includes('distinct_count') && 'distinct_count') ||
        (ops.includes('max') && 'max') ||
        (ops.includes('min') && 'min') ||
        'count';
      const aggregationOutputs: Record<string, string> = {
        count: 'count',
        count_weighted: 'count_weighted'
      };
      const fieldAliases: Record<string, string> = {
        weighted_count: 'count_weighted'
      };
      if (resolvedValueField) {
        const aliasBase = String(resolvedValueField).trim();
        if (ops.includes('sum')) {
          fieldAliases[`sum_${aliasBase}`] = 'sum';
          fieldAliases[`${aliasBase}_sum`] = 'sum';
        }
        if (ops.includes('avg')) {
          fieldAliases[`avg_${aliasBase}`] = 'avg';
          fieldAliases[`${aliasBase}_avg`] = 'avg';
        }
        if (ops.includes('min')) {
          fieldAliases[`min_${aliasBase}`] = 'min';
          fieldAliases[`${aliasBase}_min`] = 'min';
        }
        if (ops.includes('max')) {
          fieldAliases[`max_${aliasBase}`] = 'max';
          fieldAliases[`${aliasBase}_max`] = 'max';
        }
      }
      if (ops.includes('distinct_count')) aggregationOutputs.distinct_count = 'distinct_count';
      if (ops.includes('sum')) aggregationOutputs.sum = 'sum';
      if (ops.includes('avg')) aggregationOutputs.avg = 'avg';
      if (ops.includes('min')) aggregationOutputs.min = 'min';
      if (ops.includes('max')) aggregationOutputs.max = 'max';
      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          fieldCatalog,
          numericFields: outputMetricFields,
          styleableFields: outputMetricFields,
          defaultStyleField,
          aggregationOutputs,
          fieldAliases,
          details: `${mode === 'update' ? 'Updating' : 'Creating'} "${resolvedTargetLabel}" by aggregating "${
            sourceDataset.label || sourceDataset.id
          }" to H3 res ${resolution} with ${String(
            sourceGeometryMode === 'h3' ? 'intersects' : weightMode || 'area_weighted'
          )} mode and ops [${ops.join(', ')}], maxFeatures=${explicitMaxFeatures ?? 'full'}${
            resolvedDistinctField ? ` distinctField=${resolvedDistinctField}` : ''
          }${
            resolvedGroupByFields.length ? ` grouped by [${resolvedGroupByFields.join(', ')}]` : ''
          }.`
        },
        additionalData: {
          executionKey: makeExecutionKey('aggregate-dataset-to-h3'),
          datasetId: sourceDataset.id,
          geometryField,
          h3FieldName,
          sourceGeometryMode,
          resolution,
          valueFieldName: resolvedValueField || null,
          distinctFieldName: resolvedDistinctField || null,
          groupByFieldNames: resolvedGroupByFields,
          operations: ops,
          weightMode: sourceGeometryMode === 'h3' ? 'intersects' : weightMode || 'area_weighted',
          fieldCatalog,
          numericFields: outputMetricFields,
          styleableFields: outputMetricFields,
          defaultStyleField,
          aggregationOutputs,
          fieldAliases,
          targetDatasetName: resolvedTargetLabel,
          targetDatasetId: resolvedTargetDatasetId,
          maxFeatures: effectiveMaxFeatures,
          useActiveFilters: useActiveFilters !== false,
          showOnMap: showOnMap !== false
        }
      };
    },
    component: function AggregateDatasetToH3Component({
      executionKey,
      datasetId,
      geometryField,
      h3FieldName,
      sourceGeometryMode,
      resolution,
      valueFieldName,
      distinctFieldName,
      groupByFieldNames,
      operations,
      weightMode,
      targetDatasetName,
      maxFeatures,
      useActiveFilters,
      showOnMap
    }: {
      executionKey?: string;
      datasetId: string;
      geometryField: string | null;
      h3FieldName: string | null;
      sourceGeometryMode: 'geojson' | 'h3';
      resolution: number;
      valueFieldName: string | null;
      distinctFieldName: string | null;
      groupByFieldNames: string[];
      operations: Array<'count' | 'distinct_count' | 'sum' | 'avg' | 'min' | 'max'>;
      weightMode: 'intersects' | 'centroid' | 'area_weighted';
      targetDatasetName: string;
      maxFeatures: number;
      useActiveFilters: boolean;
      showOnMap: boolean;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey});
      useEffect(() => {
        let cancelled = false;
        if (shouldSkip()) return;
        const sourceDataset = localVisState?.datasets?.[datasetId];
        if (!sourceDataset) return;
        complete();
        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }
        (async () => {
          try {
            const baseIdx = Array.isArray(sourceDataset.allIndexes)
              ? sourceDataset.allIndexes
              : Array.from({length: Number(sourceDataset.length || 0)}, (_, i) => i);
            const filters = useActiveFilters
              ? (localVisState?.filters || []).filter((f: any) => filterTargetsDataset(f, sourceDataset.id))
              : [];
            const matchedIdx = await filterIndexesChunked(baseIdx, (rowIdx: number) => {
              return filters.every((filter: any) => {
                const rawFieldName = Array.isArray(filter?.name) ? filter.name[0] : filter?.name;
                if (!rawFieldName) return true;
                const resolvedFilterField = resolveDatasetFieldName(sourceDataset, String(rawFieldName));
                if (!resolvedFilterField) return true;
                const rowValue = sourceDataset.getValue(resolvedFilterField, rowIdx);
                const filterValue = filter?.value;
                if (Array.isArray(filterValue) && filterValue.length === 2 && filter?.type !== 'multiSelect') {
                  const minV = filterValue[0];
                  const maxV = filterValue[1];
                  return Number(rowValue) >= Number(minV) && Number(rowValue) <= Number(maxV);
                }
                if (Array.isArray(filterValue)) {
                  return filterValue.map(toComparable).includes(toComparable(rowValue));
                }
                return toComparable(rowValue) === toComparable(filterValue);
              });
            });
            if (cancelled) return;

            const capped = matchedIdx.slice(0, resolveOptionalFeatureCap(maxFeatures));
            if (matchedIdx.length > capped.length) {
              console.warn(
                `[qmap-ai] aggregateDatasetToH3 truncated rows ${capped.length}/${matchedIdx.length} for dataset ${
                  sourceDataset.label || sourceDataset.id
                }. Increase maxFeatures for full coverage.`
              );
            }
            const opSet = new Set(operations || ['count']);
            const groupFields = Array.from(
              new Set((groupByFieldNames || []).map(name => String(name || '').trim()).filter(Boolean))
            );
            const effectiveWeightMode: 'intersects' | 'centroid' | 'area_weighted' =
              sourceGeometryMode === 'h3' ? 'intersects' : weightMode;

            const workerRows = await mapIndexesChunked(capped, (rowIdx: number) => {
              const rawValue = valueFieldName ? sourceDataset.getValue(valueFieldName, rowIdx) : null;
              const numericValue = rawValue === null || rawValue === undefined ? null : Number(rawValue);
              const safeValue = numericValue !== null && numericValue !== undefined && Number.isFinite(Number(numericValue)) ? Number(numericValue) : null;
              const distinctValue = distinctFieldName ? sourceDataset.getValue(distinctFieldName, rowIdx) : null;
              const groupValues: Record<string, unknown> = {};
              groupFields.forEach(fieldName => {
                groupValues[fieldName] = sourceDataset.getValue(fieldName, rowIdx);
              });

              if (sourceGeometryMode === 'h3' && h3FieldName) {
                const h3Id = String(sourceDataset.getValue(h3FieldName, rowIdx) || '').trim();
                return {h3Id, value: safeValue, distinctValue, groupValues};
              }

              return {
                geometry: geometryField ? sourceDataset.getValue(geometryField, rowIdx) : null,
                value: safeValue,
                distinctValue,
                groupValues
              };
            });
            if (cancelled) return;

            const useWorkerFirst =
              typeof Worker !== 'undefined' &&
              sourceGeometryMode === 'geojson' &&
              effectiveWeightMode === 'area_weighted';
            const cooperativeYieldEvery = effectiveWeightMode === 'area_weighted' ? 1 : 20;
            let result: {cells: any[]} = {cells: []};
            if (useWorkerFirst) {
              try {
                const adaptiveTimeout = Math.min(900000, Math.max(240000, 60000 + workerRows.length * 40));
                const workerSafeRows = toWorkerSafeAggregateRows(workerRows as H3AggregateRow[]);
                result = await runH3Job({
                  name: 'aggregateGeometriesToH3',
                  payload: {
                    resolution,
                    weightMode: effectiveWeightMode,
                    groupFieldNames: groupFields,
                    rows: workerSafeRows
                  },
                  timeoutMs: adaptiveTimeout
                });
              } catch (error) {
                console.error('AggregateDatasetToH3 worker failed; fallback to local path:', error);
                result = await aggregateGeometriesToH3Rows({
                  rows: workerRows as H3AggregateRow[],
                  resolution,
                  weightMode: effectiveWeightMode,
                  groupFieldNames: groupFields,
                  cooperativeYieldEvery
                });
              }
            } else {
              result = await aggregateGeometriesToH3Rows({
                rows: workerRows as H3AggregateRow[],
                resolution,
                weightMode: effectiveWeightMode,
                groupFieldNames: groupFields,
                cooperativeYieldEvery
              });
            }
            if (cancelled) return;

            const cells = Array.isArray(result?.cells) ? result.cells : [];
            if (!cells.length) return;

            const rows = cells.map(cell => {
              const row: Record<string, unknown> = {
                h3_id: cell.h3Id,
                h3_resolution: resolution,
                count: cell.count,
                count_weighted: Number(Number(cell.countWeighted || 0).toFixed(6))
              };
              groupFields.forEach(fieldName => {
                row[fieldName] = cell.groupValues?.[fieldName];
              });
              if (opSet.has('distinct_count')) row.distinct_count = Number(cell.distinctCount || 0);
              if (opSet.has('sum')) row.sum = Number(Number(cell.sum || 0).toFixed(6));
              if (opSet.has('avg')) {
                row.avg =
                  Number(cell.avgDenominator || 0) > 0
                    ? Number((Number(cell.avgNumerator || 0) / Number(cell.avgDenominator || 1)).toFixed(6))
                    : null;
              }
              if (opSet.has('min')) row.min = cell.min;
              if (opSet.has('max')) row.max = cell.max;
              if (valueFieldName) row.source_field = valueFieldName;
              return row;
            });

            if (showOnMap && QMAP_AUTO_HIDE_SOURCE_LAYERS) {
              hideLayersForDatasetIds(localDispatch, localVisState?.layers || [], [sourceDataset.id]);
            }

            upsertH3AggregationDataset(
              localDispatch,
              localVisState?.datasets,
              targetDatasetName,
              rows,
              operations,
              groupFields,
              valueFieldName || undefined,
              showOnMap
            );
          } catch (error) {
            if (cancelled) return;
            if ((error as Error)?.name === 'AbortError') return;
            console.error('AggregateDatasetToH3 worker failed:', error);
          } finally {
            if (useLoadingIndicator) {
              localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
            }
          }
        })();
        return () => {
          // Do not cancel on re-renders: visState updates during long jobs would otherwise abort them.
        };
      }, [
        localDispatch,
        localVisState,
        executionKey,
        datasetId,
        geometryField,
        h3FieldName,
        sourceGeometryMode,
        resolution,
        valueFieldName,
        distinctFieldName,
        groupByFieldNames,
        operations,
        weightMode,
        targetDatasetName,
        maxFeatures,
        useActiveFilters,
        showOnMap,
        shouldSkip,
        complete
      ]);
      return null;
    }
  });
}

export function createPopulateTassellationFromAdminUnitsTool(ctx: QMapToolContext) {
  const {
    QMAP_VALUE_SEMANTICS_SCHEMA,
    QMAP_AVG_SUM_SCHEMA,
    QMAP_WEIGHT_MODE_SCHEMA,
    QMAP_JOIN_TYPE_SCHEMA,
    QMAP_AUTO_HIDE_SOURCE_LAYERS,
    ALL_FIELD_TYPES,
    getCurrentVisState,
    resolveDatasetByName,
    resolveH3FieldName,
    inferDatasetH3Resolution,
    resolveDatasetFieldName,
    getDatasetIndexes,
    summarizeNumericField,
    getDatasetInfoByLabel,
    isPopulationLikeField,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    shouldUseLoadingIndicator,
    wrapTo,
    setLoadingIndicator,
    filterTargetsDataset,
    filterIndexesChunked,
    toComparable,
    normalizeH3Key,
    mapIndexesChunked,
    toWorkerSafeAggregateRows,
    runH3Job,
    aggregateGeometriesToH3Rows,
    hideLayersForDatasetIds,
    replaceDataInMap,
    addDataToMap
  } = ctx;

  return extendedTool({
    description:
      'Populate an H3 tessellation with an administrative numeric field (e.g. population): aggregate source to tessellation resolution, join on H3, and create a styled-ready dataset.',
    parameters: z.object({
      tessellationDatasetName: z.string().describe('Tessellation dataset name (must contain H3 field)'),
      sourceDatasetName: z.string().describe('Administrative source dataset name (geojson or H3)'),
      sourceValueField: z.string().describe('Numeric source field to aggregate, e.g. population'),
      allocationMode: z
        .enum(['area_weighted', 'discrete', 'standard'])
        .optional()
        .describe(
          'Allocation strategy shortcut: area_weighted (areal interpolation), discrete (count-based, no fractional splits), or standard (full param control). When set, derives weightMode/valueSemantics/aggregation automatically.'
        ),
      allocationSubMode: z
        .enum(['centroid', 'intersects'])
        .optional()
        .describe('Only used when allocationMode=discrete. Default: centroid.'),
      valueSemantics: QMAP_VALUE_SEMANTICS_SCHEMA.optional().describe(
        'Value semantics: intensive->avg, extensive/count->sum. Prefer explicit value when known. Ignored when allocationMode is set.'
      ),
      aggregation: QMAP_AVG_SUM_SCHEMA.describe(
        'Optional explicit aggregation override. If omitted, derived from valueSemantics (default: sum).'
      ),
      resolution: z.number().min(4).max(11).optional().describe('Optional target H3 resolution override'),
      weightMode: QMAP_WEIGHT_MODE_SCHEMA.optional().describe('Default area_weighted. Ignored when allocationMode is set.'),
      joinType: QMAP_JOIN_TYPE_SCHEMA.describe('Default left'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default true. Set false for intermediate technical datasets kept off-map.'),
      minCoveragePct: z
        .number()
        .min(0)
        .max(100)
        .optional()
        .describe('Coverage gate. Default 60 for population-like fields, else 5.'),
      targetValueFieldName: z.string().optional().describe('Output field name in joined dataset (default source field)'),
      newDatasetName: z.string().optional().describe('Default <tessellation>_<sourceField>'),
      useActiveFilters: z.boolean().optional().describe('Apply active filters on source dataset (default true)')
    }),
    execute: async ({
      tessellationDatasetName,
      sourceDatasetName,
      sourceValueField,
      allocationMode,
      allocationSubMode,
      valueSemantics,
      aggregation,
      resolution,
      weightMode,
      joinType,
      showOnMap,
      minCoveragePct,
      targetValueFieldName,
      newDatasetName,
      useActiveFilters
    }) => {
      // Derive effective params from allocationMode shortcut when provided
      let effectiveWeightMode: string | undefined;
      let effectiveAggregation: string | undefined;
      let effectiveValueSemantics: string | undefined;
      if (allocationMode === 'area_weighted') {
        effectiveWeightMode = 'area_weighted';
        effectiveAggregation = aggregation ?? 'sum';
        effectiveValueSemantics = effectiveAggregation === 'avg' ? 'intensive' : 'extensive';
      } else if (allocationMode === 'discrete') {
        effectiveValueSemantics = 'count';
        effectiveAggregation = 'sum';
        effectiveWeightMode = allocationSubMode ?? 'centroid';
      } else {
        // 'standard' or undefined — use caller-provided params as-is
        effectiveWeightMode = weightMode ?? 'area_weighted';
        effectiveAggregation = aggregation;
        effectiveValueSemantics = valueSemantics;
      }

      const currentVisState = getCurrentVisState();
      const tessellation = resolveDatasetByName(currentVisState?.datasets || {}, tessellationDatasetName);
      const source = resolveDatasetByName(currentVisState?.datasets || {}, sourceDatasetName);
      if (!tessellation?.id) {
        return {llmResult: {success: false, details: `Tessellation dataset "${tessellationDatasetName}" not found.`}};
      }
      if (!source?.id) {
        return {llmResult: {success: false, details: `Source dataset "${sourceDatasetName}" not found.`}};
      }

      const tessH3Field = resolveH3FieldName(tessellation, 'h3_id');
      if (!tessH3Field) {
        return {llmResult: {success: false, details: 'Tessellation dataset has no H3 field (h3_id/h3__id).'}};
      }
      const inferredRes = inferDatasetH3Resolution(tessellation, tessH3Field);
      const targetResolution = Number.isFinite(Number(resolution)) ? Number(resolution) : inferredRes;
      if (!Number.isFinite(Number(targetResolution))) {
        return {
          llmResult: {
            success: false,
            details: 'Cannot infer tessellation H3 resolution. Provide "resolution" explicitly.'
          }
        };
      }

      const resolvedValueField = resolveDatasetFieldName(source, sourceValueField);
      if (!resolvedValueField) {
        return {
          llmResult: {
            success: false,
            details: `Field "${sourceValueField}" not found in source dataset "${sourceDatasetName}".`
          }
        };
      }
      const sourceRowCount = getDatasetIndexes(source).length;
      if (sourceRowCount < 2) {
        return {
          llmResult: {
            success: false,
            details:
              `Source dataset "${sourceDatasetName}" has ${sourceRowCount} row(s). ` +
              'Rebuild/load a valid source dataset before population.'
          }
        };
      }
      const valueStats = summarizeNumericField(source, resolvedValueField, 50000);
      if (valueStats.numericCount <= 0) {
        return {
          llmResult: {
            success: false,
            details: `Field "${resolvedValueField}" has no numeric values in source dataset.`
          }
        };
      }

      const geometryField = (source.fields || []).find((f: any) => f?.type === 'geojson')?.name || null;
      const sourceH3Field = resolveH3FieldName(source, 'h3_id');
      const sourceMode: 'geojson' | 'h3' | 'none' = geometryField ? 'geojson' : sourceH3Field ? 'h3' : 'none';
      if (sourceMode === 'none') {
        return {
          llmResult: {
            success: false,
            details: 'Source dataset must contain a geojson field or H3 field.'
          }
        };
      }
      if (sourceMode === 'h3') {
        const sourceRes = inferDatasetH3Resolution(source, sourceH3Field);
        if (sourceRes !== null && sourceRes !== targetResolution) {
          return {
            llmResult: {
              success: false,
              details:
                `Source H3 resolution (${sourceRes}) differs from tessellation (${targetResolution}). ` +
                'Use aggregateDatasetToH3 first to align resolution.'
            }
          };
        }
      }

      const targetFieldBase = String(targetValueFieldName || resolvedValueField).trim() || resolvedValueField;
      const normalizedAggregation = effectiveAggregation === 'avg' || effectiveAggregation === 'sum' ? effectiveAggregation : undefined;
      const resolvedAggregation: 'avg' | 'sum' =
        normalizedAggregation || (effectiveValueSemantics === 'intensive' ? 'avg' : 'sum');
      const targetDatasetName =
        String(newDatasetName || '').trim() ||
        `${tessellation.label || tessellation.id}_${targetFieldBase}`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        currentVisState?.datasets || {},
        targetDatasetName,
        'qmap_tessellation_populated'
      );
      const adaptiveCoverage = isPopulationLikeField(targetFieldBase) ? 60 : 5;
      const coverageGate = Number.isFinite(Number(minCoveragePct)) ? Number(minCoveragePct) : adaptiveCoverage;
      const leftFieldNames = (tessellation.fields || [])
        .map((field: any) => String(field?.name || '').trim())
        .filter((name: string) => Boolean(name));
      const usedFieldNames = new Set(leftFieldNames.map((name: string) => name.toLowerCase()));
      let resolvedOutputFieldName = String(targetFieldBase || '').trim() || resolvedValueField;
      let outputSuffix = 2;
      while (usedFieldNames.has(resolvedOutputFieldName.toLowerCase())) {
        resolvedOutputFieldName = `${targetFieldBase}_${outputSuffix}`;
        outputSuffix += 1;
      }
      const fieldCatalog = [...leftFieldNames, resolvedOutputFieldName];
      const aggregationOutputs: Record<string, string> = {
        [resolvedAggregation]: resolvedOutputFieldName
      };
      const fieldAliases: Record<string, string> = {};
      if (resolvedValueField) {
        const aliasBase = String(resolvedValueField).trim();
        fieldAliases[aliasBase] = resolvedOutputFieldName;
        fieldAliases[`${resolvedAggregation}_${aliasBase}`] = resolvedOutputFieldName;
      }
      if (targetFieldBase) {
        const targetAliasBase = String(targetFieldBase).trim();
        fieldAliases[targetAliasBase] = resolvedOutputFieldName;
        fieldAliases[`${resolvedAggregation}_${targetAliasBase}`] = resolvedOutputFieldName;
      }

      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          outputFieldName: resolvedOutputFieldName,
          fieldCatalog,
          numericFields: [resolvedOutputFieldName],
          styleableFields: [resolvedOutputFieldName],
          defaultStyleField: resolvedOutputFieldName,
          aggregationOutputs,
          fieldAliases,
          details:
            `Preparing populated tessellation "${resolvedTargetLabel}" from "${tessellation.label || tessellation.id}" ` +
            `and "${source.label || source.id}" at H3 r${targetResolution} ` +
            `using ${resolvedAggregation} + ${sourceMode === 'geojson' ? effectiveWeightMode || 'area_weighted' : 'intersects'} ` +
            `(coverage gate ${coverageGate}%).`
        },
        additionalData: {
          executionKey: makeExecutionKey('populate-tassellation-from-admin'),
          tessellationDatasetId: tessellation.id,
          sourceDatasetId: source.id,
          tessellationH3Field: tessH3Field,
          sourceMode,
          sourceH3Field: sourceH3Field || null,
          sourceGeometryField: geometryField,
          sourceValueField: resolvedValueField,
          aggregation: resolvedAggregation,
          targetResolution,
          weightMode: sourceMode === 'geojson' ? effectiveWeightMode || 'area_weighted' : 'intersects',
          joinType: (joinType || 'left') as 'left' | 'inner',
          showOnMap: showOnMap !== false,
          minCoveragePct: coverageGate,
          targetValueFieldBase: targetFieldBase,
          outputFieldName: resolvedOutputFieldName,
          fieldCatalog,
          numericFields: [resolvedOutputFieldName],
          styleableFields: [resolvedOutputFieldName],
          defaultStyleField: resolvedOutputFieldName,
          aggregationOutputs,
          fieldAliases,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId,
          useActiveFilters: useActiveFilters !== false
        }
      };
    },
    component: function PopulateTassellationFromAdminUnitsComponent({
      executionKey,
      tessellationDatasetId,
      sourceDatasetId,
      tessellationH3Field,
      sourceMode,
      sourceH3Field,
      sourceGeometryField,
      sourceValueField,
      aggregation,
      targetResolution,
      weightMode,
      joinType,
      showOnMap,
      minCoveragePct,
      targetValueFieldBase,
      newDatasetName,
      newDatasetId,
      useActiveFilters
    }: {
      executionKey?: string;
      tessellationDatasetId: string;
      sourceDatasetId: string;
      tessellationH3Field: string;
      sourceMode: 'geojson' | 'h3';
      sourceH3Field: string | null;
      sourceGeometryField: string | null;
      sourceValueField: string;
      aggregation: 'avg' | 'sum';
      targetResolution: number;
      weightMode: 'intersects' | 'centroid' | 'area_weighted';
      joinType: 'left' | 'inner';
      showOnMap: boolean;
      minCoveragePct: number;
      targetValueFieldBase: string;
      newDatasetName: string;
      newDatasetId: string;
      useActiveFilters: boolean;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey});
      useEffect(() => {
        let cancelled = false;
        if (shouldSkip()) return;

        const datasets = localVisState?.datasets || {};
        const tessellation = datasets?.[tessellationDatasetId];
        const source = datasets?.[sourceDatasetId];
        if (!tessellation || !source) return;

        complete();

        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }

        (async () => {
          try {
            const sourceBaseIdx = getDatasetIndexes(source);
            const filters = useActiveFilters
              ? (localVisState?.filters || []).filter((f: any) => filterTargetsDataset(f, source.id))
              : [];
            const sourceIdx = await filterIndexesChunked(sourceBaseIdx, (rowIdx: number) => {
              return filters.every((filter: any) => {
                const rawFieldName = Array.isArray(filter?.name) ? filter.name[0] : filter?.name;
                if (!rawFieldName) return true;
                const resolvedFilterField = resolveDatasetFieldName(source, String(rawFieldName));
                if (!resolvedFilterField) return true;
                const rowValue = source.getValue(resolvedFilterField, rowIdx);
                const filterValue = filter?.value;
                if (Array.isArray(filterValue) && filterValue.length === 2 && filter?.type !== 'multiSelect') {
                  return Number(rowValue) >= Number(filterValue[0]) && Number(rowValue) <= Number(filterValue[1]);
                }
                if (Array.isArray(filterValue)) {
                  return filterValue.map(toComparable).includes(toComparable(rowValue));
                }
                return toComparable(rowValue) === toComparable(filterValue);
              });
            });
            if (cancelled) return;

            const tessIdx = getDatasetIndexes(tessellation);
            const tessellationH3Set = new Set<string>();
            tessIdx.forEach((rowIdx: number) => {
              const h3 = normalizeH3Key(tessellation.getValue(tessellationH3Field, rowIdx));
              if (h3) tessellationH3Set.add(h3);
            });

            const workerRows = await mapIndexesChunked(sourceIdx, (rowIdx: number) => {
              const raw = source.getValue(sourceValueField, rowIdx);
              const value = Number(raw);
              const safeValue = Number.isFinite(value) ? value : null;
              if (sourceMode === 'h3' && sourceH3Field) {
                return {
                  h3Id: normalizeH3Key(source.getValue(sourceH3Field, rowIdx)),
                  value: safeValue
                };
              }
              return {
                geometry: sourceGeometryField ? source.getValue(sourceGeometryField, rowIdx) : null,
                value: safeValue
              };
            });
            if (cancelled) return;

            const inputRows = workerRows.filter((row: any) => {
              return row && row.value !== null && (row.h3Id || row.geometry);
            });
            if (!inputRows.length) return;

            const effectiveWeightMode = sourceMode === 'geojson' ? weightMode : 'intersects';
            const useWorkerFirst =
              typeof Worker !== 'undefined' &&
              sourceMode === 'geojson' &&
              effectiveWeightMode === 'area_weighted';
            const cooperativeYieldEvery = effectiveWeightMode === 'area_weighted' ? 1 : 20;
            let result: {cells: any[]} = {cells: []};
            if (useWorkerFirst) {
              try {
                const adaptiveTimeout = Math.min(900000, Math.max(240000, 60000 + inputRows.length * 40));
                const workerSafeRows = toWorkerSafeAggregateRows(inputRows as H3AggregateRow[]);
                result = await runH3Job({
                  name: 'aggregateGeometriesToH3',
                  payload: {
                    resolution: targetResolution,
                    weightMode: effectiveWeightMode,
                    groupFieldNames: [],
                    rows: workerSafeRows
                  },
                  timeoutMs: adaptiveTimeout
                });
              } catch (error) {
                console.error('PopulateTassellationFromAdminUnits worker failed; fallback to local path:', error);
                result = await aggregateGeometriesToH3Rows({
                  rows: inputRows as H3AggregateRow[],
                  resolution: targetResolution,
                  weightMode: effectiveWeightMode,
                  groupFieldNames: [],
                  cooperativeYieldEvery
                });
              }
            } else {
              result = await aggregateGeometriesToH3Rows({
                rows: inputRows as H3AggregateRow[],
                resolution: targetResolution,
                weightMode: effectiveWeightMode,
                groupFieldNames: [],
                cooperativeYieldEvery
              });
            }
            if (cancelled) return;

            type AggBucket = {sum: number; avgNumerator: number; avgDenominator: number};
            const aggByH3 = new Map<string, AggBucket>();
            const cells = Array.isArray(result?.cells) ? result.cells : [];
            cells.forEach((cell: any) => {
              const h3Id = normalizeH3Key(cell?.h3Id);
              if (!h3Id) return;
              if (tessellationH3Set.size > 0 && !tessellationH3Set.has(h3Id)) return;
              aggByH3.set(h3Id, {
                sum: Number(cell?.sum || 0),
                avgNumerator: Number(cell?.avgNumerator || 0),
                avgDenominator: Number(cell?.avgDenominator || 0)
              });
            });
            if (!aggByH3.size) return;

            const leftFields = (tessellation.fields || []).map((f: any) => ({
              name: String(f?.name || ''),
              type: String(f?.type || ALL_FIELD_TYPES.string)
            }));
            const existingFieldNames = new Set(leftFields.map((f: any) => String(f.name || '').toLowerCase()));
            let outputFieldName = String(targetValueFieldBase || '').trim() || sourceValueField;
            let suffix = 2;
            while (existingFieldNames.has(outputFieldName.toLowerCase())) {
              outputFieldName = `${targetValueFieldBase}_${suffix}`;
              suffix += 1;
            }

            let leftWithH3 = 0;
            let matched = 0;
            const rows: unknown[][] = [];
            tessIdx.forEach((rowIdx: number) => {
              const h3 = normalizeH3Key(tessellation.getValue(tessellationH3Field, rowIdx));
              if (!h3) return;
              leftWithH3 += 1;
              const bucket = aggByH3.get(h3);
              const hasMatch = Boolean(bucket);
              if (hasMatch) matched += 1;
              if (joinType === 'inner' && !hasMatch) return;
              const base = leftFields.map((f: any) => tessellation.getValue(f.name, rowIdx));
              const finalValue = !bucket
                ? null
                : aggregation === 'avg'
                ? bucket.avgDenominator > 0
                  ? Number((bucket.avgNumerator / bucket.avgDenominator).toFixed(6))
                  : null
                : Number(bucket.sum.toFixed(6));
              rows.push([...base, finalValue]);
            });

            const coveragePct = leftWithH3 > 0 ? Number(((matched / leftWithH3) * 100).toFixed(1)) : 0;
            if (leftWithH3 <= 0 || coveragePct < Number(minCoveragePct || 0) || !rows.length) return;

            const existing = Object.values(datasets || {}).find(
              (d: any) => String(d?.label || '').toLowerCase() === String(newDatasetName).toLowerCase()
            ) as any;
            const datasetToUse = {
              info: {
                id: existing?.id || newDatasetId,
                label: newDatasetName
              },
              data: {
                fields: [...leftFields, {name: outputFieldName, type: ALL_FIELD_TYPES.real}],
                rows
              }
            };

            if (showOnMap && QMAP_AUTO_HIDE_SOURCE_LAYERS) {
              hideLayersForDatasetIds(localDispatch, localVisState?.layers || [], [tessellation.id, source.id]);
            }

            if (existing?.id) {
              localDispatch(
                wrapTo(
                  'map',
                  replaceDataInMap({
                    datasetToReplaceId: existing.id,
                    datasetToUse,
                    options: {
                      keepExistingConfig: true,
                      centerMap: false,
                      autoCreateLayers: false
                    }
                  }) as any
                )
              );
            } else {
              localDispatch(
                wrapTo(
                  'map',
                  addDataToMap({
                    datasets: datasetToUse as any,
                    options: {autoCreateLayers: showOnMap, centerMap: false}
                  }) as any
                )
              );
            }
          } catch (error) {
            if (cancelled) return;
            if ((error as Error)?.name === 'AbortError') return;
            console.error('PopulateTassellationFromAdminUnits worker failed:', error);
          } finally {
            if (useLoadingIndicator) {
              localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
            }
          }
        })();

        return () => {
          // Do not cancel on re-renders: visState updates during long jobs would otherwise abort them.
        };
      }, [
        localDispatch,
        localVisState,
        executionKey,
        tessellationDatasetId,
        sourceDatasetId,
        tessellationH3Field,
        sourceMode,
        sourceH3Field,
        sourceGeometryField,
        sourceValueField,
        aggregation,
        targetResolution,
        weightMode,
        joinType,
        showOnMap,
        minCoveragePct,
        targetValueFieldBase,
        newDatasetName,
        newDatasetId,
        useActiveFilters,
        shouldSkip,
        complete
      ]);
      return null;
    }
  });

}

