import React, {useEffect} from 'react';
import {setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapVisState} from '../../../state/qmap-selectors';
import type {H3AggregateRow} from '../../../workers/h3-aggregate-core';
import {useToolExecution} from './use-tool-execution';

import type {QMapToolContext} from '../context/tool-context';

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

  return {
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
    }: any) => {
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
        const cancelled = false;
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
  };
}
