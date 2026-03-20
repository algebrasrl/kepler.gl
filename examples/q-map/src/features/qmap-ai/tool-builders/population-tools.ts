import {useEffect} from 'react';
import {addDataToMap, replaceDataInMap, setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapVisState} from '../../../state/qmap-selectors';
import type {H3AggregateRow} from '../../../workers/h3-aggregate-core';
import {useToolExecution} from './use-tool-execution';

import type {QMapToolContext} from '../context/tool-context';

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

  return {
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
    }: any) => {
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
        const cancelled = false;
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
  };

}
