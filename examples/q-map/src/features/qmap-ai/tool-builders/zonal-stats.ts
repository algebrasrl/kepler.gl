import React, {useEffect} from 'react';
import {setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapVisState} from '../../../state/qmap-selectors';
import {useToolExecution} from './use-tool-execution';

import type {QMapToolContext} from '../context/tool-context';

export function createZonalStatsByAdminTool(ctx: QMapToolContext) {
  const {
    QMAP_AGGREGATION_SCHEMA,
    QMAP_WEIGHT_MODE_SCHEMA,
    QMAP_DEFAULT_CHUNK_SIZE,
    QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL,
    QMAP_AUTO_HIDE_SOURCE_LAYERS,
    getCurrentVisState,
    resolveDatasetByName,
    resolveGeojsonFieldName,
    resolveH3FieldName,
    inferDatasetH3Resolution,
    resolveDatasetFieldName,
    getFilteredDatasetIndexes,
    resolveOptionalFeatureCap,
    getDatasetInfoByLabel,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    shouldUseLoadingIndicator,
    mapIndexesChunked,
    normalizeH3Key,
    yieldToMainThread,
    parseGeoJsonLike,
    h3CellToPolygonFeature,
    toTurfFeature,
    turfCentroid,
    geometryToBbox,
    geometryBboxOverlap,
    toTurfPolygonFeature,
    turfBooleanPointInPolygon,
    turfBooleanIntersects,
    isPolygonLikeFeature,
    turfIntersectSafe,
    turfArea,
    runZonalStatsJob,
    upsertDerivedDatasetRows,
    hideLayersForDatasetIds
  } = ctx;

  return {
    description:
      'Compute zonal statistics on administrative geometries/H3 from a value dataset with optional area-weighted aggregation.',
    parameters: z.object({
      adminDatasetName: z.string(),
      valueDatasetName: z.string(),
      adminGeometryField: z.string().optional(),
      valueGeometryField: z.string().optional(),
      valueField: z.string().optional().describe('Numeric field to aggregate; omit for count-only'),
      aggregation: QMAP_AGGREGATION_SCHEMA.describe('Default sum when valueField exists, else count'),
      weightMode: QMAP_WEIGHT_MODE_SCHEMA.describe('Default area_weighted'),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxAdminFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on admin features. Unset = full matched coverage (no truncation).'),
      maxValueFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap on value features. Unset = full matched coverage (no truncation).'),
      outputFieldName: z.string().optional().describe('Default zonal_value'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default false. Set true to auto-create a map layer for the output dataset.'),
      newDatasetName: z.string().optional()
    }),
    execute: async ({
      adminDatasetName,
      valueDatasetName,
      adminGeometryField,
      valueGeometryField,
      valueField,
      aggregation,
      weightMode,
      useActiveFilters,
      maxAdminFeatures,
      maxValueFeatures,
      outputFieldName,
      showOnMap,
      newDatasetName
    }: any) => {
      const currentVisState = getCurrentVisState();
      const admin = resolveDatasetByName(currentVisState?.datasets || {}, adminDatasetName);
      const values = resolveDatasetByName(currentVisState?.datasets || {}, valueDatasetName);
      if (!admin?.id) return {llmResult: {success: false, details: `Admin dataset "${adminDatasetName}" not found.`}};
      if (!values?.id) return {llmResult: {success: false, details: `Value dataset "${valueDatasetName}" not found.`}};
      const adminGeom = resolveGeojsonFieldName(admin, adminGeometryField);
      const valueGeom = resolveGeojsonFieldName(values, valueGeometryField);
      const adminH3 = resolveH3FieldName(admin, adminGeometryField || null);
      const valueH3 = resolveH3FieldName(values, valueGeometryField || null);
      const canUseH3FastPath = Boolean(adminH3 && valueH3);
      const adminH3Resolution = adminH3 ? inferDatasetH3Resolution(admin, adminH3) : null;
      const valueH3Resolution = valueH3 ? inferDatasetH3Resolution(values, valueH3) : null;
      const h3ResolutionMismatch =
        canUseH3FastPath &&
        adminH3Resolution !== null &&
        valueH3Resolution !== null &&
        adminH3Resolution !== valueH3Resolution;
      if (h3ResolutionMismatch && !adminGeom && !valueGeom) {
        return {
          llmResult: {
            success: false,
            details:
              `H3 resolution mismatch for zonal stats: admin=${adminH3Resolution}, values=${valueH3Resolution}. ` +
              'Align resolutions first (e.g. aggregateDatasetToH3), then retry.'
          }
        };
      }
      const executionMode: 'h3_fast_path' | 'geometry' = canUseH3FastPath && !h3ResolutionMismatch ? 'h3_fast_path' : 'geometry';
      if ((!adminGeom && !adminH3) || (!valueGeom && !valueH3)) {
        return {
          llmResult: {
            success: false,
            details: 'Both datasets must include either a geojson field or an H3 field (h3_id/h3__id).'
          }
        };
      }
      const valueFields = (values.fields || []).map((f: any) => String(f?.name || '')).filter(Boolean);
      const numericValueFields = valueFields.filter((fieldName: string) => {
        const field = (values.fields || []).find((f: any) => String(f?.name || '') === fieldName);
        const fieldType = String(field?.type || '').toLowerCase();
        return (
          fieldType === String(ALL_FIELD_TYPES.integer).toLowerCase() ||
          fieldType === String(ALL_FIELD_TYPES.real).toLowerCase() ||
          fieldType === 'integer' ||
          fieldType === 'real'
        );
      });
      const agg = (aggregation ||
        (valueField ? 'sum' : 'count')) as 'count' | 'sum' | 'avg' | 'min' | 'max';
      let resolvedValueField = valueField ? resolveDatasetFieldName(values, valueField) : null;
      if (!resolvedValueField && agg !== 'count') {
        const fallbackCandidates = Array.from(
          new Set(
            [
              valueField ? String(valueField).trim() : '',
              valueField ? `${String(valueField).trim()}__${agg}` : '',
              agg,
              agg === 'sum' ? 'count_weighted' : ''
            ].filter(Boolean)
          )
        );
        for (const candidate of fallbackCandidates) {
          const resolvedCandidate = resolveDatasetFieldName(values, candidate);
          if (resolvedCandidate) {
            resolvedValueField = resolvedCandidate;
            break;
          }
        }
        if (!resolvedValueField && numericValueFields.length === 1) {
          resolvedValueField = numericValueFields[0];
        }
      }
      if (agg !== 'count' && !resolvedValueField) {
        return {
          llmResult: {
            success: false,
            details:
              `Aggregation "${agg}" requires a numeric valueField on dataset "${values.label || values.id}", ` +
              `but "${valueField || 'undefined'}" was not resolved. Available numeric fields: ` +
              `${numericValueFields.slice(0, 12).join(', ') || '[none]'}.`
          }
        };
      }
      const effectiveUseActiveFilters = useActiveFilters !== false;
      const resolvedMaxAdminFeatures = resolveOptionalFeatureCap(maxAdminFeatures);
      const resolvedMaxValueFeatures = resolveOptionalFeatureCap(maxValueFeatures);
      const adminCount = getFilteredDatasetIndexes(admin, currentVisState, effectiveUseActiveFilters).length;
      const valueCount = getFilteredDatasetIndexes(values, currentVisState, effectiveUseActiveFilters).length;
      const cappedAdminCount = Math.min(adminCount, resolvedMaxAdminFeatures);
      const cappedValueCount = Math.min(valueCount, resolvedMaxValueFeatures);
      const estimatedPairEvaluations = cappedAdminCount * cappedValueCount;
      const pairEvalBudget =
        (weightMode || 'area_weighted') === 'area_weighted'
          ? Math.max(10000, Math.floor(QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL / 2))
          : QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL;
      if (executionMode === 'geometry' && estimatedPairEvaluations > pairEvalBudget) {
        const inferH3ResolutionFromLabel = (label: string): number | null => {
          const text = String(label || '').toLowerCase();
          const match = text.match(/(?:^|[_\s-])r(\d{1,2})(?:$|[_\s-])/);
          if (!match) return null;
          const parsed = Number(match[1]);
          return Number.isFinite(parsed) ? parsed : null;
        };
        const suggestStartingResolution = () => {
          if (valueH3Resolution !== null && Number.isFinite(Number(valueH3Resolution))) {
            return Number(valueH3Resolution);
          }
          const inferredFromLabel = inferH3ResolutionFromLabel(values.label || values.id);
          if (inferredFromLabel !== null) return inferredFromLabel;
          if (valueCount >= 12000) return 6;
          if (valueCount >= 4000) return 5;
          return 4;
        };
        const currentSuggestedResolution = suggestStartingResolution();
        const nextResolution = Math.max(4, currentSuggestedResolution - 1);
        const replaceOrAppendResolutionSuffix = (name: string, nextRes: number) => {
          const raw = String(name || '').trim();
          if (!raw) return `h3_agg_r${nextRes}`;
          if (/(?:^|[_\s-])r\d{1,2}(?:$|[_\s-])/i.test(raw)) {
            return raw.replace(/(?:^|[_\s-])r\d{1,2}(?:$|[_\s-])/i, match =>
              match.replace(/r\d{1,2}/i, `r${nextRes}`)
            );
          }
          return `${raw}_r${nextRes}`;
        };
        const aggregateOp = agg === 'count' ? 'count' : agg;
        const reroutedDatasetName = replaceOrAppendResolutionSuffix(
          String(values.label || values.id || 'value_dataset_h3'),
          nextResolution
        );
        const reroutedValueField =
          agg === 'count'
            ? undefined
            : aggregateOp === 'sum'
            ? 'sum'
            : aggregateOp === 'avg'
            ? 'avg'
            : aggregateOp === 'min'
            ? 'min'
            : aggregateOp === 'max'
            ? 'max'
            : undefined;
        if (currentSuggestedResolution <= 4) {
          return {
            llmResult: {
              success: false,
              details:
                `zonalStatsByAdmin aborted to prevent UI freeze: estimated pair evaluations ` +
                `${estimatedPairEvaluations.toLocaleString()} exceed budget ${pairEvalBudget.toLocaleString()} ` +
                `(admin=${cappedAdminCount.toLocaleString()}, values=${cappedValueCount.toLocaleString()}, mode=${
                  weightMode || 'area_weighted'
                }). Deterministic H3 fallback reached minimum resolution r4; cannot proceed without truncation.`
            }
          };
        }
        return {
          llmResult: {
            success: false,
            retryWithTool: 'aggregateDatasetToH3',
            retryWithArgs: {
              datasetName: values.label || values.id,
              resolution: nextResolution,
              valueField: agg === 'count' ? undefined : resolvedValueField || valueField || undefined,
              operations: [aggregateOp],
              targetDatasetName: reroutedDatasetName,
              weightMode: valueH3 ? 'intersects' : 'area_weighted',
              useActiveFilters: effectiveUseActiveFilters,
              showOnMap: false
            },
            retryReason: 'zonal-ui-freeze-budget',
            suggestedNextZonalArgs: {
              adminDatasetName: admin.label || admin.id,
              valueDatasetName: reroutedDatasetName,
              valueField: reroutedValueField,
              aggregation: agg,
              outputFieldName: String(outputFieldName || '').trim() || 'zonal_value',
              showOnMap: showOnMap === true,
              newDatasetName: String(newDatasetName || '').trim() || `${admin.label || admin.id}_zonal_${reroutedDatasetName}`
            },
            details:
              `zonalStatsByAdmin aborted to prevent UI freeze: estimated pair evaluations ` +
              `${estimatedPairEvaluations.toLocaleString()} exceed budget ${pairEvalBudget.toLocaleString()} ` +
              `(admin=${cappedAdminCount.toLocaleString()}, values=${cappedValueCount.toLocaleString()}, mode=${
                weightMode || 'area_weighted'
              }). Deterministic fallback selected: aggregate value dataset to H3 r${nextResolution} ` +
              `("${reroutedDatasetName}") and rerun zonal stats. If it still exceeds budget, repeat with r${
                Math.max(4, nextResolution - 1)
              } until r4; then stop with explicit failure.`
          }
        };
      }
      const targetField = String(outputFieldName || '').trim() || 'zonal_value';
      const target =
        String(newDatasetName || '').trim() || `${admin.label || admin.id}_zonal_${values.label || values.id}`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        currentVisState?.datasets || {},
        target,
        'qmap_zonal_stats'
      );
      const fieldCatalog = Array.from(
        new Set([
          ...((admin.fields || []).map((field: any) => String(field?.name || '').trim()).filter(Boolean) as string[]),
          targetField
        ])
      );
      const aggregationOutputs: Record<string, string> = {
        [String(agg || '').trim().toLowerCase() || 'count']: targetField
      };
      const fieldAliases: Record<string, string> = {};
      if (resolvedValueField) {
        const aliasBase = String(resolvedValueField).trim();
        const aggKey = String(agg || '').trim().toLowerCase() || 'count';
        fieldAliases[`${aggKey}_${aliasBase}`] = targetField;
        fieldAliases[`${aliasBase}_${aggKey}`] = targetField;
      }
      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          outputFieldName: targetField,
          fieldCatalog,
          numericFields: [targetField],
          styleableFields: [targetField],
          defaultStyleField: targetField,
          aggregationOutputs,
          fieldAliases,
          details:
            `Computing zonal stats (${agg}, ${weightMode || 'area_weighted'}, mode=${executionMode}).` +
            `${showOnMap === true ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('zonal-stats-by-admin'),
          adminDatasetId: admin.id,
          valueDatasetId: values.id,
          adminGeometryField: adminGeom || null,
          valueGeometryField: valueGeom || null,
          adminH3Field: adminH3 || null,
          valueH3Field: valueH3 || null,
          valueField: resolvedValueField,
          aggregation: agg,
          weightMode: (weightMode || 'area_weighted') as 'intersects' | 'centroid' | 'area_weighted',
          executionMode,
          useActiveFilters: effectiveUseActiveFilters,
          maxAdminFeatures: resolvedMaxAdminFeatures,
          maxValueFeatures: resolvedMaxValueFeatures,
          estimatedPairEvaluations,
          pairEvalBudget,
          adminH3Resolution,
          valueH3Resolution,
          outputFieldName: targetField,
          fieldCatalog,
          numericFields: [targetField],
          styleableFields: [targetField],
          defaultStyleField: targetField,
          aggregationOutputs,
          fieldAliases,
          showOnMap: showOnMap === true,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function ZonalStatsByAdminComponent({
      executionKey,
      adminDatasetId,
      valueDatasetId,
      adminGeometryField,
      valueGeometryField,
      adminH3Field,
      valueH3Field,
      valueField,
      aggregation,
      weightMode,
      executionMode,
      useActiveFilters,
      maxAdminFeatures,
      maxValueFeatures,
      outputFieldName,
      showOnMap,
      newDatasetName
    }: {
      executionKey?: string;
      adminDatasetId: string;
      valueDatasetId: string;
      adminGeometryField: string | null;
      valueGeometryField: string | null;
      adminH3Field: string | null;
      valueH3Field: string | null;
      valueField: string | null;
      aggregation: 'count' | 'sum' | 'avg' | 'min' | 'max';
      weightMode: 'intersects' | 'centroid' | 'area_weighted';
      executionMode: 'h3_fast_path' | 'geometry';
      useActiveFilters: boolean;
      maxAdminFeatures: number;
      maxValueFeatures: number;
      outputFieldName: string;
      showOnMap: boolean;
      newDatasetName: string;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const cancelledRef = React.useRef(false);
      const activeAbortControllersRef = React.useRef<Set<AbortController>>(new Set());
      const {shouldSkip, complete} = useToolExecution({
        executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });
      useEffect(() => {
        return () => {
          cancelledRef.current = true;
          activeAbortControllersRef.current.forEach(controller => {
            try {
              controller.abort();
            } catch {
              // ignore
            }
          });
          activeAbortControllersRef.current.clear();
        };
      }, []);
      useEffect(() => {
        if (shouldSkip()) return;
        const datasets = localVisState?.datasets || {};
        const admin = datasets[adminDatasetId];
        const values = datasets[valueDatasetId];
        if (!admin || !values) return;
        complete();

        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }

        (async () => {
          try {
            const adminIdx = getFilteredDatasetIndexes(admin, localVisState, useActiveFilters).slice(0, maxAdminFeatures);
            const valueIdx = getFilteredDatasetIndexes(values, localVisState, useActiveFilters).slice(0, maxValueFeatures);
            const rowsOut: Array<Record<string, unknown>> = [];
            const loopYieldEvery = Math.max(20, Math.floor(QMAP_DEFAULT_CHUNK_SIZE / 3));
            const runWithAbortSignal = async <T,>(runner: (signal: AbortSignal) => Promise<T>): Promise<T> => {
              const controller = new AbortController();
              activeAbortControllersRef.current.add(controller);
              try {
                return await runner(controller.signal);
              } finally {
                activeAbortControllersRef.current.delete(controller);
              }
            };

            if (executionMode === 'h3_fast_path' && adminH3Field && valueH3Field) {
              type H3Bucket = {count: number; sum: number; denom: number; min: number | null; max: number | null};
              const valueLookup = new Map<string, H3Bucket>();
              for (let i = 0; i < valueIdx.length; i += 1) {
                if (cancelledRef.current) return;
                const rowIdx = valueIdx[i];
                const h3 = normalizeH3Key(values.getValue(valueH3Field, rowIdx));
                if (!h3) continue;
                const bucket = valueLookup.get(h3) || {count: 0, sum: 0, denom: 0, min: null, max: null};
                bucket.count += 1;
                if (valueField) {
                  const raw = Number(values.getValue(valueField, rowIdx));
                  if (Number.isFinite(raw)) {
                    bucket.sum += raw;
                    bucket.denom += 1;
                    bucket.min = bucket.min === null ? raw : Math.min(bucket.min, raw);
                    bucket.max = bucket.max === null ? raw : Math.max(bucket.max, raw);
                  }
                }
                valueLookup.set(h3, bucket);
                if (i > 0 && i % loopYieldEvery === 0) {
                  await yieldToMainThread();
                  if (cancelledRef.current) return;
                }
              }

              if (!valueLookup.size) return;
              for (let i = 0; i < adminIdx.length; i += 1) {
                if (cancelledRef.current) return;
                const rowIdx = adminIdx[i];
                const adminH3 = normalizeH3Key(admin.getValue(adminH3Field, rowIdx));
                if (!adminH3) {
                  if (i > 0 && i % loopYieldEvery === 0) {
                    await yieldToMainThread();
                  }
                  continue;
                }
                const bucket = valueLookup.get(adminH3) || null;
                const base: Record<string, unknown> = {};
                (admin.fields || []).forEach((f: any) => {
                  base[f.name] = admin.getValue(f.name, rowIdx);
                });
                if (aggregation === 'count' || !valueField) {
                  base[outputFieldName] = bucket ? bucket.count : 0;
                } else if (aggregation === 'sum') {
                  base[outputFieldName] = bucket ? bucket.sum : 0;
                } else if (aggregation === 'avg') {
                  base[outputFieldName] = bucket && bucket.denom > 0 ? bucket.sum / bucket.denom : null;
                } else if (aggregation === 'min') {
                  base[outputFieldName] = bucket ? bucket.min : null;
                } else if (aggregation === 'max') {
                  base[outputFieldName] = bucket ? bucket.max : null;
                }
                rowsOut.push(base);
                if (i > 0 && i % loopYieldEvery === 0) {
                  await yieldToMainThread();
                }
              }
            } else {
              const pairEstimate = adminIdx.length * Math.max(1, valueIdx.length);
              const localFallbackBudget =
                weightMode === 'area_weighted'
                  ? Math.max(10000, Math.floor(QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL / 2))
                  : QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL;

              const runLocalGeometryFallback = async () => {
                const valueFeaturesRaw = await mapIndexesChunked(
                  valueIdx,
                  (rowIdx: number) => {
                    const valueGeometryRaw = valueGeometryField
                      ? parseGeoJsonLike(values.getValue(valueGeometryField, rowIdx))
                      : h3CellToPolygonFeature(values.getValue(String(valueH3Field || ''), rowIdx));
                    const feature = toTurfFeature(valueGeometryRaw);
                    if (!feature) return null;
                    const centroid = turfCentroid(feature as any);
                    return {
                      feature,
                      value: valueField ? Number(values.getValue(valueField, rowIdx)) : NaN,
                      bbox: geometryToBbox((feature as any)?.geometry),
                      centroid
                    };
                  },
                  Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
                );
                if (cancelledRef.current) return;
                const valueFeatures = valueFeaturesRaw.filter(Boolean) as Array<{
                  feature: any;
                  value: number;
                  bbox: [number, number, number, number] | null;
                  centroid: any;
                }>;
                if (!valueFeatures.length) return;

                for (let i = 0; i < adminIdx.length; i += 1) {
                  if (cancelledRef.current) return;
                  const rowIdx = adminIdx[i];
                  const adminGeometryRaw = adminGeometryField
                    ? parseGeoJsonLike(admin.getValue(adminGeometryField, rowIdx))
                    : h3CellToPolygonFeature(admin.getValue(String(adminH3Field || ''), rowIdx));
                  const adminFeature = toTurfPolygonFeature(adminGeometryRaw);
                  if (!adminFeature) {
                    if (i > 0 && i % loopYieldEvery === 0) {
                      await yieldToMainThread();
                    }
                    continue;
                  }
                  const adminBbox = geometryToBbox((adminFeature as any)?.geometry);
                  const candidates = adminBbox
                    ? valueFeatures.filter(item => !item.bbox || geometryBboxOverlap(adminBbox, item.bbox))
                    : valueFeatures;
                  let sum = 0;
                  let denom = 0;
                  let min: number | null = null;
                  let max: number | null = null;
                  let count = 0;

                  for (let j = 0; j < candidates.length; j += 1) {
                    const item = candidates[j];
                    let matched = false;
                    let weight = 1;
                    try {
                      if (weightMode === 'centroid') {
                        matched = turfBooleanPointInPolygon(item.centroid, adminFeature);
                      } else if (weightMode === 'intersects') {
                        matched = turfBooleanIntersects(item.feature, adminFeature);
                      } else if (isPolygonLikeFeature(item.feature)) {
                        const inter = turfIntersectSafe(adminFeature, item.feature);
                        if (inter) {
                          const interArea = turfArea(inter as any);
                          const total = Math.max(1e-12, turfArea(item.feature as any));
                          weight = Math.max(0, interArea / total);
                          matched = weight > 0;
                        }
                      } else {
                        matched = turfBooleanPointInPolygon(item.centroid, adminFeature);
                      }
                    } catch {
                      matched = false;
                    }
                    if (!matched) continue;
                    count += 1;
                    if (valueField && Number.isFinite(item.value)) {
                      const v = Number(item.value);
                      const weighted = v * Math.max(0, weight);
                      sum += weighted;
                      denom += Math.max(0, weight);
                      min = min === null ? v : Math.min(min, v);
                      max = max === null ? v : Math.max(max, v);
                    }
                    if (j > 0 && j % loopYieldEvery === 0) {
                      await yieldToMainThread();
                      if (cancelledRef.current) return;
                    }
                  }

                  const base: Record<string, unknown> = {};
                  (admin.fields || []).forEach((f: any) => {
                    base[f.name] = admin.getValue(f.name, rowIdx);
                  });
                  if (aggregation === 'count' || !valueField) {
                    base[outputFieldName] = count;
                  } else if (aggregation === 'sum') {
                    base[outputFieldName] = sum;
                  } else if (aggregation === 'avg') {
                    base[outputFieldName] = denom > 0 ? sum / denom : null;
                  } else if (aggregation === 'min') {
                    base[outputFieldName] = min;
                  } else if (aggregation === 'max') {
                    base[outputFieldName] = max;
                  }
                  rowsOut.push(base);

                  if (i > 0 && i % loopYieldEvery === 0) {
                    await yieldToMainThread();
                  }
                }
              };

              let workerApplied = false;
              if (typeof Worker !== 'undefined') {
                try {
                  const adminRowsPayload = await mapIndexesChunked(
                    adminIdx,
                    (rowIdx: number) =>
                      adminGeometryField
                        ? {rowIdx, geometry: admin.getValue(adminGeometryField, rowIdx)}
                        : {rowIdx, h3Id: admin.getValue(String(adminH3Field || ''), rowIdx)},
                    Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
                  );
                  if (cancelledRef.current) return;
                  const valueRowsPayload = await mapIndexesChunked(
                    valueIdx,
                    (rowIdx: number) => ({
                      rowIdx,
                      ...(valueGeometryField
                        ? {geometry: values.getValue(valueGeometryField, rowIdx)}
                        : {h3Id: values.getValue(String(valueH3Field || ''), rowIdx)}),
                      value: valueField ? Number(values.getValue(valueField, rowIdx)) : null
                    }),
                    Math.max(50, Math.min(200, QMAP_DEFAULT_CHUNK_SIZE))
                  );
                  if (cancelledRef.current) return;

                  const adaptiveTimeout = Math.min(900000, Math.max(180000, 60000 + pairEstimate * 0.03));
                  const workerResult = (await runWithAbortSignal(signal =>
                    runZonalStatsJob({
                      payload: {
                        weightMode,
                        includeValue: Boolean(valueField),
                        adminRows: adminRowsPayload,
                        valueRows: valueRowsPayload
                      },
                      timeoutMs: adaptiveTimeout,
                      signal
                    })
                  )) as any;
                  if (cancelledRef.current) return;

                  const statsByRow = new Map<
                    number,
                    {count: number; sum: number; denom: number; min: number | null; max: number | null}
                  >();
                  (workerResult?.statsByRow || []).forEach((stat: any) => {
                    const rowIdx = Number(stat?.rowIdx);
                    if (!Number.isFinite(rowIdx)) return;
                    statsByRow.set(rowIdx, {
                      count: Number(stat?.count || 0),
                      sum: Number(stat?.sum || 0),
                      denom: Number(stat?.denom || 0),
                      min: stat?.min === null || stat?.min === undefined ? null : Number(stat.min),
                      max: stat?.max === null || stat?.max === undefined ? null : Number(stat.max)
                    });
                  });

                  for (let i = 0; i < adminIdx.length; i += 1) {
                    if (cancelledRef.current) return;
                    const rowIdx = adminIdx[i];
                    const stat = statsByRow.get(rowIdx) || null;
                    const base: Record<string, unknown> = {};
                    (admin.fields || []).forEach((f: any) => {
                      base[f.name] = admin.getValue(f.name, rowIdx);
                    });
                    if (aggregation === 'count' || !valueField) {
                      base[outputFieldName] = stat ? stat.count : 0;
                    } else if (aggregation === 'sum') {
                      base[outputFieldName] = stat ? stat.sum : 0;
                    } else if (aggregation === 'avg') {
                      base[outputFieldName] = stat && stat.denom > 0 ? stat.sum / stat.denom : null;
                    } else if (aggregation === 'min') {
                      base[outputFieldName] = stat ? stat.min : null;
                    } else if (aggregation === 'max') {
                      base[outputFieldName] = stat ? stat.max : null;
                    }
                    rowsOut.push(base);
                    if (i > 0 && i % loopYieldEvery === 0) {
                      await yieldToMainThread();
                    }
                  }
                  workerApplied = true;
                } catch (error) {
                  if ((error as Error)?.name === 'AbortError' && cancelledRef.current) return;
                  console.error('zonalStatsByAdmin worker failed; fallback to local path:', error);
                }
              }

              if (!workerApplied) {
                if (pairEstimate > localFallbackBudget) {
                  console.error(
                    `[qmap-ai] zonal local fallback aborted to avoid UI freeze (pairEstimate=${pairEstimate}, threshold=${localFallbackBudget}).`
                  );
                  return;
                }
                await runLocalGeometryFallback();
              }
            }
            if (cancelledRef.current) return;
            if (!rowsOut.length) return;
            upsertDerivedDatasetRows(localDispatch, datasets, newDatasetName, rowsOut, 'qmap_zonal_stats', showOnMap);
            if (showOnMap && QMAP_AUTO_HIDE_SOURCE_LAYERS) {
              hideLayersForDatasetIds(localDispatch, localVisState?.layers || [], [admin.id, values.id]);
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
        executionKey,
        adminDatasetId,
        valueDatasetId,
        adminGeometryField,
        valueGeometryField,
        adminH3Field,
        valueH3Field,
        valueField,
        aggregation,
        weightMode,
        executionMode,
        useActiveFilters,
        maxAdminFeatures,
        maxValueFeatures,
        outputFieldName,
        showOnMap,
        newDatasetName,
        shouldSkip,
        complete
      ]);
      return null;
    }
  };

}
