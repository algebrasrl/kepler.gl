import React, {useEffect} from 'react';
import {layerConfigChange, setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {extendedTool} from '../tool-shim';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapLayers, selectQMapVisState} from '../../../state/qmap-selectors';
import {useToolExecution} from './use-tool-execution';

import type {QMapToolContext} from '../context/tool-context';

// ─── LQ colour preset ─────────────────────────────────────────────────────────

const LQ_COLOR_STOPS = [
  {threshold: 0.5, color: '#4575b4'},  // under-represented – blue
  {threshold: 1.5, color: '#f7f7f7'},  // near-parity – white/gray
  {threshold: Infinity, color: '#d73027'} // over-represented – red
] as const;

// ─── Pure computation helpers ─────────────────────────────────────────────────

export function computeGini(values: number[]): number {
  const sorted = [...values].filter(v => Number.isFinite(v) && v >= 0).sort((a, b) => a - b);
  const n = sorted.length;
  if (n === 0) return 0;
  const sum = sorted.reduce((a, v) => a + v, 0);
  if (sum === 0) return 0;
  const numerator = sorted.reduce((acc, v, i) => acc + (i + 1) * v, 0);
  return (2 * numerator) / (n * sum) - (n + 1) / n;
}

export function computeTheilT(values: number[]): number {
  const valid = values.filter(v => Number.isFinite(v) && v > 0);
  const n = valid.length;
  if (n === 0) return 0;
  const mu = valid.reduce((a, v) => a + v, 0) / n;
  if (mu === 0) return 0;
  const t = valid.reduce((acc, v) => {
    const ratio = v / mu;
    return acc + ratio * Math.log(ratio);
  }, 0);
  return t / n;
}

export function computeConcentrationRatio(values: number[], k: number): number {
  const valid = values.filter(v => Number.isFinite(v) && v >= 0);
  if (valid.length === 0) return 0;
  const total = valid.reduce((a, v) => a + v, 0);
  if (total === 0) return 0;
  const topK = [...valid].sort((a, b) => b - a).slice(0, k);
  const topKSum = topK.reduce((a, v) => a + v, 0);
  return topKSum / total;
}

export function computeDissimilarity(a: number[], b: number[]): number {
  const A = a.reduce((acc, v) => acc + (Number.isFinite(v) ? v : 0), 0);
  const B = b.reduce((acc, v) => acc + (Number.isFinite(v) ? v : 0), 0);
  if (A === 0 || B === 0) return 0;
  let d = 0;
  for (let i = 0; i < a.length; i += 1) {
    const ai = Number.isFinite(a[i]) ? a[i] : 0;
    const bi = Number.isFinite(b[i]) ? b[i] : 0;
    d += Math.abs(ai / A - bi / B);
  }
  return 0.5 * d;
}

export function computeIsolation(a: number[], t: number[]): number {
  const A = a.reduce((acc, v) => acc + (Number.isFinite(v) ? v : 0), 0);
  if (A === 0) return 0;
  let xPx = 0;
  for (let i = 0; i < a.length; i += 1) {
    const ai = Number.isFinite(a[i]) ? a[i] : 0;
    const ti = Number.isFinite(t[i]) && t[i] > 0 ? t[i] : null;
    if (ti === null || ai === 0) continue;
    xPx += (ai / A) * (ai / ti);
  }
  return xPx;
}

export function computeExposure(a: number[], b: number[], t: number[]): number {
  const A = a.reduce((acc, v) => acc + (Number.isFinite(v) ? v : 0), 0);
  if (A === 0) return 0;
  let xPy = 0;
  for (let i = 0; i < a.length; i += 1) {
    const ai = Number.isFinite(a[i]) ? a[i] : 0;
    const bi = Number.isFinite(b[i]) ? b[i] : 0;
    const ti = Number.isFinite(t[i]) && t[i] > 0 ? t[i] : null;
    if (ti === null || ai === 0) continue;
    xPy += (ai / A) * (bi / ti);
  }
  return xPy;
}

export function computeLocationQuotients(x: number[], t: number[]): number[] {
  const X = x.reduce((acc, v) => acc + (Number.isFinite(v) ? v : 0), 0);
  const T = t.reduce((acc, v) => acc + (Number.isFinite(v) ? v : 0), 0);
  if (X === 0 || T === 0) return x.map(() => 0);
  const globalRate = X / T;
  return x.map((xi, i) => {
    const ti = Number.isFinite(t[i]) && t[i] > 0 ? t[i] : null;
    if (!Number.isFinite(xi) || ti === null) return 0;
    const localRate = xi / ti;
    return localRate / globalRate;
  });
}

function describeSeverity(g: number, label: 'Gini'): string {
  if (label === 'Gini') {
    if (g < 0.2) return 'low inequality';
    if (g < 0.35) return 'moderate inequality';
    if (g < 0.5) return 'high inequality';
    return 'very high inequality';
  }
  return '';
}

function describeDissimilarity(d: number): string {
  if (d < 0.3) return 'low segregation';
  if (d < 0.6) return 'moderate segregation';
  return 'high segregation';
}

// ─── Tool factory ─────────────────────────────────────────────────────────────

export function createComputeQMapEquityIndicesTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    shouldUseLoadingIndicator,
    getFilteredDatasetIndexes,
    mapIndexesChunked,
    upsertDerivedDatasetRows,
    ensureColorRange,
    resolveStyleTargetLayer
  } = ctx;

  return extendedTool({
    description:
      'Compute standard equity and segregation metrics over spatial units (municipalities, H3 cells, etc.). ' +
      'Always computes: Gini coefficient, Theil T entropy index, Concentration Ratio CR-k, and Location Quotient (LQ) per unit. ' +
      'Optionally computes segregation indices (Dissimilarity D, Isolation xPx, Exposure xPy) when groupAField is provided. ' +
      'Adds lq_<valueField> column to a derived dataset and optionally applies diverging LQ colour styling.',
    parameters: z.object({
      datasetName: z.string().describe('Dataset to analyse (name or id)'),
      valueField: z.string().describe('Numeric field to analyse (e.g. population, emissions, income)'),
      totalField: z
        .string()
        .optional()
        .describe('Total/denominator field for LQ (e.g. total_population). If omitted, all units weighted equally.'),
      groupAField: z
        .string()
        .optional()
        .describe('Group A count field for segregation indices (e.g. foreign_population)'),
      groupBField: z
        .string()
        .optional()
        .describe(
          'Group B count field for segregation indices (e.g. native_population). If omitted, groupB = totalField - groupAField.'
        ),
      topK: z.number().min(1).max(20).optional().describe('K for concentration ratio CR-k. Default 5.'),
      newDatasetName: z.string().optional().describe('Output dataset name. Default: <dataset>_equity'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Auto-create layer for output dataset and apply diverging LQ colour preset. Default true.')
    }),
    execute: async ({datasetName, valueField, totalField, groupAField, groupBField, topK, newDatasetName, showOnMap}) => {
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

      const resolvedTotalField = totalField ? resolveDatasetFieldName(dataset, totalField) : null;
      if (totalField && !resolvedTotalField) {
        return {
          llmResult: {
            success: false,
            details: `Total field "${totalField}" not found in dataset "${datasetName}".`
          }
        };
      }

      const resolvedGroupAField = groupAField ? resolveDatasetFieldName(dataset, groupAField) : null;
      if (groupAField && !resolvedGroupAField) {
        return {
          llmResult: {
            success: false,
            details: `Group A field "${groupAField}" not found in dataset "${datasetName}".`
          }
        };
      }

      const resolvedGroupBField = groupBField ? resolveDatasetFieldName(dataset, groupBField) : null;
      if (groupBField && !resolvedGroupBField) {
        return {
          llmResult: {
            success: false,
            details: `Group B field "${groupBField}" not found in dataset "${datasetName}".`
          }
        };
      }

      // Segregation requires at least groupA + total
      const canComputeSegregation = !!(resolvedGroupAField && resolvedTotalField);

      const effectiveK = Math.max(1, Math.min(20, Number(topK || 5)));
      const lqColName = `lq_${resolvedValueField}`;
      const outName = String(newDatasetName || '').trim() || `${dataset.label || dataset.id}_equity`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        vis?.datasets || {},
        outName,
        'qmap_equity'
      );

      const plannedMetrics: string[] = ['gini', 'theil_t', `cr_${effectiveK}`, 'location_quotient'];
      if (canComputeSegregation) {
        plannedMetrics.push('dissimilarity_d', 'isolation_xPx', 'exposure_xPy');
      }

      const fieldCatalog = Array.from(
        new Set([
          ...((dataset.fields || []).map((f: any) => String(f?.name || '')).filter(Boolean) as string[]),
          lqColName
        ])
      );

      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          fieldCatalog,
          numericFields: [lqColName],
          styleableFields: [lqColName],
          defaultStyleField: lqColName,
          plannedMetrics,
          details:
            `Computing equity indices on field "${resolvedValueField}" (dataset: "${dataset.label || dataset.id}"). ` +
            `Metrics: ${plannedMetrics.join(', ')}. ` +
            `Output dataset: "${resolvedTargetLabel}".` +
            `${showOnMap !== false ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('compute-equity-indices'),
          datasetId: dataset.id,
          valueField: resolvedValueField,
          totalField: resolvedTotalField,
          groupAField: resolvedGroupAField,
          groupBField: resolvedGroupBField,
          topK: effectiveK,
          lqColName,
          showOnMap: showOnMap !== false,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function ComputeQMapEquityIndicesComponent({
      executionKey,
      datasetId,
      valueField,
      totalField,
      groupAField,
      groupBField,
      topK,
      lqColName,
      showOnMap,
      newDatasetName
    }: {
      executionKey?: string;
      datasetId: string;
      valueField: string;
      totalField: string | null;
      groupAField: string | null;
      groupBField: string | null;
      topK: number;
      lqColName: string;
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
            const idx = getFilteredDatasetIndexes(dataset, localVisState, true);
            if (!idx.length) return;

            // Collect raw values for all needed fields
            const rawRows = await mapIndexesChunked(
              idx,
              (rowIdx: number) => {
                const xRaw = dataset.getValue(valueField, rowIdx);
                const tRaw = totalField ? dataset.getValue(totalField, rowIdx) : null;
                const aRaw = groupAField ? dataset.getValue(groupAField, rowIdx) : null;
                const bRaw = groupBField ? dataset.getValue(groupBField, rowIdx) : null;
                return {rowIdx, xRaw, tRaw, aRaw, bRaw};
              },
              500
            );

            if (cancelledRef.current) return;

            type RawRow = {rowIdx: number; xRaw: unknown; tRaw: unknown; aRaw: unknown; bRaw: unknown};
            const rows = rawRows as RawRow[];

            // Parse numeric values
            const xVals: number[] = rows.map(r => {
              const v = Number(r.xRaw);
              return Number.isFinite(v) ? v : 0;
            });

            // For LQ: use totalField if present, else treat each unit as weight=1
            const tVals: number[] = rows.map((r, _i) => {
              if (r.tRaw !== null && r.tRaw !== undefined) {
                const v = Number(r.tRaw);
                return Number.isFinite(v) && v > 0 ? v : 1;
              }
              return 1;
            });

            // Segregation arrays
            const aVals: number[] = groupAField
              ? rows.map(r => {
                  const v = Number(r.aRaw);
                  return Number.isFinite(v) ? v : 0;
                })
              : [];

            // groupB = explicit field OR totalField - groupAField
            const bVals: number[] = groupAField
              ? rows.map((r, i) => {
                  if (groupBField && r.bRaw !== null && r.bRaw !== undefined) {
                    const v = Number(r.bRaw);
                    return Number.isFinite(v) ? v : 0;
                  }
                  // derive groupB from total - groupA
                  return Math.max(0, tVals[i] - aVals[i]);
                })
              : [];

            if (cancelledRef.current) return;

            // Compute global metrics
            const gini = computeGini(xVals);
            const theilT = computeTheilT(xVals);
            const crK = computeConcentrationRatio(xVals, topK);
            const lqValues = computeLocationQuotients(xVals, tVals);

            let dissimilarity: number | null = null;
            let isolation: number | null = null;
            let exposure: number | null = null;

            const canSegregate = groupAField && aVals.length > 0;
            if (canSegregate) {
              dissimilarity = computeDissimilarity(aVals, bVals);
              isolation = computeIsolation(aVals, tVals);
              exposure = computeExposure(aVals, bVals, tVals);
            }

            if (cancelledRef.current) return;

            // Build output rows
            const outRows: Array<Record<string, unknown>> = [];
            for (let fi = 0; fi < rows.length; fi += 1) {
              const {rowIdx} = rows[fi];
              const row: Record<string, unknown> = {};
              (dataset.fields || []).forEach((f: any) => {
                row[f.name] = dataset.getValue(f.name, rowIdx);
              });
              row[lqColName] = Number.isFinite(lqValues[fi]) ? Math.round(lqValues[fi] * 10000) / 10000 : null;
              outRows.push(row);
            }

            if (cancelledRef.current || !outRows.length) return;

            upsertDerivedDatasetRows(
              localDispatch,
              datasets,
              newDatasetName,
              outRows,
              'qmap_equity',
              showOnMap
            );

            // Apply LQ diverging colour styling if showOnMap
            if (showOnMap) {
              // Wait a tick for the dataset to register
              await new Promise<void>(resolve => setTimeout(resolve, 0));
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
                  const lqField = (outputDataset.fields || []).find(
                    (f: any) => String(f?.name || '') === lqColName
                  );
                  if (lqField) {
                    const colorRange = ensureColorRange({
                      name: 'qmap.lq_diverging',
                      type: 'custom',
                      category: 'Custom',
                      colors: [LQ_COLOR_STOPS[0].color, LQ_COLOR_STOPS[1].color, LQ_COLOR_STOPS[2].color]
                    });
                    const nextConfig: any = {
                      colorField: lqField,
                      colorScale: 'quantize',
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

            // Build a summary for potential future diagnostic use
            const lqMin = Math.min(...lqValues.filter(Number.isFinite));
            const lqMax = Math.max(...lqValues.filter(Number.isFinite));
            const crPct = Math.round(crK * 1000) / 10;

            void {
              gini,
              theilT,
              crK: crPct,
              lqRange: [lqMin, lqMax],
              dissimilarity,
              isolation,
              exposure,
              n: rows.length
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
        totalField,
        groupAField,
        groupBField,
        topK,
        lqColName,
        showOnMap,
        newDatasetName,
        shouldSkip,
        complete
      ]);

      return null;
    }
  });
}
