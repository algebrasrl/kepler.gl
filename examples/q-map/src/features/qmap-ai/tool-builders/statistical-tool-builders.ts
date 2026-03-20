import {useEffect} from 'react';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets} from '../../../state/qmap-selectors';
import type {QMapToolContext} from '../context/tool-context';
import {useToolExecution} from './use-tool-execution';

export function createDescribeQMapFieldTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getFilteredDatasetIndexes} = ctx;
  return {
    description:
      'Compute descriptive statistics for a numeric field in a dataset: count, null count, min, max, mean, median, std, variance, skewness, and configurable percentiles. ' +
      'Use this before any analysis or visualization to understand the field distribution. ' +
      'Differs from summarizeQMapTimeSeries (which previews temporal sequences) — this is field-level univariate statistics.',
    parameters: z.object({
      datasetName: z.string().describe('Dataset name from listQMapDatasets'),
      fieldName: z.string().describe('Numeric field to describe'),
      useActiveFilters: z
        .boolean()
        .optional()
        .describe('Apply active map filters when sampling values. Default true.'),
      percentiles: z
        .array(z.number().min(0).max(100))
        .optional()
        .describe('Percentile values to compute (0–100). Default: [5, 25, 50, 75, 95].')
    }),
    execute: async ({datasetName, fieldName, useActiveFilters, percentiles}: any) => {
      const vis = getCurrentVisState();
      const dataset = resolveDatasetByName(vis?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }
      const resolvedField = resolveDatasetFieldName(dataset, fieldName);
      if (!resolvedField) {
        return {
          llmResult: {success: false, details: `Field "${fieldName}" not found in dataset "${datasetName}".`}
        };
      }

      const effectivePercentiles =
        Array.isArray(percentiles) && percentiles.length > 0 ? percentiles : [5, 25, 50, 75, 95];
      const useFilters = useActiveFilters !== false;
      const idx = getFilteredDatasetIndexes(dataset, vis, useFilters);

      const vals: number[] = [];
      let nullCount = 0;
      for (const rowIdx of idx) {
        const raw = dataset.getValue(resolvedField, rowIdx);
        const v = Number(raw);
        if (Number.isFinite(v)) {
          vals.push(v);
        } else {
          nullCount += 1;
        }
      }

      const totalRows = idx.length;
      const n = vals.length;
      const nullPct = totalRows > 0 ? (nullCount / totalRows) * 100 : 0;

      if (n === 0) {
        return {
          llmResult: {
            success: true,
            dataset: dataset.label || dataset.id,
            field: resolvedField,
            count: 0,
            nullCount: totalRows,
            nullPct: 100,
            min: null,
            max: null,
            range: null,
            mean: null,
            median: null,
            std: null,
            variance: null,
            skewness: null,
            percentiles: {},
            details: `Field "${resolvedField}" has no valid numeric values (${totalRows} total rows, all null/non-numeric).`
          }
        };
      }

      vals.sort((a, b) => a - b);

      const min = vals[0];
      const max = vals[n - 1];
      const range = max - min;
      const mean = vals.reduce((acc, v) => acc + v, 0) / n;
      const variance = vals.reduce((acc, v) => acc + (v - mean) * (v - mean), 0) / n;
      const std = Math.sqrt(variance);

      let median: number;
      if (n % 2 === 1) {
        median = vals[Math.floor(n / 2)];
      } else {
        median = (vals[n / 2 - 1] + vals[n / 2]) / 2;
      }

      // Pearson's 2nd skewness coefficient
      const skewness = std > 0 ? (mean - median) / std : 0;

      // Percentile with linear interpolation
      function computePercentile(p: number): number {
        if (n === 1) return vals[0];
        const pos = (p / 100) * (n - 1);
        const lower = Math.floor(pos);
        const upper = Math.ceil(pos);
        if (lower === upper) return vals[lower];
        const frac = pos - lower;
        return vals[lower] * (1 - frac) + vals[upper] * frac;
      }

      const pctResult: Record<string, number> = {};
      for (const p of effectivePercentiles) {
        pctResult[`p${p}`] = Number(computePercentile(p).toFixed(6));
      }

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          field: resolvedField,
          count: n,
          nullCount,
          nullPct: Number(nullPct.toFixed(2)),
          min,
          max,
          range: Number(range.toFixed(6)),
          mean: Number(mean.toFixed(6)),
          median: Number(median.toFixed(6)),
          std: Number(std.toFixed(6)),
          variance: Number(variance.toFixed(6)),
          skewness: Number(skewness.toFixed(6)),
          percentiles: pctResult,
          details: `Described field "${resolvedField}" on "${dataset.label || dataset.id}": n=${n}, null=${nullCount} (${nullPct.toFixed(1)}%), mean=${mean.toFixed(4)}, std=${std.toFixed(4)}, range=[${min}, ${max}].`
        }
      };
    }
  };
}

// ─── Composite Index Tool ─────────────────────────────────────────────────────

export function createCompositeIndexTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getDatasetIndexes, upsertDerivedDatasetRows, getDatasetInfoByLabel, makeExecutionKey, EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey} = ctx;
  return {
    description:
      'Build a weighted composite index (e.g. Cumulative Burden Index, vulnerability score) from multiple numeric fields. ' +
      'Each component is min-max normalized to [0,1]; direction controls burden orientation (asc: higher value = higher burden, desc: lower value = higher burden). ' +
      'The weighted sum is optionally re-normalized to [0,1] and stored as a new field in a derived dataset.',
    parameters: z.object({
      datasetName: z.string().describe('Source dataset name from listQMapDatasets'),
      components: z
        .array(
          z.object({
            fieldName: z.string().describe('Numeric field name to include in the composite'),
            weight: z.number().min(0).describe('Relative weight (will be normalized so all weights sum to 1)'),
            direction: z
              .enum(['asc', 'desc'])
              .describe('asc: higher value = higher burden/score; desc: lower value = higher burden/score (inverted)')
          })
        )
        .min(2)
        .describe('At least 2 components required'),
      outputFieldName: z.string().optional().describe('Name for the composite score field (default: composite_score)'),
      normalize: z.boolean().optional().describe('Re-normalize composite to [0,1] after weighting (default true)'),
      newDatasetName: z.string().optional().describe('Output dataset name (default: <source>_composite)')
    }),
    execute: async ({datasetName, components, outputFieldName, normalize, newDatasetName}: any) => {
      const vis = getCurrentVisState();
      const dataset = resolveDatasetByName(vis?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }

      const resolvedComponents: Array<{field: string; weight: number; direction: 'asc' | 'desc'}> = [];
      for (const comp of components) {
        const resolved = resolveDatasetFieldName(dataset, comp.fieldName);
        if (!resolved) {
          return {
            llmResult: {success: false, details: `Field "${comp.fieldName}" not found in dataset "${datasetName}".`}
          };
        }
        resolvedComponents.push({field: resolved, weight: Math.max(0, comp.weight), direction: comp.direction});
      }

      const totalWeight = resolvedComponents.reduce((s, c) => s + c.weight, 0);
      if (totalWeight <= 0) {
        return {llmResult: {success: false, details: 'All component weights are zero.'}};
      }

      const idx = getDatasetIndexes(dataset);
      if (!idx.length) {
        return {llmResult: {success: false, details: 'Dataset has no rows.'}};
      }

      // Collect raw values and compute per-field min/max for normalization
      const rawByField: Record<string, number[]> = {};
      const minByField: Record<string, number> = {};
      const maxByField: Record<string, number> = {};

      for (const comp of resolvedComponents) {
        const vals: number[] = [];
        for (const rowIdx of idx) {
          const v = Number(dataset.getValue(comp.field, rowIdx));
          vals.push(Number.isFinite(v) ? v : NaN);
        }
        rawByField[comp.field] = vals;
        const finite = vals.filter(Number.isFinite);
        minByField[comp.field] = finite.length ? Math.min(...finite) : 0;
        maxByField[comp.field] = finite.length ? Math.max(...finite) : 1;
      }

      const outField = String(outputFieldName || 'composite_score').trim() || 'composite_score';
      const wantNormalize = normalize !== false;

      // Compute composite scores
      const compositeRaw: number[] = idx.map((_rowIdx: any, i: any) => {
        let score = 0;
        for (const comp of resolvedComponents) {
          const raw = rawByField[comp.field][i];
          if (!Number.isFinite(raw)) continue;
          const mn = minByField[comp.field];
          const mx = maxByField[comp.field];
          let norm = mx > mn ? (raw - mn) / (mx - mn) : 0.5;
          if (comp.direction === 'desc') norm = 1 - norm;
          score += (comp.weight / totalWeight) * norm;
        }
        return score;
      });

      let compositeMin = Math.min(...compositeRaw.filter(Number.isFinite));
      let compositeMax = Math.max(...compositeRaw.filter(Number.isFinite));
      if (!Number.isFinite(compositeMin)) compositeMin = 0;
      if (!Number.isFinite(compositeMax)) compositeMax = 1;

      const targetName =
        String(newDatasetName || '').trim() ||
        `${dataset.label || dataset.id}_composite`;
      const {label: outLabel, datasetId: outDatasetId} = getDatasetInfoByLabel(
        vis?.datasets || {},
        targetName,
        'qmap_composite'
      );

      const rows: Array<Record<string, unknown>> = idx.map((rowIdx: any, i: any) => {
        // Copy all original fields
        const row: Record<string, unknown> = {};
        const fields = dataset.fields || [];
        for (const f of fields) {
          try {
            row[f.name] = dataset.getValue(f.name, rowIdx);
          } catch {
            // skip unreadable field
          }
        }
        const raw = compositeRaw[i];
        let final: number;
        if (!Number.isFinite(raw)) {
          final = NaN;
        } else if (wantNormalize && compositeMax > compositeMin) {
          final = (raw - compositeMin) / (compositeMax - compositeMin);
        } else {
          final = raw;
        }
        row[outField] = Number.isFinite(final) ? Number(final.toFixed(6)) : null;
        return row;
      });

      const componentSummary = resolvedComponents.map(c => ({
        field: c.field,
        weight: Number(((c.weight / totalWeight) * 100).toFixed(1)) + '%',
        direction: c.direction,
        min: Number(minByField[c.field].toFixed(4)),
        max: Number(maxByField[c.field].toFixed(4))
      }));

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          outputDataset: outLabel,
          outputDatasetId: outDatasetId,
          outputField: outField,
          componentsCount: resolvedComponents.length,
          rowsProcessed: idx.length,
          compositeRange: {
            min: Number(compositeMin.toFixed(6)),
            max: Number(compositeMax.toFixed(6))
          },
          normalized: wantNormalize,
          components: componentSummary,
          details: `Composite index "${outField}" computed from ${resolvedComponents.length} fields over ${idx.length} rows, written to "${outLabel}".`
        },
        additionalData: {
          executionKey: makeExecutionKey('composite-index'),
          rows,
          newDatasetName: outLabel,
          newDatasetId: outDatasetId
        }
      };
    },
    component: function CompositeIndexComponent({
      executionKey,
      rows,
      newDatasetName
    }: {
      executionKey?: string;
      rows: Array<Record<string, unknown>>;
      newDatasetName: string;
    }) {
      const localDispatch = useDispatch<any>();
      const datasets = useSelector(selectQMapDatasets);
      const {shouldSkip, complete} = useToolExecution({
        executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });

      useEffect(() => {
        if (shouldSkip()) return;
        if (!rows?.length || !newDatasetName) return;
        complete();
        upsertDerivedDatasetRows(localDispatch, datasets, newDatasetName, rows, 'qmap_composite', true);
      }, [localDispatch, datasets, executionKey, rows, newDatasetName, shouldSkip, complete]);

      return null;
    }
  };
}

// ─── Data Quality Report Tool ─────────────────────────────────────────────────

export function createDataQualityReportTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getDatasetIndexes} = ctx;
  return {
    description:
      'Generate a data quality report for a dataset: null completeness, outlier detection (IQR or z-score), and basic range statistics per field. ' +
      'Run this before spatial statistics or equity analysis on unknown datasets to catch missing-data or outlier issues early. ' +
      'Pure read-only — does not mutate any dataset.',
    parameters: z.object({
      datasetName: z.string().describe('Dataset name from listQMapDatasets'),
      fields: z
        .array(z.string())
        .optional()
        .describe('Specific numeric fields to audit. If omitted, all numeric fields are audited (up to 30).'),
      outlierMethod: z
        .enum(['iqr', 'zscore'])
        .optional()
        .describe('Outlier detection method: iqr (default, threshold=1.5×IQR) or zscore (threshold=3)'),
      outlierThreshold: z
        .number()
        .min(0)
        .optional()
        .describe('Override default threshold: 1.5 for IQR, 3.0 for z-score')
    }),
    execute: async ({datasetName, fields, outlierMethod, outlierThreshold}: any) => {
      const vis = getCurrentVisState();
      const dataset = resolveDatasetByName(vis?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }

      const method = outlierMethod === 'zscore' ? 'zscore' : 'iqr';
      const threshold = Number.isFinite(Number(outlierThreshold))
        ? Number(outlierThreshold)
        : method === 'zscore'
          ? 3.0
          : 1.5;

      // Determine fields to audit
      let targetFields: string[];
      if (Array.isArray(fields) && fields.length > 0) {
        const resolved: string[] = [];
        for (const f of fields) {
          const r = resolveDatasetFieldName(dataset, f);
          if (r) resolved.push(r);
        }
        if (!resolved.length) {
          return {llmResult: {success: false, details: 'None of the specified fields found in the dataset.'}};
        }
        targetFields = resolved;
      } else {
        // Auto-detect numeric fields (up to 30)
        const allFields: Array<{name: string; type?: string}> = dataset.fields || [];
        targetFields = allFields
          .filter(f => f.type === 'real' || f.type === 'integer' || f.type === 'float' || f.type === 'int')
          .slice(0, 30)
          .map(f => f.name);
        if (!targetFields.length) {
          return {llmResult: {success: false, details: 'No numeric fields found in dataset for quality audit.'}};
        }
      }

      const idx = getDatasetIndexes(dataset);
      const totalRows = idx.length;

      if (!totalRows) {
        return {llmResult: {success: false, details: 'Dataset has no rows.'}};
      }

      const fieldReports: Array<{
        field: string;
        totalRows: number;
        nullCount: number;
        nullPct: number;
        min: number | null;
        max: number | null;
        mean: number | null;
        std: number | null;
        outlierCount: number;
        outlierPct: number;
        qualityFlag: 'ok' | 'high_nulls' | 'outliers' | 'high_nulls_and_outliers' | 'all_null';
      }> = [];

      for (const fieldName of targetFields) {
        const vals: number[] = [];
        let nullCount = 0;

        for (const rowIdx of idx) {
          const raw = dataset.getValue(fieldName, rowIdx);
          const v = Number(raw);
          if (Number.isFinite(v)) {
            vals.push(v);
          } else {
            nullCount += 1;
          }
        }

        const nullPct = (nullCount / totalRows) * 100;

        if (!vals.length) {
          fieldReports.push({
            field: fieldName,
            totalRows,
            nullCount,
            nullPct: 100,
            min: null,
            max: null,
            mean: null,
            std: null,
            outlierCount: 0,
            outlierPct: 0,
            qualityFlag: 'all_null'
          });
          continue;
        }

        vals.sort((a, b) => a - b);
        const n = vals.length;
        const min = vals[0];
        const max = vals[n - 1];
        const mean = vals.reduce((s, v) => s + v, 0) / n;
        const variance = vals.reduce((s, v) => s + (v - mean) * (v - mean), 0) / n;
        const std = Math.sqrt(variance);

        let outlierCount = 0;
        if (method === 'iqr') {
          const q1 = vals[Math.floor(n * 0.25)];
          const q3 = vals[Math.floor(n * 0.75)];
          const iqr = q3 - q1;
          const lo = q1 - threshold * iqr;
          const hi = q3 + threshold * iqr;
          outlierCount = vals.filter(v => v < lo || v > hi).length;
        } else {
          // zscore
          if (std > 0) {
            outlierCount = vals.filter(v => Math.abs((v - mean) / std) > threshold).length;
          }
        }

        const outlierPct = (outlierCount / totalRows) * 100;
        const highNulls = nullPct > 20;
        const hasOutliers = outlierCount > 0;
        let qualityFlag: (typeof fieldReports)[0]['qualityFlag'] = 'ok';
        if (highNulls && hasOutliers) qualityFlag = 'high_nulls_and_outliers';
        else if (highNulls) qualityFlag = 'high_nulls';
        else if (hasOutliers) qualityFlag = 'outliers';

        fieldReports.push({
          field: fieldName,
          totalRows,
          nullCount,
          nullPct: Number(nullPct.toFixed(2)),
          min: Number(min.toFixed(6)),
          max: Number(max.toFixed(6)),
          mean: Number(mean.toFixed(6)),
          std: Number(std.toFixed(6)),
          outlierCount,
          outlierPct: Number(outlierPct.toFixed(2)),
          qualityFlag
        });
      }

      const flaggedFields = fieldReports.filter(r => r.qualityFlag !== 'ok');
      const allNullFields = fieldReports.filter(r => r.qualityFlag === 'all_null');
      const highNullFields = fieldReports.filter(r => r.nullPct > 20 && r.qualityFlag !== 'all_null');

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          totalRows,
          fieldsAudited: fieldReports.length,
          outlierMethod: method,
          outlierThreshold: threshold,
          flaggedFieldsCount: flaggedFields.length,
          allNullFields: allNullFields.map(r => r.field),
          highNullFields: highNullFields.map(r => `${r.field} (${r.nullPct}% null)`),
          fieldReports,
          details: `Quality report for "${dataset.label || dataset.id}": ${fieldReports.length} fields audited, ${flaggedFields.length} flagged (${allNullFields.length} all-null, ${highNullFields.length} high-null, outlier method: ${method} threshold=${threshold}).`
        }
      };
    }
  };
}
