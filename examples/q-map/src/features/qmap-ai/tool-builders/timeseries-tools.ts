import {useEffect} from 'react';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets} from '../../../state/qmap-selectors';
import type {QMapToolContext} from '../context/tool-context';
import {useToolExecution} from './use-tool-execution';

export function createSummarizeQMapTimeSeriesTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, toComparable} = ctx;
  return {
    description:
      'Summarize and preview a time series from a dataset using time/value fields (safe fallback when chart tools are unavailable).',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      timeFieldName: z.string().describe('Time field, e.g. data or timestamp'),
      valueFieldName: z.string().describe('Numeric value field, e.g. valore'),
      groupFieldName: z.string().optional().describe('Optional grouping field, e.g. idsensore'),
      groupValue: z.union([z.string(), z.number()]).optional().describe('Optional group value'),
      limit: z.number().min(1).max(500).optional().describe('Preview points count, default 100')
    }),
    execute: async ({datasetName, timeFieldName, valueFieldName, groupFieldName, groupValue, limit}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }
      const resolvedTime = resolveDatasetFieldName(dataset, timeFieldName);
      const resolvedValue = resolveDatasetFieldName(dataset, valueFieldName);
      if (!resolvedTime) {
        return {llmResult: {success: false, details: `Time field "${timeFieldName}" not found.`}};
      }
      if (!resolvedValue) {
        return {llmResult: {success: false, details: `Value field "${valueFieldName}" not found.`}};
      }
      const resolvedGroup =
        groupFieldName && String(groupFieldName).trim()
          ? resolveDatasetFieldName(dataset, String(groupFieldName))
          : null;

      const idx = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);
      const points: Array<{t: number; tRaw: unknown; v: number}> = [];
      idx.forEach((rowIdx: number) => {
        if (resolvedGroup && groupValue !== undefined) {
          const g = dataset.getValue(resolvedGroup, rowIdx);
          if (toComparable(g) !== toComparable(groupValue)) return;
        }
        const tRaw = dataset.getValue(resolvedTime, rowIdx);
        const vRaw = dataset.getValue(resolvedValue, rowIdx);
        const v = Number(vRaw);
        if (!Number.isFinite(v)) return;
        let t = Number(tRaw);
        if (!Number.isFinite(t)) {
          const parsed = Date.parse(String(tRaw || ''));
          if (!Number.isFinite(parsed)) return;
          t = parsed;
        } else if (t < 1e12) {
          // likely seconds epoch -> ms
          t = t * 1000;
        }
        points.push({t, tRaw, v});
      });

      if (!points.length) {
        return {
          llmResult: {
            success: false,
            details: 'No valid time series points found with provided fields/filters.'
          }
        };
      }

      points.sort((a, b) => a.t - b.t);
      const capped = points.slice(0, Math.max(1, Number(limit || 100)));
      const values = points.map(p => p.v);
      const min = Math.min(...values);
      const max = Math.max(...values);
      const avg = values.reduce((acc, n) => acc + n, 0) / values.length;
      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          timeField: resolvedTime,
          valueField: resolvedValue,
          groupField: resolvedGroup || null,
          groupValue: groupValue ?? null,
          count: points.length,
          min,
          max,
          avg: Number(avg.toFixed(6)),
          points: capped.map(p => ({
            timestamp: p.t,
            iso: new Date(p.t).toISOString(),
            value: p.v
          })),
          details: `Extracted ${points.length} time-series points from "${dataset.label || dataset.id}".`
        }
      };
    }
  };
}

export function createAggregateQMapTimeSeriesTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getDatasetInfoByLabel, toComparable, upsertDerivedDatasetRows, makeExecutionKey, EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey} = ctx;
  return {
    description:
      'Aggregate a time series dataset into temporal buckets (hour/day/week/month/year) and optionally detect a monotonic trend via Mann-Kendall test. ' +
      'Set materialize=true to write a derived non-spatial dataset with columns window_start_iso, window_end_iso, n_observations, aggregated_value.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      timeFieldName: z.string().describe('Time field (timestamp or epoch ms)'),
      valueFieldName: z.string().describe('Numeric value field to aggregate'),
      windowUnit: z.enum(['hour', 'day', 'week', 'month', 'year']).describe('Temporal bucket unit'),
      windowSize: z.number().min(1).max(366).optional().describe('Bucket size in windowUnit (default 1)'),
      aggregation: z.enum(['sum', 'avg', 'min', 'max', 'count']).describe('Aggregation function'),
      groupFieldName: z.string().optional().describe('Optional grouping field'),
      groupValue: z.union([z.string(), z.number()]).optional().describe('Optional group value filter'),
      detectTrend: z.boolean().optional().describe('Run Mann-Kendall trend test on bucket values (default true)'),
      materialize: z.boolean().optional().describe('Write derived non-spatial dataset (default false)'),
      newDatasetName: z.string().optional().describe('Output dataset name (only used when materialize=true)')
    }),
    execute: async ({
      datasetName,
      timeFieldName,
      valueFieldName,
      windowUnit,
      windowSize,
      aggregation,
      groupFieldName,
      groupValue,
      detectTrend,
      materialize,
      newDatasetName
    }: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }
      const resolvedTime = resolveDatasetFieldName(dataset, timeFieldName);
      if (!resolvedTime) {
        return {llmResult: {success: false, details: `Time field "${timeFieldName}" not found.`}};
      }
      const resolvedValue = resolveDatasetFieldName(dataset, valueFieldName);
      if (!resolvedValue) {
        return {llmResult: {success: false, details: `Value field "${valueFieldName}" not found.`}};
      }
      const resolvedGroup =
        groupFieldName && String(groupFieldName).trim()
          ? resolveDatasetFieldName(dataset, String(groupFieldName))
          : null;

      const size = Math.max(1, Math.min(366, Number(windowSize || 1)));

      // Bucket key helpers
      function getBucketKey(t: number): string {
        const d = new Date(t);
        switch (windowUnit) {
          case 'hour': {
            const h = Math.floor(d.getUTCHours() / size) * size;
            return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}T${String(h).padStart(2, '0')}`;
          }
          case 'day': {
            const dayOfYear = Math.floor((t - Date.UTC(d.getUTCFullYear(), 0, 0)) / 86400000);
            const bucketDay = Math.floor(dayOfYear / size) * size;
            return `${d.getUTCFullYear()}-day-${bucketDay}`;
          }
          case 'week': {
            const dayOfWeek = (d.getUTCDay() + 6) % 7; // Monday=0
            const weekStart = t - dayOfWeek * 86400000;
            const ws = new Date(weekStart);
            const weekOfYear = Math.floor((weekStart - Date.UTC(ws.getUTCFullYear(), 0, 0)) / (7 * 86400000));
            const bucketWeek = Math.floor(weekOfYear / size) * size;
            return `${ws.getUTCFullYear()}-week-${bucketWeek}`;
          }
          case 'month': {
            const totalMonths = d.getUTCFullYear() * 12 + d.getUTCMonth();
            const bucketMonth = Math.floor(totalMonths / size) * size;
            return `bucket-month-${bucketMonth}`;
          }
          case 'year': {
            const bucketYear = Math.floor(d.getUTCFullYear() / size) * size;
            return `year-${bucketYear}`;
          }
          default:
            return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
        }
      }

      function getBucketStartEnd(key: string): {start: number; end: number} {
        // Reconstruct start/end from bucket key
        if (windowUnit === 'hour') {
          const parts = key.split('T');
          const datePart = parts[0].split('-');
          const hourPart = Number(parts[1]);
          const start = Date.UTC(Number(datePart[0]), Number(datePart[1]) - 1, Number(datePart[2]), hourPart);
          return {start, end: start + size * 3600000 - 1};
        }
        if (windowUnit === 'day') {
          const parts = key.split('-day-');
          const year = Number(parts[0]);
          const dayOfYear = Number(parts[1]);
          const start = Date.UTC(year, 0, 0) + dayOfYear * 86400000;
          return {start, end: start + size * 86400000 - 1};
        }
        if (windowUnit === 'week') {
          const parts = key.split('-week-');
          const year = Number(parts[0]);
          const weekOfYear = Number(parts[1]);
          const start = Date.UTC(year, 0, 0) + weekOfYear * 7 * 86400000;
          return {start, end: start + size * 7 * 86400000 - 1};
        }
        if (windowUnit === 'month') {
          const bucketMonth = Number(key.replace('bucket-month-', ''));
          const year = Math.floor(bucketMonth / 12);
          const month = bucketMonth % 12;
          const start = Date.UTC(year, month, 1);
          const endDate = new Date(Date.UTC(year, month + size, 1));
          return {start, end: endDate.getTime() - 1};
        }
        if (windowUnit === 'year') {
          const bucketYear = Number(key.replace('year-', ''));
          const start = Date.UTC(bucketYear, 0, 1);
          return {start, end: Date.UTC(bucketYear + size, 0, 1) - 1};
        }
        const start = new Date(key).getTime();
        return {start, end: start + 86400000 - 1};
      }

      const idx = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);

      const buckets = new Map<string, number[]>();
      idx.forEach((rowIdx: number) => {
        if (resolvedGroup && groupValue !== undefined) {
          const g = dataset.getValue(resolvedGroup, rowIdx);
          if (toComparable(g) !== toComparable(groupValue)) return;
        }
        const tRaw = dataset.getValue(resolvedTime, rowIdx);
        const vRaw = dataset.getValue(resolvedValue, rowIdx);
        const v = Number(vRaw);
        if (!Number.isFinite(v)) return;
        let t = Number(tRaw);
        if (!Number.isFinite(t)) {
          const parsed = Date.parse(String(tRaw || ''));
          if (!Number.isFinite(parsed)) return;
          t = parsed;
        } else if (t < 1e12) {
          t = t * 1000;
        }
        const key = getBucketKey(t);
        if (!buckets.has(key)) buckets.set(key, []);
        buckets.get(key)!.push(v);
      });

      if (buckets.size === 0) {
        return {llmResult: {success: false, details: 'No valid data points found for bucketing.'}};
      }

      const sortedKeys = Array.from(buckets.keys()).sort();
      const bucketResults = sortedKeys.map(key => {
        const vals = buckets.get(key)!;
        let aggValue: number;
        switch (aggregation) {
          case 'sum': aggValue = vals.reduce((a: number, b: number) => a + b, 0); break;
          case 'avg': aggValue = vals.reduce((a: number, b: number) => a + b, 0) / vals.length; break;
          case 'min': aggValue = Math.min(...vals); break;
          case 'max': aggValue = Math.max(...vals); break;
          case 'count': aggValue = vals.length; break;
          default: aggValue = vals.reduce((a: number, b: number) => a + b, 0);
        }
        const {start, end} = getBucketStartEnd(key);
        return {key, aggValue, n: vals.length, start, end};
      });

      // Mann-Kendall trend detection
      let trend: 'increasing' | 'decreasing' | 'stable' = 'stable';
      let mk_z: number | null = null;
      const shouldDetect = detectTrend !== false;
      if (shouldDetect) {
        const k = bucketResults.length;
        if (k < 3) {
          trend = 'stable';
        } else {
          const x = bucketResults.map(b => b.aggValue);
          let S = 0;
          for (let i = 0; i < k - 1; i += 1) {
            for (let j = i + 1; j < k; j += 1) {
              const diff = x[j] - x[i];
              if (diff > 0) S += 1;
              else if (diff < 0) S -= 1;
            }
          }
          const varS = k * (k - 1) * (2 * k + 5) / 18;
          const Z = varS > 0 ? S / Math.sqrt(varS) : 0;
          mk_z = Number(Z.toFixed(4));
          if (Math.abs(Z) < 1.96) trend = 'stable';
          else if (Z >= 1.96) trend = 'increasing';
          else trend = 'decreasing';
        }
      }

      const wantMaterialize = materialize === true;
      let outDatasetLabel: string | null = null;
      let outDatasetId: string | null = null;
      if (wantMaterialize) {
        const targetName = String(newDatasetName || '').trim() || `${dataset.label || dataset.id}_${windowUnit}_${aggregation}`;
        const {label, datasetId} = getDatasetInfoByLabel(
          currentVisState?.datasets || {},
          targetName,
          'qmap_timeseries'
        );
        outDatasetLabel = label;
        outDatasetId = datasetId;
      }

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          windowUnit,
          windowSize: size,
          aggregation,
          buckets_count: bucketResults.length,
          trend: shouldDetect ? trend : null,
          mk_z: shouldDetect ? mk_z : null,
          stats: {
            first_window: sortedKeys[0] || null,
            last_window: sortedKeys[sortedKeys.length - 1] || null,
            min_agg: Math.min(...bucketResults.map(b => b.aggValue)),
            max_agg: Math.max(...bucketResults.map(b => b.aggValue)),
            total_n: bucketResults.reduce((a, b) => a + b.n, 0)
          },
          materialize: wantMaterialize,
          ...(wantMaterialize ? {outputDataset: outDatasetLabel, outputDatasetId: outDatasetId} : {}),
          details: `Aggregated ${bucketResults.length} ${windowUnit} buckets by ${aggregation}${shouldDetect ? `, trend: ${trend}` : ''}.`
        },
        ...(wantMaterialize
          ? {
              additionalData: {
                executionKey: makeExecutionKey('aggregate-timeseries'),
                materialize: true,
                rows: bucketResults.map(b => ({
                  window_start_iso: new Date(b.start).toISOString(),
                  window_end_iso: new Date(b.end).toISOString(),
                  n_observations: b.n,
                  aggregated_value: b.aggValue
                })),
                newDatasetName: outDatasetLabel,
                newDatasetId: outDatasetId
              }
            }
          : {})
      };
    },
    component: function AggregateQMapTimeSeriesComponent({
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
        upsertDerivedDatasetRows(localDispatch, datasets, newDatasetName, rows, 'qmap_timeseries', true);
      }, [localDispatch, datasets, executionKey, rows, newDatasetName, shouldSkip, complete]);

      return null;
    }
  };
}
