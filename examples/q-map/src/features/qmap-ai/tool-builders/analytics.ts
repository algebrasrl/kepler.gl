import React, {useEffect} from 'react';
import {extendedTool} from '../tool-shim';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets} from '../../../state/qmap-selectors';
import type {QMapToolContext} from '../context/tool-context';
import {useToolExecution} from './use-tool-execution';

export function createSummarizeQMapTimeSeriesTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, toComparable} = ctx;
  return extendedTool({
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
    execute: async ({datasetName, timeFieldName, valueFieldName, groupFieldName, groupValue, limit}) => {
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
  });
}

export function createAggregateQMapTimeSeriesTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getDatasetInfoByLabel, toComparable, upsertDerivedDatasetRows, makeExecutionKey, EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey} = ctx;
  return extendedTool({
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
    }) => {
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
          case 'sum': aggValue = vals.reduce((a, b) => a + b, 0); break;
          case 'avg': aggValue = vals.reduce((a, b) => a + b, 0) / vals.length; break;
          case 'min': aggValue = Math.min(...vals); break;
          case 'max': aggValue = Math.max(...vals); break;
          case 'count': aggValue = vals.length; break;
          default: aggValue = vals.reduce((a, b) => a + b, 0);
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
  });
}

export function createWordCloudTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getDatasetIndexes, WordCloudToolComponent} = ctx;
  return extendedTool({
    description: 'Render a word cloud from a text field by token frequency.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      textFieldName: z.string().describe('Text field used to extract words'),
      maxWords: z.number().min(5).max(200).optional().describe('Top words to render, default 60'),
      minWordLength: z.number().min(1).max(20).optional().describe('Ignore words shorter than this, default 3'),
      stopWords: z.array(z.string()).optional().describe('Additional stopwords to ignore')
    }),
    execute: async ({datasetName, textFieldName, maxWords, minWordLength, stopWords}) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }
      const resolvedField = resolveDatasetFieldName(dataset, textFieldName);
      if (!resolvedField) {
        return {llmResult: {success: false, details: `Field "${textFieldName}" not found in "${datasetName}".`}};
      }

      const idx = getDatasetIndexes(dataset).slice(0, 120000);
      const minLen = Math.max(1, Number(minWordLength || 3));
      const top = Math.max(5, Number(maxWords || 60));
      const baseStopWords = new Set(
        [
          'the',
          'and',
          'for',
          'with',
          'that',
          'this',
          'from',
          'are',
          'was',
          'have',
          'has',
          'you',
          'your',
          'dei',
          'delle',
          'della',
          'del',
          'dell',
          'per',
          'con',
          'una',
          'uno',
          'che',
          'non',
          'nel',
          'nei',
          'sul',
          'sui',
          'all',
          'alla',
          'alle',
          'gli',
          'dei',
          'dai'
        ].map(v => v.toLowerCase())
      );
      (stopWords || []).forEach((w: string) => {
        const token = String(w || '').trim().toLowerCase();
        if (token) baseStopWords.add(token);
      });

      const freq = new Map<string, number>();
      idx.forEach((rowIdx: number) => {
        const raw = dataset.getValue(resolvedField, rowIdx);
        const text = String(raw ?? '').toLowerCase();
        if (!text) return;
        const tokens = text.split(/[^a-z0-9]+/gi).filter(Boolean);
        tokens.forEach(token => {
          const t = String(token || '').trim().toLowerCase();
          if (!t || t.length < minLen) return;
          if (baseStopWords.has(t)) return;
          freq.set(t, (freq.get(t) || 0) + 1);
        });
      });
      const sorted = Array.from(freq.entries()).sort((a, b) => b[1] - a[1]).slice(0, top);
      if (!sorted.length) {
        return {llmResult: {success: false, details: 'No words found after filtering.'}};
      }
      const maxFreq = Math.max(...sorted.map(([, n]) => n), 1);
      const minFreq = Math.min(...sorted.map(([, n]) => n), maxFreq);
      const palette = ['#0f172a', '#1d4ed8', '#0f766e', '#be123c', '#9333ea', '#b45309'];
      const words = sorted.map(([text, value], i) => {
        const t = maxFreq === minFreq ? 0 : (value - minFreq) / (maxFreq - minFreq);
        const size = Math.round(12 + t * 24);
        return {text, value, size, color: palette[i % palette.length]};
      });
      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          field: resolvedField,
          wordsCount: words.length,
          details: `Rendered word cloud with ${words.length} words from "${resolvedField}".`
        },
        additionalData: {
          title: `Word Cloud - ${dataset.label || dataset.id} / ${resolvedField}`,
          datasetName: dataset.label || dataset.id,
          fieldName: resolvedField,
          words
        }
      };
    },
    component: WordCloudToolComponent as any
  });
}

export function createCategoryBarsTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getDatasetIndexes, CategoryBarsToolComponent} = ctx;
  return extendedTool({
    description: 'Render top categories as bars for a categorical/text field.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      categoryFieldName: z.string().describe('Categorical field name'),
      topN: z.number().min(3).max(100).optional().describe('Number of categories, default 20')
    }),
    execute: async ({datasetName, categoryFieldName, topN}) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }
      const resolvedField = resolveDatasetFieldName(dataset, categoryFieldName);
      if (!resolvedField) {
        return {
          llmResult: {
            success: false,
            details: `Field "${categoryFieldName}" not found in "${datasetName}".`
          }
        };
      }

      const counts = new Map<string, number>();
      getDatasetIndexes(dataset).forEach((rowIdx: number) => {
        const raw = dataset.getValue(resolvedField, rowIdx);
        const key = String(raw ?? '').trim();
        if (!key) return;
        counts.set(key, (counts.get(key) || 0) + 1);
      });
      const items = Array.from(counts.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, Math.max(3, Number(topN || 20)))
        .map(([label, value]) => ({label, value}));
      if (!items.length) {
        return {llmResult: {success: false, details: 'No categories found for selected field.'}};
      }
      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          field: resolvedField,
          categories: items.length,
          details: `Rendered top ${items.length} categories from "${resolvedField}".`
        },
        additionalData: {
          title: `Top Categories - ${dataset.label || dataset.id} / ${resolvedField}`,
          datasetName: dataset.label || dataset.id,
          fieldName: resolvedField,
          items
        }
      };
    },
    component: CategoryBarsToolComponent as any
  });
}

export function createGrammarAnalyzeTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getDatasetIndexes} = ctx;
  return extendedTool({
    description:
      'Deterministic frontend text analysis (tokenization, sentence split, token frequencies, optional bigrams) for a dataset text field.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      textFieldName: z.string().describe('Text field to analyze'),
      language: z
        .enum(['it', 'en'])
        .optional()
        .describe('Language hint for tokenization/stopwords, default it'),
      maxRows: z.number().min(1).max(200000).optional().describe('Rows to sample, default 50000'),
      topN: z.number().min(5).max(200).optional().describe('Top tokens to return, default 30'),
      minTokenLength: z.number().min(1).max(20).optional().describe('Ignore shorter tokens, default 2'),
      includeBigrams: z.boolean().optional().describe('Include top bigrams, default true')
    }),
    execute: async ({datasetName, textFieldName, language, maxRows, topN, minTokenLength, includeBigrams}) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }
      const resolvedField = resolveDatasetFieldName(dataset, textFieldName);
      if (!resolvedField) {
        return {llmResult: {success: false, details: `Field "${textFieldName}" not found in "${datasetName}".`}};
      }

      const lang = String(language || 'it').toLowerCase().startsWith('en') ? 'en' : 'it';
      const rowLimit = Math.max(1, Number(maxRows || 50000));
      const tokenLimit = Math.max(5, Number(topN || 30));
      const minLen = Math.max(1, Number(minTokenLength || 2));
      const wantBigrams = includeBigrams !== false;
      const rows = getDatasetIndexes(dataset).slice(0, rowLimit);

      const stopWordsIt = new Set([
        'a',
        'ad',
        'al',
        'alla',
        'alle',
        'allo',
        'ai',
        'agli',
        'all',
        'con',
        'col',
        'da',
        'dal',
        'dalla',
        'dalle',
        'dello',
        'dei',
        'degli',
        'dell',
        'del',
        'di',
        'e',
        'ed',
        'in',
        'il',
        'la',
        'le',
        'lo',
        'gli',
        'i',
        'un',
        'una',
        'uno',
        'su',
        'sul',
        'sui',
        'tra',
        'fra',
        'per',
        'che',
        'chi',
        'non',
        'si'
      ]);
      const stopWordsEn = new Set([
        'a',
        'an',
        'and',
        'are',
        'as',
        'at',
        'be',
        'by',
        'for',
        'from',
        'in',
        'is',
        'it',
        'of',
        'on',
        'or',
        'that',
        'the',
        'to',
        'was',
        'were',
        'with'
      ]);
      const stopWords = lang === 'en' ? stopWordsEn : stopWordsIt;

      const sentenceSegmenter =
        typeof Intl !== 'undefined' && (Intl as any).Segmenter
          ? new Intl.Segmenter(lang, {granularity: 'sentence'})
          : null;
      const wordSegmenter =
        typeof Intl !== 'undefined' && (Intl as any).Segmenter
          ? new Intl.Segmenter(lang, {granularity: 'word'})
          : null;

      let rowWithText = 0;
      let sentenceCount = 0;
      let tokenCount = 0;
      let uniqueTokenCount = 0;
      let totalCharCount = 0;
      let alphaTokenCount = 0;

      const tokenFreq = new Map<string, number>();
      const bigramFreq = new Map<string, number>();

      const addToken = (tokenRaw: string) => {
        const token = String(tokenRaw || '').trim().toLowerCase();
        if (!token || token.length < minLen) return null;
        if (!/[\p{L}\p{N}]/u.test(token)) return null;
        if (stopWords.has(token)) return null;
        tokenFreq.set(token, (tokenFreq.get(token) || 0) + 1);
        tokenCount += 1;
        if (/^\p{L}+$/u.test(token)) {
          alphaTokenCount += 1;
        }
        return token;
      };

      rows.forEach((rowIdx: number) => {
        const raw = dataset.getValue(resolvedField, rowIdx);
        const text = String(raw ?? '').trim();
        if (!text) return;
        rowWithText += 1;
        totalCharCount += text.length;

        if (sentenceSegmenter) {
          sentenceCount += Array.from(sentenceSegmenter.segment(text)).filter(Boolean).length;
        } else {
          const chunks = text.split(/[.!?]+/g).map(s => s.trim()).filter(Boolean);
          sentenceCount += chunks.length || 1;
        }

        const rowTokens: string[] = [];
        if (wordSegmenter) {
          for (const chunk of wordSegmenter.segment(text) as any) {
            if (!chunk?.isWordLike) continue;
            const normalized = addToken(String(chunk.segment || ''));
            if (normalized) rowTokens.push(normalized);
          }
        } else {
          text
            .split(/[^\p{L}\p{N}]+/u)
            .filter(Boolean)
            .forEach(t => {
              const normalized = addToken(t);
              if (normalized) rowTokens.push(normalized);
            });
        }

        if (wantBigrams && rowTokens.length > 1) {
          for (let i = 0; i < rowTokens.length - 1; i += 1) {
            const bigram = `${rowTokens[i]} ${rowTokens[i + 1]}`;
            bigramFreq.set(bigram, (bigramFreq.get(bigram) || 0) + 1);
          }
        }
      });

      uniqueTokenCount = tokenFreq.size;
      if (!rowWithText) {
        return {llmResult: {success: false, details: 'No non-empty text rows found in selected field.'}};
      }

      const topTokens = Array.from(tokenFreq.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, tokenLimit)
        .map(([token, count]) => ({token, count}));
      const topBigrams = wantBigrams
        ? Array.from(bigramFreq.entries())
            .sort((a, b) => b[1] - a[1])
            .slice(0, Math.min(20, tokenLimit))
            .map(([bigram, count]) => ({bigram, count}))
        : [];

      const avgTokensPerSentence = sentenceCount > 0 ? tokenCount / sentenceCount : 0;
      const avgCharsPerRow = rowWithText > 0 ? totalCharCount / rowWithText : 0;
      const lexicalDiversity = tokenCount > 0 ? uniqueTokenCount / tokenCount : 0;
      const alphaRatio = tokenCount > 0 ? alphaTokenCount / tokenCount : 0;

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          field: resolvedField,
          language: lang,
          rowsScanned: rows.length,
          rowsWithText: rowWithText,
          sentenceCount,
          tokenCount,
          uniqueTokenCount,
          lexicalDiversity: Number(lexicalDiversity.toFixed(6)),
          alphaTokenRatio: Number(alphaRatio.toFixed(6)),
          avgTokensPerSentence: Number(avgTokensPerSentence.toFixed(4)),
          avgCharsPerRow: Number(avgCharsPerRow.toFixed(2)),
          topTokens,
          topBigrams,
          details: `Analyzed "${resolvedField}" with deterministic tokenization over ${rowWithText} text rows.`
        }
      };
    }
  });
}

export function createDescribeQMapFieldTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getFilteredDatasetIndexes} = ctx;
  return extendedTool({
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
    execute: async ({datasetName, fieldName, useActiveFilters, percentiles}) => {
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
  });
}

// ─── Composite Index Tool ─────────────────────────────────────────────────────

export function createCompositeIndexTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getDatasetIndexes, upsertDerivedDatasetRows, getDatasetInfoByLabel, makeExecutionKey, EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey} = ctx;
  return extendedTool({
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
    execute: async ({datasetName, components, outputFieldName, normalize, newDatasetName}) => {
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
      const compositeRaw: number[] = idx.map((rowIdx: any, i: any) => {
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
  });
}

// ─── Data Quality Report Tool ─────────────────────────────────────────────────

export function createDataQualityReportTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, getDatasetIndexes} = ctx;
  return extendedTool({
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
    execute: async ({datasetName, fields, outlierMethod, outlierThreshold}) => {
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
  });
}
