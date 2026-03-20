import {z} from 'zod';

import type {QMapToolContext} from '../context/tool-context';

export function createPreviewQMapDatasetRowsTool(ctx: QMapToolContext) {
  const {
    QMAP_SORT_DIRECTION_SCHEMA,
    QMAP_DEFAULT_CHUNK_SIZE,
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    yieldToMainThread
  } = ctx;

  return {
    description: 'Preview dataset rows with selected fields to inspect records.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      limit: z
        .number()
        .min(1)
        .max(500)
        .optional()
        .describe(
          'Default 8. For analytical rankings use orderBy + sortDirection; without orderBy keep limit <= 50.'
        ),
      fields: z.array(z.string()).optional().describe('Optional field names to include'),
      orderBy: z.string().optional().describe('Optional field name used to sort rows before preview'),
      sortDirection: QMAP_SORT_DIRECTION_SCHEMA.describe('Sort direction for orderBy (default asc)')
    }),
    execute: async ({datasetName, limit, fields, orderBy, sortDirection}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found.`
          }
        };
      }

      const allFields: string[] = (dataset.fields || []).map((f: any) => String(f?.name || '')).filter(Boolean);
      const normalizeFieldToken = (value: unknown) =>
        String(value || '')
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, '_')
          .replace(/^_+|_+$/g, '');
      const isGeometryFieldName = (fieldName: string) => {
        const token = normalizeFieldToken(fieldName);
        return (
          token === '_geojson' ||
          token === '_geometry' ||
          token === 'geojson' ||
          token === 'geometry' ||
          token.endsWith('_geojson') ||
          token.endsWith('_geometry')
        );
      };
      const scoreFieldName = (fieldName: string) => {
        const token = normalizeFieldToken(fieldName);
        if (token === 'name' || token === 'name_en' || token === 'lv') return 100;
        if (token === 'population' || token === 'hasc' || token === 'lv2_hasc') return 90;
        if (token === 'kontur_boundaries_id' || token === 'kontur_boundaries_lv7_id' || token === 'kontur_boundaries_lv9_id')
          return 80;
        if (token.includes('__lv') || token.endsWith('_id')) return 70;
        if (token.includes('name')) return 60;
        return 0;
      };
      const nonGeometryFields: string[] = allFields.filter((fieldName: string) => !isGeometryFieldName(fieldName));
      const defaultFields = nonGeometryFields
        .slice()
        .sort((a: string, b: string) => {
          const scoreDiff = scoreFieldName(b) - scoreFieldName(a);
          return scoreDiff !== 0 ? scoreDiff : a.localeCompare(b);
        })
        .slice(0, 12);
      const requested =
        Array.isArray(fields) && fields.length ? fields : defaultFields.length ? defaultFields : allFields.slice(0, 12);
      const resolvedFields = requested
        .map((name: string) => resolveDatasetFieldName(dataset, name))
        .filter(Boolean) as string[];
      const uniqueFields = Array.from(new Set(resolvedFields));
      if (!uniqueFields.length) {
        return {
          llmResult: {
            success: false,
            details: 'No valid fields to preview.'
          }
        };
      }

      const resolvedOrderBy = orderBy ? resolveDatasetFieldName(dataset, orderBy) : null;
      if (orderBy && !resolvedOrderBy) {
        return {
          llmResult: {
            success: false,
            details: `Sort field "${orderBy}" not found in dataset "${dataset.label || dataset.id}".`
          }
        };
      }

      const idx = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);
      const normalizedSortDirection = sortDirection === 'desc' ? 'desc' : 'asc';
      const normalizedLimit = Math.max(1, Number(limit || 8));
      if (!resolvedOrderBy && normalizedLimit > 50) {
        return {
          llmResult: {
            success: false,
            details:
              'For unsorted previews, limit must be <= 50 to avoid non-representative large samples. ' +
              'For precise ranking analysis, provide orderBy + sortDirection.'
          }
        };
      }
      const compareRowIndexes = (a: number, b: number) => {
        if (!resolvedOrderBy) return a - b;
        const leftValue = dataset.getValue(resolvedOrderBy, a);
        const rightValue = dataset.getValue(resolvedOrderBy, b);

        const isMissing = (value: unknown) =>
          value === null || value === undefined || (typeof value === 'string' && value.trim() === '');
        const leftMissing = isMissing(leftValue);
        const rightMissing = isMissing(rightValue);
        if (leftMissing || rightMissing) {
          if (leftMissing && rightMissing) return a - b;
          return leftMissing ? 1 : -1;
        }

        const toNumeric = (value: unknown): number | null => {
          if (typeof value === 'number') {
            return Number.isFinite(value) ? value : null;
          }
          if (typeof value === 'bigint') {
            const asNumber = Number(value);
            return Number.isFinite(asNumber) ? asNumber : null;
          }
          if (typeof value === 'string') {
            const trimmed = value.trim();
            if (!trimmed) return null;
            const direct = Number(trimmed);
            if (Number.isFinite(direct)) return direct;

            const compact = trimmed.replace(/\s+/g, '');
            const noThousandsComma = Number(compact.replace(/,/g, ''));
            if (Number.isFinite(noThousandsComma)) return noThousandsComma;

            const noThousandsDot = Number(compact.replace(/\./g, '').replace(',', '.'));
            if (Number.isFinite(noThousandsDot)) return noThousandsDot;

            const commaDecimal = Number(compact.replace(',', '.'));
            return Number.isFinite(commaDecimal) ? commaDecimal : null;
          }
          return null;
        };

        const leftNumeric = toNumeric(leftValue);
        const rightNumeric = toNumeric(rightValue);
        if (leftNumeric !== null && rightNumeric !== null) {
          if (leftNumeric === rightNumeric) return a - b;
          return normalizedSortDirection === 'asc' ? leftNumeric - rightNumeric : rightNumeric - leftNumeric;
        }

        const leftString = String(leftValue).toLowerCase();
        const rightString = String(rightValue).toLowerCase();
        const compared = leftString.localeCompare(rightString);
        if (compared === 0) return a - b;
        return normalizedSortDirection === 'asc' ? compared : -compared;
      };

      let capped: number[] = [];
      if (resolvedOrderBy) {
        const topIndexes: number[] = [];
        const loopYieldEvery = Math.max(100, QMAP_DEFAULT_CHUNK_SIZE);
        for (let i = 0; i < idx.length; i += 1) {
          const rowIdx = idx[i];
          let insertAt = topIndexes.length;
          for (let j = 0; j < topIndexes.length; j += 1) {
            if (compareRowIndexes(rowIdx, topIndexes[j]) < 0) {
              insertAt = j;
              break;
            }
          }
          topIndexes.splice(insertAt, 0, rowIdx);
          if (topIndexes.length > normalizedLimit) {
            topIndexes.pop();
          }
          if (i > 0 && i % loopYieldEvery === 0) {
            await yieldToMainThread();
          }
        }
        capped = topIndexes;
      } else {
        capped = idx.slice(0, normalizedLimit);
      }
      const rows = capped.map((rowIdx: number) => {
        const out: Record<string, unknown> = {};
        uniqueFields.forEach((fieldName: string) => {
          const rawValue = dataset.getValue(fieldName, rowIdx);
          if (isGeometryFieldName(fieldName)) {
            out[fieldName] = '[geojson omitted]';
            return;
          }
          if (rawValue === null || rawValue === undefined) {
            out[fieldName] = rawValue;
            return;
          }
          if (typeof rawValue === 'string') {
            out[fieldName] = rawValue.length > 180 ? `${rawValue.slice(0, 177)}...` : rawValue;
            return;
          }
          if (typeof rawValue === 'number' || typeof rawValue === 'boolean') {
            out[fieldName] = rawValue;
            return;
          }
          out[fieldName] = '[complex value omitted]';
        });
        return out;
      });

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          fields: uniqueFields,
          orderBy: resolvedOrderBy || null,
          sortDirection: resolvedOrderBy ? normalizedSortDirection : null,
          rowCount: rows.length,
          totalRows: idx.length,
          rankingExact: Boolean(resolvedOrderBy),
          samplingMode: resolvedOrderBy ? 'ordered_top_k' : 'head_sample',
          rows,
          details: resolvedOrderBy
            ? `Previewed ${rows.length} rows from "${dataset.label || dataset.id}" ordered by "${resolvedOrderBy}" (${normalizedSortDirection}).`
            : `Previewed ${rows.length} rows from "${dataset.label || dataset.id}".`
        }
      };
    }
  };

}

export function createRankQMapDatasetRowsTool(ctx: QMapToolContext) {
  const {
    QMAP_SORT_DIRECTION_SCHEMA,
    QMAP_DEFAULT_CHUNK_SIZE,
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    yieldToMainThread,
    lastRankContextRef
  } = ctx;

  return {
    description:
      'Compute exact top/bottom ranking on a loaded dataset by metric field. Use for analytical ranking (top N/bottom N) instead of unsorted previews.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      metricFieldName: z.string().describe('Field used as ranking metric (prefer numeric fields)'),
      topN: z.number().min(1).max(500).optional().describe('Default 10'),
      sortDirection: QMAP_SORT_DIRECTION_SCHEMA.describe('desc = highest values first (default), asc = lowest values first'),
      fields: z.array(z.string()).optional().describe('Optional fields to include in ranked rows')
    }),
    execute: async ({datasetName, metricFieldName, topN, sortDirection, fields}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found.`
          }
        };
      }

      const allFields = (dataset.fields || []).map((f: any) => String(f?.name || '')).filter(Boolean);
      const isGeometryFieldName = (fieldName: string) => {
        const token = String(fieldName || '')
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, '_')
          .replace(/^_+|_+$/g, '');
        return (
          token === '_geojson' ||
          token === '_geometry' ||
          token === 'geojson' ||
          token === 'geometry' ||
          token.endsWith('_geojson') ||
          token.endsWith('_geometry')
        );
      };
      const idx = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);
      if (!idx.length) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${dataset.label || dataset.id}" has no rows.`
          }
        };
      }

      const requestedMetricFieldName = String(metricFieldName || '').trim();
      if (!requestedMetricFieldName) {
        return {
          llmResult: {
            success: false,
            details: 'Missing ranking metric field. Pass metricFieldName in rankQMapDatasetRows.'
          }
        };
      }

      const parseNumericMetricValue = (value: unknown): number | null => {
        if (typeof value === 'number') {
          return Number.isFinite(value) ? value : null;
        }
        if (typeof value === 'bigint') {
          const asNumber = Number(value);
          return Number.isFinite(asNumber) ? asNumber : null;
        }
        if (typeof value === 'string') {
          const trimmed = value.trim();
          if (!trimmed) return null;
          const direct = Number(trimmed);
          if (Number.isFinite(direct)) return direct;

          const compact = trimmed.replace(/\s+/g, '');
          const noThousandsComma = Number(compact.replace(/,/g, ''));
          if (Number.isFinite(noThousandsComma)) return noThousandsComma;

          const noThousandsDot = Number(compact.replace(/\./g, '').replace(',', '.'));
          if (Number.isFinite(noThousandsDot)) return noThousandsDot;

          const commaDecimal = Number(compact.replace(',', '.'));
          return Number.isFinite(commaDecimal) ? commaDecimal : null;
        }
        return null;
      };

      const nonGeometryFields = allFields.filter((fieldName: string) => !isGeometryFieldName(fieldName));
      const stopTokens = new Set([
        'field',
        'value',
        'values',
        'metric',
        'metrics',
        'data',
        'dataset',
        'sum',
        'count',
        'avg',
        'mean'
      ]);
      const tokenizeMetricName = (name: string) =>
        String(name || '')
          .toLowerCase()
          .split(/[^a-z0-9]+/)
          .map(token => token.trim())
          .filter(token => token.length > 1 && !stopTokens.has(token));
      const isLikelyIdentifierField = (fieldName: string) => {
        const tokenized = String(fieldName || '')
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, '_')
          .replace(/^_+|_+$/g, '');
        return (
          /(^|_)(id|code|uid|uuid|hasc|iso|fips|nuts|gid|pk|key)(_|$)/.test(tokenized) ||
          /(^|_)lv\d{0,2}(_|$)/.test(tokenized)
        );
      };
      const numericSamplingCache = new Map<
        string,
        {
          numericCount: number;
          sampleCount: number;
          coverage: number;
        }
      >();
      const getNumericSamplingStats = (fieldName: string) => {
        const cached = numericSamplingCache.get(fieldName);
        if (cached) return cached;
        const sampleTarget = Math.min(400, idx.length);
        const step = Math.max(1, Math.floor(idx.length / sampleTarget));
        let sampleCount = 0;
        let numericCount = 0;
        for (let i = 0; i < idx.length && sampleCount < sampleTarget; i += step) {
          sampleCount += 1;
          const rowIdx = idx[i];
          const parsed = parseNumericMetricValue(dataset.getValue(fieldName, rowIdx));
          if (parsed !== null) numericCount += 1;
        }
        const coverage = sampleCount > 0 ? numericCount / sampleCount : 0;
        const stats = {numericCount, sampleCount, coverage};
        numericSamplingCache.set(fieldName, stats);
        return stats;
      };
      type FallbackMetricField = {
        fieldName: string;
        score: number;
        numericCount: number;
        sampleCount: number;
        coverage: number;
      };
      const resolveFallbackMetricField = (requestedFieldName: string): FallbackMetricField | null => {
        const requestedTokens = tokenizeMetricName(requestedFieldName);
        const requestedTokenSet = new Set(requestedTokens);
        const metricHintRegex = /(sum|count|total|area|ha|km2|value|metric|score|ratio|rate|mean|avg|median|quant)/i;
        const nameHintRegex = /(^|[_-])(name|nome|comune|municip|province|provincia|region|regione)($|[_-])/i;

        let best: FallbackMetricField | null = null;

        nonGeometryFields.forEach((candidateField: string) => {
          const stats = getNumericSamplingStats(candidateField);
          if (!stats.numericCount) return;

          const candidateTokens = tokenizeMetricName(candidateField);
          const overlapCount = candidateTokens.filter(token => requestedTokenSet.has(token)).length;
          const overlapRatio = requestedTokens.length > 0 ? overlapCount / requestedTokens.length : 0;
          let score = overlapRatio * 120 + stats.coverage * 80;
          if (metricHintRegex.test(candidateField)) score += 24;
          if (nameHintRegex.test(candidateField)) score -= 30;
          if (isLikelyIdentifierField(candidateField)) score -= 35;
          score += Math.min(20, stats.numericCount);

          if (!best || score > best.score) {
            best = {
              fieldName: candidateField,
              score,
              numericCount: stats.numericCount,
              sampleCount: stats.sampleCount,
              coverage: stats.coverage
            };
          }
        });
        return best;
      };

      let resolvedMetricField = resolveDatasetFieldName(dataset, requestedMetricFieldName);
      let autoRetry:
        | {
            attempted: boolean;
            fromTool: string;
            toTool: string | null;
            success: boolean;
            reason: string;
          }
        | undefined;
      if (!resolvedMetricField) {
        const fallback = resolveFallbackMetricField(requestedMetricFieldName);
        if (!fallback) {
          const availableNumericFields = nonGeometryFields
            .filter((fieldName: string) => getNumericSamplingStats(fieldName).numericCount > 0)
            .slice(0, 12);
          return {
            llmResult: {
              success: false,
              autoRetry: {
                attempted: true,
                fromTool: requestedMetricFieldName,
                toTool: null,
                success: false,
                reason: 'metric_field_not_found'
              },
              availableNumericFields,
              details:
                `Metric field "${requestedMetricFieldName}" not found in dataset "${dataset.label || dataset.id}". ` +
                'No fallback numeric metric field available.'
            }
          };
        }
        resolvedMetricField = fallback.fieldName;
        autoRetry = {
          attempted: true,
          fromTool: requestedMetricFieldName,
          toTool: fallback.fieldName,
          success: true,
          reason: 'metric_field_not_found'
        };
      }

      const resolvedMetricFieldName = String(resolvedMetricField || '').trim();
      if (!resolvedMetricFieldName) {
        return {
          llmResult: {
            success: false,
            details: `Metric field "${requestedMetricFieldName}" could not be resolved in dataset "${dataset.label || dataset.id}".`
          }
        };
      }

      if (isGeometryFieldName(resolvedMetricFieldName)) {
        return {
          llmResult: {
            success: false,
            details: `Metric field "${resolvedMetricFieldName}" is geometric and not suitable for ranking.`
          }
        };
      }

      const nameLikeDefaults = nonGeometryFields.filter((fieldName: string) =>
        /(^|[_-])(name|nome|comune|municip|province|provincia|region|regione)($|[_-])/i.test(fieldName)
      );
      const requestedFields =
        Array.isArray(fields) && fields.length ? fields : [...nameLikeDefaults.slice(0, 3), resolvedMetricFieldName];
      const resolvedFields = requestedFields
        .map((name: string) => resolveDatasetFieldName(dataset, name))
        .filter(Boolean) as string[];
      const uniqueFields: string[] = Array.from(new Set([resolvedMetricFieldName, ...resolvedFields]));
      if (!uniqueFields.length) {
        return {
          llmResult: {
            success: false,
            details: 'No valid fields to return for ranking output.'
          }
        };
      }

      const normalizedSortDirection = sortDirection === 'asc' ? 'asc' : 'desc';
      const rawTopN = topN ?? 10;
      const parsedTopN = Number(rawTopN);
      const normalizedTopN = Number.isFinite(parsedTopN) ? Math.max(1, Math.floor(parsedTopN)) : 10;
      const sortValueCache = new Map<
        number,
        {
          raw: unknown;
          numeric: number | null;
          text: string;
          missing: boolean;
        }
      >();
      const getSortValue = (rowIdx: number) => {
        const cached = sortValueCache.get(rowIdx);
        if (cached) return cached;
        const rawValue = dataset.getValue(resolvedMetricFieldName, rowIdx);
        const isMissing =
          rawValue === null ||
          rawValue === undefined ||
          (typeof rawValue === 'string' && rawValue.trim() === '');
        const value = {
          raw: rawValue,
          numeric: parseNumericMetricValue(rawValue),
          text: String(rawValue ?? '').toLowerCase(),
          missing: isMissing
        };
        sortValueCache.set(rowIdx, value);
        return value;
      };
      const compareRowIndexes = (a: number, b: number) => {
        const left = getSortValue(a);
        const right = getSortValue(b);
        if (left.missing || right.missing) {
          if (left.missing && right.missing) return a - b;
          return left.missing ? 1 : -1;
        }
        const leftNumeric = left.numeric;
        const rightNumeric = right.numeric;
        if (leftNumeric !== null && rightNumeric !== null) {
          if (leftNumeric === rightNumeric) return a - b;
          return normalizedSortDirection === 'asc' ? leftNumeric - rightNumeric : rightNumeric - leftNumeric;
        }
        if (leftNumeric !== null || rightNumeric !== null) {
          return leftNumeric !== null ? -1 : 1;
        }
        const compared = left.text.localeCompare(right.text);
        if (compared === 0) return a - b;
        return normalizedSortDirection === 'asc' ? compared : -compared;
      };

      const topIndexes: number[] = [];
      let nonMissingMetricRows = 0;
      let numericMetricRows = 0;
      const loopYieldEvery = Math.max(100, QMAP_DEFAULT_CHUNK_SIZE);
      for (let i = 0; i < idx.length; i += 1) {
        const rowIdx = idx[i];
        const metricValue = getSortValue(rowIdx);
        if (!metricValue.missing) {
          nonMissingMetricRows += 1;
          if (metricValue.numeric !== null) numericMetricRows += 1;
        }

        let insertAt = topIndexes.length;
        for (let j = 0; j < topIndexes.length; j += 1) {
          if (compareRowIndexes(rowIdx, topIndexes[j]) < 0) {
            insertAt = j;
            break;
          }
        }
        topIndexes.splice(insertAt, 0, rowIdx);
        if (topIndexes.length > normalizedTopN) {
          topIndexes.pop();
        }

        if (i > 0 && i % loopYieldEvery === 0) {
          await yieldToMainThread();
        }
      }

      if (!nonMissingMetricRows) {
        return {
          llmResult: {
            success: false,
            details:
              `Metric field "${resolvedMetricFieldName}" has only null/empty values in dataset "${dataset.label || dataset.id}".`
          }
        };
      }
      if (!numericMetricRows) {
        return {
          llmResult: {
            success: false,
            details:
              `Metric field "${resolvedMetricFieldName}" has no numeric values. ` +
              'For analytical ranking, choose a numeric metric field.'
          }
        };
      }

      const rows = topIndexes.map((rowIdx: number, rankIdx: number) => {
        const out: Record<string, unknown> = {rank: rankIdx + 1};
        uniqueFields.forEach((fieldName: string) => {
          const rawValue = dataset.getValue(fieldName, rowIdx);
          if (isGeometryFieldName(fieldName)) {
            out[fieldName] = '[geojson omitted]';
            return;
          }
          if (rawValue === null || rawValue === undefined) {
            out[fieldName] = rawValue;
            return;
          }
          if (typeof rawValue === 'string') {
            out[fieldName] = rawValue.length > 180 ? `${rawValue.slice(0, 177)}...` : rawValue;
            return;
          }
          if (typeof rawValue === 'number' || typeof rawValue === 'boolean') {
            out[fieldName] = rawValue;
            return;
          }
          out[fieldName] = '[complex value omitted]';
        });
        return out;
      });
      const nonNumericMetricRows = Math.max(0, nonMissingMetricRows - numericMetricRows);
      const numericCoveragePct = Number(((numericMetricRows / nonMissingMetricRows) * 100).toFixed(2));
      const mixedTypeWarning =
        nonNumericMetricRows > 0
          ? ` ${nonNumericMetricRows} non-numeric metric rows were ranked after numeric values.`
          : '';
      const autoRetryWarning =
        autoRetry && autoRetry.success
          ? ` Auto-recovered metric field from "${requestedMetricFieldName}" to "${resolvedMetricFieldName}".`
          : '';
      lastRankContextRef.current = {
        datasetKeys: Array.from(
          new Set(
            [datasetName, dataset?.label, dataset?.id]
              .map(value => String(value || '').trim().toLowerCase())
              .filter(Boolean)
          )
        ),
        rows,
        metricFieldName: resolvedMetricFieldName,
        updatedAtMs: Date.now()
      };
      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          metricField: resolvedMetricFieldName,
          sortDirection: normalizedSortDirection,
          requestedTopN: normalizedTopN,
          returned: rows.length,
          totalRows: idx.length,
          nonMissingMetricRows,
          numericMetricRows,
          numericCoveragePct,
          rankingExact: true,
          samplingMode: 'ordered_top_k',
          autoRetry,
          fields: uniqueFields,
          rows,
          details:
            `Ranked top ${rows.length} rows from "${dataset.label || dataset.id}" by "${resolvedMetricFieldName}" ` +
            `(${normalizedSortDirection}). Numeric coverage ${numericMetricRows}/${nonMissingMetricRows} (${numericCoveragePct}%).` +
            mixedTypeWarning +
            autoRetryWarning
        }
      };
    }
  };

}

export function createDistinctQMapFieldValuesTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName} = ctx;

  return {
    description: 'List distinct values for a dataset field.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      fieldName: z.string().describe('Field name'),
      limit: z.number().min(1).max(500).optional().describe('Default 50')
    }),
    execute: async ({datasetName, fieldName, limit}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found.`
          }
        };
      }
      const resolvedFieldName = resolveDatasetFieldName(dataset, fieldName);
      if (!resolvedFieldName) {
        return {
          llmResult: {
            success: false,
            details: `Field "${fieldName}" not found in dataset "${datasetName}".`
          }
        };
      }

      const idx = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);
      const values = new Set<string>();
      idx.forEach((rowIdx: number) => {
        const v = dataset.getValue(resolvedFieldName, rowIdx);
        if (v === null || v === undefined || String(v).trim() === '') return;
        values.add(String(v));
      });
      const out = Array.from(values).sort((a, b) => a.localeCompare(b)).slice(0, Math.max(1, Number(limit || 50)));
      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          field: resolvedFieldName,
          count: out.length,
          values: out,
          details: `Found ${out.length} distinct values for "${resolvedFieldName}".`
        }
      };
    }
  };

}

export function createSearchQMapFieldValuesTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName} = ctx;

  return {
    description: 'Search field values by case-insensitive contains and return matching distinct values.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      fieldName: z.string().describe('Field name'),
      contains: z.string().describe('Case-insensitive substring to search'),
      limit: z.number().min(1).max(500).optional().describe('Default 100')
    }),
    execute: async ({datasetName, fieldName, contains, limit}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found.`
          }
        };
      }
      const resolvedFieldName = resolveDatasetFieldName(dataset, fieldName);
      if (!resolvedFieldName) {
        return {
          llmResult: {
            success: false,
            details: `Field "${fieldName}" not found in dataset "${datasetName}".`
          }
        };
      }
      const needle = String(contains || '').toLowerCase();
      if (!needle) {
        return {
          llmResult: {
            success: false,
            details: 'Search string "contains" is required.'
          }
        };
      }

      const idx = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);
      const values = new Set<string>();
      idx.forEach((rowIdx: number) => {
        const raw = dataset.getValue(resolvedFieldName, rowIdx);
        const value = String(raw ?? '').trim();
        if (!value) return;
        if (value.toLowerCase().includes(needle)) {
          values.add(value);
        }
      });
      const out = Array.from(values).sort((a, b) => a.localeCompare(b)).slice(0, Math.max(1, Number(limit || 100)));
      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          field: resolvedFieldName,
          contains,
          count: out.length,
          values: out,
          details: out.length
            ? `Found ${out.length} matching values in "${resolvedFieldName}".`
            : `No matching values found in "${resolvedFieldName}".`
        }
      };
    }
  };

}
