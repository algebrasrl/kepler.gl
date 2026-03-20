import {z} from 'zod';

import type {QMapToolContext} from '../context/tool-context';

export function createCountQMapRowsTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName, evaluateFilter} = ctx;
  return {
    description: 'Count rows in a dataset, optionally using a simple field filter.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      fieldName: z.string().optional().describe('Optional field name for filtering'),
      operator: z
        .enum(['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'contains', 'startsWith', 'endsWith', 'in'])
        .optional(),
      value: z.union([z.number(), z.string(), z.boolean(), z.array(z.union([z.number(), z.string()]))]).optional()
    }),
    execute: async ({datasetName, fieldName, operator, value}: any) => {
      const resolveDatasetEventually = async () => {
        const maxAttempts = 12;
        const retryDelayMs = 150;
        for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
          const snapshot = getCurrentVisState();
          const found = resolveDatasetByName(snapshot?.datasets || {}, datasetName);
          if (found?.id) {
            return found;
          }
          if (attempt < maxAttempts - 1) {
            await new Promise(resolve => setTimeout(resolve, retryDelayMs));
          }
        }
        return null;
      };

      const dataset = await resolveDatasetEventually();
      if (!dataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found.`
          }
        };
      }

      const idx = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);

      if (!fieldName) {
        return {
          llmResult: {
            success: true,
            dataset: dataset.label || dataset.id,
            count: idx.length,
            details: `Dataset "${dataset.label || dataset.id}" has ${idx.length} rows.`
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
      if (value === undefined) {
        return {
          llmResult: {
            success: false,
            details: 'Missing "value" for filtered count.'
          }
        };
      }

      const op = operator || 'eq';
      const count = idx.reduce((acc: number, rowIdx: number) => {
        const rowValue = dataset.getValue(resolvedFieldName, rowIdx);
        return evaluateFilter(op, rowValue, value) ? acc + 1 : acc;
      }, 0);

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          field: resolvedFieldName,
          operator: op,
          value,
          count,
          details: `Counted ${count} rows where ${resolvedFieldName} ${op} ${JSON.stringify(value)}.`
        }
      };
    }
  };
}

export function createDebugQMapActiveFiltersTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName} = ctx;
  return {
    description:
      'Debug helper: list active filters currently applied to datasets (optionally scoped to one dataset).',
    parameters: z.object({
      datasetName: z.string().optional().describe('Optional dataset name/id to scope filters')
    }),
    execute: async ({datasetName}: any) => {
      const vis = getCurrentVisState();
      const filters = Array.isArray(vis?.filters) ? vis.filters : [];
      const datasets = vis?.datasets || {};
      const resolved = datasetName ? resolveDatasetByName(datasets, datasetName) : null;
      const targetId = resolved?.id || (datasetName ? String(datasetName) : null);
      const scoped = targetId
        ? filters.filter((f: any) => {
            const dataId = Array.isArray(f?.dataId) ? f.dataId : [f?.dataId];
            return dataId.map((d: any) => String(d)).includes(String(targetId));
          })
        : filters;

      const mapped = scoped.map((f: any) => ({
        id: f?.id || null,
        name: Array.isArray(f?.name) ? f.name[0] : f?.name || null,
        type: f?.type || null,
        value: f?.value ?? null,
        dataId: f?.dataId ?? null,
        enabled: f?.isEnlarged !== false
      }));
      return {
        llmResult: {
          success: true,
          dataset: resolved?.label || resolved?.id || null,
          datasetId: resolved?.id || null,
          filterCount: mapped.length,
          filters: mapped,
          details: targetId
            ? `Found ${mapped.length} active filter(s) for dataset "${resolved?.label || targetId}".`
            : `Found ${mapped.length} active filter(s) in current map.`
        }
      };
    }
  };
}
