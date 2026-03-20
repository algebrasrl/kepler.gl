import {useEffect} from 'react';
import {setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapVisState} from '../../../state/qmap-selectors';
import type {QMapToolContext} from '../context/tool-context';
import {useToolExecution} from './use-tool-execution';

export function createAddComputedFieldTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    yieldToMainThread,
    mapIndexesChunked,
    upsertDerivedDatasetRows,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    shouldUseLoadingIndicator,
    getFilteredDatasetIndexes
  } = ctx;

  const BANNED_KEYWORDS = [
    'window', 'document', 'globalThis', 'eval', 'Function',
    'require', 'import', 'fetch', '__proto__', 'prototype'
  ];

  return {
    description:
      'Add a computed field to a dataset by evaluating a JavaScript-like expression per row. ' +
      'The expression has access to all dataset fields as variables plus Math.*. ' +
      'Useful for ratio fields (e.g. "population / area * 1000"), per-capita indices ' +
      '("value / population * 1000"), log transforms ("Math.log(x + 1)"), and manual normalizations.',
    parameters: z.object({
      datasetName: z.string().describe('Source dataset name'),
      expression: z
        .string()
        .describe(
          'JS expression evaluated per row (field names as variables + Math.*). E.g. "population / area * 1000"'
        ),
      outputFieldName: z.string().describe('Name for the new computed field'),
      useActiveFilters: z
        .boolean()
        .optional()
        .describe('Apply active filters when selecting rows. Default false (all rows).'),
      newDatasetName: z.string().optional().describe('Output dataset name. Default: <dataset>_computed'),
      showOnMap: z.boolean().optional().describe('Auto-create layer for output dataset. Default true.')
    }),
    execute: async ({datasetName, expression, outputFieldName, useActiveFilters, newDatasetName, showOnMap}: any) => {
      // Validate expression for banned keywords
      if (BANNED_KEYWORDS.some(k => expression.includes(k))) {
        return {
          llmResult: {
            success: false,
            details: `Expression contains a forbidden keyword. Banned: ${BANNED_KEYWORDS.join(', ')}.`
          }
        };
      }
      // Test-compile the expression
      try {
        new Function('Math', '"use strict"; return (' + expression + ');');
      } catch (e: any) {
        return {
          llmResult: {
            success: false,
            details: `Expression syntax error: ${e?.message || String(e)}.`
          }
        };
      }

      const vis = getCurrentVisState();
      const dataset = resolveDatasetByName(vis?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }

      const fieldNames: string[] = (dataset.fields || [])
        .map((f: any) => String(f?.name || ''))
        .filter(Boolean);

      const outName = String(newDatasetName || '').trim() || `${dataset.label || dataset.id}_computed`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        vis?.datasets || {},
        outName,
        'qmap_computed'
      );

      const fieldCatalog = Array.from(new Set([...fieldNames, outputFieldName]));

      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          outputFieldName,
          fieldCatalog,
          details: `Computing field "${outputFieldName}" = "${expression}" on dataset "${dataset.label || dataset.id}".`
        },
        additionalData: {
          executionKey: makeExecutionKey('computed-field'),
          datasetId: dataset.id,
          useActiveFilters: useActiveFilters === true,
          expression,
          outputFieldName,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId,
          showOnMap: showOnMap !== false
        }
      };
    },
    component: function AddComputedFieldComponent({
      executionKey,
      datasetId,
      useActiveFilters,
      expression,
      outputFieldName,
      newDatasetName,
      showOnMap
    }: {
      executionKey?: string;
      datasetId: string;
      useActiveFilters: boolean;
      expression: string;
      outputFieldName: string;
      newDatasetName: string;
      showOnMap: boolean;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const localDatasets = useSelector(selectQMapDatasets) as Record<string, any>;
      const {shouldSkip, complete} = useToolExecution({
        executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });

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
            const fieldNames: string[] = (dataset.fields || [])
              .map((f: any) => String(f?.name || ''))
              .filter(Boolean);

            const rowIndices = getFilteredDatasetIndexes(dataset, localVisState, useActiveFilters);
            if (!rowIndices.length) return;

            // Build the per-row eval function
            let evalFn: (...args: any[]) => unknown;
            try {
              evalFn = new Function(...fieldNames, 'Math', '"use strict"; return (' + expression + ');') as any;
            } catch {
              return;
            }

            const outRows: Array<Record<string, unknown>> = [];

            await mapIndexesChunked(
              rowIndices,
              (rowIdx: number) => {
                const row: Record<string, unknown> = {};
                const fieldValues: number[] = [];
                (dataset.fields || []).forEach((f: any) => {
                  const val = dataset.getValue(f.name, rowIdx);
                  row[f.name] = val;
                  fieldValues.push(val !== null && val !== undefined && val !== '' && Number.isFinite(Number(val)) ? Number(val) : NaN);
                });
                let computed: unknown = null;
                try {
                  const result = evalFn(...fieldValues, Math);
                  computed = result !== null && result !== undefined && Number.isFinite(Number(result)) ? Number(result) : null;
                } catch {
                  computed = null;
                }
                row[outputFieldName] = computed;
                outRows.push(row);
                return row;
              },
              250
            );

            await yieldToMainThread();

            if (!outRows.length) return;

            upsertDerivedDatasetRows(
              localDispatch,
              localDatasets,
              newDatasetName,
              outRows,
              'qmap_computed',
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
        localDatasets,
        executionKey,
        datasetId,
        useActiveFilters,
        expression,
        outputFieldName,
        newDatasetName,
        showOnMap,
        shouldSkip,
        complete
      ]);

      return null;
    }
  };
}
