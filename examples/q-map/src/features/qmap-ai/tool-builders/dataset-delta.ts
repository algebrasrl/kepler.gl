import {useEffect} from 'react';
import {setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapVisState} from '../../../state/qmap-selectors';
import type {QMapToolContext} from '../context/tool-context';
import {useToolExecution} from './use-tool-execution';

export function createComputeQMapDatasetDeltaTool(ctx: QMapToolContext) {
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
    shouldUseLoadingIndicator
  } = ctx;

  return {
    description:
      'Compute change detection between two dataset snapshots (baseline t1 vs current t2). ' +
      'Joins on joinKeyField and adds delta_<field> (absolute), delta_pct_<field> (relative %), ' +
      'change_class (new/removed/increased/decreased/stable), and changed_fields columns.',
    parameters: z.object({
      baselineDatasetName: z.string().describe('Baseline dataset (t1)'),
      currentDatasetName: z.string().describe('Current dataset (t2)'),
      joinKeyField: z.string().describe('Key field used to match rows between the two datasets'),
      numericFields: z
        .array(z.string())
        .optional()
        .describe('Numeric fields to compute delta for. Default: all common numeric fields.'),
      deltaMode: z
        .enum(['absolute', 'relative', 'both'])
        .optional()
        .describe('Output delta columns: absolute, relative (%), or both. Default: both.'),
      changeThresholdPct: z
        .number()
        .min(0)
        .max(100)
        .optional()
        .describe('Minimum absolute % change to classify as increased/decreased (vs stable). Default 1.'),
      includeUnchangedRows: z.boolean().optional().describe('Include rows with no change. Default true.'),
      showOnMap: z.boolean().optional().describe('Auto-create layer for output dataset. Default false.'),
      newDatasetName: z.string().optional().describe('Output dataset name. Default: <current>_delta')
    }),
    execute: async ({
      baselineDatasetName,
      currentDatasetName,
      joinKeyField,
      numericFields,
      deltaMode,
      changeThresholdPct,
      includeUnchangedRows,
      showOnMap,
      newDatasetName
    }: any) => {
      const vis = getCurrentVisState();
      const baselineDs = resolveDatasetByName(vis?.datasets || {}, baselineDatasetName);
      if (!baselineDs?.id) {
        return {llmResult: {success: false, details: `Baseline dataset "${baselineDatasetName}" not found.`}};
      }
      const currentDs = resolveDatasetByName(vis?.datasets || {}, currentDatasetName);
      if (!currentDs?.id) {
        return {llmResult: {success: false, details: `Current dataset "${currentDatasetName}" not found.`}};
      }

      const resolvedJoinKey = resolveDatasetFieldName(currentDs, joinKeyField);
      if (!resolvedJoinKey) {
        return {
          llmResult: {
            success: false,
            details: `Join key field "${joinKeyField}" not found in current dataset.`
          }
        };
      }

      // Identify numeric fields to delta
      const currentNumericFields: string[] = (currentDs.fields || [])
        .filter((f: any) => {
          const t = String(f?.type || '').toLowerCase();
          return t === ALL_FIELD_TYPES?.real || t === ALL_FIELD_TYPES?.integer || t === 'real' || t === 'integer';
        })
        .map((f: any) => String(f.name));

      const baselineNumericFields: string[] = (baselineDs.fields || [])
        .filter((f: any) => {
          const t = String(f?.type || '').toLowerCase();
          return t === ALL_FIELD_TYPES?.real || t === ALL_FIELD_TYPES?.integer || t === 'real' || t === 'integer';
        })
        .map((f: any) => String(f.name));

      const commonNumericFields = currentNumericFields.filter(f => baselineNumericFields.includes(f));

      let resolvedNumericFields: string[];
      if (numericFields && numericFields.length > 0) {
        resolvedNumericFields = numericFields
          .map((f: any) => resolveDatasetFieldName(currentDs, f))
          .filter((f: any): f is string => f !== null);
      } else {
        resolvedNumericFields = commonNumericFields;
      }

      if (!resolvedNumericFields.length) {
        return {
          llmResult: {
            success: false,
            details: 'No common numeric fields found between baseline and current dataset.'
          }
        };
      }

      const outName =
        String(newDatasetName || '').trim() || `${currentDs.label || currentDs.id}_delta`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        vis?.datasets || {},
        outName,
        'qmap_delta'
      );

      const effectiveDeltaMode = deltaMode || 'both';
      const effectiveThreshold = Number.isFinite(Number(changeThresholdPct)) ? Number(changeThresholdPct) : 1;

      const deltaFieldNames: string[] = [];
      for (const f of resolvedNumericFields) {
        if (effectiveDeltaMode === 'absolute' || effectiveDeltaMode === 'both') {
          deltaFieldNames.push(`delta_${f}`);
        }
        if (effectiveDeltaMode === 'relative' || effectiveDeltaMode === 'both') {
          deltaFieldNames.push(`delta_pct_${f}`);
        }
      }

      const fieldCatalog = Array.from(
        new Set([
          ...((currentDs.fields || []).map((f: any) => String(f?.name || '')).filter(Boolean) as string[]),
          ...deltaFieldNames,
          'change_class',
          'changed_fields'
        ])
      );

      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          fieldCatalog,
          numericFields: deltaFieldNames,
          details:
            `Computing dataset delta: baseline="${baselineDs.label || baselineDs.id}" vs current="${currentDs.label || currentDs.id}" ` +
            `joined on "${resolvedJoinKey}", fields: [${resolvedNumericFields.join(', ')}], mode: ${effectiveDeltaMode}.`
        },
        additionalData: {
          executionKey: makeExecutionKey('dataset-delta'),
          baselineDatasetId: baselineDs.id,
          currentDatasetId: currentDs.id,
          joinKeyField: resolvedJoinKey,
          numericFields: resolvedNumericFields,
          deltaMode: effectiveDeltaMode,
          changeThresholdPct: effectiveThreshold,
          includeUnchangedRows: includeUnchangedRows !== false,
          showOnMap: showOnMap === true,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function ComputeQMapDatasetDeltaComponent({
      executionKey,
      baselineDatasetId,
      currentDatasetId,
      joinKeyField,
      numericFields,
      deltaMode,
      changeThresholdPct,
      includeUnchangedRows,
      showOnMap,
      newDatasetName
    }: {
      executionKey?: string;
      baselineDatasetId: string;
      currentDatasetId: string;
      joinKeyField: string;
      numericFields: string[];
      deltaMode: 'absolute' | 'relative' | 'both';
      changeThresholdPct: number;
      includeUnchangedRows: boolean;
      showOnMap: boolean;
      newDatasetName: string;
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
        const baselineDs = datasets[baselineDatasetId];
        const currentDs = datasets[currentDatasetId];
        if (!baselineDs || !currentDs) return;
        complete();

        const useLoadingIndicator = shouldUseLoadingIndicator();
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }

        (async () => {
          try {
            // Index baseline rows by joinKeyField
            const baselineIndex = new Map<string, number>();
            const baselineCount = baselineDs.length || 0;
            for (let i = 0; i < baselineCount; i += 1) {
              const keyVal = baselineDs.getValue(joinKeyField, i);
              if (keyVal !== null && keyVal !== undefined) {
                baselineIndex.set(String(keyVal).trim(), i);
              }
            }

            // Process current rows in chunks
            const currentCount = currentDs.length || 0;
            const currentIdx = Array.from({length: currentCount}, (_, i) => i);

            const outRows: Array<Record<string, unknown>> = [];

            await mapIndexesChunked(
              currentIdx,
              async (rowIdx: number) => {
                const keyVal = currentDs.getValue(joinKeyField, rowIdx);
                const keyStr = keyVal !== null && keyVal !== undefined ? String(keyVal).trim() : '';

                const row: Record<string, unknown> = {};
                (currentDs.fields || []).forEach((f: any) => {
                  row[f.name] = currentDs.getValue(f.name, rowIdx);
                });

                const baselineRowIdx = keyStr ? baselineIndex.get(keyStr) : undefined;

                let changeClass: string;
                const changedFields: string[] = [];

                if (baselineRowIdx === undefined) {
                  changeClass = 'new';
                  for (const field of numericFields) {
                    if (deltaMode === 'absolute' || deltaMode === 'both') row[`delta_${field}`] = null;
                    if (deltaMode === 'relative' || deltaMode === 'both') row[`delta_pct_${field}`] = null;
                  }
                } else {
                  let firstChange: string | null = null;
                  for (const field of numericFields) {
                    const currentVal = currentDs.getValue(field, rowIdx);
                    const baselineVal = baselineDs.getValue(field, baselineRowIdx);
                    const cNum = currentVal !== null && currentVal !== undefined && currentVal !== '' && Number.isFinite(Number(currentVal)) ? Number(currentVal) : null;
                    const bNum = baselineVal !== null && baselineVal !== undefined && baselineVal !== '' && Number.isFinite(Number(baselineVal)) ? Number(baselineVal) : null;

                    let absD: number | null = null;
                    let pctD: number | null = null;

                    if (cNum !== null && bNum !== null) {
                      absD = cNum - bNum;
                      pctD = bNum !== 0 ? ((cNum - bNum) / Math.abs(bNum)) * 100 : cNum !== 0 ? 100 : 0;
                      if (Math.abs(pctD) >= changeThresholdPct) {
                        changedFields.push(field);
                        if (!firstChange) firstChange = absD > 0 ? 'increased' : 'decreased';
                      }
                    }

                    if (deltaMode === 'absolute' || deltaMode === 'both') row[`delta_${field}`] = absD;
                    if (deltaMode === 'relative' || deltaMode === 'both') row[`delta_pct_${field}`] = pctD;
                  }
                  changeClass = firstChange || 'stable';
                }

                row.change_class = changeClass;
                row.changed_fields = changedFields.join(',');

                if (!includeUnchangedRows && changeClass === 'stable') return null;

                outRows.push(row);
                return row;
              },
              250
            );

            await yieldToMainThread();

            // Also add rows only in baseline (removed)
            const currentKeys = new Set<string>();
            for (let i = 0; i < currentCount; i += 1) {
              const kv = currentDs.getValue(joinKeyField, i);
              if (kv !== null && kv !== undefined) currentKeys.add(String(kv).trim());
            }

            if (includeUnchangedRows) {
              for (const [key, bIdx] of baselineIndex.entries()) {
                if (!currentKeys.has(key)) {
                  const row: Record<string, unknown> = {};
                  // Use baseline fields structure
                  (baselineDs.fields || []).forEach((f: any) => {
                    row[f.name] = baselineDs.getValue(f.name, bIdx);
                  });
                  // Fill current-only fields as null
                  (currentDs.fields || []).forEach((f: any) => {
                    if (!(f.name in row)) row[f.name] = null;
                  });
                  for (const field of numericFields) {
                    if (deltaMode === 'absolute' || deltaMode === 'both') row[`delta_${field}`] = null;
                    if (deltaMode === 'relative' || deltaMode === 'both') row[`delta_pct_${field}`] = null;
                  }
                  row.change_class = 'removed';
                  row.changed_fields = '';
                  outRows.push(row);
                }
              }
            }

            if (!outRows.length) return;

            upsertDerivedDatasetRows(
              localDispatch,
              localDatasets,
              newDatasetName,
              outRows,
              'qmap_delta',
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
        baselineDatasetId,
        currentDatasetId,
        joinKeyField,
        numericFields,
        deltaMode,
        changeThresholdPct,
        includeUnchangedRows,
        showOnMap,
        newDatasetName,
        shouldSkip,
        complete
      ]);

      return null;
    }
  };
}
