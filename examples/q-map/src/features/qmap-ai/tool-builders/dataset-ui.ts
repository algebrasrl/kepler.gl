import {useEffect, useRef} from 'react';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapFilters, selectQMapVisState} from '../../../state/qmap-selectors';
import type {QMapToolContext} from '../context/tool-context';
import {useToolExecution} from './use-tool-execution';

export function createSetQMapFieldEqualsFilterTool(ctx: QMapToolContext) {
  const {
    assistantBaseUrl,
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    isNameLikeField,
    toComparable,
    callMcpToolParsed,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    EXECUTED_FILTER_TOOL_SIGNATURES,
    removeFilter,
    wrapTo,
    createOrUpdateFilter,
    rememberExecutedToolComponentKey,
    rememberExecutedFilterToolSignature,
    isLevelLikeField
  } = ctx;

  return {
    description:
      'Apply an equality filter to a loaded dataset field in the current map (e.g. lv = 6).',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets tool'),
      fieldName: z.string().describe('Exact field name'),
      value: z.union([z.number(), z.string(), z.boolean()])
    }),
    execute: async ({datasetName, fieldName, value}: any) => {
      const normalizedValue = value;
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);

      if (!dataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found. Call listQMapDatasets first.`
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

      let effectiveValue: string | number | boolean = normalizedValue as any;
      if (typeof normalizedValue === 'string' && isNameLikeField(resolvedFieldName)) {
        const indices = Array.isArray(dataset.allIndexes)
          ? dataset.allIndexes
          : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);
        const needle = normalizedValue.trim().toLowerCase();
        const values = new Set<string>();
        let hasExact = false;
        indices.forEach((rowIdx: number) => {
          const raw = dataset.getValue(resolvedFieldName, rowIdx);
          const candidate = String(raw ?? '').trim();
          if (!candidate) return;
          const low = candidate.toLowerCase();
          if (low === needle) {
            hasExact = true;
          }
          if (low.includes(needle)) {
            values.add(candidate);
          }
        });
        if (!hasExact) {
          const candidates = Array.from(values).sort((a, b) => a.localeCompare(b));
          if (candidates.length === 1) {
            effectiveValue = candidates[0];
          } else if (candidates.length > 1) {
            return {
              llmResult: {
                success: false,
                details: `Ambiguous name "${normalizedValue}" for field "${resolvedFieldName}" in dataset "${datasetName}". Possible values: ${candidates
                  .slice(0, 30)
                  .join(', ')}. Ask user to choose one exact value.`
              }
            };
          }
        }
      }

      // Guardrail: for equality-like filter, ensure at least one matching row exists.
      const datasetIndices = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);
      const matchCount = datasetIndices.reduce((acc: number, rowIdx: number) => {
        const rowValue = dataset.getValue(resolvedFieldName, rowIdx);
        return toComparable(rowValue) === toComparable(effectiveValue) ? acc + 1 : acc;
      }, 0);

      if (matchCount === 0) {
        let hint = '';
        if (typeof effectiveValue === 'string' && isNameLikeField(resolvedFieldName)) {
          const needle = effectiveValue.trim().toLowerCase();
          const candidates = new Set<string>();
          datasetIndices.forEach((rowIdx: number) => {
            const v = String(dataset.getValue(resolvedFieldName, rowIdx) ?? '').trim();
            if (!v) return;
            if (v.toLowerCase().includes(needle) || needle.includes(v.toLowerCase())) {
              candidates.add(v);
            }
          });
          const top = Array.from(candidates).sort((a, b) => a.localeCompare(b)).slice(0, 20);
          if (top.length) {
            hint = ` Possible values: ${top.join(', ')}.`;
          }
        }
        return {
          llmResult: {
            success: false,
            details: `No rows matched ${resolvedFieldName} = ${String(effectiveValue)} in dataset "${datasetName}".${hint}`
          }
        };
      }

      const actionPayload = await callMcpToolParsed(
        assistantBaseUrl,
        ['build_equals_filter_action'],
        {
          datasetName: dataset.label || dataset.id,
          fieldName: resolvedFieldName,
          value: effectiveValue
        }
      );

      const args = actionPayload?.action?.args || {};
      const actionDatasetName = String(args.datasetName || dataset.label || dataset.id);
      const actionFieldName = String(resolvedFieldName);

      return {
        llmResult: {
          success: true,
          details: `Applying filter ${actionFieldName} = ${String(effectiveValue)} on dataset "${actionDatasetName}".`
        },
        additionalData: {
          executionKey: makeExecutionKey('set-filter'),
          datasetId: dataset.id,
          fieldName: actionFieldName,
          value: effectiveValue
        }
      };
    },
    component: function SetQMapFieldEqualsFilterComponent({
      executionKey,
      datasetId,
      fieldName,
      value
    }: {
      executionKey?: string;
      datasetId: string;
      fieldName: string;
      value: string | number | boolean;
    }) {
      const localDispatch = useDispatch<any>();
      const localDatasets = useSelector(selectQMapDatasets);
      const localFilters = useSelector(selectQMapFilters);
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey});
      const appliedRef = useRef<string | null>(null);
      useEffect(() => {
        if (shouldSkip()) return;
        const rawSignature = `${datasetId}::${fieldName}::${JSON.stringify(value)}`;
        if (EXECUTED_FILTER_TOOL_SIGNATURES.has(rawSignature)) return;
        const dataset = localDatasets?.[datasetId];
        if (!dataset) return;
        const resolvedFieldName = resolveDatasetFieldName(dataset, fieldName);
        const hasDataset = (filter: any) =>
          Array.isArray(filter?.dataId) ? filter.dataId.includes(datasetId) : String(filter?.dataId || '') === datasetId;
        const validFieldNames = new Set(
          (Array.isArray(dataset?.fields) ? dataset.fields : [])
            .map((f: any) => String(f?.name || ''))
            .filter(Boolean)
        );
        const legacyFilterId = `qmap_${datasetId}_active`;
        // Remove invalid filters targeting this dataset (field no longer exists).
        const invalidIndices = (localFilters || [])
          .map((f: any, idx: number) => ({f, idx}))
          .filter((entry: {f: any; idx: number}) => hasDataset(entry.f) && !validFieldNames.has(String(entry.f?.name || '')))
          .map((entry: {f: any; idx: number}) => entry.idx)
          .sort((a: number, b: number) => b - a);
        invalidIndices.forEach((idx: number) => localDispatch(wrapTo('map', removeFilter(idx))));

        if (!resolvedFieldName) {
          const staleIndices = (localFilters || [])
            .map((f: any, idx: number) => ({id: String(f?.id || ''), idx}))
            .filter((entry: {id: string; idx: number}) => entry.id === legacyFilterId)
            .map((entry: {id: string; idx: number}) => entry.idx)
            .sort((a: number, b: number) => b - a);
          staleIndices.forEach((idx: number) => localDispatch(wrapTo('map', removeFilter(idx))));
          rememberExecutedFilterToolSignature(rawSignature);
          return;
        }
        const signature = `${datasetId}::${resolvedFieldName}::${JSON.stringify(value)}`;
        if (appliedRef.current === signature) return;
        appliedRef.current = signature;
        const filterId = `qmap_${datasetId}_${resolvedFieldName}`;
        const currentActiveFilter = (localFilters || []).find(
          (f: any) => String(f?.id || '') === filterId
        );
        if (
          currentActiveFilter &&
          isNameLikeField(String(currentActiveFilter?.name || '')) &&
          isLevelLikeField(resolvedFieldName)
        ) {
          // Keep the named-entity filter (e.g. Veneto) and ignore weaker level-only override.
          return;
        }
        const staleLegacyIndices = (localFilters || [])
          .map((f: any, idx: number) => ({f, id: String(f?.id || ''), idx}))
          .filter(
            (entry: {f: any; id: string; idx: number}) =>
              hasDataset(entry.f) && entry.id === legacyFilterId
          )
          .map((entry: {f: any; id: string; idx: number}) => entry.idx)
          .sort((a: number, b: number) => b - a);
        staleLegacyIndices.forEach((idx: number) => localDispatch(wrapTo('map', removeFilter(idx))));
        const normalizedValue =
          typeof value === 'number' ? [value, value] : Array.isArray(value) ? value : [value];
        localDispatch(
          wrapTo('map', createOrUpdateFilter(filterId, datasetId, resolvedFieldName, normalizedValue))
        );
        complete();
        rememberExecutedFilterToolSignature(rawSignature);
      }, [localDispatch, localDatasets, localFilters, executionKey, datasetId, fieldName, value, shouldSkip, complete]);
      return null;
    }
  };

}

export function createSetQMapTooltipFieldsTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    interactionConfigChange,
    wrapTo
  } = ctx;

  return {
    description:
      'Configure visible tooltip fields for a dataset (replace or append), with optional tooltip auto-enable.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      fieldNames: z.array(z.string()).min(1).describe('Fields to show in tooltip, in display order'),
      mode: z
        .enum(['replace', 'append'])
        .optional()
        .describe('replace = overwrite current tooltip fields for dataset; append = add missing fields'),
      enableTooltip: z.boolean().optional().describe('Default true')
    }),
    execute: async ({datasetName, fieldNames, mode, enableTooltip}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, String(datasetName || ''));

      if (!dataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found. Call listQMapDatasets first.`
          }
        };
      }

      const requested = (fieldNames || []).map((v: any) => String(v || '').trim()).filter(Boolean);
      const resolved: string[] = [];
      const invalid: string[] = [];
      requested.forEach((name: string) => {
        const found = resolveDatasetFieldName(dataset, name);
        if (found) {
          if (!resolved.includes(found)) {
            resolved.push(found);
          }
        } else {
          invalid.push(name);
        }
      });
      if (!resolved.length) {
        return {
          llmResult: {
            success: false,
            details:
              `None of the requested fields exist in dataset "${datasetName}". ` +
              `Missing: ${invalid.join(', ')}.`
          }
        };
      }

      const tooltipCfg = currentVisState?.interactionConfig?.tooltip;
      const currentForDataset = Array.isArray(tooltipCfg?.config?.fieldsToShow?.[dataset.id])
        ? tooltipCfg.config.fieldsToShow[dataset.id]
        : [];
      const currentNames = currentForDataset
        .map((f: any) => String(f?.name || ''))
        .filter(Boolean);
      const nextNames =
        String(mode || 'replace') === 'append'
          ? Array.from(new Set([...currentNames, ...resolved]))
          : resolved;

      return {
        llmResult: {
          success: true,
          datasetName: dataset.label || dataset.id,
          datasetId: dataset.id,
          fieldsApplied: nextNames,
          fieldsIgnored: invalid,
          details:
            `Updating tooltip fields for "${dataset.label || dataset.id}" (${String(
              mode || 'replace'
            )}): [${nextNames.join(', ')}].` +
            (invalid.length ? ` Ignored unknown fields: ${invalid.join(', ')}.` : '')
        },
        additionalData: {
          executionKey: makeExecutionKey('tooltip-fields'),
          datasetId: dataset.id,
          fieldNames: nextNames,
          enableTooltip: enableTooltip !== false
        }
      };
    },
    component: function SetQMapTooltipFieldsComponent({
      executionKey,
      datasetId,
      fieldNames,
      enableTooltip
    }: {
      executionKey?: string;
      datasetId: string;
      fieldNames: string[];
      enableTooltip: boolean;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const {shouldSkip, abort, complete} = useToolExecution({executionKey, executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS, rememberExecutedToolComponentKey});
      const cancelledRef = useRef(false);
      useEffect(() => {
        return () => {
          cancelledRef.current = true;
        };
      }, []);
      useEffect(() => {
        if (shouldSkip()) return;
        const dataset = localVisState?.datasets?.[datasetId];
        const tooltip = localVisState?.interactionConfig?.tooltip;
        if (!dataset || !tooltip?.id || !tooltip?.config) return;

        const existingMap = tooltip.config.fieldsToShow || {};
        const currentFields = Array.isArray(existingMap?.[datasetId]) ? existingMap[datasetId] : [];
        const currentFormats = new Map<string, any>();
        currentFields.forEach((f: any) => {
          const n = String(f?.name || '');
          if (n) currentFormats.set(n, f?.format ?? null);
        });

        const normalized = (fieldNames || [])
          .map(name => resolveDatasetFieldName(dataset, name) || '')
          .filter(Boolean);
        if (!normalized.length) {
          abort();
          return;
        }
        const nextFields = normalized.map(name => ({name, format: currentFormats.get(name) ?? null}));

        const nextTooltip = {
          ...tooltip,
          enabled: enableTooltip ? true : Boolean(tooltip.enabled),
          config: {
            ...tooltip.config,
            fieldsToShow: {
              ...existingMap,
              [datasetId]: nextFields
            }
          }
        };

        complete();
        localDispatch(wrapTo('map', interactionConfigChange(nextTooltip as any)));
      }, [localDispatch, localVisState, executionKey, datasetId, fieldNames, enableTooltip, shouldSkip, abort, complete]);
      return null;
    }
  };

}
