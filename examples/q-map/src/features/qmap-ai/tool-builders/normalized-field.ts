import {useEffect} from 'react';
import {addDataToMap, replaceDataInMap, wrapTo} from '@kepler.gl/actions';
import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets} from '../../../state/qmap-selectors';
import type {QMapToolContext} from '../context/tool-context';
import {useToolExecution} from './use-tool-execution';

function upsertDatasetInMap({
  localDispatch,
  datasets,
  newDatasetName,
  newDatasetId,
  datasetFields,
  rows,
  showOnMap
}: {
  localDispatch: any;
  datasets: any;
  newDatasetName: string;
  newDatasetId: string;
  datasetFields: any[];
  rows: any[][];
  showOnMap: boolean;
}) {
  const existing = Object.values(datasets || {}).find(
    (d: any) => String(d?.label || '').toLowerCase() === String(newDatasetName).toLowerCase()
  ) as any;
  const datasetToUse = {
    info: {
      id: existing?.id || newDatasetId,
      label: newDatasetName
    },
    data: {
      fields: datasetFields,
      rows
    }
  };
  if (existing?.id) {
    localDispatch(
      wrapTo(
        'map',
        replaceDataInMap({
          datasetToReplaceId: existing.id,
          datasetToUse,
          options: {
            keepExistingConfig: true,
            centerMap: false,
            autoCreateLayers: false
          }
        }) as any
      )
    );
  } else {
    localDispatch(
      wrapTo(
        'map',
        addDataToMap({
          datasets: datasetToUse as any,
          options: {autoCreateLayers: showOnMap, centerMap: false}
        }) as any
      )
    );
  }
}

export function createDatasetWithNormalizedFieldTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    buildNormalizedDenominatorPlan,
    getFilteredDatasetIndexes,
    parseCoordinateValue,
    computeNormalizedDenominatorValue,
    getDatasetInfoByLabel,
    describeNormalizedDenominatorPlan,
    makeExecutionKey,
    executedToolComponentKeys,
    rememberExecutedToolComponentKey
  } = ctx;
  return {
    description:
      'Create a derived dataset with a normalized numeric metric computed as (numerator / denominator) * multiplier. Useful for per-capita/per-100k choropleths.',
    parameters: z.object({
      datasetName: z.string().describe('Exact source dataset name'),
      numeratorFieldName: z.string().describe('Numeric numerator field, e.g. area_pressione_ha'),
      denominatorFieldName: z.string().describe('Numeric denominator field, e.g. population'),
      outputFieldName: z
        .string()
        .optional()
        .describe('Output metric field name (default: <numerator>_per_100k)'),
      multiplier: z.number().positive().optional().describe('Multiplier applied after ratio (default 100000)'),
      precision: z
        .number()
        .int()
        .min(0)
        .max(12)
        .optional()
        .describe('Decimal precision for output values (default 6)'),
      useActiveFilters: z
        .boolean()
        .optional()
        .describe('If true, include only rows matching active filters for this dataset (default true)'),
      newDatasetName: z.string().optional().describe('Target dataset name'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default false. Set true to auto-create a map layer for the output dataset.')
    }),
    execute: async ({
      datasetName,
      numeratorFieldName,
      denominatorFieldName,
      outputFieldName,
      multiplier,
      precision,
      useActiveFilters,
      newDatasetName,
      showOnMap
    }: any) => {
      const currentVisState = getCurrentVisState();
      const sourceDataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!sourceDataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found. Call listQMapDatasets first.`
          }
        };
      }

      const resolvedNumeratorField = resolveDatasetFieldName(sourceDataset, String(numeratorFieldName || ''));
      if (!resolvedNumeratorField) {
        return {
          llmResult: {
            success: false,
            details: `Numerator field "${numeratorFieldName}" not found in dataset "${sourceDataset.label || sourceDataset.id}".`
          }
        };
      }
      const denominatorResolution = buildNormalizedDenominatorPlan(sourceDataset, String(denominatorFieldName || ''));
      if ('error' in denominatorResolution) {
        return {
          llmResult: {
            success: false,
            details: denominatorResolution.error
          }
        };
      }
      const denominatorPlan = denominatorResolution.plan;

      const normalizedBaseField =
        String(resolvedNumeratorField)
          .trim()
          .replace(/[^a-zA-Z0-9]+/g, '_')
          .replace(/^_+|_+$/g, '')
          .toLowerCase() || 'metric';
      const resolvedOutputFieldName = String(outputFieldName || '').trim() || `${normalizedBaseField}_per_100k`;
      const outputExists = (sourceDataset.fields || []).some(
        (f: any) => String(f?.name || '').toLowerCase() === resolvedOutputFieldName.toLowerCase()
      );
      if (outputExists) {
        return {
          llmResult: {
            success: false,
            details:
              `Field "${resolvedOutputFieldName}" already exists in dataset "${sourceDataset.label || sourceDataset.id}". ` +
              'Choose a different outputFieldName or style the existing field directly.'
          }
        };
      }

      const applyFilters = useActiveFilters !== false;
      const matchedIndices = getFilteredDatasetIndexes(sourceDataset, currentVisState, applyFilters);
      if (!matchedIndices.length) {
        return {
          llmResult: {
            success: false,
            details:
              `No rows available to derive normalized metric for dataset "${sourceDataset.label || sourceDataset.id}". ` +
              (applyFilters ? 'Active filters may be too restrictive.' : '')
          }
        };
      }

      const multiplierValue = Number.isFinite(Number(multiplier)) && Number(multiplier) > 0 ? Number(multiplier) : 100000;
      const precisionValue =
        Number.isFinite(Number(precision)) && Number(precision) >= 0 ? Math.min(12, Math.max(0, Math.floor(Number(precision)))) : 6;
      let validRows = 0;
      let invalidNumeratorRows = 0;
      let invalidDenominatorRows = 0;
      matchedIndices.forEach((rowIdx: number) => {
        const rawNumerator = sourceDataset.getValue(resolvedNumeratorField, rowIdx);
        const numerator = parseCoordinateValue(rawNumerator);
        const denominator = computeNormalizedDenominatorValue(sourceDataset, rowIdx, denominatorPlan);
        if (numerator === null || !Number.isFinite(numerator)) {
          invalidNumeratorRows += 1;
          return;
        }
        if (denominator === null || !Number.isFinite(denominator) || denominator <= 0) {
          invalidDenominatorRows += 1;
          return;
        }
        validRows += 1;
      });
      if (validRows <= 0) {
        return {
          llmResult: {
            success: false,
            details:
              `Cannot compute normalized metric on dataset "${sourceDataset.label || sourceDataset.id}" ` +
              `because all candidate rows have invalid numerator/denominator values (denominator must be > 0).`
          }
        };
      }

      const targetName = String(newDatasetName || '').trim() || `${sourceDataset.label || sourceDataset.id}_normalized`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        currentVisState?.datasets || {},
        targetName,
        'qmap_norm'
      );
      const denominatorExpression = describeNormalizedDenominatorPlan(denominatorPlan);
      const fieldCatalog = Array.from(
        new Set([
          ...((sourceDataset.fields || []).map((field: any) => String(field?.name || '').trim()).filter(Boolean) as string[]),
          resolvedOutputFieldName
        ])
      );
      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          outputFieldName: resolvedOutputFieldName,
          fieldCatalog,
          numericFields: [resolvedOutputFieldName],
          styleableFields: [resolvedOutputFieldName],
          defaultStyleField: resolvedOutputFieldName,
          details:
            `Creating dataset "${resolvedTargetLabel}" with normalized field "${resolvedOutputFieldName}" = ` +
            `(${resolvedNumeratorField}/${denominatorExpression})*${multiplierValue}. ` +
            `Rows: total=${matchedIndices.length}, valid=${validRows}, invalidNumerator=${invalidNumeratorRows}, invalidDenominator=${invalidDenominatorRows}.` +
            ` ${denominatorResolution.detailHint}` +
            `${showOnMap === true ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('create-dataset-with-normalized-field'),
          sourceDatasetId: sourceDataset.id,
          rowIndices: matchedIndices,
          numeratorFieldName: resolvedNumeratorField,
          denominatorPlan,
          outputFieldName: resolvedOutputFieldName,
          fieldCatalog,
          numericFields: [resolvedOutputFieldName],
          styleableFields: [resolvedOutputFieldName],
          defaultStyleField: resolvedOutputFieldName,
          multiplierValue,
          precisionValue,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId,
          showOnMap: showOnMap === true
        }
      };
    },
    component: function CreateDatasetWithNormalizedFieldComponent({
      executionKey,
      sourceDatasetId,
      rowIndices,
      numeratorFieldName,
      denominatorPlan,
      outputFieldName,
      multiplierValue,
      precisionValue,
      newDatasetName,
      newDatasetId,
      showOnMap
    }: {
      executionKey?: string;
      sourceDatasetId: string;
      rowIndices: number[];
      numeratorFieldName: string;
      denominatorPlan: any;
      outputFieldName: string;
      multiplierValue: number;
      precisionValue: number;
      newDatasetName: string;
      newDatasetId: string;
      showOnMap: boolean;
    }) {
      const localDispatch = useDispatch<any>();
      const datasets = useSelector(selectQMapDatasets);
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys, rememberExecutedToolComponentKey});
      useEffect(() => {
        if (shouldSkip()) return;
        const sourceDataset = datasets?.[sourceDatasetId];
        if (!sourceDataset) return;

        const sourceFields = (sourceDataset.fields || []).map((f: any) => ({name: f.name, type: f.type}));
        const datasetFields = [...sourceFields, {name: outputFieldName, type: ALL_FIELD_TYPES.real}];
        const scale = Number.isFinite(Number(multiplierValue)) ? Number(multiplierValue) : 100000;
        const decimals =
          Number.isFinite(Number(precisionValue)) && Number(precisionValue) >= 0
            ? Math.min(12, Math.max(0, Math.floor(Number(precisionValue))))
            : 6;
        const decimalFactor = Math.pow(10, decimals);
        const rows = (rowIndices || []).map((rowIdx: number) => {
          const baseValues = sourceFields.map((f: any) => sourceDataset.getValue(f.name, rowIdx));
          const numerator = parseCoordinateValue(sourceDataset.getValue(numeratorFieldName, rowIdx));
          const denominator = computeNormalizedDenominatorValue(sourceDataset, rowIdx, denominatorPlan);
          let normalizedValue: number | null = null;
          if (
            numerator !== null &&
            Number.isFinite(numerator) &&
            denominator !== null &&
            Number.isFinite(denominator) &&
            denominator > 0
          ) {
            const rawValue = (numerator / denominator) * scale;
            if (Number.isFinite(rawValue)) {
              normalizedValue =
                Number.isFinite(decimalFactor) && decimalFactor > 0
                  ? Math.round(rawValue * decimalFactor) / decimalFactor
                  : rawValue;
            }
          }
          return [...baseValues, normalizedValue];
        });

        if (!rows.length) return;
        complete();
        upsertDatasetInMap({
          localDispatch,
          datasets,
          newDatasetName,
          newDatasetId,
          datasetFields,
          rows,
          showOnMap
        });
      }, [
        localDispatch,
        datasets,
        executionKey,
        sourceDatasetId,
        rowIndices,
        numeratorFieldName,
        denominatorPlan,
        outputFieldName,
        multiplierValue,
        precisionValue,
        newDatasetName,
        newDatasetId,
        showOnMap,
        shouldSkip,
        complete
      ]);
      return null;
    }
  };
}
