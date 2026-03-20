import React, {useEffect} from 'react';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapVisState} from '../../../state/qmap-selectors';
import {useToolExecution} from './use-tool-execution';

import type {QMapToolContext} from '../context/tool-context';

// ─── Tool 1: Linear Regression ───────────────────────────────────────────────

export function createRegressQMapFieldsTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    upsertDerivedDatasetRows
  } = ctx;

  return {
    description:
      '[PREFERRED for regression] OLS linear regression Y~X between two numeric fields. Returns slope, intercept, R², equation. NOT spatial — use this instead of computeQMapBivariateCorrelation when the question is about regression/prediction/R².',
    parameters: z.object({
      datasetName: z.string().describe('Dataset name from listQMapDatasets'),
      dependentField: z.string().describe('Y field (numeric)'),
      independentField: z.string().describe('X field (numeric)'),
      newDatasetName: z.string().optional().describe('Name for derived dataset'),
      showOnMap: z.boolean().optional().describe('Create derived dataset on map. Default false.')
    }),
    execute: async ({datasetName, dependentField, independentField, newDatasetName, showOnMap}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }

      const resolvedDependent = resolveDatasetFieldName(dataset, dependentField);
      if (!resolvedDependent) {
        return {
          llmResult: {
            success: false,
            details: `Dependent field "${dependentField}" not found in dataset "${datasetName}".`
          }
        };
      }

      const resolvedIndependent = resolveDatasetFieldName(dataset, independentField);
      if (!resolvedIndependent) {
        return {
          llmResult: {
            success: false,
            details: `Independent field "${independentField}" not found in dataset "${datasetName}".`
          }
        };
      }

      const idx = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);

      const pairs: Array<[number, number]> = [];
      const rowData: Array<{rowIdx: number; x: number; y: number}> = [];

      idx.forEach((rowIdx: number) => {
        const xRaw = dataset.getValue(resolvedIndependent, rowIdx);
        const yRaw = dataset.getValue(resolvedDependent, rowIdx);
        const x = Number(xRaw);
        const y = Number(yRaw);
        if (Number.isFinite(x) && Number.isFinite(y)) {
          pairs.push([x, y]);
          rowData.push({rowIdx, x, y});
        }
      });

      if (pairs.length < 2) {
        return {
          llmResult: {
            success: false,
            details: `Insufficient valid numeric pairs (${pairs.length}). Need at least 2 rows where both fields are finite numbers.`
          }
        };
      }

      const {linearRegression, linearRegressionLine, rSquared} = await import('simple-statistics');
      const reg = linearRegression(pairs);
      const line = linearRegressionLine(reg);
      const r2 = rSquared(pairs, line);

      const wantMap = showOnMap === true;
      let outLabel: string | null = null;
      let outDatasetId: string | null = null;

      if (wantMap) {
        const targetName = String(newDatasetName || '').trim() || `${dataset.label || dataset.id}_regression`;
        const info = getDatasetInfoByLabel(currentVisState?.datasets || {}, targetName, 'qmap_regression');
        outLabel = info.label;
        outDatasetId = info.datasetId;
      }

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          independentField: resolvedIndependent,
          dependentField: resolvedDependent,
          slope: Number(reg.m.toFixed(6)),
          intercept: Number(reg.b.toFixed(6)),
          rSquared: Number(r2.toFixed(6)),
          equation: `y = ${reg.m.toFixed(4)}x + ${reg.b.toFixed(4)}`,
          sampleCount: pairs.length,
          ...(wantMap ? {outputDataset: outLabel, outputDatasetId: outDatasetId} : {}),
          details:
            `Linear regression on "${resolvedDependent}" ~ "${resolvedIndependent}" (n=${pairs.length}). ` +
            `R²=${r2.toFixed(4)}, slope=${reg.m.toFixed(4)}, intercept=${reg.b.toFixed(4)}.`
        },
        ...(wantMap
          ? {
              additionalData: {
                executionKey: makeExecutionKey('regress-fields'),
                datasetId: dataset.id,
                independentField: resolvedIndependent,
                dependentField: resolvedDependent,
                slope: reg.m,
                intercept: reg.b,
                rowData,
                showOnMap: true,
                newDatasetName: outLabel,
                newDatasetId: outDatasetId
              }
            }
          : {})
      };
    },
    component: function RegressQMapFieldsComponent({
      executionKey,
      datasetId,
      independentField,
      dependentField,
      slope,
      intercept,
      rowData,
      showOnMap,
      newDatasetName
    }: {
      executionKey?: string;
      datasetId: string;
      independentField: string;
      dependentField: string;
      slope: number;
      intercept: number;
      rowData: Array<{rowIdx: number; x: number; y: number}>;
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
        if (!showOnMap || !rowData?.length || !newDatasetName) return;
        const datasets = localVisState?.datasets || {};
        const dataset = datasets[datasetId];
        if (!dataset) return;
        complete();

        const outRows: Array<Record<string, unknown>> = [];
        for (let i = 0; i < rowData.length; i += 1) {
          const {rowIdx, x, y} = rowData[i];
          const row: Record<string, unknown> = {};
          (dataset.fields || []).forEach((f: any) => {
            row[f.name] = dataset.getValue(f.name, rowIdx);
          });
          const predicted = slope * x + intercept;
          row.predicted = Number.isFinite(predicted) ? Math.round(predicted * 10000) / 10000 : null;
          row.residual = Number.isFinite(y - predicted) ? Math.round((y - predicted) * 10000) / 10000 : null;
          outRows.push(row);
        }

        if (outRows.length) {
          upsertDerivedDatasetRows(localDispatch, datasets, newDatasetName, outRows, 'qmap_regression', true);
        }
      }, [
        localDispatch,
        localVisState,
        localDatasets,
        executionKey,
        datasetId,
        independentField,
        dependentField,
        slope,
        intercept,
        rowData,
        showOnMap,
        newDatasetName,
        shouldSkip,
        complete
      ]);

      return null;
    }
  };
}

// ─── Tool 2: Natural Break Classification (Ckmeans) ─────────────────────────

export function createClassifyQMapFieldBreaksTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    upsertDerivedDatasetRows
  } = ctx;

  return {
    description:
      '[PREFERRED for classification/grouping] Classify a numeric field into optimal natural-break classes (Ckmeans/Jenks). Creates a derived dataset with class labels. NOT a styling tool — use this instead of setQMapLayerColorByStatsThresholds when the question asks to classify/group/categorize into N groups.',
    parameters: z.object({
      datasetName: z.string().describe('Dataset name from listQMapDatasets'),
      fieldName: z.string().describe('Numeric field to classify'),
      classes: z.number().min(2).max(10).optional().describe('Number of classes. Default 5.'),
      newDatasetName: z.string().optional().describe('Name for derived dataset'),
      classFieldName: z.string().optional().describe('Classification field name. Default: {fieldName}_class'),
      showOnMap: z.boolean().optional().describe('Create derived dataset on map. Default false.')
    }),
    execute: async ({datasetName, fieldName, classes, newDatasetName, classFieldName, showOnMap}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }

      const resolvedField = resolveDatasetFieldName(dataset, fieldName);
      if (!resolvedField) {
        return {
          llmResult: {
            success: false,
            details: `Field "${fieldName}" not found in dataset "${datasetName}".`
          }
        };
      }

      const nClasses = Math.max(2, Math.min(10, Number(classes || 5)));
      const classCol = String(classFieldName || '').trim() || `${resolvedField}_class`;

      const idx = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);

      const values: number[] = [];
      const rowIndexes: number[] = [];

      idx.forEach((rowIdx: number) => {
        const raw = dataset.getValue(resolvedField, rowIdx);
        const v = Number(raw);
        if (Number.isFinite(v)) {
          values.push(v);
          rowIndexes.push(rowIdx);
        }
      });

      if (values.length < nClasses) {
        return {
          llmResult: {
            success: false,
            details: `Insufficient valid numeric values (${values.length}). Need at least ${nClasses} for ${nClasses} classes.`
          }
        };
      }

      const {ckmeans} = await import('simple-statistics');
      const clusters = ckmeans(values, nClasses);

      // Build break points (max of each cluster) and class assignments
      const breakPoints: number[] = [];
      const classSummary: Array<{class: number; min: number; max: number; count: number}> = [];
      const valueToClass = new Map<number, number>();

      // Track position within original values array for class assignment
      let valueIdx = 0;
      for (let c = 0; c < clusters.length; c += 1) {
        const cluster = clusters[c];
        const clusterMin = Math.min(...cluster);
        const clusterMax = Math.max(...cluster);
        breakPoints.push(clusterMax);
        classSummary.push({
          class: c + 1,
          min: Number(clusterMin.toFixed(6)),
          max: Number(clusterMax.toFixed(6)),
          count: cluster.length
        });
        for (let j = 0; j < cluster.length; j += 1) {
          valueToClass.set(valueIdx, c + 1);
          valueIdx += 1;
        }
      }

      const wantMap = showOnMap === true;
      let outLabel: string | null = null;
      let outDatasetId: string | null = null;

      if (wantMap) {
        const targetName =
          String(newDatasetName || '').trim() || `${dataset.label || dataset.id}_classified`;
        const info = getDatasetInfoByLabel(currentVisState?.datasets || {}, targetName, 'qmap_classify');
        outLabel = info.label;
        outDatasetId = info.datasetId;
      }

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          field: resolvedField,
          classField: classCol,
          classes: nClasses,
          breakPoints: breakPoints.map(v => Number(v.toFixed(6))),
          classSummary,
          totalRows: values.length,
          ...(wantMap ? {outputDataset: outLabel, outputDatasetId: outDatasetId} : {}),
          details:
            `Classified "${resolvedField}" into ${nClasses} natural-break classes (n=${values.length}). ` +
            `Break points: [${breakPoints.map(v => v.toFixed(4)).join(', ')}].`
        },
        ...(wantMap
          ? {
              additionalData: {
                executionKey: makeExecutionKey('classify-field-breaks'),
                datasetId: dataset.id,
                resolvedField,
                classCol,
                rowIndexes,
                valueToClassEntries: Array.from(valueToClass.entries()),
                showOnMap: true,
                newDatasetName: outLabel,
                newDatasetId: outDatasetId
              }
            }
          : {})
      };
    },
    component: function ClassifyQMapFieldBreaksComponent({
      executionKey,
      datasetId,
      resolvedField,
      classCol,
      rowIndexes,
      valueToClassEntries,
      showOnMap,
      newDatasetName
    }: {
      executionKey?: string;
      datasetId: string;
      resolvedField: string;
      classCol: string;
      rowIndexes: number[];
      valueToClassEntries: Array<[number, number]>;
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
        if (!showOnMap || !rowIndexes?.length || !newDatasetName) return;
        const datasets = localVisState?.datasets || {};
        const dataset = datasets[datasetId];
        if (!dataset) return;
        complete();

        const valueToClass = new Map<number, number>(valueToClassEntries);

        const outRows: Array<Record<string, unknown>> = [];
        for (let i = 0; i < rowIndexes.length; i += 1) {
          const rowIdx = rowIndexes[i];
          const row: Record<string, unknown> = {};
          (dataset.fields || []).forEach((f: any) => {
            row[f.name] = dataset.getValue(f.name, rowIdx);
          });
          row[classCol] = valueToClass.get(i) ?? null;
          outRows.push(row);
        }

        if (outRows.length) {
          upsertDerivedDatasetRows(localDispatch, datasets, newDatasetName, outRows, 'qmap_classify', true);
        }
      }, [
        localDispatch,
        localVisState,
        localDatasets,
        executionKey,
        datasetId,
        resolvedField,
        classCol,
        rowIndexes,
        valueToClassEntries,
        showOnMap,
        newDatasetName,
        shouldSkip,
        complete
      ]);

      return null;
    }
  };
}

// ─── Tool 3: Pairwise Pearson Correlation Matrix ─────────────────────────────

export function createCorrelateQMapFieldsTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName} = ctx;

  return {
    description:
      '[PREFERRED for field correlation] Pairwise Pearson r correlation matrix between 2-10 numeric fields. Returns r values + strength (strong/moderate/weak). NOT spatial — use this instead of computeQMapBivariateCorrelation when the question is about simple field-to-field correlation without spatial weights.',
    parameters: z.object({
      datasetName: z.string().describe('Dataset name from listQMapDatasets'),
      fieldNames: z.array(z.string()).min(2).max(10).describe('Numeric fields to correlate')
    }),
    execute: async ({datasetName, fieldNames}: any) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {llmResult: {success: false, details: `Dataset "${datasetName}" not found.`}};
      }

      const resolvedFields: string[] = [];
      for (const fn of fieldNames) {
        const resolved = resolveDatasetFieldName(dataset, fn);
        if (!resolved) {
          return {
            llmResult: {
              success: false,
              details: `Field "${fn}" not found in dataset "${datasetName}".`
            }
          };
        }
        resolvedFields.push(resolved);
      }

      const idx = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);

      // Read all field values
      const fieldValues: Array<Array<number | null>> = resolvedFields.map(() => []);
      idx.forEach((rowIdx: number) => {
        for (let f = 0; f < resolvedFields.length; f += 1) {
          const raw = dataset.getValue(resolvedFields[f], rowIdx);
          const v = Number(raw);
          fieldValues[f].push(Number.isFinite(v) ? v : null);
        }
      });

      const {sampleCorrelation} = await import('simple-statistics');

      const correlations: Array<{fieldA: string; fieldB: string; r: number; strength: string}> = [];

      for (let i = 0; i < resolvedFields.length; i += 1) {
        for (let j = i + 1; j < resolvedFields.length; j += 1) {
          const xValues: number[] = [];
          const yValues: number[] = [];
          for (let k = 0; k < fieldValues[i].length; k += 1) {
            const xi = fieldValues[i][k];
            const yj = fieldValues[j][k];
            if (xi !== null && yj !== null) {
              xValues.push(xi);
              yValues.push(yj);
            }
          }

          if (xValues.length < 3) {
            correlations.push({
              fieldA: resolvedFields[i],
              fieldB: resolvedFields[j],
              r: 0,
              strength: 'insufficient_data'
            });
            continue;
          }

          let r: number;
          try {
            r = sampleCorrelation(xValues, yValues);
          } catch {
            correlations.push({
              fieldA: resolvedFields[i],
              fieldB: resolvedFields[j],
              r: 0,
              strength: 'computation_error'
            });
            continue;
          }

          const absR = Math.abs(r);
          let strength: string;
          if (absR >= 0.8) strength = 'strong';
          else if (absR >= 0.5) strength = 'moderate';
          else if (absR >= 0.3) strength = 'weak';
          else strength = 'negligible';

          correlations.push({
            fieldA: resolvedFields[i],
            fieldB: resolvedFields[j],
            r: Number(r.toFixed(6)),
            strength
          });
        }
      }

      // Sort by |r| descending
      correlations.sort((a, b) => Math.abs(b.r) - Math.abs(a.r));

      return {
        llmResult: {
          success: true,
          dataset: dataset.label || dataset.id,
          fields: resolvedFields,
          correlations,
          pairCount: correlations.length,
          details:
            `Computed ${correlations.length} pairwise Pearson correlations across ${resolvedFields.length} fields (n=${idx.length} rows).`
        }
      };
    }
  };
}
