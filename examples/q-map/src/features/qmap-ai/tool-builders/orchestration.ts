import {useEffect} from 'react';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapLayers, selectQMapVisState} from '../../../state/qmap-selectors';
import {preprocessDualDatasetArgs} from '../tool-args-normalization';
import {useToolExecution} from './use-tool-execution';

import type {QMapToolContext} from '../context/tool-context';

export function createJoinQMapDatasetsOnH3Tool(ctx: QMapToolContext) {
  const {
    QMAP_H3_JOIN_METRIC_SCHEMA,
    QMAP_JOIN_TYPE_SCHEMA,
    QMAP_DEFAULT_CHUNK_SIZE,
    QMAP_AUTO_HIDE_SOURCE_LAYERS,
    getCurrentVisState,
    resolveDatasetByName,
    resolveH3FieldName,
    inferDatasetH3Resolution,
    resolveDatasetFieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    isPopulationLikeField,
    isCategoricalJoinField,
    normalizeH3Key,
    yieldToMainThread,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    hideLayersForDatasetIds,
    addDataToMap,
    replaceDataInMap,
    wrapTo,
    ALL_FIELD_TYPES
  } = ctx;

  return {
    description:
      'Join two loaded datasets on H3 id and create a derived dataset for mapping/styling.',
    parameters: z.preprocess(preprocessDualDatasetArgs, z.object({
      leftDatasetName: z.string().describe('Primary dataset to keep geometry/coverage from'),
      rightDatasetName: z.string().describe('Dataset providing fields to append'),
      leftH3Field: z.string().optional().describe('Optional explicit H3 field in left dataset'),
      rightH3Field: z.string().optional().describe('Optional explicit H3 field in right dataset'),
      includeRightFields: z.array(z.string()).optional().describe('Fields to import from right dataset'),
      metric: QMAP_H3_JOIN_METRIC_SCHEMA.describe('Aggregation metric for numeric collisions'),
      joinType: QMAP_JOIN_TYPE_SCHEMA.describe('Default inner'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default true. Set false for intermediate technical datasets kept off-map.'),
      minCoveragePct: z
        .number()
        .min(0)
        .max(100)
        .optional()
        .describe('Fail join when matched left coverage is below this threshold (default 5).'),
      newDatasetName: z.string().optional().describe('Default <left>_joined_<right>')
    })),
    execute: async ({
      leftDatasetName,
      rightDatasetName,
      leftH3Field,
      rightH3Field,
      includeRightFields,
      metric,
      joinType,
      showOnMap,
      minCoveragePct,
      newDatasetName
    }: any) => {
      const currentVisState = getCurrentVisState();
      const leftDataset = resolveDatasetByName(currentVisState?.datasets || {}, leftDatasetName);
      const rightDataset = resolveDatasetByName(currentVisState?.datasets || {}, rightDatasetName);
      if (!leftDataset?.id) {
        return {llmResult: {success: false, details: `Left dataset "${leftDatasetName}" not found.`}};
      }
      if (!rightDataset?.id) {
        return {llmResult: {success: false, details: `Right dataset "${rightDatasetName}" not found.`}};
      }

      const resolvedLeftH3 = resolveH3FieldName(leftDataset, leftH3Field);
      const resolvedRightH3 = resolveH3FieldName(rightDataset, rightH3Field);
      if (!resolvedLeftH3 || !resolvedRightH3) {
        return {
          llmResult: {
            success: false,
            details: 'Both datasets must expose an H3 field (h3_id/h3__id).'
          }
        };
      }
      const leftRes = inferDatasetH3Resolution(leftDataset, resolvedLeftH3);
      const rightRes = inferDatasetH3Resolution(rightDataset, resolvedRightH3);
      if (leftRes !== null && rightRes !== null && leftRes !== rightRes) {
        return {
          llmResult: {
            success: false,
            details:
              `H3 resolution mismatch: left=${leftRes}, right=${rightRes}. ` +
              'Align resolutions first (e.g. aggregateDatasetToH3 to target resolution), then retry join.'
          }
        };
      }

      const rightFieldsAll = (rightDataset.fields || []).map((f: any) => String(f?.name || '')).filter(Boolean);
      const requestedRight = (includeRightFields || [])
        .map((fieldName: string) => resolveDatasetFieldName(rightDataset, fieldName))
        .filter(Boolean) as string[];
      const rightFields = (requestedRight.length ? requestedRight : rightFieldsAll).filter(
        (fieldName: string) => fieldName !== resolvedRightH3
      );
      if (!rightFields.length) {
        return {
          llmResult: {
            success: false,
            details: 'No right-side fields selected for join.'
          }
        };
      }

      const targetName =
        String(newDatasetName || '').trim() ||
        `${leftDataset.label || leftDataset.id}_joined_${rightDataset.label || rightDataset.id}`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        currentVisState?.datasets || {},
        targetName,
        'qmap_h3_join'
      );
      const leftFields = (leftDataset.fields || []).map((f: any) => ({
        name: String(f?.name || ''),
        type: String(f?.type || ALL_FIELD_TYPES.string)
      }));
      const rightFieldMeta = (rightDataset.fields || []).reduce((acc: Record<string, string>, f: any) => {
        const name = String(f?.name || '');
        if (name) acc[name] = String(f?.type || ALL_FIELD_TYPES.string);
        return acc;
      }, {});
      const usedOutputNames = new Set(leftFields.map((f: any) => String(f.name || '').toLowerCase()).filter(Boolean));
      const appended = rightFields.map((fieldName: string) => {
        const baseName = String(fieldName || '').trim();
        let outputName = baseName;
        let suffix = 2;
        while (usedOutputNames.has(outputName.toLowerCase())) {
          outputName = `${baseName}_${suffix}`;
          suffix += 1;
        }
        usedOutputNames.add(outputName.toLowerCase());
        const sourceType = rightFieldMeta[fieldName] || ALL_FIELD_TYPES.string;
        const aggregationMode: 'numeric' | 'categorical' = isCategoricalJoinField(fieldName, sourceType)
          ? 'categorical'
          : 'numeric';
        return {
          sourceName: fieldName,
          outputName,
          type: aggregationMode === 'categorical' ? ALL_FIELD_TYPES.string : sourceType,
          aggregationMode
        };
      });
      const numericOutputFields = appended
        .filter((fieldDef: {aggregationMode: string}) => fieldDef.aggregationMode === 'numeric')
        .map((fieldDef: {outputName: string}) => fieldDef.outputName);
      const fieldCatalog = [
        ...leftFields.map((field: {name: string}) => field.name),
        ...appended.map((field: {outputName: string}) => field.outputName)
      ];
      const defaultStyleField = numericOutputFields[0] || '';
      const fieldAliases = appended.reduce((acc: Record<string, string>, fieldDef: {
        sourceName: string;
        outputName: string;
        aggregationMode: string;
      }) => {
        if (fieldDef.aggregationMode === 'numeric') {
          acc[String(fieldDef.sourceName || '').trim()] = String(fieldDef.outputName || '').trim();
        }
        return acc;
      }, {});

      const leftIdx = Array.isArray(leftDataset.allIndexes)
        ? leftDataset.allIndexes
        : Array.from({length: Number(leftDataset.length || 0)}, (_, i) => i);
      const rightIdx = Array.isArray(rightDataset.allIndexes)
        ? rightDataset.allIndexes
        : Array.from({length: Number(rightDataset.length || 0)}, (_, i) => i);

      const rightH3Set = new Set<string>();
      const loopYieldEvery = Math.max(100, QMAP_DEFAULT_CHUNK_SIZE);
      for (let i = 0; i < rightIdx.length; i += 1) {
        const rowIdx = rightIdx[i];
        const h3 = normalizeH3Key(rightDataset.getValue(resolvedRightH3, rowIdx));
        if (h3) rightH3Set.add(h3);
        if (i > 0 && i % loopYieldEvery === 0) {
          await yieldToMainThread();
        }
      }

      let leftWithH3 = 0;
      let matchedLeftRows = 0;
      for (let i = 0; i < leftIdx.length; i += 1) {
        const rowIdx = leftIdx[i];
        const h3 = normalizeH3Key(leftDataset.getValue(resolvedLeftH3, rowIdx));
        if (!h3) continue;
        leftWithH3 += 1;
        if (rightH3Set.has(h3)) {
          matchedLeftRows += 1;
        }
        if (i > 0 && i % loopYieldEvery === 0) {
          await yieldToMainThread();
        }
      }
      const unmatchedLeftRows = Math.max(0, leftWithH3 - matchedLeftRows);
      const coveragePct = leftWithH3 > 0 ? Number(((matchedLeftRows / leftWithH3) * 100).toFixed(1)) : 0;
      const adaptiveCoverageGate =
        rightFields.some((fieldName: string) => isPopulationLikeField(fieldName)) ? 60 : 5;
      const coverageGate = Number.isFinite(Number(minCoveragePct)) ? Number(minCoveragePct) : adaptiveCoverageGate;
      if (leftWithH3 <= 0) {
        return {
          llmResult: {
            success: false,
            details: `Left dataset "${leftDataset.label || leftDataset.id}" has no valid H3 ids in field "${resolvedLeftH3}".`
          }
        };
      }
      if (coveragePct < coverageGate) {
        return {
          llmResult: {
            success: false,
            details:
              `H3 join aborted: coverage ${coveragePct}% is below minimum ${coverageGate}%. ` +
              `Join stats: leftH3=${leftWithH3}, matched=${matchedLeftRows}, unmatched=${unmatchedLeftRows}. ` +
              'Likely causes: different H3 resolution, wrong boundary subset, or incompatible H3 index field. ' +
              `To proceed despite low coverage, retry with minCoveragePct=${Math.max(0, Math.floor(coveragePct))} or lower.`
          }
        };
      }

      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          fieldCatalog,
          numericFields: numericOutputFields,
          styleableFields: numericOutputFields,
          defaultStyleField,
          fieldAliases,
          details:
            `Joining "${leftDataset.label || leftDataset.id}" with "${
              rightDataset.label || rightDataset.id
            }" on H3 (${resolvedLeftH3} = ${resolvedRightH3}) -> "${resolvedTargetLabel}". ` +
            `Join stats: leftH3=${leftWithH3}, matched=${matchedLeftRows}, unmatched=${unmatchedLeftRows}, coverage=${coveragePct}%.`
        },
        additionalData: {
          executionKey: makeExecutionKey('join-datasets-on-h3'),
          leftDatasetId: leftDataset.id,
          rightDatasetId: rightDataset.id,
          leftH3Field: resolvedLeftH3,
          rightH3Field: resolvedRightH3,
          rightFields,
          fieldCatalog,
          numericFields: numericOutputFields,
          styleableFields: numericOutputFields,
          defaultStyleField,
          fieldAliases,
          metric: metric || 'avg',
          joinType: joinType || 'inner',
          showOnMap: showOnMap !== false,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function JoinQMapDatasetsOnH3Component({
      executionKey,
      leftDatasetId,
      rightDatasetId,
      leftH3Field,
      rightH3Field,
      rightFields,
      metric,
      joinType,
      showOnMap,
      newDatasetName,
      newDatasetId
    }: {
      executionKey?: string;
      leftDatasetId: string;
      rightDatasetId: string;
      leftH3Field: string;
      rightH3Field: string;
      rightFields: string[];
      metric: 'avg' | 'sum' | 'max' | 'first';
      joinType: 'inner' | 'left';
      showOnMap: boolean;
      newDatasetName: string;
      newDatasetId: string;
    }) {
      const localDispatch = useDispatch<any>();
      const datasets = useSelector(selectQMapDatasets);
      const localLayers = useSelector(selectQMapLayers);
      const {shouldSkip, complete} = useToolExecution({
        executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });
      useEffect(() => {
        if (shouldSkip()) return;
        const leftDataset = datasets?.[leftDatasetId];
        const rightDataset = datasets?.[rightDatasetId];
        if (!leftDataset || !rightDataset) return;

        const leftFields = (leftDataset.fields || []).map((f: any) => ({
          name: String(f?.name || ''),
          type: String(f?.type || ALL_FIELD_TYPES.string)
        }));
        const rightFieldMeta = (rightDataset.fields || []).reduce((acc: Record<string, string>, f: any) => {
          const name = String(f?.name || '');
          if (name) acc[name] = String(f?.type || ALL_FIELD_TYPES.string);
          return acc;
        }, {});

        const usedOutputNames = new Set(
          leftFields.map((f: any) => String(f.name || '').toLowerCase()).filter(Boolean)
        );
        const appended = rightFields.map((fieldName: string) => {
          const baseName = String(fieldName || '').trim();
          let outputName = baseName;
          let suffix = 2;
          while (usedOutputNames.has(outputName.toLowerCase())) {
            outputName = `${baseName}_${suffix}`;
            suffix += 1;
          }
          usedOutputNames.add(outputName.toLowerCase());
          const sourceType = rightFieldMeta[fieldName] || ALL_FIELD_TYPES.string;
          const aggregationMode: 'numeric' | 'categorical' = isCategoricalJoinField(fieldName, sourceType)
            ? 'categorical'
            : 'numeric';
          return {
            sourceName: fieldName,
            outputName,
            type: aggregationMode === 'categorical' ? ALL_FIELD_TYPES.string : sourceType,
            aggregationMode
          };
        });

        const rightIdx = Array.isArray(rightDataset.allIndexes)
          ? rightDataset.allIndexes
          : Array.from({length: Number(rightDataset.length || 0)}, (_, i) => i);

        type Bucket = {
          first: unknown;
          sum: number;
          count: number;
          max: number | null;
          categoricalValues: string[];
          categoricalValuesSet: Set<string>;
        };
        const rightLookup = new Map<string, Record<string, Bucket>>();
        rightIdx.forEach((rowIdx: number) => {
          const h3Value = normalizeH3Key(rightDataset.getValue(rightH3Field, rowIdx));
          if (!h3Value) return;
          const perField = rightLookup.get(h3Value) || {};
          appended.forEach(fieldDef => {
            const raw = rightDataset.getValue(fieldDef.sourceName, rowIdx);
            const prev = perField[fieldDef.sourceName] || {
              first: raw,
              sum: 0,
              count: 0,
              max: null,
              categoricalValues: [],
              categoricalValuesSet: new Set<string>()
            };
            if (prev.first === undefined || prev.first === null || prev.first === '') {
              prev.first = raw;
            }
            if (fieldDef.aggregationMode === 'categorical') {
              if (raw !== null && raw !== undefined && raw !== '') {
                const normalizedRaw = String(raw);
                if (!prev.categoricalValuesSet.has(normalizedRaw)) {
                  prev.categoricalValuesSet.add(normalizedRaw);
                  prev.categoricalValues.push(normalizedRaw);
                }
              }
            } else {
              const num = Number(raw);
              if (Number.isFinite(num)) {
                prev.sum += num;
                prev.count += 1;
                prev.max = prev.max === null ? num : Math.max(prev.max, num);
              }
            }
            perField[fieldDef.sourceName] = prev;
          });
          rightLookup.set(h3Value, perField);
        });

        const leftIdx = Array.isArray(leftDataset.allIndexes)
          ? leftDataset.allIndexes
          : Array.from({length: Number(leftDataset.length || 0)}, (_, i) => i);
        const rows: unknown[][] = [];
        leftIdx.forEach((rowIdx: number) => {
          const leftH3 = normalizeH3Key(leftDataset.getValue(leftH3Field, rowIdx));
          if (!leftH3) return;
          const buckets = rightLookup.get(leftH3);
          if (joinType === 'inner' && !buckets) return;

          const leftRow = leftFields.map((f: any) => leftDataset.getValue(f.name, rowIdx));
          const rightValues = appended.map(fieldDef => {
            const bucket = buckets?.[fieldDef.sourceName];
            if (!bucket) return null;
            if (fieldDef.aggregationMode === 'categorical') {
              if (metric === 'first') return bucket.first ?? null;
              if (bucket.categoricalValues.length === 0) return bucket.first ?? null;
              if (bucket.categoricalValues.length === 1) return bucket.categoricalValues[0];
              return bucket.categoricalValues.join(' | ');
            }
            if (metric === 'first') return bucket.first ?? null;
            if (metric === 'sum') return bucket.count > 0 ? Number(bucket.sum.toFixed(6)) : null;
            if (metric === 'max') return bucket.max;
            return bucket.count > 0 ? Number((bucket.sum / bucket.count).toFixed(6)) : null;
          });
          rows.push([...leftRow, ...rightValues]);
        });

        if (!rows.length) return;
        const datasetFields = [
          ...leftFields,
          ...appended.map(fieldDef => ({name: fieldDef.outputName, type: fieldDef.type}))
        ];
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

        complete();
        if (showOnMap && QMAP_AUTO_HIDE_SOURCE_LAYERS) {
          hideLayersForDatasetIds(localDispatch, localLayers || [], [leftDataset.id, rightDataset.id]);
        }
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
      }, [
        localDispatch,
        datasets,
        executionKey,
        leftDatasetId,
        rightDatasetId,
        leftH3Field,
        rightH3Field,
        rightFields,
        metric,
        joinType,
        showOnMap,
        localLayers,
        newDatasetName,
        newDatasetId,
        shouldSkip,
        complete
      ]);
      return null;
    }
  };

}

export function createFitQMapToDatasetTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    makeExecutionKey,
    resolveOptionalFeatureCap,
    EXECUTED_TOOL_COMPONENT_KEYS,
    rememberExecutedToolComponentKey,
    filterTargetsDataset,
    resolveDatasetFieldName,
    toComparable,
    cellToBoundary,
    updateBoundsFromGeometry,
    scheduleMergedMapFit
  } = ctx;

  return {
    description: 'Fit/zoom map viewport to a dataset extent (optionally considering active filters).',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name from listQMapDatasets'),
      useActiveFilters: z.boolean().optional().describe('Default true'),
      maxFeatures: z
        .number()
        .optional()
        .describe('Optional explicit cap for fit computation. Unset = full matched coverage (no truncation).')
    }),
    execute: async ({datasetName, useActiveFilters, maxFeatures}: any) => {
      const sourceDataset = resolveDatasetByName(getCurrentVisState()?.datasets || {}, datasetName);
      if (!sourceDataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found.`
          }
        };
      }
      const geometryField = (sourceDataset.fields || []).find((f: any) => f?.type === 'geojson')?.name;
      const h3Field = (sourceDataset.fields || []).find((f: any) => {
        const t = String(f?.type || '').toLowerCase();
        const n = String(f?.name || '').toLowerCase();
        return t === 'h3' || n === 'h3_id';
      })?.name;
      const fitMode = geometryField ? 'geojson' : h3Field ? 'h3' : 'none';
      if (fitMode === 'none') {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" has no geojson or h3 field for fit.`
          }
        };
      }

      return {
        llmResult: {
          success: true,
          details: `Fitting map viewport to dataset "${sourceDataset.label || sourceDataset.id}".`
        },
        additionalData: {
          executionKey: makeExecutionKey('fit-to-dataset'),
          datasetId: sourceDataset.id,
          fitMode,
          geometryField: geometryField || h3Field,
          useActiveFilters: useActiveFilters !== false,
          maxFeatures: resolveOptionalFeatureCap(maxFeatures)
        }
      };
    },
    component: function FitQMapToDatasetComponent({
      executionKey,
      datasetId,
      fitMode,
      geometryField,
      useActiveFilters,
      maxFeatures
    }: {
      executionKey?: string;
      datasetId: string;
      fitMode: 'geojson' | 'h3' | 'none';
      geometryField: string;
      useActiveFilters: boolean;
      maxFeatures: number;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const {shouldSkip, complete} = useToolExecution({
        executionKey,
        executedToolComponentKeys: EXECUTED_TOOL_COMPONENT_KEYS,
        rememberExecutedToolComponentKey
      });

      useEffect(() => {
        if (shouldSkip()) return;
        const sourceDataset = localVisState?.datasets?.[datasetId];
        if (!sourceDataset) return;

        const baseIdx = Array.isArray(sourceDataset.allIndexes)
          ? sourceDataset.allIndexes
          : Array.from({length: Number(sourceDataset.length || 0)}, (_, i) => i);
        const filters = useActiveFilters
          ? (localVisState?.filters || []).filter((f: any) => filterTargetsDataset(f, sourceDataset.id))
          : [];
        const matchedIdx = baseIdx.filter((rowIdx: number) => {
          return filters.every((filter: any) => {
            const rawFieldName = Array.isArray(filter?.name) ? filter.name[0] : filter?.name;
            if (!rawFieldName) return true;
            const resolvedFilterField = resolveDatasetFieldName(sourceDataset, String(rawFieldName));
            if (!resolvedFilterField) return true;
            const rowValue = sourceDataset.getValue(resolvedFilterField, rowIdx);
            const filterValue = filter?.value;
            if (Array.isArray(filterValue) && filterValue.length === 2 && filter?.type !== 'multiSelect') {
              const minV = filterValue[0];
              const maxV = filterValue[1];
              return Number(rowValue) >= Number(minV) && Number(rowValue) <= Number(maxV);
            }
            if (Array.isArray(filterValue)) {
              return filterValue.map(toComparable).includes(toComparable(rowValue));
            }
            return toComparable(rowValue) === toComparable(filterValue);
          });
        });

        const capped = matchedIdx.slice(0, resolveOptionalFeatureCap(maxFeatures));
        const resolvedGeometryField = resolveDatasetFieldName(sourceDataset, geometryField) || geometryField;
        if (!resolvedGeometryField) return;
        const bounds = {
          minLng: Number.POSITIVE_INFINITY,
          minLat: Number.POSITIVE_INFINITY,
          maxLng: Number.NEGATIVE_INFINITY,
          maxLat: Number.NEGATIVE_INFINITY
        };

        capped.forEach((rowIdx: number) => {
          if (fitMode === 'h3') {
            const h3Cell = String(sourceDataset.getValue(resolvedGeometryField, rowIdx) || '').trim();
            if (!h3Cell) return;
            try {
              const boundary = cellToBoundary(h3Cell, false);
              boundary.forEach(([lat, lng]: [number, number]) => {
                if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
                bounds.minLng = Math.min(bounds.minLng, lng);
                bounds.maxLng = Math.max(bounds.maxLng, lng);
                bounds.minLat = Math.min(bounds.minLat, lat);
                bounds.maxLat = Math.max(bounds.maxLat, lat);
              });
            } catch {
              return;
            }
            return;
          }

          let geom: any = sourceDataset.getValue(resolvedGeometryField, rowIdx);
          if (typeof geom === 'string') {
            try {
              geom = JSON.parse(geom);
            } catch {
              return;
            }
          }
          updateBoundsFromGeometry(geom, bounds);
        });

        if (
          !Number.isFinite(bounds.minLng) ||
          !Number.isFinite(bounds.minLat) ||
          !Number.isFinite(bounds.maxLng) ||
          !Number.isFinite(bounds.maxLat)
        ) {
          return;
        }

        complete();
        scheduleMergedMapFit(localDispatch, bounds);
      }, [localDispatch, localVisState, executionKey, datasetId, fitMode, geometryField, useActiveFilters, maxFeatures, shouldSkip, complete]);

      return null;
    }
  };

}

export function createWaitForQMapDatasetTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    WAIT_DATASET_RETRY_TRACKER,
    WAIT_DATASET_RETRY_TTL_MS
  } = ctx;

  return {
    description:
      'Wait until a dataset is available in current q-map visState (useful after async operations like tessellation/load).',
    parameters: z.object({
      datasetName: z.string().describe('Expected dataset name/id'),
      timeoutMs: z.number().min(500).max(300000).optional().describe('Default 60000')
    }),
    execute: async (rawArgs: any) => {
      const target = String(rawArgs?.datasetName || '').trim();
      if (!target) {
        return {llmResult: {success: false, details: 'datasetName is required.'}};
      }
      const retryKey = target.toLowerCase();
      const previousRetryState = WAIT_DATASET_RETRY_TRACKER.get(retryKey);
      if (
        previousRetryState &&
        Date.now() - Number(previousRetryState.lastFailureAt || 0) > WAIT_DATASET_RETRY_TTL_MS
      ) {
        WAIT_DATASET_RETRY_TRACKER.delete(retryKey);
      }
      const retryState = WAIT_DATASET_RETRY_TRACKER.get(retryKey);
      if ((retryState?.failedAttempts || 0) >= 2) {
        return {
          llmResult: {
            success: false,
            details:
              `Retry budget exhausted for dataset "${target}" after previous timeouts. ` +
              'Do not restart the same wait loop; continue with failure handling or rebuild the workflow once.'
          }
        };
      }

      const requestedTimeout = Math.max(500, Number(rawArgs?.timeoutMs || 60000));
      const lowerTarget = target.toLowerCase();
      const isClipDataset =
        lowerTarget.includes('qmap_clip_') ||
        lowerTarget.includes('clip_') ||
        lowerTarget.includes('_clipped_') ||
        lowerTarget.includes('ritagli');
      const isLikelyHeavyDataset =
        lowerTarget.includes('tassellation') ||
        lowerTarget.includes('tessellation') ||
        lowerTarget.includes('population') ||
        lowerTarget.includes('popolaz') ||
        lowerTarget.includes('aggregate') ||
        lowerTarget.includes('joined') ||
        lowerTarget.includes('zonal') ||
        lowerTarget.includes('h3') ||
        lowerTarget.includes('clip');
      const baseTimeout = Math.max(requestedTimeout, isClipDataset ? 120000 : 60000);
      const heavyExtraGraceMs = isLikelyHeavyDataset
        ? Math.min(120000, Math.max(15000, Math.floor(baseTimeout * 0.5)))
        : 0;
      const hardTimeout = baseTimeout + heavyExtraGraceMs;
      const pollEvery = 200;
      const startedAt = Date.now();
      let loadingObserved = false;
      while (Date.now() - startedAt < hardTimeout) {
        const datasets = getCurrentVisState()?.datasets || {};
        const found = resolveDatasetByName(datasets, target);
        if (found?.id) {
          const rowCount = Number(found.length || 0);
          WAIT_DATASET_RETRY_TRACKER.delete(retryKey);
          return {
            llmResult: {
              success: true,
              dataset: found.label || found.id,
              rowCount,
              details: `Dataset "${found.label || found.id}" is available (${rowCount} rows).`
            }
          };
        }
        const elapsed = Date.now() - startedAt;
        const loadingValue = Number(getCurrentVisState()?.loadingIndicatorValue || 0);
        if (loadingValue > 0) {
          loadingObserved = true;
        }
        if (elapsed >= baseTimeout && loadingValue <= 0) {
          break;
        }
        if (elapsed >= hardTimeout) {
          break;
        }
        await new Promise(resolve => setTimeout(resolve, pollEvery));
      }

      const failedAttempts = (WAIT_DATASET_RETRY_TRACKER.get(retryKey)?.failedAttempts || 0) + 1;
      WAIT_DATASET_RETRY_TRACKER.set(retryKey, {
        failedAttempts,
        lastFailureAt: Date.now()
      });
      return {
        llmResult: {
          success: false,
          details: loadingObserved
            ? failedAttempts >= 2
              ? `Timeout waiting for dataset "${target}" after ${hardTimeout}ms while processing was active. Retry budget exhausted for this dataset.`
              : `Timeout waiting for dataset "${target}" after ${hardTimeout}ms while processing was active. One retry with longer timeout is still allowed.`
            : failedAttempts >= 2
            ? `Timeout waiting for dataset "${target}". Retry budget exhausted for this dataset.`
            : `Timeout waiting for dataset "${target}". One retry with longer timeout is still allowed.`
        }
      };
    }
  };

}
