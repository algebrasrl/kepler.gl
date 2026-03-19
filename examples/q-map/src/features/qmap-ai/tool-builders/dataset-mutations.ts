import {useEffect, MutableRefObject} from 'react';
import {addDataToMap, layerConfigChange, replaceDataInMap, wrapTo} from '@kepler.gl/actions';
import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {extendedTool} from '../tool-shim';
import {useDispatch, useSelector, useStore} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets} from '../../../state/qmap-selectors';
import type {QMapToolContext} from '../context/tool-context';
import {preprocessFlatFilterToolArgs} from '../tool-args-normalization';
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

export function createDatasetFromFilterTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    evaluateFilter,
    getDatasetInfoByLabel,
    makeExecutionKey,
    lastRankContextRef,
    executedToolComponentKeys,
    rememberExecutedToolComponentKey
  } = ctx;
  return extendedTool({
    description:
      'Create a new dataset from an existing dataset by applying a field filter. Uses q-hive addDataToMap action.',
    parameters: z.preprocess(
      preprocessFlatFilterToolArgs,
      z.object({
        datasetName: z.string().describe('Exact dataset name'),
        fieldName: z.string().describe('Field to filter'),
        operator: z
          .enum(['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'contains', 'startsWith', 'endsWith', 'in'])
          .optional()
          .describe('Filter operator, default eq'),
        value: z.union([z.number(), z.string(), z.boolean(), z.array(z.union([z.number(), z.string()]))]),
        newDatasetName: z.string().optional(),
        showOnMap: z
          .boolean()
          .optional()
          .describe('Default false. Set true to auto-create a map layer for the output dataset.')
      })
    ),
    execute: async ({datasetName, fieldName, operator, value, newDatasetName, showOnMap}) => {
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

      const field = (sourceDataset.fields || []).find(
        (f: any) => String(f?.name || '').toLowerCase() === String(fieldName || '').toLowerCase()
      );
      if (!field?.name) {
        return {
          llmResult: {
            success: false,
            details: `Field "${fieldName}" not found in dataset "${datasetName}".`
          }
        };
      }

      const resolvedOperator = String(operator || 'eq').toLowerCase();
      const normalizedFieldType = String(field?.type || '').toLowerCase();
      const isTextLikeField =
        normalizedFieldType === String(ALL_FIELD_TYPES.string).toLowerCase() || normalizedFieldType.includes('string');
      let resolvedFilterValue: unknown = value;
      let filterRecoveryNote = '';
      if (isTextLikeField && typeof value === 'number' && Number.isFinite(value)) {
        const rankContext = lastRankContextRef.current;
        const sourceDatasetKeys = new Set(
          [datasetName, sourceDataset.label, sourceDataset.id]
            .map(item => String(item || '').trim().toLowerCase())
            .filter(Boolean)
        );
        const hasCompatibleRankContext =
          !!rankContext &&
          Date.now() - Number(rankContext.updatedAtMs || 0) <= 5 * 60 * 1000 &&
          Array.isArray(rankContext.datasetKeys) &&
          rankContext.datasetKeys.some((key: string) => sourceDatasetKeys.has(String(key || '').trim().toLowerCase()));
        const rankedRows = hasCompatibleRankContext ? rankContext?.rows || [] : [];
        const candidateFields = Array.from(
          new Set(
            [field.name, 'name', 'name_en']
              .map(item => String(item || '').trim())
              .filter(Boolean)
          )
        );
        let recoveredTextValue = '';
        for (const row of rankedRows) {
          if (!row || typeof row !== 'object' || Array.isArray(row)) continue;
          for (const candidateField of candidateFields) {
            const rawCandidate = (row as Record<string, unknown>)[candidateField];
            if (typeof rawCandidate === 'string' && rawCandidate.trim()) {
              recoveredTextValue = rawCandidate.trim();
              break;
            }
          }
          if (recoveredTextValue) break;
        }
        if (recoveredTextValue) {
          resolvedFilterValue = recoveredTextValue;
          filterRecoveryNote =
            ` Auto-recovered text filter value "${recoveredTextValue}" from latest ranking evidence for dataset "` +
            `${sourceDataset.label || sourceDataset.id}".`;
        } else {
          return {
            llmResult: {
              success: false,
              details:
                `Invalid filter value for text field "${field.name}": received numeric value ${String(value)}. ` +
                'Use the exact text value (for example a region name) or call searchQMapFieldValues first.'
            }
          };
        }
      }
      if (
        ['contains', 'startswith', 'endswith'].includes(resolvedOperator) &&
        typeof resolvedFilterValue !== 'string'
      ) {
        return {
          llmResult: {
            success: false,
            details: `Operator "${resolvedOperator}" requires a string value for field "${field.name}".`
          }
        };
      }

      const indices = Array.isArray(sourceDataset.allIndexes)
        ? sourceDataset.allIndexes
        : Array.from({length: Number(sourceDataset.length || 0)}, (_, i) => i);
      const matchedIndices = indices.filter((rowIdx: number) =>
        evaluateFilter(resolvedOperator, sourceDataset.getValue(field.name, rowIdx), resolvedFilterValue)
      );

      const targetName = String(newDatasetName || '').trim() || `${sourceDataset.label || sourceDataset.id}_filtered`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        currentVisState?.datasets || {},
        targetName,
        'qmap_filter'
      );
      if (!matchedIndices.length) {
        return {
          llmResult: {
            success: false,
            details:
              `No rows matched filter ${field.name} ${operator || 'eq'} ${String(resolvedFilterValue)} on dataset "${sourceDataset.label || sourceDataset.id}".` +
              filterRecoveryNote
          }
        };
      }

      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          details:
            `Creating dataset "${resolvedTargetLabel}" with ${matchedIndices.length} rows.` +
            `${showOnMap === true ? '' : ' Output dataset will be created without auto layer.'}` +
            filterRecoveryNote
        },
        additionalData: {
          executionKey: makeExecutionKey('create-dataset-from-filter'),
          sourceDatasetId: sourceDataset.id,
          sourceDatasetLabel: sourceDataset.label || sourceDataset.id,
          rowIndices: matchedIndices,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId,
          showOnMap: showOnMap === true
        }
      };
    },
    component: function CreateDatasetFromFilterComponent({
      executionKey,
      sourceDatasetId,
      rowIndices,
      newDatasetName,
      newDatasetId,
      showOnMap
    }: {
      executionKey?: string;
      sourceDatasetId: string;
      rowIndices: number[];
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

        const datasetFields = (sourceDataset.fields || []).map((f: any) => ({name: f.name, type: f.type}));
        const rows = (rowIndices || []).map((rowIdx: number) =>
          datasetFields.map((f: any) => sourceDataset.getValue(f.name, rowIdx))
        );
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
      }, [localDispatch, datasets, executionKey, sourceDatasetId, rowIndices, newDatasetName, newDatasetId, showOnMap, shouldSkip, complete]);
      return null;
    }
  });
}

export function createDatasetFromCurrentFiltersTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    filterTargetsDataset,
    resolveDatasetFieldName,
    toComparable,
    getDatasetInfoByLabel,
    makeExecutionKey,
    executedToolComponentKeys,
    rememberExecutedToolComponentKey
  } = ctx;
  return extendedTool({
    description:
      'Create a new dataset from currently active UI filters for a dataset. Uses q-hive addDataToMap action.',
    parameters: z.object({
      datasetName: z.string().describe('Exact dataset name'),
      newDatasetName: z.string().optional(),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default false. Set true to auto-create a map layer for the output dataset.')
    }),
    execute: async ({datasetName, newDatasetName, showOnMap}) => {
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

      const filters = (currentVisState?.filters || []).filter((f: any) => filterTargetsDataset(f, sourceDataset.id));
      if (!filters.length) {
        return {
          llmResult: {
            success: false,
            details:
              `No active filters found for dataset "${sourceDataset.label || sourceDataset.id}". ` +
              'Apply/confirm filter first, then retry dataset materialization.'
          }
        };
      }
      const indices = Array.isArray(sourceDataset.allIndexes)
        ? sourceDataset.allIndexes
        : Array.from({length: Number(sourceDataset.length || 0)}, (_, i) => i);

      const matchedIndices = indices.filter((rowIdx: number) => {
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

      const targetName =
        String(newDatasetName || '').trim() || `${sourceDataset.label || sourceDataset.id}_from_current_filters`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        currentVisState?.datasets || {},
        targetName,
        'qmap_filter'
      );
      if (!matchedIndices.length) {
        return {
          llmResult: {
            success: false,
            details: `No rows matched current filters for dataset "${sourceDataset.label || sourceDataset.id}".`
          }
        };
      }

      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          details:
            `Creating dataset "${resolvedTargetLabel}" from active filters (${matchedIndices.length} rows).` +
            `${showOnMap === true ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('create-dataset-from-active-filters'),
          sourceDatasetId: sourceDataset.id,
          rowIndices: matchedIndices,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId,
          showOnMap: showOnMap === true
        }
      };
    },
    component: function CreateDatasetFromCurrentFiltersComponent({
      executionKey,
      sourceDatasetId,
      rowIndices,
      newDatasetName,
      newDatasetId,
      showOnMap
    }: {
      executionKey?: string;
      sourceDatasetId: string;
      rowIndices: number[];
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

        const datasetFields = (sourceDataset.fields || []).map((f: any) => ({name: f.name, type: f.type}));
        const rows = (rowIndices || []).map((rowIdx: number) =>
          datasetFields.map((f: any) => sourceDataset.getValue(f.name, rowIdx))
        );
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
      }, [localDispatch, datasets, executionKey, sourceDatasetId, rowIndices, newDatasetName, newDatasetId, showOnMap, shouldSkip, complete]);
      return null;
    }
  });
}

export function createMergeQMapDatasetsTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    getDatasetInfoByLabel,
    normalizeMergeGeometryMode,
    normalizeCrsCode,
    buildMergeFieldDefinitions,
    getMergeDatasetGeometryReadiness,
    getDatasetIndexes,
    normalizeFieldToken,
    isGeojsonMergeFieldDefinition,
    ensureUniqueMergeFieldName,
    geometryModeSchema,
    makeExecutionKey,
    resolveDatasetPointFieldPair,
    resolveGeojsonFieldName,
    normalizeGeojsonCellValue,
    parseCoordinateValue,
    convertPointToWgs84,
    yieldToMainThread,
    defaultChunkSize,
    proj4Transform,
    executedToolComponentKeys,
    rememberExecutedToolComponentKey
  } = ctx;
  return extendedTool({
    description:
      'Merge multiple loaded datasets into one dataset using deterministic schema union (no SQL). Optionally keep only merged layer visible.',
    parameters: z.object({
      datasetNames: z
        .array(z.string())
        .optional()
        .describe('Optional dataset names/refs from listQMapDatasets. If omitted, merge all loaded datasets.'),
      newDatasetName: z.string().optional().describe('Target dataset label (default qmap_merged_dataset).'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default true. Auto-create layer for the merged dataset output.'),
      showOnlyMergedLayer: z
        .boolean()
        .optional()
        .describe('Default false. If true and showOnMap=true, hide all non-merged layers after merge.'),
      includeSourceDatasetField: z
        .boolean()
        .optional()
        .describe('Default true. Include source_dataset field in output rows.'),
      includeSourceDatasetIdField: z
        .boolean()
        .optional()
        .describe('Default false. Include source_dataset_id field in output rows.'),
      geometryMode: geometryModeSchema.describe(
        'Geometry strategy: auto (default), preserve_only, derive_from_latlon, none. Use derive_from_latlon to enforce point geometry generation.'
      ),
      latitudeField: z
        .string()
        .optional()
        .describe('Optional latitude field name hint for point derivation (applies to all source datasets).'),
      longitudeField: z
        .string()
        .optional()
        .describe('Optional longitude field name hint for point derivation (applies to all source datasets).'),
      sourceCrs: z
        .string()
        .optional()
        .describe('CRS of latitude/longitude columns used for point derivation (default EPSG:4326).'),
      outputGeometryField: z
        .string()
        .optional()
        .describe('Output geometry field name (default _geojson).'),
      strictValidation: z
        .boolean()
        .optional()
        .describe('Default false. If true, fail when merged output would not contain any geometry values.')
    }),
    execute: async ({
      datasetNames,
      newDatasetName,
      showOnMap,
      showOnlyMergedLayer,
      includeSourceDatasetField,
      includeSourceDatasetIdField,
      geometryMode,
      latitudeField,
      longitudeField,
      sourceCrs,
      outputGeometryField,
      strictValidation
    }) => {
      const vis = getCurrentVisState();
      const datasetsMap = vis?.datasets || {};
      const loadedDatasets = Object.values(datasetsMap || {}) as any[];
      if (!loadedDatasets.length) {
        return {
          llmResult: {
            success: false,
            details: 'No loaded datasets to merge. Load datasets first and retry.'
          }
        };
      }

      const requestedDatasetNames = Array.isArray(datasetNames)
        ? datasetNames.map(name => String(name || '').trim()).filter(Boolean)
        : [];

      const selectedDatasets: any[] = [];
      const selectedIds = new Set<string>();
      const missingDatasets: string[] = [];

      if (requestedDatasetNames.length) {
        requestedDatasetNames.forEach(requestedName => {
          const dataset = resolveDatasetByName(datasetsMap, requestedName);
          if (!dataset?.id) {
            missingDatasets.push(requestedName);
            return;
          }
          const datasetId = String(dataset.id);
          if (selectedIds.has(datasetId)) return;
          selectedIds.add(datasetId);
          selectedDatasets.push(dataset);
        });
      } else {
        loadedDatasets.forEach((dataset: any) => {
          const datasetId = String(dataset?.id || '').trim();
          if (!datasetId || selectedIds.has(datasetId)) return;
          selectedIds.add(datasetId);
          selectedDatasets.push(dataset);
        });
      }

      if (missingDatasets.length) {
        const available = loadedDatasets
          .map((dataset: any) => String(dataset?.label || dataset?.id || '').trim())
          .filter(Boolean)
          .slice(0, 20)
          .join(', ');
        return {
          llmResult: {
            success: false,
            missingDatasets,
            details: `Dataset not found: ${missingDatasets.join(', ')}.${available ? ` Available datasets: ${available}.` : ''}`
          }
        };
      }

      if (!selectedDatasets.length) {
        return {
          llmResult: {
            success: false,
            details: 'No valid datasets selected for merge.'
          }
        };
      }

      const targetName =
        String(newDatasetName || '').trim() ||
        (selectedDatasets.length === 1
          ? `${selectedDatasets[0]?.label || selectedDatasets[0]?.id || 'dataset'}_merged`
          : 'qmap_merged_dataset');
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        datasetsMap,
        targetName,
        'qmap_merged'
      );
      const sourcesToMerge = requestedDatasetNames.length
        ? selectedDatasets
        : selectedDatasets.filter((dataset: any) => String(dataset?.id || '') !== String(resolvedTargetDatasetId));
      if (!sourcesToMerge.length) {
        return {
          llmResult: {
            success: false,
            details:
              'No source datasets left to merge after excluding the output dataset. ' +
              'Specify datasetNames explicitly or load additional datasets.'
          }
        };
      }

      const strictMode = strictValidation === true;
      const normalizedGeometryMode = normalizeMergeGeometryMode(geometryMode);
      const normalizedSourceCrs = normalizeCrsCode(sourceCrs);
      const requestedOutputGeometryFieldName = String(outputGeometryField || '').trim() || '_geojson';

      const {fields: baseFields, schemaConflicts} = buildMergeFieldDefinitions(sourcesToMerge, {
        includeSourceDatasetField: includeSourceDatasetField !== false,
        includeSourceDatasetIdField: includeSourceDatasetIdField === true
      });
      const fieldDefinitions = [...baseFields];
      if (!fieldDefinitions.length) {
        return {
          llmResult: {
            success: false,
            details: 'No mergeable fields found in selected datasets.'
          }
        };
      }

      const readinessByDataset = sourcesToMerge.map((dataset: any) => {
        const readiness = getMergeDatasetGeometryReadiness(dataset, latitudeField, longitudeField);
        return {
          id: String(dataset?.id || ''),
          name: String(dataset?.label || dataset?.id || ''),
          rows: getDatasetIndexes(dataset).length,
          hasGeometryField: readiness.hasGeometryField,
          geometryField: readiness.geometryFieldName,
          hasPointPair: readiness.hasPointPair,
          latitudeField: readiness.pointFields.latField,
          longitudeField: readiness.pointFields.lonField
        };
      });

      const datasetsWithGeometryField = readinessByDataset.filter(item => item.hasGeometryField).length;
      const datasetsWithPointPair = readinessByDataset.filter(item => item.hasPointPair).length;
      const canDeriveAnyGeometry = datasetsWithPointPair > 0;
      const canPreserveAnyGeometry = datasetsWithGeometryField > 0;
      const usedKeys = new Set(
        fieldDefinitions.map(field => String(field?.key || '').trim().toLowerCase()).filter(Boolean)
      );
      const requestedOutputGeometryKey = normalizeFieldToken(requestedOutputGeometryFieldName);
      const existingRequestedField = fieldDefinitions.find(field => field.key === requestedOutputGeometryKey) || null;
      const existingGeojsonField = fieldDefinitions.find(field => isGeojsonMergeFieldDefinition(field)) || null;

      let outputGeometryFieldDef: any = null;
      if (normalizedGeometryMode !== 'none') {
        if (existingRequestedField && isGeojsonMergeFieldDefinition(existingRequestedField)) {
          outputGeometryFieldDef = existingRequestedField;
        } else if (normalizedGeometryMode !== 'derive_from_latlon' && existingGeojsonField && !outputGeometryField) {
          outputGeometryFieldDef = existingGeojsonField;
        } else if (
          normalizedGeometryMode === 'preserve_only' &&
          existingRequestedField &&
          !isGeojsonMergeFieldDefinition(existingRequestedField)
        ) {
          outputGeometryFieldDef = existingGeojsonField;
        } else {
          const uniqueFieldName = ensureUniqueMergeFieldName(requestedOutputGeometryFieldName, usedKeys);
          outputGeometryFieldDef = {
            name: uniqueFieldName.name,
            type: String(ALL_FIELD_TYPES.geojson || 'geojson'),
            key: uniqueFieldName.key,
            source: 'derivedPointGeojson'
          };
          fieldDefinitions.push(outputGeometryFieldDef);
        }
      }

      if (normalizedGeometryMode !== 'none' && normalizedGeometryMode !== 'preserve_only') {
        if (normalizedSourceCrs.toUpperCase() !== 'EPSG:4326') {
          try {
            proj4Transform(normalizedSourceCrs, 'EPSG:4326');
          } catch (error: any) {
            return {
              llmResult: {
                success: false,
                details: `Invalid sourceCrs "${normalizedSourceCrs}" for point derivation: ${String(
                  error?.message || error
                )}`
              }
            };
          }
        }
      }

      if (strictMode && normalizedGeometryMode !== 'none') {
        const preserveIsUnavailable = normalizedGeometryMode === 'preserve_only' && !canPreserveAnyGeometry;
        const deriveIsUnavailable = normalizedGeometryMode === 'derive_from_latlon' && !canDeriveAnyGeometry;
        const autoIsUnavailable = normalizedGeometryMode === 'auto' && !canPreserveAnyGeometry && !canDeriveAnyGeometry;
        const noOutputGeometryField = !outputGeometryFieldDef;
        if (preserveIsUnavailable || deriveIsUnavailable || autoIsUnavailable || noOutputGeometryField) {
          const detailBits = [
            `geometryMode=${normalizedGeometryMode}`,
            `datasetsWithGeometryField=${datasetsWithGeometryField}/${sourcesToMerge.length}`,
            `datasetsWithPointPair=${datasetsWithPointPair}/${sourcesToMerge.length}`
          ];
          return {
            llmResult: {
              success: false,
              details: `Strict merge validation failed: cannot ensure output geometry field. ${detailBits.join(', ')}.`
            }
          };
        }
      }

      const inputRows = sourcesToMerge.reduce((acc: number, dataset: any) => acc + getDatasetIndexes(dataset).length, 0);
      const showMergedOnMap = showOnMap !== false;
      const isolateLayer = showMergedOnMap && showOnlyMergedLayer === true;
      const warnings: string[] = [];

      if (schemaConflicts.length > 0) {
        warnings.push(
          `Schema conflicts resolved by deterministic type precedence (${schemaConflicts.length} conflict${
            schemaConflicts.length === 1 ? '' : 's'
          }).`
        );
      }
      if (normalizedGeometryMode !== 'none') {
        if (!outputGeometryFieldDef) {
          warnings.push('No output geometry field configured; merged dataset may not be usable for spatial tools.');
        } else if (normalizedGeometryMode === 'preserve_only' && !canPreserveAnyGeometry) {
          warnings.push('No source dataset exposes a geometry field; preserve_only mode will produce null geometries.');
        } else if (
          (normalizedGeometryMode === 'auto' || normalizedGeometryMode === 'derive_from_latlon') &&
          !canDeriveAnyGeometry &&
          !canPreserveAnyGeometry
        ) {
          warnings.push('No source geometry fields and no detectable lat/lon pairs found.');
        }
      }

      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          geometryMode: normalizedGeometryMode,
          outputGeometryField: outputGeometryFieldDef?.name || null,
          sourceCrsForDerivedPoints:
            normalizedGeometryMode === 'auto' || normalizedGeometryMode === 'derive_from_latlon'
              ? normalizedSourceCrs
              : null,
          strictValidation: strictMode,
          schemaConflicts: schemaConflicts.slice(0, 50),
          warnings,
          geometryReadiness: {
            datasetsWithGeometryField,
            datasetsWithPointPair,
            totalDatasets: sourcesToMerge.length
          },
          sourceDatasets: sourcesToMerge.map((dataset: any) => ({
            id: dataset?.id || '',
            name: dataset?.label || dataset?.id || ''
          })),
          details:
            `Merging ${sourcesToMerge.length} dataset(s) into "${resolvedTargetLabel}" ` +
            `with schema union (${fieldDefinitions.length} fields, ${inputRows} input rows).` +
            `${
              outputGeometryFieldDef
                ? ` Geometry field: "${outputGeometryFieldDef.name}" (${normalizedGeometryMode}).`
                : normalizedGeometryMode === 'none'
                  ? ' Geometry disabled.'
                  : ' Geometry field unavailable.'
            }` +
            `${showMergedOnMap ? '' : ' Output dataset will be created without auto layer.'}` +
            `${isolateLayer ? ' Visibility will be isolated to merged output layer.' : ''}` +
            `${warnings.length ? ` Warnings: ${warnings.join(' ')}` : ''}`
        },
        additionalData: {
          executionKey: makeExecutionKey('merge-qmap-datasets'),
          sourceDatasetIds: sourcesToMerge.map((dataset: any) => String(dataset?.id || '')).filter(Boolean),
          mergeFields: fieldDefinitions,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId,
          showOnMap: showMergedOnMap,
          showOnlyMergedLayer: isolateLayer,
          geometryMode: normalizedGeometryMode,
          latitudeField: latitudeField ? String(latitudeField) : null,
          longitudeField: longitudeField ? String(longitudeField) : null,
          sourceCrs: normalizedSourceCrs,
          outputGeometryFieldName: outputGeometryFieldDef?.name || null,
          outputGeometryFieldKey: outputGeometryFieldDef?.key || null,
          strictValidation: strictMode,
          schemaConflicts: schemaConflicts.slice(0, 100),
          readinessByDataset
        }
      };
    },
    component: function MergeQMapDatasetsComponent({
      executionKey,
      sourceDatasetIds,
      mergeFields,
      newDatasetName,
      newDatasetId,
      showOnMap,
      showOnlyMergedLayer,
      geometryMode,
      latitudeField,
      longitudeField,
      sourceCrs,
      outputGeometryFieldName,
      outputGeometryFieldKey,
      strictValidation
    }: {
      executionKey?: string;
      sourceDatasetIds: string[];
      mergeFields: any[];
      newDatasetName: string;
      newDatasetId: string;
      showOnMap: boolean;
      showOnlyMergedLayer: boolean;
      geometryMode: any;
      latitudeField: string | null;
      longitudeField: string | null;
      sourceCrs: string;
      outputGeometryFieldName: string | null;
      outputGeometryFieldKey: string | null;
      strictValidation: boolean;
    }) {
      const localDispatch = useDispatch<any>();
      const localStore = useStore<any>();
      const localDatasets = useSelector(selectQMapDatasets);
      const {shouldSkip, complete} = useToolExecution({executionKey, executedToolComponentKeys, rememberExecutedToolComponentKey});

      useEffect(() => {
        if (shouldSkip()) return;

        const selectedDatasets = (sourceDatasetIds || []).map(datasetId => localDatasets?.[datasetId]).filter(Boolean);
        if (!selectedDatasets.length) return;
        if (!Array.isArray(mergeFields) || !mergeFields.length) return;

        complete();

        let cancelled = false;
        const run = async () => {
          const normalizedGeometryMode = normalizeMergeGeometryMode(geometryMode);
          const shouldDeriveGeometry =
            normalizedGeometryMode === 'auto' || normalizedGeometryMode === 'derive_from_latlon';
          const normalizedSourceCrs = normalizeCrsCode(sourceCrs);
          const normalizedOutputGeometryFieldKey = String(outputGeometryFieldKey || '')
            .trim()
            .toLowerCase();
          const normalizedFieldDefs = (mergeFields || [])
            .map((field: any) => ({
              name: String(field?.name || '').trim(),
              type: String(field?.type || ALL_FIELD_TYPES.string || 'string'),
              key: String(field?.key || '').trim() || normalizeFieldToken(field?.name),
              source: field?.source
            }))
            .filter((field: any) => Boolean(field.name) && Boolean(field.key));
          if (!normalizedFieldDefs.length) return;

          const mergeStats = {
            totalRows: 0,
            rowsWithGeometry: 0,
            rowsWithoutGeometry: 0,
            rowsWithDerivedGeometry: 0,
            rowsWithInvalidCoordinates: 0
          };
          const mergedRows: any[][] = [];
          for (const sourceDataset of selectedDatasets) {
            if (cancelled) return;

            const sourceFieldByKey = new Map<string, string>();
            const pointFieldPair = resolveDatasetPointFieldPair(sourceDataset, latitudeField, longitudeField);
            const datasetGeometryField = resolveGeojsonFieldName(sourceDataset, outputGeometryFieldName);
            (Array.isArray(sourceDataset?.fields) ? sourceDataset.fields : []).forEach((field: any) => {
              const fieldName = String(field?.name || '').trim();
              if (!fieldName) return;
              const key = normalizeFieldToken(fieldName);
              if (key && !sourceFieldByKey.has(key)) {
                sourceFieldByKey.set(key, fieldName);
              }
            });

            const rowIndexes = getDatasetIndexes(sourceDataset);
            for (let rowOffset = 0; rowOffset < rowIndexes.length; rowOffset += 1) {
              if (cancelled) return;
              const rowIdx = rowIndexes[rowOffset];
              mergeStats.totalRows += 1;
              let rowHasGeometry = false;
              let rowDerivedGeometry = false;
              const nextRow = normalizedFieldDefs.map((field: any) => {
                if (field.source === 'datasetName') {
                  return String(sourceDataset?.label || sourceDataset?.id || '');
                }
                if (field.source === 'datasetId') {
                  return String(sourceDataset?.id || '');
                }
                const isOutputGeometryField =
                  Boolean(normalizedOutputGeometryFieldKey) &&
                  String(field?.key || '').toLowerCase() === normalizedOutputGeometryFieldKey;
                if (isOutputGeometryField) {
                  let geometryValue: any = null;
                  const sourceFieldName = sourceFieldByKey.get(field.key) || datasetGeometryField;
                  if (sourceFieldName) {
                    const rawGeometry = sourceDataset.getValue(sourceFieldName, rowIdx);
                    geometryValue = normalizeGeojsonCellValue(rawGeometry);
                  }

                  if (!geometryValue && shouldDeriveGeometry) {
                    const latFieldName = pointFieldPair.latField;
                    const lonFieldName = pointFieldPair.lonField;
                    if (latFieldName && lonFieldName) {
                      const lat = parseCoordinateValue(sourceDataset.getValue(latFieldName, rowIdx));
                      const lon = parseCoordinateValue(sourceDataset.getValue(lonFieldName, rowIdx));
                      if (lat !== null && lon !== null && Number.isFinite(lat) && Number.isFinite(lon)) {
                        let outLon = lon;
                        let outLat = lat;
                        if (normalizedSourceCrs.toUpperCase() !== 'EPSG:4326') {
                          const transformed = convertPointToWgs84(lon, lat, normalizedSourceCrs);
                          if (transformed) {
                            outLon = Number(transformed[0]);
                            outLat = Number(transformed[1]);
                          } else {
                            mergeStats.rowsWithInvalidCoordinates += 1;
                            return null;
                          }
                        }
                        if (Math.abs(outLat) <= 90 && Math.abs(outLon) <= 180) {
                          geometryValue = {
                            type: 'Feature',
                            geometry: {
                              type: 'Point',
                              coordinates: [outLon, outLat]
                            },
                            properties: {}
                          };
                          rowDerivedGeometry = true;
                        } else {
                          mergeStats.rowsWithInvalidCoordinates += 1;
                        }
                      }
                    }
                  }
                  if (geometryValue) {
                    rowHasGeometry = true;
                  }
                  return geometryValue;
                }
                const sourceFieldName = sourceFieldByKey.get(field.key);
                if (!sourceFieldName) return null;
                return sourceDataset.getValue(sourceFieldName, rowIdx);
              });
              if (normalizedOutputGeometryFieldKey) {
                if (rowHasGeometry) {
                  mergeStats.rowsWithGeometry += 1;
                  if (rowDerivedGeometry) {
                    mergeStats.rowsWithDerivedGeometry += 1;
                  }
                } else {
                  mergeStats.rowsWithoutGeometry += 1;
                }
              }
              mergedRows.push(nextRow);
              if (rowOffset > 0 && rowOffset % defaultChunkSize === 0) {
                await yieldToMainThread();
              }
            }
          }

          if (cancelled) return;
          if (
            strictValidation &&
            normalizedOutputGeometryFieldKey &&
            mergeStats.totalRows > 0 &&
            mergeStats.rowsWithGeometry <= 0
          ) {
            console.warn(
              `mergeQMapDatasets strictValidation prevented output "${newDatasetName}": no valid geometry rows.`,
              mergeStats
            );
            return;
          }

          const outputDatasetPayload = {
            info: {
              id: newDatasetId,
              label: newDatasetName
            },
            data: {
              fields: normalizedFieldDefs.map((field: any) => ({name: field.name, type: field.type})),
              rows: mergedRows
            }
          };

          const existing = Object.values(localDatasets || {}).find(
            (dataset: any) => String(dataset?.label || '').toLowerCase() === String(newDatasetName || '').toLowerCase()
          ) as any;
          if (existing?.id) {
            localDispatch(
              wrapTo(
                'map',
                replaceDataInMap({
                  datasetToReplaceId: existing.id,
                  datasetToUse: outputDatasetPayload,
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
                  datasets: outputDatasetPayload as any,
                  options: {autoCreateLayers: showOnMap, centerMap: false}
                })
              )
            );
          }

          if (cancelled || !showOnMap || !showOnlyMergedLayer) return;

          let keepLayerIds = new Set<string>();
          for (let attempt = 0; attempt < 30; attempt += 1) {
            if (cancelled) return;
            const layers = (localStore.getState()?.demo?.keplerGl?.map?.visState?.layers || []) as any[];
            keepLayerIds = new Set(
              layers
                .filter((layer: any) => String(layer?.config?.dataId || '') === String(newDatasetId))
                .map((layer: any) => String(layer?.id || ''))
                .filter(Boolean)
            );
            if (keepLayerIds.size > 0) break;
            await new Promise(resolve => setTimeout(resolve, 120));
          }
          if (!keepLayerIds.size) return;

          const layers = (localStore.getState()?.demo?.keplerGl?.map?.visState?.layers || []) as any[];
          layers.forEach((layer: any) => {
            const layerId = String(layer?.id || '');
            const isVisible = keepLayerIds.has(layerId);
            localDispatch(wrapTo('map', layerConfigChange(layer, {isVisible})));
          });
        };

        run();
        return () => {
          cancelled = true;
        };
      }, [
        localDispatch,
        localStore,
        localDatasets,
        executionKey,
        sourceDatasetIds,
        mergeFields,
        newDatasetName,
        newDatasetId,
        showOnMap,
        showOnlyMergedLayer,
        geometryMode,
        latitudeField,
        longitudeField,
        sourceCrs,
        outputGeometryFieldName,
        outputGeometryFieldKey,
        strictValidation,
        shouldSkip,
        complete
      ]);

      return null;
    }
  });
}
