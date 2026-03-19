import {useEffect} from 'react';
import {addDataToMap, replaceDataInMap, setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {extendedTool} from '../tool-shim';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapDatasets, selectQMapVisState} from '../../../state/qmap-selectors';
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

export function createDatasetWithGeometryAreaTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    getFilteredDatasetIndexes,
    getDatasetInfoByLabel,
    makeExecutionKey,
    parseGeoJsonLike,
    toTurfPolygonFeature,
    turfArea,
    executedToolComponentKeys,
    rememberExecutedToolComponentKey
  } = ctx;
  return extendedTool({
    description:
      'Create a derived dataset adding polygon area (m2) computed from GeoJSON geometry. Useful before area-based choropleth.',
    parameters: z.object({
      datasetName: z.string().describe('Exact source dataset name'),
      geometryField: z.string().optional().describe('GeoJSON field (default: auto-detect)'),
      areaFieldName: z.string().optional().describe('Output area field name (default: area_m2)'),
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
    execute: async ({datasetName, geometryField, areaFieldName, useActiveFilters, newDatasetName, showOnMap}) => {
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
      const resolvedGeometryField =
        resolveDatasetFieldName(sourceDataset, String(geometryField || '_geojson')) ||
        (sourceDataset.fields || []).find((f: any) => String(f?.type || '').toLowerCase() === 'geojson')?.name ||
        null;
      if (!resolvedGeometryField) {
        return {
          llmResult: {
            success: false,
            details: `No GeoJSON field found in dataset "${sourceDataset.label || sourceDataset.id}".`
          }
        };
      }

      const outputAreaField = String(areaFieldName || 'area_m2').trim() || 'area_m2';
      const alreadyExists = (sourceDataset.fields || []).some(
        (f: any) => String(f?.name || '').toLowerCase() === outputAreaField.toLowerCase()
      );
      if (alreadyExists) {
        return {
          llmResult: {
            success: false,
            details:
              `Field "${outputAreaField}" already exists in dataset "${sourceDataset.label || sourceDataset.id}". ` +
              'Use setQMapLayerColorByField directly on that field or choose a different areaFieldName.'
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
              `No rows available to derive area for dataset "${sourceDataset.label || sourceDataset.id}". ` +
              (applyFilters ? 'Active filters may be too restrictive.' : '')
          }
        };
      }

      const targetName = String(newDatasetName || '').trim() || `${sourceDataset.label || sourceDataset.id}_with_area`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        currentVisState?.datasets || {},
        targetName,
        'qmap_area'
      );
      const fieldCatalog = Array.from(
        new Set([
          ...((sourceDataset.fields || []).map((field: any) => String(field?.name || '').trim()).filter(Boolean) as string[]),
          outputAreaField
        ])
      );
      return {
        llmResult: {
          success: true,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          fieldCatalog,
          numericFields: [outputAreaField],
          styleableFields: [outputAreaField],
          defaultStyleField: outputAreaField,
          outputAreaField,
          details:
            `Creating dataset "${resolvedTargetLabel}" with derived field "${outputAreaField}" from geometry field "${resolvedGeometryField}" ` +
            `(${matchedIndices.length} rows${applyFilters ? ', active filters applied' : ''}).` +
            `${showOnMap === true ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('create-dataset-with-geometry-area'),
          sourceDatasetId: sourceDataset.id,
          rowIndices: matchedIndices,
          geometryField: resolvedGeometryField,
          areaFieldName: outputAreaField,
          fieldCatalog,
          numericFields: [outputAreaField],
          styleableFields: [outputAreaField],
          defaultStyleField: outputAreaField,
          outputAreaField,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId,
          showOnMap: showOnMap === true
        }
      };
    },
    component: function CreateDatasetWithGeometryAreaComponent({
      executionKey,
      sourceDatasetId,
      rowIndices,
      geometryField,
      areaFieldName,
      newDatasetName,
      newDatasetId,
      showOnMap
    }: {
      executionKey?: string;
      sourceDatasetId: string;
      rowIndices: number[];
      geometryField: string;
      areaFieldName: string;
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
        const datasetFields = [...sourceFields, {name: areaFieldName, type: ALL_FIELD_TYPES.real}];
        const rows = (rowIndices || []).map((rowIdx: number) => {
          const baseValues = sourceFields.map((f: any) => sourceDataset.getValue(f.name, rowIdx));
          const parsed = parseGeoJsonLike(sourceDataset.getValue(geometryField, rowIdx));
          const feature = toTurfPolygonFeature(parsed);
          const areaM2 = feature ? Number(turfArea(feature as any)) : null;
          return [...baseValues, Number.isFinite(areaM2 as number) ? areaM2 : null];
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
        geometryField,
        areaFieldName,
        newDatasetName,
        newDatasetId,
        showOnMap,
        shouldSkip,
        complete
      ]);
      return null;
    }
  });
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
  return extendedTool({
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
    }) => {
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
  });
}

export function createReprojectQMapDatasetCrsTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveGeojsonFieldName,
    resolveDatasetFieldName,
    getDatasetInfoByLabel,
    makeExecutionKey,
    proj4Transform,
    shouldSkipToolComponentByExecutionKey,
    executedToolComponentKeys,
    rememberToolComponentExecutionKey,
    rememberExecutedToolComponentKey,
    getFilteredDatasetIndexes,
    parseGeoJsonLike,
    reprojectGeoJsonLike,
    parseCoordinateValue,
    shouldUseLoadingIndicator,
    runReprojectJob,
    upsertDerivedDatasetRows
  } = ctx;
  return extendedTool({
    description:
      'Reproject dataset geometry/coordinates from one CRS to another and materialize a new dataset.',
    parameters: z.object({
      datasetName: z.string().describe('Source dataset name'),
      sourceCrs: z.string().describe('Source CRS, e.g. EPSG:3857 or EPSG:32632'),
      targetCrs: z.string().describe('Target CRS, e.g. EPSG:4326'),
      geometryField: z.string().optional().describe('GeoJSON field to reproject (default: auto)'),
      latitudeField: z.string().optional().describe('Optional latitude field to transform'),
      longitudeField: z.string().optional().describe('Optional longitude field to transform'),
      outputGeometryField: z.string().optional().describe('Output geometry field name (default: same field)'),
      outputLatitudeField: z.string().optional().describe('Output latitude field name (default: lat_reprojected)'),
      outputLongitudeField: z.string().optional().describe('Output longitude field name (default: lon_reprojected)'),
      useActiveFilters: z
        .boolean()
        .optional()
        .describe('Apply active UI filters before reprojection (default true)'),
      showOnMap: z
        .boolean()
        .optional()
        .describe('Default false. Set true to auto-create a map layer for the output dataset.'),
      newDatasetName: z.string().optional().describe('Target dataset label')
    }),
    execute: async ({
      datasetName,
      sourceCrs,
      targetCrs,
      geometryField,
      latitudeField,
      longitudeField,
      outputGeometryField,
      outputLatitudeField,
      outputLongitudeField,
      useActiveFilters,
      showOnMap,
      newDatasetName
    }) => {
      const vis = getCurrentVisState();
      const source = resolveDatasetByName(vis?.datasets || {}, datasetName);
      if (!source) {
        return {llmResult: {success: false, details: `Dataset not found: ${datasetName}`}};
      }
      const sourceCode = String(sourceCrs || '').trim();
      const targetCode = String(targetCrs || '').trim();
      if (!sourceCode || !targetCode) {
        return {llmResult: {success: false, details: 'sourceCrs and targetCrs are required.'}};
      }
      try {
        proj4Transform(sourceCode, targetCode);
      } catch (error: any) {
        return {
          llmResult: {
            success: false,
            details: `Invalid CRS transformation ${sourceCode} -> ${targetCode}: ${String(
              error?.message || error
            )}`
          }
        };
      }

      const resolvedGeometryField = resolveGeojsonFieldName(source, geometryField);
      const resolvedLatField = latitudeField ? resolveDatasetFieldName(source, latitudeField) : null;
      const resolvedLonField = longitudeField ? resolveDatasetFieldName(source, longitudeField) : null;
      if (!resolvedGeometryField && !(resolvedLatField && resolvedLonField)) {
        return {
          llmResult: {
            success: false,
            details: 'No transformable fields found. Provide geometryField or both latitudeField and longitudeField.'
          }
        };
      }

      const outGeomFieldName = String(outputGeometryField || resolvedGeometryField || '_geojson');
      const outLatFieldName = String(outputLatitudeField || 'lat_reprojected');
      const outLonFieldName = String(outputLongitudeField || 'lon_reprojected');
      const targetName =
        String(newDatasetName || '').trim() || `${source.label || source.id}_${targetCode.replace(/[^a-zA-Z0-9]+/g, '_')}`;
      const {label: resolvedTargetLabel, datasetId: resolvedTargetDatasetId} = getDatasetInfoByLabel(
        vis?.datasets || {},
        targetName,
        'qmap_reproject'
      );
      return {
        llmResult: {
          success: true,
          sourceDataset: source.label || source.id,
          sourceCrs: sourceCode,
          targetCrs: targetCode,
          outputDatasetName: resolvedTargetLabel,
          dataset: resolvedTargetLabel,
          datasetId: resolvedTargetDatasetId,
          details:
            `Reprojecting "${source.label || source.id}" from ${sourceCode} to ${targetCode}.` +
            `${showOnMap === true ? '' : ' Output dataset will be created without auto layer.'}`
        },
        additionalData: {
          executionKey: makeExecutionKey('reproject-qmap-dataset-crs'),
          sourceDatasetId: source.id,
          sourceCrs: sourceCode,
          targetCrs: targetCode,
          geometryField: resolvedGeometryField,
          latitudeField: resolvedLatField,
          longitudeField: resolvedLonField,
          outputGeometryField: outGeomFieldName,
          outputLatitudeField: outLatFieldName,
          outputLongitudeField: outLonFieldName,
          useActiveFilters: useActiveFilters !== false,
          showOnMap: showOnMap === true,
          newDatasetName: resolvedTargetLabel,
          newDatasetId: resolvedTargetDatasetId
        }
      };
    },
    component: function ReprojectQMapDatasetCrsComponent({
      executionKey,
      sourceDatasetId,
      sourceCrs,
      targetCrs,
      geometryField,
      latitudeField,
      longitudeField,
      outputGeometryField,
      outputLatitudeField,
      outputLongitudeField,
      useActiveFilters,
      showOnMap,
      newDatasetName
    }: {
      executionKey?: string;
      sourceDatasetId: string;
      sourceCrs: string;
      targetCrs: string;
      geometryField: string | null;
      latitudeField: string | null;
      longitudeField: string | null;
      outputGeometryField: string;
      outputLatitudeField: string;
      outputLongitudeField: string;
      useActiveFilters: boolean;
      showOnMap: boolean;
      newDatasetName: string;
    }) {
      const localDispatch = useDispatch<any>();
      const localVisState = useSelector(selectQMapVisState);
      const localDatasets = localVisState?.datasets || {};

      useEffect(() => {
        if (
          shouldSkipToolComponentByExecutionKey({
            executionKey,
            executedToolComponentKeys
          })
        ) {
          return;
        }
        const source = localDatasets?.[sourceDatasetId];
        if (!source?.id) return;

        const indexes = getFilteredDatasetIndexes(source, localVisState, useActiveFilters);
        if (!indexes.length) return;
        const sourceFields = Array.isArray(source.fields) ? source.fields.map((f: any) => String(f?.name || '')) : [];
        const baseRows = indexes.map((rowIdx: number) => {
          const row: Record<string, unknown> = {};
          sourceFields.forEach((fieldName: string) => {
            row[fieldName] = source.getValue(fieldName, rowIdx);
          });
          return row;
        });

        const runFallbackLocal = () => {
          const outRows: Array<Record<string, unknown>> = [];
          let transformedGeometryRows = 0;
          let transformedCoordinateRows = 0;
          const forward = proj4Transform(sourceCrs, targetCrs).forward as (xy: [number, number]) => [number, number];

          baseRows.forEach((row: Record<string, unknown>) => {
            const out = {...row};
            if (geometryField) {
              const parsed = parseGeoJsonLike(out[geometryField]);
              if (parsed) {
                const transformed = reprojectGeoJsonLike(parsed, sourceCrs, targetCrs);
                if (transformed) {
                  out[outputGeometryField] = transformed;
                  transformedGeometryRows += 1;
                }
              }
            }
            if (latitudeField && longitudeField) {
              const lat = parseCoordinateValue(out[latitudeField]);
              const lon = parseCoordinateValue(out[longitudeField]);
              if (lat !== null && lon !== null) {
                try {
                  const [x2, y2] = forward([lon, lat]);
                  if (Number.isFinite(x2) && Number.isFinite(y2)) {
                    out[outputLongitudeField] = x2;
                    out[outputLatitudeField] = y2;
                    transformedCoordinateRows += 1;
                  }
                } catch {
                  // skip invalid coordinate transformation
                }
              }
            }
            outRows.push(out);
          });

          return {rows: outRows, transformedGeometryRows, transformedCoordinateRows, mode: 'fallback-local'};
        };

        let cancelled = false;
        const useLoadingIndicator = shouldUseLoadingIndicator();
        rememberToolComponentExecutionKey({executionKey, rememberExecutedToolComponentKey});
        if (useLoadingIndicator) {
          localDispatch(wrapTo('map', setLoadingIndicator({change: 1}) as any));
        }

        (async () => {
          try {
            const workerPreferred = typeof Worker !== 'undefined' && baseRows.length >= 250;
            let result:
              | {
                  rows: Array<Record<string, unknown>>;
                  transformedGeometryRows: number;
                  transformedCoordinateRows: number;
                  mode: string;
                }
              | null = null;

            if (workerPreferred) {
              try {
                const workerRes = await runReprojectJob({
                  payload: {
                    rows: baseRows,
                    sourceCrs,
                    targetCrs,
                    geometryField,
                    outputGeometryField,
                    latitudeField,
                    longitudeField,
                    outputLatitudeField,
                    outputLongitudeField
                  },
                  timeoutMs: 240000
                });
                result = {...workerRes, mode: 'worker'};
              } catch {
                result = runFallbackLocal();
              }
            } else {
              result = runFallbackLocal();
            }

            if (cancelled || !result || !result.rows.length) return;
            upsertDerivedDatasetRows(localDispatch, localDatasets, newDatasetName, result.rows, 'qmap_reproject', showOnMap);
          } finally {
            if (useLoadingIndicator) {
              localDispatch(wrapTo('map', setLoadingIndicator({change: -1}) as any));
            }
          }
        })();

        return () => {
          cancelled = true;
        };
      }, [
        localDispatch,
        localVisState,
        localDatasets,
        executionKey,
        sourceDatasetId,
        sourceCrs,
        targetCrs,
        geometryField,
        latitudeField,
        longitudeField,
        outputGeometryField,
        outputLatitudeField,
        outputLongitudeField,
        useActiveFilters,
        showOnMap,
        newDatasetName
      ]);

      return null;
    }
  });
}

// ─── Dataset Delta Tool ───────────────────────────────────────────────────────

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

  return extendedTool({
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
    }) => {
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
  });
}

// ─── Add Computed Field Tool ──────────────────────────────────────────────────

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

  return extendedTool({
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
    execute: async ({datasetName, expression, outputFieldName, useActiveFilters, newDatasetName, showOnMap}) => {
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
  });
}
