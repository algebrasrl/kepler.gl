import {useEffect} from 'react';
import {setLoadingIndicator, wrapTo} from '@kepler.gl/actions';
import {useDispatch, useSelector} from 'react-redux';
import {z} from 'zod';

import {selectQMapVisState} from '../../../state/qmap-selectors';
import type {QMapToolContext} from '../context/tool-context';

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
  return {
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
    }: any) => {
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
  };
}
