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
  return {
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
    execute: async ({datasetName, geometryField, areaFieldName, useActiveFilters, newDatasetName, showOnMap}: any) => {
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
  };
}
