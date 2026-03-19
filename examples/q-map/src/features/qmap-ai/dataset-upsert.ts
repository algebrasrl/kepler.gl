import {addDataToMap, replaceDataInMap, wrapTo} from '@kepler.gl/actions';
import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {H3AggregateRow} from '../../workers/h3-aggregate-core';
import {inferFieldTypeFromValue, mapIndexesChunked} from './dataset-utils';

export const DEFAULT_TASSELLATION_DATASET = 'Tassellation';

export async function upsertIntermediateDataset(
  dispatchFn: any,
  datasets: any,
  sourceDataset: any,
  rowIndexes: number[],
  targetName: string
) {
  const normalizedTargetName = String(targetName || 'Intermediate')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
  const generatedDatasetId = `qmap_intermediate_${normalizedTargetName || 'default'}`;

  const existingDataset = Object.values(datasets || {}).find(
    (d: any) => String(d?.label || '').toLowerCase() === String(targetName).toLowerCase()
  ) as any;

  const fields = (sourceDataset?.fields || []).map((f: any) => ({name: f.name, type: f.type}));
  const rows = await mapIndexesChunked(rowIndexes, (rowIdx: number) =>
    fields.map((f: any) => sourceDataset.getValue(f.name, rowIdx))
  );

  const datasetToUse = {
    info: {
      id: existingDataset?.id || generatedDatasetId,
      label: targetName
    },
    data: {
      fields,
      rows
    }
  };

  if (existingDataset?.id) {
    dispatchFn(
      wrapTo(
        'map',
        replaceDataInMap({
          datasetToReplaceId: existingDataset.id,
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
    dispatchFn(
      wrapTo(
        'map',
        addDataToMap({
          datasets: datasetToUse as any,
          options: {
            keepExistingConfig: true,
            centerMap: false,
            autoCreateLayers: false
          }
        }) as any
      )
    );
  }
}

export function upsertDerivedDatasetRows(
  dispatchFn: any,
  datasets: any,
  datasetName: string,
  rowsAsObjects: Array<Record<string, unknown>>,
  preferredIdPrefix = 'qmap_derived',
  autoCreateLayers = true
) {
  if (!rowsAsObjects.length) return;
  const {label: resolvedLabel, datasetId} = getDatasetInfoByLabel(datasets, datasetName, preferredIdPrefix);
  const existing = Object.values(datasets || {}).find(
    (d: any) => String(d?.label || '').toLowerCase() === String(resolvedLabel).toLowerCase()
  ) as any;
  const fieldNames = Array.from(
    rowsAsObjects.reduce((acc, row) => {
      Object.keys(row || {}).forEach(k => acc.add(k));
      return acc;
    }, new Set<string>())
  );
  const fields = fieldNames.map(name => {
    const sample = rowsAsObjects.find(row => row && row[name] !== undefined && row[name] !== null)?.[name];
    return {name, type: inferFieldTypeFromValue(sample)};
  });
  const rows = rowsAsObjects.map(row => fields.map(f => row?.[f.name] ?? null));
  const datasetToUse = {
    info: {
      id: existing?.id || datasetId,
      label: resolvedLabel
    },
    data: {
      fields,
      rows
    }
  };
  if (existing?.id) {
    dispatchFn(
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
    dispatchFn(
      wrapTo(
        'map',
        addDataToMap({
          datasets: datasetToUse as any,
          options: {autoCreateLayers, centerMap: false}
        }) as any
      )
    );
  }
}

export function normalizeDatasetIdSeed(label: string): string {
  return String(label || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

export function toWorkerSafeAggregateRows(rows: H3AggregateRow[]): H3AggregateRow[] {
  return (rows || []).map(row => {
    const safeRow: H3AggregateRow = {
      h3Id: row?.h3Id || null,
      value: row?.value ?? null,
      groupValues: row?.groupValues
    };
    if (row?.geometry === null || row?.geometry === undefined) {
      safeRow.geometry = null;
      return safeRow;
    }
    if (typeof row.geometry === 'string') {
      safeRow.geometry = row.geometry;
      return safeRow;
    }
    try {
      safeRow.geometry = JSON.stringify(row.geometry);
    } catch {
      safeRow.geometry = null;
    }
    return safeRow;
  });
}

export function getDatasetInfoByLabel(
  datasets: any,
  labelInput: string,
  idPrefix: string
): {label: string; datasetId: string; existingId: string | null} {
  const label = String(labelInput || '').trim() || 'dataset';
  const needle = label.toLowerCase();
  const existing = Object.values(datasets || {}).find(
    (d: any) => String(d?.label || '').toLowerCase() === needle
  ) as any;
  const normalized = normalizeDatasetIdSeed(label);
  const datasetId = existing?.id || `${idPrefix}_${normalized || 'default'}`;
  return {label, datasetId, existingId: existing?.id || null};
}

export function getTassellationDatasetInfo(
  targetName: string,
  datasets?: any
): {label: string; datasetId: string} {
  const {label, datasetId} = getDatasetInfoByLabel(
    datasets,
    String(targetName || DEFAULT_TASSELLATION_DATASET),
    'qmap_tassellation'
  );
  return {label, datasetId};
}

export function upsertTassellationDataset(
  dispatchFn: any,
  datasets: any,
  ids: string[],
  resolution: number,
  targetName: string,
  appendToExisting = false,
  autoCreateLayers = true
) {
  const {label: targetLabel, datasetId: generatedDatasetId} = getTassellationDatasetInfo(
    targetName || DEFAULT_TASSELLATION_DATASET,
    datasets
  );

  const existingDataset = Object.values(datasets || {}).find(
    (d: any) => String(d?.label || '').toLowerCase() === String(targetLabel).toLowerCase()
  ) as any;

  const existingRows: Array<[string, number]> = [];
  if (appendToExisting && existingDataset?.id) {
    const idx = Array.isArray(existingDataset.allIndexes)
      ? existingDataset.allIndexes
      : Array.from({length: Number(existingDataset.length || 0)}, (_, i) => i);
    idx.forEach((rowIdx: number) => {
      const id = existingDataset.getValue('h3_id', rowIdx);
      const res = existingDataset.getValue('h3_resolution', rowIdx);
      if (id) existingRows.push([String(id), Number.isFinite(Number(res)) ? Number(res) : resolution]);
    });
  }

  const seen = new Set(existingRows.map(r => r[0]));
  const mergedRows = [...existingRows];
  let addedCount = 0;
  ids.forEach(id => {
    if (!seen.has(id)) {
      seen.add(id);
      mergedRows.push([id, resolution]);
      addedCount += 1;
    }
  });

  // Avoid dispatch loops when effect reruns with unchanged tessellation output.
  if (existingDataset?.id && appendToExisting && addedCount === 0) {
    return;
  }

  const datasetToUse = {
    info: {
      id: existingDataset?.id || generatedDatasetId,
      label: targetLabel
    },
    data: {
      fields: [
        {name: 'h3_id', type: ALL_FIELD_TYPES.h3},
        {name: 'h3_resolution', type: ALL_FIELD_TYPES.integer}
      ],
      rows: mergedRows
    }
  };

  if (existingDataset?.id) {
    dispatchFn(
      wrapTo(
        'map',
        replaceDataInMap({
          datasetToReplaceId: existingDataset.id,
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
    dispatchFn(
      wrapTo(
        'map',
        addDataToMap({
          datasets: datasetToUse as any,
          options: {
            keepExistingConfig: true,
            centerMap: false,
            autoCreateLayers
          }
        }) as any
      )
    );
  }
}

export function upsertH3AggregationDataset(
  dispatchFn: any,
  datasets: any,
  targetName: string,
  rows: Array<Record<string, unknown>>,
  operations: Array<'count' | 'distinct_count' | 'sum' | 'avg' | 'min' | 'max'>,
  groupByFieldNames: string[] = [],
  valueFieldName?: string,
  autoCreateLayers = true
) {
  const {label: resolvedLabel, datasetId: generatedDatasetId} = getDatasetInfoByLabel(
    datasets,
    String(targetName || 'H3 Aggregation'),
    'qmap_h3_aggregation'
  );
  const existingDataset = Object.values(datasets || {}).find(
    (d: any) => String(d?.label || '').toLowerCase() === String(resolvedLabel).toLowerCase()
  ) as any;

  const opSet = new Set(operations || []);
  const groupFields = Array.from(
    new Set(
      (groupByFieldNames || [])
        .map(name => String(name || '').trim())
        .filter(Boolean)
        .filter(
          name =>
            !['h3_id', 'h3_resolution', 'count', 'count_weighted', 'distinct_count', 'sum', 'avg', 'min', 'max'].includes(
              name
            )
        )
    )
  );
  const inferFieldTypeFromRows = (fieldName: string): string => {
    for (const row of rows) {
      const value = row?.[fieldName];
      if (value !== null && value !== undefined) {
        return inferFieldTypeFromValue(value);
      }
    }
    return ALL_FIELD_TYPES.string;
  };
  const fields: Array<{name: string; type: string}> = [
    {name: 'h3_id', type: ALL_FIELD_TYPES.h3},
    {name: 'h3_resolution', type: ALL_FIELD_TYPES.integer},
    ...groupFields.map(name => ({name, type: inferFieldTypeFromRows(name)})),
    {name: 'count', type: ALL_FIELD_TYPES.integer},
    {name: 'count_weighted', type: ALL_FIELD_TYPES.real}
  ];
  if (opSet.has('distinct_count')) fields.push({name: 'distinct_count', type: ALL_FIELD_TYPES.integer});
  if (opSet.has('sum')) fields.push({name: 'sum', type: ALL_FIELD_TYPES.real});
  if (opSet.has('avg')) fields.push({name: 'avg', type: ALL_FIELD_TYPES.real});
  if (opSet.has('min')) fields.push({name: 'min', type: ALL_FIELD_TYPES.real});
  if (opSet.has('max')) fields.push({name: 'max', type: ALL_FIELD_TYPES.real});
  if (valueFieldName) fields.push({name: 'source_field', type: ALL_FIELD_TYPES.string});

  const rowsAsArrays = rows.map(row => fields.map(f => row[f.name]));
  const datasetToUse = {
    info: {
      id: existingDataset?.id || generatedDatasetId,
      label: resolvedLabel
    },
    data: {
      fields,
      rows: rowsAsArrays
    }
  };

  if (existingDataset?.id) {
    dispatchFn(
      wrapTo(
        'map',
        replaceDataInMap({
          datasetToReplaceId: existingDataset.id,
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
    dispatchFn(
      wrapTo(
        'map',
        addDataToMap({
          datasets: datasetToUse as any,
          options: {
            keepExistingConfig: true,
            centerMap: false,
            autoCreateLayers
          }
        }) as any
      )
    );
  }
}
