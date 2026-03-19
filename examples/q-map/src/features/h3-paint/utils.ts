import {cellArea, latLngToCell, getBaseCellNumber, isPentagon, isResClassIII, cellToLatLng} from 'h3-js-v4';
import {addDataToMap, replaceDataInMap, wrapTo} from '@kepler.gl/actions';
import {ALL_FIELD_TYPES} from '@kepler.gl/constants';

const KEPLER_INSTANCE_ID = 'map';
export const H3_PAINT_DATASET_LABEL_PREFIX = 'Hex_Paint_r';
export const H3_PAINT_DATASET_ID_PREFIX = 'qmap_hex_paint_r';
export const H3_PAINT_FIELDS = [
  {name: 'h3_id', type: ALL_FIELD_TYPES.h3},
  {name: 'h3_resolution', type: ALL_FIELD_TYPES.integer},
  {name: 'h3_lat', type: ALL_FIELD_TYPES.real},
  {name: 'h3_lng', type: ALL_FIELD_TYPES.real},
  {name: 'h3_area_km2', type: ALL_FIELD_TYPES.real},
  {name: 'h3_base_cell', type: ALL_FIELD_TYPES.integer},
  {name: 'h3_is_pentagon', type: ALL_FIELD_TYPES.boolean},
  {name: 'h3_is_class_iii', type: ALL_FIELD_TYPES.boolean}
] as const;

export type H3PaintRow = [string, number, number, number, number, number, boolean, boolean];

export function getH3PaintDataset(datasets: Record<string, any>, resolution: number) {
  const targetLabel = `${H3_PAINT_DATASET_LABEL_PREFIX}${resolution}`;
  const targetId = `${H3_PAINT_DATASET_ID_PREFIX}${resolution}`;
  return Object.values(datasets || {}).find((dataset: any) => {
    const label = String(dataset?.label || '').toLowerCase();
    const id = String(dataset?.id || '').toLowerCase();
    return label === targetLabel.toLowerCase() || id === targetId.toLowerCase();
  }) as any;
}

export function readH3Rows(existingDataset: any, fallbackResolution: number): Array<[string, number]> {
  if (!existingDataset?.id) return [];
  const indexes = Array.isArray(existingDataset.allIndexes)
    ? existingDataset.allIndexes
    : Array.from({length: Number(existingDataset.length || 0)}, (_, i) => i);
  const rows: Array<[string, number]> = [];
  indexes.forEach((rowIdx: number) => {
    const id = existingDataset.getValue('h3_id', rowIdx);
    const resolution = existingDataset.getValue('h3_resolution', rowIdx);
    if (!id) return;
    rows.push([String(id), Number.isFinite(Number(resolution)) ? Number(resolution) : fallbackResolution]);
  });
  return rows;
}

function safeNumber(value: any): number | null {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function buildH3PaintRow(
  h3Id: string,
  resolution: number,
  partial?: {
    lat?: any;
    lng?: any;
    areaKm2?: any;
    baseCell?: any;
    isPentagon?: any;
    isClassIII?: any;
  }
): H3PaintRow {
  const [computedLat, computedLng] = cellToLatLng(h3Id);
  const computedAreaKm2 = Number(cellArea(h3Id, 'km2'));
  const computedBaseCell = Number(getBaseCellNumber(h3Id));
  const computedPentagon = Boolean(isPentagon(h3Id));
  const computedClassIII = Boolean(isResClassIII(h3Id));

  const lat = safeNumber(partial?.lat) ?? Number(computedLat);
  const lng = safeNumber(partial?.lng) ?? Number(computedLng);
  const areaKm2 = safeNumber(partial?.areaKm2) ?? computedAreaKm2;
  const baseCell = safeNumber(partial?.baseCell) ?? computedBaseCell;
  const isPentagonValue = typeof partial?.isPentagon === 'boolean' ? partial.isPentagon : computedPentagon;
  const isClassIIIValue = typeof partial?.isClassIII === 'boolean' ? partial.isClassIII : computedClassIII;

  return [h3Id, resolution, lat, lng, areaKm2, baseCell, isPentagonValue, isClassIIIValue];
}

export function readH3PaintRows(existingDataset: any, fallbackResolution: number): H3PaintRow[] {
  if (!existingDataset?.id) return [];
  const indexes = Array.isArray(existingDataset.allIndexes)
    ? existingDataset.allIndexes
    : Array.from({length: Number(existingDataset.length || 0)}, (_, i) => i);
  const rows: H3PaintRow[] = [];
  indexes.forEach((rowIdx: number) => {
    const id = String(existingDataset.getValue('h3_id', rowIdx) || '').trim();
    if (!id) return;
    const resolution = Number(existingDataset.getValue('h3_resolution', rowIdx));
    rows.push(
      buildH3PaintRow(id, Number.isFinite(resolution) ? resolution : fallbackResolution, {
        lat: existingDataset.getValue('h3_lat', rowIdx),
        lng: existingDataset.getValue('h3_lng', rowIdx),
        areaKm2: existingDataset.getValue('h3_area_km2', rowIdx),
        baseCell: existingDataset.getValue('h3_base_cell', rowIdx),
        isPentagon: existingDataset.getValue('h3_is_pentagon', rowIdx),
        isClassIII: existingDataset.getValue('h3_is_class_iii', rowIdx)
      })
    );
  });
  return rows;
}

export function upsertH3PaintHex({
  dispatch,
  datasets,
  resolution,
  lng,
  lat
}: {
  dispatch: any;
  datasets: Record<string, any>;
  resolution: number;
  lng: number;
  lat: number;
}) {
  if (!Number.isFinite(lng) || !Number.isFinite(lat)) return;
  const hexId = latLngToCell(lat, lng, resolution);
  if (!hexId) return;

  const existingDataset = getH3PaintDataset(datasets, resolution);
  const existingRows = readH3PaintRows(existingDataset, resolution);
  const seen = new Set(existingRows.map(row => row[0]));
  if (seen.has(hexId)) return;

  const targetLabel = `${H3_PAINT_DATASET_LABEL_PREFIX}${resolution}`;
  const targetId = `${H3_PAINT_DATASET_ID_PREFIX}${resolution}`;
  const datasetToUse = {
    info: {
      id: existingDataset?.id || targetId,
      label: targetLabel
    },
    data: {
      fields: H3_PAINT_FIELDS as any,
      rows: [...existingRows, buildH3PaintRow(hexId, resolution)]
    }
  };

  if (existingDataset?.id) {
    dispatch(
      wrapTo(
        KEPLER_INSTANCE_ID,
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
    return;
  }

  dispatch(
    wrapTo(
      KEPLER_INSTANCE_ID,
      addDataToMap({
        datasets: datasetToUse as any,
        options: {
          keepExistingConfig: true,
          centerMap: false,
          autoCreateLayers: true
        }
      }) as any
    )
  );
}
