import {addDataToMap, layerConfigChange, replaceDataInMap, wrapTo} from '@kepler.gl/actions';
import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {area as turfArea, length as turfLength, polygonToLine} from '@turf/turf';
import type {AnyAction, Dispatch} from 'redux';
import {
  getQMapDrawDatasetConfig,
  isQMapDrawTarget,
  QMAP_DRAW_DRAFT_PROPERTY,
  QMAP_DRAW_TARGET_PROPERTY,
  QMAP_DRAW_TOOL_PROPERTY,
  type QMapDrawTarget,
  type QMapDrawTool
} from './constants';
import {selectQMapDatasets, selectQMapEditorFeatures, selectQMapLayers} from '../../state/qmap-selectors';
import type {QMapRootState} from '../../state/qmap-state-types';

const QMAP_DRAW_DATASET_FIELDS = [
  {name: '_geojson', type: ALL_FIELD_TYPES.geojson},
  {name: 'draw_target', type: ALL_FIELD_TYPES.string},
  {name: 'draw_tool', type: ALL_FIELD_TYPES.string},
  {name: 'feature_id', type: ALL_FIELD_TYPES.string},
  {name: 'geometry_type', type: ALL_FIELD_TYPES.string},
  {name: 'vertex_count', type: ALL_FIELD_TYPES.integer},
  {name: 'length_m', type: ALL_FIELD_TYPES.real},
  {name: 'perimeter_m', type: ALL_FIELD_TYPES.real},
  {name: 'area_m2', type: ALL_FIELD_TYPES.real}
];

const QMAP_DRAW_TARGET_COLORS: Record<QMapDrawTarget, [number, number, number]> = {
  stressor: [116, 210, 255],
  operations: [255, 165, 0]
};

export const QMAP_LINE_DOUBLE_CLICK_WINDOW_MS = 300;
export const QMAP_LINE_DOUBLE_CLICK_DISTANCE_PX = 12;

export type QMapLineLastClick = {
  time: number;
  lng: number;
  lat: number;
  x: number | null;
  y: number | null;
};

type QMapDrawRuntimeStore = {
  getState: () => QMapRootState;
  dispatch: Dispatch<AnyAction>;
};

export function createQMapLineLastClickState(): Record<QMapDrawTarget, QMapLineLastClick | null> {
  return {
    stressor: null,
    operations: null
  };
}

export function isDraftLineFeature(feature: any, target?: QMapDrawTarget): boolean {
  if (!feature || getDrawToolFromFeature(feature) !== 'line') {
    return false;
  }
  if (target && getDrawTargetFromFeature(feature) !== target) {
    return false;
  }
  return Boolean(feature?.properties?.[QMAP_DRAW_DRAFT_PROPERTY]);
}

export function getWrappedPayload(action: any) {
  return action?.payload && action?.payload?.meta?._id_ ? action.payload : action;
}

export function getWrappedActionInstanceId(action: any): string | null {
  const payload = getWrappedPayload(action);
  const instanceId = String(payload?.meta?._id_ || '').trim();
  return instanceId || null;
}

export function getDrawTargetFromFeature(feature: any): QMapDrawTarget | null {
  const target = feature?.properties?.[QMAP_DRAW_TARGET_PROPERTY];
  return isQMapDrawTarget(target) ? target : null;
}

export function getDrawToolFromFeature(feature: any): QMapDrawTool | null {
  const tool = String(feature?.properties?.[QMAP_DRAW_TOOL_PROPERTY] || '').trim();
  if (tool === 'point' || tool === 'line' || tool === 'polygon' || tool === 'rectangle' || tool === 'radius') {
    return tool;
  }
  return null;
}

export function resolveDrawTool(value: unknown): QMapDrawTool | null {
  const tool = String(value || '').trim();
  if (tool === 'point' || tool === 'line' || tool === 'polygon' || tool === 'rectangle' || tool === 'radius') {
    return tool;
  }
  return null;
}

export function parseQMapDrawDatasetId(
  dataId: unknown
): {target: QMapDrawTarget; tool: QMapDrawTool} | null {
  const normalized = String(dataId || '').trim();
  const newFormatMatch = normalized.match(
    /^(stressor_perimeter|stressor_operations)__(point|line|polygon|rectangle|radius)$/
  );
  if (!newFormatMatch) {
    return null;
  }
  const target: QMapDrawTarget = newFormatMatch[1] === 'stressor_perimeter' ? 'stressor' : 'operations';
  const tool = resolveDrawTool(newFormatMatch[2]);
  if (!target || !tool) {
    return null;
  }
  return {target, tool};
}

export function withDrawProperties(feature: any, target: QMapDrawTarget | null, tool: QMapDrawTool | null) {
  if (!feature || !target || !tool) {
    return feature;
  }
  if (getDrawTargetFromFeature(feature) === target && getDrawToolFromFeature(feature) === tool) {
    return feature;
  }
  return {
    ...feature,
    properties: {
      ...(feature.properties || {}),
      [QMAP_DRAW_TARGET_PROPERTY]: target,
      [QMAP_DRAW_TOOL_PROPERTY]: tool
    }
  };
}

function roundMetric(value: number | null): number | null {
  if (!Number.isFinite(Number(value))) {
    return null;
  }
  return Math.round(Number(value) * 100) / 100;
}

function countGeometryVertices(geometry: any): number | null {
  const type = String(geometry?.type || '');
  const coords = geometry?.coordinates;
  if (!Array.isArray(coords)) {
    return null;
  }

  if (type === 'Point') return 1;
  if (type === 'MultiPoint' || type === 'LineString') return coords.length;
  if (type === 'Polygon') {
    return coords.reduce((acc: number, ring: any) => acc + (Array.isArray(ring) ? ring.length : 0), 0);
  }
  if (type === 'MultiLineString') {
    return coords.reduce((acc: number, line: any) => acc + (Array.isArray(line) ? line.length : 0), 0);
  }
  if (type === 'MultiPolygon') {
    return coords.reduce((acc: number, poly: any) => {
      if (!Array.isArray(poly)) return acc;
      return acc + poly.reduce((ringAcc: number, ring: any) => ringAcc + (Array.isArray(ring) ? ring.length : 0), 0);
    }, 0);
  }
  return null;
}

function computeFeatureSpatialMetrics(geometry: any) {
  const geometryType = String(geometry?.type || '');
  const feature = {
    type: 'Feature',
    geometry,
    properties: {}
  } as any;
  let lengthMeters: number | null = null;
  let perimeterMeters: number | null = null;
  let areaSquareMeters: number | null = null;

  try {
    if (geometryType === 'LineString' || geometryType === 'MultiLineString') {
      const km = Number(turfLength(feature, {units: 'kilometers'}));
      if (Number.isFinite(km)) {
        lengthMeters = km * 1000;
      }
    } else if (geometryType === 'Polygon' || geometryType === 'MultiPolygon') {
      const area = Number(turfArea(feature));
      if (Number.isFinite(area)) {
        areaSquareMeters = area;
      }
      const border = polygonToLine(feature as any) as any;
      if (border?.type === 'FeatureCollection') {
        const km = (border.features || []).reduce((acc: number, lineFeature: any) => {
          const len = Number(turfLength(lineFeature, {units: 'kilometers'}));
          return Number.isFinite(len) ? acc + len : acc;
        }, 0);
        perimeterMeters = km * 1000;
      } else {
        const km = Number(turfLength(border, {units: 'kilometers'}));
        if (Number.isFinite(km)) {
          perimeterMeters = km * 1000;
        }
      }
    }
  } catch {
    // keep null metrics when geometry computation fails
  }

  return {
    geometryType: geometryType || null,
    vertexCount: countGeometryVertices(geometry),
    lengthMeters: roundMetric(lengthMeters),
    perimeterMeters: roundMetric(perimeterMeters),
    areaSquareMeters: roundMetric(areaSquareMeters)
  };
}

function getDatasetFieldIndexByName(dataset: any, fieldName: string): number {
  const fields = Array.isArray(dataset?.fields) ? dataset.fields : [];
  const byFieldIdx = fields.find(
    (field: any) =>
      String(field?.name || '').trim() === fieldName &&
      Number.isFinite(Number(field?.fieldIdx))
  );
  if (byFieldIdx) {
    return Number(byFieldIdx.fieldIdx);
  }
  return fields.findIndex((field: any) => String(field?.name || '').trim() === fieldName);
}

function readDatasetRows(dataset: any): any[][] {
  const dataContainer = dataset?.dataContainer;
  if (
    !dataContainer ||
    typeof dataContainer?.numRows !== 'function' ||
    typeof dataContainer?.rowAsArray !== 'function'
  ) {
    return [];
  }
  const rows: any[][] = [];
  const total = Number(dataContainer.numRows());
  for (let index = 0; index < total; index++) {
    const row = dataContainer.rowAsArray(index);
    if (Array.isArray(row)) {
      rows.push([...row]);
    }
  }
  return rows;
}

function applyDefaultDrawLayerStyle(
  store: QMapDrawRuntimeStore,
  target: QMapDrawTarget,
  datasetId: string,
  keplerInstanceId: string
) {
  const layerList = selectQMapLayers(store.getState());
  const color = QMAP_DRAW_TARGET_COLORS[target];
  layerList.forEach((layer: any) => {
    const dataId = layer?.config?.dataId;
    const matchesDataset = Array.isArray(dataId)
      ? dataId.includes(datasetId)
      : String(dataId || '') === datasetId;
    if (!matchesDataset) {
      return;
    }
    const nextVisConfig = {
      ...(layer?.config?.visConfig || {}),
      strokeColor: color,
      fillColor: color
    };
    store.dispatch(
      wrapTo(
        keplerInstanceId,
        layerConfigChange(layer, {
          color,
          visConfig: nextVisConfig
        }) as any
      )
    );
  });
}

function setLayerVisibilityIfNeeded(
  store: QMapDrawRuntimeStore,
  layer: any,
  isVisible: boolean,
  keplerInstanceId: string
) {
  const current = Boolean(layer?.config?.isVisible);
  if (current === isVisible) {
    return;
  }
  store.dispatch(
    wrapTo(
      keplerInstanceId,
      layerConfigChange(layer, {
        isVisible
      }) as any
    )
  );
}

export function syncDrawDatasetLayerVisibility(store: QMapDrawRuntimeStore, keplerInstanceId: string) {
  const layers = selectQMapLayers(store.getState());
  layers.forEach((layer: any) => {
    const dataId = layer?.config?.dataId;
    const dataIds = Array.isArray(dataId) ? dataId : [dataId];
    const parsedTargets = dataIds
      .map((id: any) => parseQMapDrawDatasetId(id)?.target || null)
      .filter((target: QMapDrawTarget | null): target is QMapDrawTarget => Boolean(target));

    if (!parsedTargets.length) {
      return;
    }
    setLayerVisibilityIfNeeded(store, layer, true, keplerInstanceId);
  });
}

function syncDrawDatasetByTool(
  store: QMapDrawRuntimeStore,
  target: QMapDrawTarget,
  tool: QMapDrawTool,
  keplerInstanceId: string
) {
  const state = store.getState();
  const datasets = selectQMapDatasets(state);
  const features = selectQMapEditorFeatures(state);
  const datasetConfig = getQMapDrawDatasetConfig(target, tool);
  const existingDataset = datasets?.[datasetConfig.id] || null;
  const incomingRows = features
    .filter(
      (feature: any) =>
        getDrawTargetFromFeature(feature) === target &&
        getDrawToolFromFeature(feature) === tool &&
        feature?.geometry &&
        !isDraftLineFeature(feature, target)
    )
    .map((feature: any) => {
      const geometry = feature.geometry;
      const metrics = computeFeatureSpatialMetrics(geometry);
      return [
        geometry,
        datasetConfig.targetLabel,
        datasetConfig.toolLabel,
        String(feature?.id || ''),
        metrics.geometryType,
        metrics.vertexCount,
        metrics.lengthMeters,
        metrics.perimeterMeters,
        metrics.areaSquareMeters
      ];
    });

  if (!existingDataset?.id && incomingRows.length === 0) {
    return;
  }

  let rows = incomingRows;
  if (existingDataset?.id) {
    const existingRows = readDatasetRows(existingDataset);
    const featureIdFieldIndex = (() => {
      const idx = getDatasetFieldIndexByName(existingDataset, 'feature_id');
      return idx >= 0 ? idx : 3;
    })();
    const mergedByFeatureId = new Map<string, any[]>();
    const rowsWithoutFeatureId: any[][] = [];

    const upsertRow = (row: any[]) => {
      const featureId =
        featureIdFieldIndex >= 0 && featureIdFieldIndex < row.length
          ? String(row[featureIdFieldIndex] || '').trim()
          : '';
      if (featureId) {
        mergedByFeatureId.set(featureId, row);
      } else {
        rowsWithoutFeatureId.push(row);
      }
    };

    existingRows.forEach(upsertRow);
    incomingRows.forEach(upsertRow);
    rows = [...mergedByFeatureId.values(), ...rowsWithoutFeatureId];
  }

  const datasetToUse = {
    info: {
      id: datasetConfig.id,
      label: datasetConfig.label
    },
    data: {
      fields: QMAP_DRAW_DATASET_FIELDS as any,
      rows
    }
  };

  if (existingDataset?.id) {
    store.dispatch(
      wrapTo(
        keplerInstanceId,
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

  store.dispatch(
    wrapTo(
      keplerInstanceId,
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
  applyDefaultDrawLayerStyle(store, target, datasetToUse.info.id, keplerInstanceId);
}

export function syncAllDrawDatasets(store: QMapDrawRuntimeStore, keplerInstanceId: string) {
  (['stressor', 'operations'] as QMapDrawTarget[]).forEach(target => {
    (['point', 'line', 'polygon', 'rectangle', 'radius'] as QMapDrawTool[]).forEach(tool => {
      syncDrawDatasetByTool(store, target, tool, keplerInstanceId);
    });
  });
  syncDrawDatasetLayerVisibility(store, keplerInstanceId);
}
