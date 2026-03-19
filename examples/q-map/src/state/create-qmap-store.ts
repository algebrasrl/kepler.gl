import {applyMiddleware, combineReducers, compose, createStore} from 'redux';
import type {AnyAction, Dispatch, MiddlewareAPI} from 'redux';
import {
  ActionTypes,
  addNotification,
  addDataToMap,
  mapStyleChange,
  removeDataset,
  removeNotification,
  replaceDataInMap,
  setEditorMode,
  setSelectedFeature,
  toggleMapControl,
  wrapTo
} from '@kepler.gl/actions';
import {COMPARE_TYPES, EDITOR_MODES} from '@kepler.gl/constants';
import {errorNotification} from '@kepler.gl/utils';
import keplerGlReducer, {enhanceReduxMiddleware} from '@kepler.gl/reducers';
import {aiAssistantReducer} from '@kepler.gl/ai-assistant';
import qMapAiReducer from '../features/qmap-ai/reducer';
import qMapH3PaintReducer from '../features/h3-paint/reducer';
import qMapDrawReducer from '../features/qmap-draw/reducer';
import {QMAP_MODE_SET_MODE} from '../features/qmap-mode/actions';
import {upsertH3PaintHex} from '../features/h3-paint/utils';
import {
  QMAP_DRAW_CLEAR_ACTIVE_TOOL,
  QMAP_DRAW_SET_ACTIVE_TOOL,
  clearQMapDrawActiveTool,
  setQMapDrawLineStart
} from '../features/qmap-draw/actions';
import {createQMapDrawMiddleware} from '../features/qmap-draw/middleware';
import {
  QMAP_DRAW_DRAFT_PROPERTY,
  QMAP_DRAW_SKIP_DATASET_SYNC_FLAG,
  isQMapDrawTarget
} from '../features/qmap-draw/constants';
import {
  createQMapLineLastClickState,
  getDrawTargetFromFeature,
  getDrawToolFromFeature,
  getWrappedActionInstanceId,
  getWrappedPayload,
  isDraftLineFeature,
  parseQMapDrawDatasetId,
  QMAP_LINE_DOUBLE_CLICK_DISTANCE_PX,
  QMAP_LINE_DOUBLE_CLICK_WINDOW_MS,
  resolveDrawTool,
  syncAllDrawDatasets,
  syncDrawDatasetLayerVisibility,
  withDrawProperties
} from '../features/qmap-draw/runtime';
import {
  applyQMapModeToMapControls,
  getQMapModeConfig,
  getQMapUserModeContextFromUiState,
  resolveQMapModeForUser,
  resolveQMapSidePanelId
} from '../mode/qmap-mode';
import {
  selectQMapDatasets,
  selectQMapEditorFeatures,
  selectQMapH3PaintState,
  selectQMapKeplerMapState,
  selectQMapUiState
} from './qmap-selectors';
import type {QMapRootState} from './qmap-state-types';
import {runH3Job} from '../workers/h3-runner';
import {buildH3PaintRow, H3_PAINT_FIELDS, readH3PaintRows} from '../features/h3-paint/utils';
import {evaluateGeotokenTessellationArea, getPolygonsFromGeometry} from '../geo';

export function createQMapStore({
  keplerInstanceId,
  initialMapViewport,
  initialBasemap,
  initialUiState,
  drawStressorDefaultBasemap,
  toggleReadOnlyActionType,
  applyQMapUiPresetActionType,
  applyQMapUiPresetToUiState
}: {
  keplerInstanceId: string;
  initialMapViewport: {latitude: number; longitude: number; zoom: number; bearing: number; pitch: number};
  initialBasemap: string;
  initialUiState: {
    readOnly: boolean;
    activeSidePanel: string | null;
    locale: string;
    qmapUserType: string;
    qmapUserGroupSlug: string | null;
    qmapMode: string;
    mapControls: Record<string, any>;
  };
  drawStressorDefaultBasemap: string;
  toggleReadOnlyActionType: string;
  applyQMapUiPresetActionType: string;
  applyQMapUiPresetToUiState: (currentUiState: any, rawPreset: any) => any;
}) {
  type QMapStoreAccess = {getState: () => QMapRootState; dispatch: Dispatch<AnyAction>};
  type QMapMiddlewareStoreApi = MiddlewareAPI<Dispatch<AnyAction>, QMapRootState>;
  const GEOTOKEN_TESSELLATION_RESOLUTION = 11;
  const GEOTOKEN_MAX_TESSELLATION_AREA_KM2 = 100;
  const GEOTOKEN_TESSELLATION_DATASET_ID = `qmap_tassellation_r${GEOTOKEN_TESSELLATION_RESOLUTION}`;
  const GEOTOKEN_TESSELLATION_DATASET_LABEL = `Tassellation_r${GEOTOKEN_TESSELLATION_RESOLUTION}`;
  const GEOTOKEN_AREA_LIMIT_NOTIFICATION_ID = 'QMAP_GEOTOKEN_AREA_LIMIT';

  const qMapLineLastClick = createQMapLineLastClickState();
  let geotokenTessellationRequestId = 0;

  function syncDrawDatasetLayerVisibilityForInstance(store: QMapStoreAccess) {
    syncDrawDatasetLayerVisibility(store, keplerInstanceId);
  }

  function syncAllDrawDatasetsForInstance(store: QMapStoreAccess) {
    syncAllDrawDatasets(store, keplerInstanceId);
  }

  function armGeotokenDrawSession(
    store: QMapMiddlewareStoreApi | QMapStoreAccess,
    sourceState?: QMapRootState | null,
    options?: {deferEditorMode?: boolean}
  ) {
    const currentState = sourceState || store.getState();
    const currentUiState = selectQMapUiState(currentState);
    const activeMode = resolveQMapModeForUser(
      currentUiState?.qmapMode,
      getQMapUserModeContextFromUiState(currentUiState)
    );
    if (activeMode !== 'geotoken') {
      return;
    }

    const mapStyle = selectQMapKeplerMapState(currentState)?.mapStyle;
    const currentStyleType = String(mapStyle?.styleType || '').trim();
    const hasGeotokenDefaultStyle = Boolean(mapStyle?.mapStyles?.[drawStressorDefaultBasemap]);
    if (hasGeotokenDefaultStyle && currentStyleType !== drawStressorDefaultBasemap) {
      store.dispatch(wrapTo(keplerInstanceId, mapStyleChange(drawStressorDefaultBasemap) as any));
    }

    const isMapDrawActive = Boolean((currentUiState?.mapControls as any)?.mapDraw?.active);
    if (!isMapDrawActive) {
      store.dispatch(wrapTo(keplerInstanceId, toggleMapControl('mapDraw', 0)));
    }
    store.dispatch(clearQMapDrawActiveTool());
    store.dispatch(setQMapDrawLineStart('stressor', null));
    store.dispatch(wrapTo(keplerInstanceId, setSelectedFeature(null)));
    store.dispatch(wrapTo(keplerInstanceId, setEditorMode(EDITOR_MODES.DRAW_POLYGON)));

    if (!options?.deferEditorMode || typeof globalThis.setTimeout !== 'function') {
      return;
    }

    globalThis.setTimeout(() => {
      const latestState = store.getState();
      const latestUiState = selectQMapUiState(latestState);
      const latestMode = resolveQMapModeForUser(
        latestUiState?.qmapMode,
        getQMapUserModeContextFromUiState(latestUiState)
      );
      if (latestMode !== 'geotoken') {
        return;
      }
      const isLatestMapDrawActive = Boolean((latestUiState?.mapControls as any)?.mapDraw?.active);
      if (!isLatestMapDrawActive) {
        store.dispatch(wrapTo(keplerInstanceId, toggleMapControl('mapDraw', 0)));
      }
      store.dispatch(wrapTo(keplerInstanceId, setEditorMode(EDITOR_MODES.DRAW_POLYGON)));
    }, 150);
  }

  function getGeotokenTessellationDataset(datasets: Record<string, any>) {
    return Object.values(datasets || {}).find((dataset: any) => {
      const label = String(dataset?.label || '').trim().toLowerCase();
      const id = String(dataset?.id || '').trim().toLowerCase();
      return (
        label === GEOTOKEN_TESSELLATION_DATASET_LABEL.toLowerCase() ||
        id === GEOTOKEN_TESSELLATION_DATASET_ID.toLowerCase()
      );
    }) as any;
  }

  function areSortedIdsEqual(left: string[], right: string[]) {
    if (left.length !== right.length) {
      return false;
    }
    for (let index = 0; index < left.length; index += 1) {
      if (left[index] !== right[index]) {
        return false;
      }
    }
    return true;
  }

  function clearGeotokenAreaLimitNotification(store: QMapMiddlewareStoreApi | QMapStoreAccess) {
    store.dispatch(wrapTo(keplerInstanceId, removeNotification(GEOTOKEN_AREA_LIMIT_NOTIFICATION_ID)));
  }

  function showGeotokenAreaLimitNotification(
    store: QMapMiddlewareStoreApi | QMapStoreAccess,
    selectedAreaKm2: number
  ) {
    const roundedAreaKm2 = Math.round(selectedAreaKm2 * 100) / 100;
    store.dispatch(
      wrapTo(
        keplerInstanceId,
        addNotification(
          errorNotification({
            id: GEOTOKEN_AREA_LIMIT_NOTIFICATION_ID,
            topic: 'global',
            message: `Area too large for geotoken tessellation: ${roundedAreaKm2} km2 selected, limit is ${GEOTOKEN_MAX_TESSELLATION_AREA_KM2} km2.`
          })
        )
      )
    );
  }

  async function syncGeotokenTessellation(store: QMapMiddlewareStoreApi, requestId: number) {
    const state = store.getState();
    const uiState = selectQMapUiState(state);
    const activeMode = resolveQMapModeForUser(uiState?.qmapMode, getQMapUserModeContextFromUiState(uiState));
    if (activeMode !== 'geotoken') {
      clearGeotokenAreaLimitNotification(store);
      return;
    }

    const datasets = selectQMapDatasets(state);
    const existingDataset = getGeotokenTessellationDataset(datasets);
    const geometries = selectQMapEditorFeatures(state)
      .filter((feature: any) => {
        return (
          feature?.geometry &&
          getPolygonsFromGeometry(feature.geometry).length > 0
        );
      })
      .map((feature: any) => feature.geometry);

    if (!geometries.length) {
      clearGeotokenAreaLimitNotification(store);
      if (existingDataset?.id) {
        store.dispatch(wrapTo(keplerInstanceId, removeDataset(existingDataset.id) as any));
      }
      return;
    }

    const areaCheck = evaluateGeotokenTessellationArea(geometries, GEOTOKEN_MAX_TESSELLATION_AREA_KM2);
    if (areaCheck.exceedsLimit) {
      if (existingDataset?.id) {
        store.dispatch(wrapTo(keplerInstanceId, removeDataset(existingDataset.id) as any));
      }
      showGeotokenAreaLimitNotification(store, areaCheck.areaKm2);
      return;
    }

    clearGeotokenAreaLimitNotification(store);

    let nextIds: string[] = [];
    try {
      const result = await runH3Job({
        name: 'tessellateGeometries',
        payload: {
          resolution: GEOTOKEN_TESSELLATION_RESOLUTION,
          geometries
        }
      });
      nextIds = Array.from(
        new Set(
          (Array.isArray(result?.ids) ? result.ids : [])
            .map((value: unknown) => String(value || '').trim())
            .filter(Boolean)
        )
      ).sort();
    } catch (error) {
      console.error('Geotoken tessellation failed:', error);
      return;
    }

    if (requestId !== geotokenTessellationRequestId) {
      return;
    }

    const latestState = store.getState();
    const latestUiState = selectQMapUiState(latestState);
    const latestMode = resolveQMapModeForUser(
      latestUiState?.qmapMode,
      getQMapUserModeContextFromUiState(latestUiState)
    );
    if (latestMode !== 'geotoken') {
      return;
    }

    const latestDatasets = selectQMapDatasets(latestState);
    const latestExistingDataset = getGeotokenTessellationDataset(latestDatasets);
    const existingIds = readH3PaintRows(latestExistingDataset, GEOTOKEN_TESSELLATION_RESOLUTION)
      .map(row => row[0])
      .sort();

    if (areSortedIdsEqual(existingIds, nextIds)) {
      return;
    }

    if (!nextIds.length) {
      if (latestExistingDataset?.id) {
        store.dispatch(wrapTo(keplerInstanceId, removeDataset(latestExistingDataset.id) as any));
      }
      return;
    }

    const datasetToUse = {
      info: {
        id: latestExistingDataset?.id || GEOTOKEN_TESSELLATION_DATASET_ID,
        label: GEOTOKEN_TESSELLATION_DATASET_LABEL
      },
      data: {
        fields: H3_PAINT_FIELDS as any,
        rows: nextIds.map(id => buildH3PaintRow(id, GEOTOKEN_TESSELLATION_RESOLUTION))
      }
    };

    if (latestExistingDataset?.id) {
      store.dispatch(
        wrapTo(
          keplerInstanceId,
          replaceDataInMap({
            datasetToReplaceId: latestExistingDataset.id,
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
  }

  const customizedKeplerGlReducer = keplerGlReducer
    .initialState({
      mapState: {
        latitude: initialMapViewport.latitude,
        longitude: initialMapViewport.longitude,
        zoom: initialMapViewport.zoom,
        bearing: initialMapViewport.bearing,
        pitch: initialMapViewport.pitch,
        minZoom: 3.5
      },
      mapStyle: {
        styleType: initialBasemap
      },
      visState: {
        interactionConfig: {
          tooltip: {
            id: 'tooltip',
            label: 'interactions.tooltip',
            enabled: true,
            config: {
              fieldsToShow: {},
              compareMode: false,
              compareType: COMPARE_TYPES.ABSOLUTE
            }
          },
          geocoder: {
            id: 'geocoder',
            label: 'interactions.geocoder',
            enabled: true,
            position: null
          },
          brush: {
            id: 'brush',
            label: 'interactions.brush',
            enabled: false,
            config: {
              size: 0.5
            }
          },
          coordinate: {
            id: 'coordinate',
            label: 'interactions.coordinate',
            enabled: false,
            position: null
          }
        }
      },
      uiState: {
        readOnly: initialUiState.readOnly,
        activeSidePanel: initialUiState.activeSidePanel,
        currentModal: null,
        locale: initialUiState.locale,
        qmapUserType: initialUiState.qmapUserType,
        qmapUserGroupSlug: initialUiState.qmapUserGroupSlug,
        qmapMode: initialUiState.qmapMode,
        mapControls: initialUiState.mapControls
      }
    })
    .plugin({
      [toggleReadOnlyActionType]: (state: any) => ({
        ...state,
        uiState: {
          ...state.uiState,
          readOnly: !state.uiState.readOnly
        }
      }),
      [applyQMapUiPresetActionType]: (state: any, action: any) => {
        const nextUiState = applyQMapUiPresetToUiState(state?.uiState, action?.payload);
        if (nextUiState === state?.uiState) {
          return state;
        }
        return {
          ...state,
          uiState: nextUiState
        };
      },
      [ActionTypes.TOGGLE_SIDE_PANEL]: (state: any, action: any) => {
        const requestedPanelId = String(action?.payload || '').trim();
        if (!requestedPanelId) {
          return state;
        }
        const activeMode = resolveQMapModeForUser(
          state?.uiState?.qmapMode,
          getQMapUserModeContextFromUiState(state?.uiState)
        );
        const modeConfig = getQMapModeConfig(activeMode);
        const resolvedPanelId = resolveQMapSidePanelId(requestedPanelId, modeConfig);
        if (resolvedPanelId === requestedPanelId) {
          return state;
        }
        return {
          ...state,
          uiState: {
            ...state.uiState,
            activeSidePanel: resolvedPanelId
          }
        };
      },
      [QMAP_MODE_SET_MODE]: (state: any, action: any) => {
        const userContext = getQMapUserModeContextFromUiState(state?.uiState);
        const nextMode = resolveQMapModeForUser(action?.payload, userContext);
        const modeConfig = getQMapModeConfig(nextMode);
        const nextActiveSidePanel =
          modeConfig.defaultSidePanel === null
            ? null
            : resolveQMapSidePanelId(state?.uiState?.activeSidePanel, modeConfig);
        return {
          ...state,
          uiState: {
            ...state.uiState,
            qmapMode: nextMode,
            activeSidePanel: nextActiveSidePanel,
            mapControls: applyQMapModeToMapControls(state?.uiState?.mapControls, modeConfig)
          }
        };
      }
    });

  const reducers = combineReducers({
    demo: combineReducers({
      keplerGl: customizedKeplerGlReducer,
      aiAssistant: aiAssistantReducer,
      qmapAi: qMapAiReducer,
      h3Paint: qMapH3PaintReducer,
      qmapDraw: qMapDrawReducer
    })
  });

  const qMapH3PaintMiddleware = (store: QMapMiddlewareStoreApi) => (next: Dispatch<AnyAction>) => (action: AnyAction) => {
    const prevState = store.getState();
    const result = next(action);
    if (action?.type === QMAP_MODE_SET_MODE) {
      const nextState = store.getState();
      const prevUiState = selectQMapUiState(prevState);
      const nextUiState = selectQMapUiState(nextState);
      const prevMode = resolveQMapModeForUser(prevUiState?.qmapMode, getQMapUserModeContextFromUiState(prevUiState));
      const nextMode = resolveQMapModeForUser(nextUiState?.qmapMode, getQMapUserModeContextFromUiState(nextUiState));
      const enteringDrawStressor = prevMode !== 'draw-stressor' && nextMode === 'draw-stressor';
      const enteringGeotoken = prevMode !== 'geotoken' && nextMode === 'geotoken';
      if (enteringDrawStressor) {
        const mapStyle = selectQMapKeplerMapState(nextState)?.mapStyle;
        const currentStyleType = String(mapStyle?.styleType || '').trim();
        const hasStressorDefaultStyle = Boolean(mapStyle?.mapStyles?.[drawStressorDefaultBasemap]);
        if (hasStressorDefaultStyle && currentStyleType !== drawStressorDefaultBasemap) {
          store.dispatch(wrapTo(keplerInstanceId, mapStyleChange(drawStressorDefaultBasemap) as any));
        }
      }
      if (enteringGeotoken) {
        armGeotokenDrawSession(store, nextState);
      }
    }
    if (
      action?.type === QMAP_DRAW_SET_ACTIVE_TOOL ||
      action?.type === QMAP_DRAW_CLEAR_ACTIVE_TOOL ||
      action?.type === QMAP_MODE_SET_MODE
    ) {
      syncDrawDatasetLayerVisibilityForInstance(store);
    }
    if (action?.type !== ActionTypes.LAYER_CLICK) {
      return result;
    }

    const state = store.getState();
    const h3PaintState = selectQMapH3PaintState(state);
    const isActive = Boolean(h3PaintState?.active);
    if (!isActive) {
      return result;
    }

    const resolution = Number(h3PaintState?.resolution || 7);
    const datasets = selectQMapDatasets(state);
    const layerClickInfo = action?.payload?.info ?? action?.info ?? null;
    if (!layerClickInfo) {
      return result;
    }
    const coordinate = Array.isArray(layerClickInfo?.coordinate) ? layerClickInfo.coordinate : null;
    if (!coordinate || coordinate.length < 2) {
      return result;
    }

    const lng = Number(coordinate[0]);
    const lat = Number(coordinate[1]);
    if (!Number.isFinite(lng) || !Number.isFinite(lat)) {
      return result;
    }

    upsertH3PaintHex({
      dispatch: store.dispatch,
      datasets,
      resolution,
      lng,
      lat
    });

    return result;
  };

  const qMapDrawMiddleware = createQMapDrawMiddleware({
    keplerInstanceId,
    qMapDrawSetActiveToolType: QMAP_DRAW_SET_ACTIVE_TOOL,
    qMapDrawSkipDatasetSyncFlag: QMAP_DRAW_SKIP_DATASET_SYNC_FLAG,
    qMapDrawDraftProperty: QMAP_DRAW_DRAFT_PROPERTY,
    qMapLineDoubleClickWindowMs: QMAP_LINE_DOUBLE_CLICK_WINDOW_MS,
    qMapLineDoubleClickDistancePx: QMAP_LINE_DOUBLE_CLICK_DISTANCE_PX,
    qMapLineLastClick,
    getWrappedPayload,
    getWrappedActionInstanceId,
    parseQMapDrawDatasetId,
    getDrawTargetFromFeature,
    getDrawToolFromFeature,
    resolveDrawTool,
    isQMapDrawTarget,
    withDrawProperties,
    isDraftLineFeature,
    syncDrawDatasetLayerVisibility: syncDrawDatasetLayerVisibilityForInstance,
    syncAllDrawDatasets: syncAllDrawDatasetsForInstance,
    setQMapDrawLineStart
  });

  const qMapGeotokenTessellationMiddleware =
    (store: QMapMiddlewareStoreApi) => (next: Dispatch<AnyAction>) => (action: AnyAction) => {
      const prevState = store.getState();
      const prevFeatures = selectQMapEditorFeatures(prevState);
      const prevUiState = selectQMapUiState(prevState);
      const prevMode = resolveQMapModeForUser(
        prevUiState?.qmapMode,
        getQMapUserModeContextFromUiState(prevUiState)
      );
      const result = next(action);
      const nextState = store.getState();
      const nextFeatures = selectQMapEditorFeatures(nextState);
      const nextUiState = selectQMapUiState(nextState);
      const nextMode = resolveQMapModeForUser(
        nextUiState?.qmapMode,
        getQMapUserModeContextFromUiState(nextUiState)
      );
      const editorFeaturesChanged = prevFeatures !== nextFeatures;
      const modeChanged = prevMode !== nextMode;
      if ((nextMode === 'geotoken' && editorFeaturesChanged) || modeChanged) {
        geotokenTessellationRequestId += 1;
        void syncGeotokenTessellation(store, geotokenTessellationRequestId);
      }
      return result;
    };

  const middleWares = enhanceReduxMiddleware([
    qMapGeotokenTessellationMiddleware,
    qMapDrawMiddleware,
    qMapH3PaintMiddleware
  ]);
  const enhancers = applyMiddleware(...middleWares);
  const initialState = {};
  const store = createStore(reducers, initialState, compose(enhancers));
  const keplerGlGetState = (state: QMapRootState) => state.demo?.keplerGl;
  const startupStore = store as unknown as QMapStoreAccess;
  const startupState = startupStore.getState();

  const startupUiState = selectQMapUiState(startupState);
  const startupMode = resolveQMapModeForUser(
    startupUiState?.qmapMode,
    getQMapUserModeContextFromUiState(startupUiState)
  );
  if (startupMode === 'geotoken') {
    armGeotokenDrawSession(startupStore, startupState, {deferEditorMode: true});
  }

  return {store, keplerGlGetState};
}
