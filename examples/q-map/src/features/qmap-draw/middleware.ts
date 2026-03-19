import {ActionTypes, setEditorMode, setFeatures, setSelectedFeature, wrapTo} from '@kepler.gl/actions';
import {EDITOR_MODES} from '@kepler.gl/constants';
import {generateHashId} from '@kepler.gl/common-utils';
import type {AnyAction, Dispatch} from 'redux';
import type {QMapDrawTarget, QMapDrawTool} from './constants';
import {selectQMapDrawState, selectQMapEditorFeatures} from '../../state/qmap-selectors';
import type {QMapRootState} from '../../state/qmap-state-types';

type QMapDrawDatasetIdParseResult = {target: QMapDrawTarget; tool: QMapDrawTool} | null;
type QMapDrawMiddlewareStore = {
  getState: () => QMapRootState;
  dispatch: Dispatch<AnyAction>;
};

type QMapLineLastClick = {
  time: number;
  lng: number;
  lat: number;
  x: number | null;
  y: number | null;
};

type QMapDrawMiddlewareDeps = {
  keplerInstanceId: string;
  qMapDrawSetActiveToolType: string;
  qMapDrawSkipDatasetSyncFlag: string;
  qMapDrawDraftProperty: string;
  qMapLineDoubleClickWindowMs: number;
  qMapLineDoubleClickDistancePx: number;
  qMapLineLastClick: Record<QMapDrawTarget, QMapLineLastClick | null>;
  getWrappedPayload: (action: any) => any;
  getWrappedActionInstanceId: (action: any) => string | null;
  parseQMapDrawDatasetId: (dataId: unknown) => QMapDrawDatasetIdParseResult;
  getDrawTargetFromFeature: (feature: any) => QMapDrawTarget | null;
  getDrawToolFromFeature: (feature: any) => QMapDrawTool | null;
  resolveDrawTool: (value: unknown) => QMapDrawTool | null;
  isQMapDrawTarget: (value: unknown) => value is QMapDrawTarget;
  withDrawProperties: (feature: any, target: QMapDrawTarget | null, tool: QMapDrawTool | null) => any;
  isDraftLineFeature: (feature: any, target?: QMapDrawTarget) => boolean;
  syncDrawDatasetLayerVisibility: (store: QMapDrawMiddlewareStore) => void;
  syncAllDrawDatasets: (store: QMapDrawMiddlewareStore) => void;
  setQMapDrawLineStart: (target: QMapDrawTarget, coords: [number, number] | null) => any;
};

export function createQMapDrawMiddleware(deps: QMapDrawMiddlewareDeps) {
  return (store: QMapDrawMiddlewareStore) => (next: Dispatch<AnyAction>) => (action: AnyAction) => {
    if (action?.type === ActionTypes.REMOVE_DATASET) {
      const instanceId = deps.getWrappedActionInstanceId(action);
      if (instanceId && instanceId !== deps.keplerInstanceId) {
        return next(action);
      }

      const payload = deps.getWrappedPayload(action);
      const removedDataId = String(payload?.dataId || '').trim();
      const parsed = deps.parseQMapDrawDatasetId(removedDataId);
      if (!parsed) {
        return next(action);
      }

      const prevState = store.getState();
      const currentFeatures = selectQMapEditorFeatures(prevState);
      const nextFeatures = currentFeatures.filter((feature: any) => {
        const featureTarget = deps.getDrawTargetFromFeature(feature);
        const featureTool = deps.getDrawToolFromFeature(feature);
        return !(featureTarget === parsed.target && featureTool === parsed.tool);
      });

      const result = next(action);
      if (nextFeatures.length !== currentFeatures.length) {
        if (parsed.tool === 'line') {
          deps.qMapLineLastClick[parsed.target] = null;
          store.dispatch(deps.setQMapDrawLineStart(parsed.target, null));
        }
        store.dispatch(wrapTo(deps.keplerInstanceId, setFeatures(nextFeatures as any)));
        store.dispatch(wrapTo(deps.keplerInstanceId, setSelectedFeature(null)));
      }
      deps.syncDrawDatasetLayerVisibility(store);
      return result;
    }

    if (action?.type === ActionTypes.SET_SELECTED_FEATURE && deps.getWrappedActionInstanceId(action) === deps.keplerInstanceId) {
      const state = store.getState();
      const drawState = selectQMapDrawState(state);
      const activeTool = deps.resolveDrawTool(drawState?.activeTool);
      if (activeTool === 'line') {
        const payload = deps.getWrappedPayload(action);
        if (payload?.feature) {
          const nextAction =
            payload === action
              ? {...action, feature: null, selectionContext: undefined}
              : {
                  ...action,
                  payload: {
                    ...action.payload,
                    feature: null,
                    selectionContext: undefined
                  }
                };
          return next(nextAction);
        }
      }
    }

    if (action?.type === ActionTypes.SET_FEATURES && deps.getWrappedActionInstanceId(action) === deps.keplerInstanceId) {
      const prevState = store.getState();
      const prevFeatures = selectQMapEditorFeatures(prevState);
      const prevFeatureById = new Map(prevFeatures.map((feature: any) => [String(feature?.id || ''), feature]));
      const prevDrawState = selectQMapDrawState(prevState);
      const activeTarget: QMapDrawTarget | null = deps.isQMapDrawTarget(prevDrawState?.activeTarget)
        ? (prevDrawState.activeTarget as QMapDrawTarget)
        : null;
      const activeTool = deps.resolveDrawTool(prevDrawState?.activeTool);
      const payload = deps.getWrappedPayload(action);
      const skipDatasetSync = Boolean(payload?.[deps.qMapDrawSkipDatasetSyncFlag]);
      const inputFeatures = Array.isArray(payload?.features) ? payload.features : [];

      let changed = false;
      const taggedFeatures = inputFeatures.map((feature: any) => {
        const featureId = String(feature?.id || '');
        const previous = prevFeatureById.get(featureId);
        const resolvedTarget =
          deps.getDrawTargetFromFeature(feature) || deps.getDrawTargetFromFeature(previous) || activeTarget;
        const resolvedTool = deps.getDrawToolFromFeature(feature) || deps.getDrawToolFromFeature(previous) || activeTool;
        const nextFeature = deps.withDrawProperties(feature, resolvedTarget, resolvedTool);
        if (nextFeature !== feature) {
          changed = true;
        }
        return nextFeature;
      });

      const nextAction =
        changed && payload === action
          ? {...action, features: taggedFeatures}
          : changed
            ? {
                ...action,
                payload: {
                  ...action.payload,
                  features: taggedFeatures
                }
              }
            : action;

      const result = next(nextAction);
      if (!skipDatasetSync) {
        deps.syncAllDrawDatasets(store);
      } else {
        deps.syncDrawDatasetLayerVisibility(store);
      }
      return result;
    }

    if (action?.type === deps.qMapDrawSetActiveToolType) {
      const prevState = store.getState();
      const prevDrawState = selectQMapDrawState(prevState);
      const prevTarget: QMapDrawTarget | null = deps.isQMapDrawTarget(prevDrawState?.activeTarget)
        ? (prevDrawState.activeTarget as QMapDrawTarget)
        : null;
      const prevTool = deps.resolveDrawTool(prevDrawState?.activeTool);
      const nextTarget = deps.isQMapDrawTarget(action?.payload?.target) ? (action.payload.target as QMapDrawTarget) : null;
      const nextTool = deps.resolveDrawTool(action?.payload?.tool);

      const result = next(action);
      const switchedTool =
        Boolean(prevTarget) &&
        Boolean(prevTool) &&
        Boolean(nextTarget) &&
        Boolean(nextTool) &&
        (prevTarget !== nextTarget || prevTool !== nextTool);

      if (!switchedTool || !prevTarget || !prevTool) {
        deps.syncDrawDatasetLayerVisibility(store);
        return result;
      }

      const state = store.getState();
      const currentFeatures = selectQMapEditorFeatures(state);
      const nextFeatures = currentFeatures.filter((feature: any) => {
        const featureTarget = deps.getDrawTargetFromFeature(feature);
        const featureTool = deps.getDrawToolFromFeature(feature);
        return !(featureTarget === prevTarget && featureTool === prevTool);
      });

      if (nextFeatures.length !== currentFeatures.length) {
        const wrappedAction: any = wrapTo(deps.keplerInstanceId, setFeatures(nextFeatures as any));
        if (wrappedAction?.payload?.meta?._id_) {
          wrappedAction.payload[deps.qMapDrawSkipDatasetSyncFlag] = true;
        } else {
          wrappedAction[deps.qMapDrawSkipDatasetSyncFlag] = true;
        }
        store.dispatch(wrappedAction);
        store.dispatch(wrapTo(deps.keplerInstanceId, setSelectedFeature(null)));
      }

      if (prevTool === 'line') {
        deps.qMapLineLastClick[prevTarget] = null;
        store.dispatch(deps.setQMapDrawLineStart(prevTarget, null));
      }
      deps.syncDrawDatasetLayerVisibility(store);
      return result;
    }

    const result = next(action);
    if (action?.type !== ActionTypes.LAYER_CLICK) {
      return result;
    }

    const state = store.getState();
    const drawState = selectQMapDrawState(state);
    const activeTarget: QMapDrawTarget | null = deps.isQMapDrawTarget(drawState?.activeTarget)
      ? (drawState.activeTarget as QMapDrawTarget)
      : null;
    const activeTool = deps.resolveDrawTool(drawState?.activeTool);
    if (!activeTarget || (activeTool !== 'point' && activeTool !== 'line')) {
      return result;
    }

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
    const clickXRaw = Number(layerClickInfo?.x);
    const clickYRaw = Number(layerClickInfo?.y);
    const clickX = Number.isFinite(clickXRaw) ? clickXRaw : null;
    const clickY = Number.isFinite(clickYRaw) ? clickYRaw : null;

    const currentFeatures = selectQMapEditorFeatures(state);

    if (activeTool === 'point') {
      const pointFeature = deps.withDrawProperties(
        {
          id: generateHashId(8),
          type: 'Feature',
          geometry: {type: 'Point', coordinates: [lng, lat]},
          properties: {}
        },
        activeTarget,
        'point'
      );
      store.dispatch(wrapTo(deps.keplerInstanceId, setFeatures([...currentFeatures, pointFeature] as any)));
      store.dispatch(wrapTo(deps.keplerInstanceId, setSelectedFeature(null)));
      store.dispatch(wrapTo(deps.keplerInstanceId, setEditorMode(EDITOR_MODES.EDIT)));
      deps.syncDrawDatasetLayerVisibility(store);
      return result;
    }

    const draftIndex = currentFeatures.findIndex((feature: any) => deps.isDraftLineFeature(feature, activeTarget));
    if (draftIndex < 0) {
      const draftFeature = deps.withDrawProperties(
        {
          id: generateHashId(8),
          type: 'Feature',
          geometry: {type: 'LineString', coordinates: [[lng, lat]]},
          properties: {[deps.qMapDrawDraftProperty]: true}
        },
        activeTarget,
        'line'
      );
      deps.qMapLineLastClick[activeTarget] = {time: Date.now(), lng, lat, x: clickX, y: clickY};
      store.dispatch(deps.setQMapDrawLineStart(activeTarget, [lng, lat]));
      store.dispatch(wrapTo(deps.keplerInstanceId, setFeatures([...currentFeatures, draftFeature] as any)));
      store.dispatch(wrapTo(deps.keplerInstanceId, setSelectedFeature(null)));
      store.dispatch(wrapTo(deps.keplerInstanceId, setEditorMode(EDITOR_MODES.EDIT)));
      deps.syncDrawDatasetLayerVisibility(store);
      return result;
    }

    const now = Date.now();
    const draftFeature = currentFeatures[draftIndex];
    const baseCoords = Array.isArray(draftFeature?.geometry?.coordinates)
      ? draftFeature.geometry.coordinates
      : [];
    if (baseCoords.length < 1) {
      return result;
    }

    const lastClick = deps.qMapLineLastClick[activeTarget];
    const clickDetailRaw = Number(layerClickInfo?.srcEvent?.detail ?? layerClickInfo?.event?.detail);
    const hasNativeDoubleClickDetail = Number.isFinite(clickDetailRaw) && clickDetailRaw >= 2;
    const pixelDistance =
      lastClick &&
      clickX !== null &&
      clickY !== null &&
      lastClick.x !== null &&
      lastClick.y !== null
        ? Math.hypot(clickX - lastClick.x, clickY - lastClick.y)
        : null;
    const isDoubleClick = Boolean(
      hasNativeDoubleClickDetail ||
        (lastClick &&
          now - lastClick.time <= deps.qMapLineDoubleClickWindowMs &&
          (pixelDistance === null || pixelDistance <= deps.qMapLineDoubleClickDistancePx))
    );
    deps.qMapLineLastClick[activeTarget] = {time: now, lng, lat, x: clickX, y: clickY};

    if (isDoubleClick) {
      const dedupedFinalCoords = baseCoords.filter((coord: any, idx: number) => {
        if (idx === 0) return true;
        const prev = baseCoords[idx - 1] || [];
        return Number(prev[0]) !== Number(coord?.[0]) || Number(prev[1]) !== Number(coord?.[1]);
      });
      if (dedupedFinalCoords.length < 2) {
        const nextFeatures = currentFeatures.filter((_: any, idx: number) => idx !== draftIndex);
        deps.qMapLineLastClick[activeTarget] = null;
        store.dispatch(deps.setQMapDrawLineStart(activeTarget, null));
        store.dispatch(wrapTo(deps.keplerInstanceId, setFeatures(nextFeatures as any)));
        store.dispatch(wrapTo(deps.keplerInstanceId, setSelectedFeature(null)));
        store.dispatch(wrapTo(deps.keplerInstanceId, setEditorMode(EDITOR_MODES.EDIT)));
        deps.syncDrawDatasetLayerVisibility(store);
        return result;
      }
      const finalizedLineFeature = deps.withDrawProperties(
        {
          ...draftFeature,
          id: generateHashId(8),
          geometry: {
            ...(draftFeature.geometry || {}),
            type: 'LineString',
            coordinates: dedupedFinalCoords
          },
          properties: {
            ...(draftFeature.properties || {}),
            [deps.qMapDrawDraftProperty]: false
          }
        },
        activeTarget,
        'line'
      );
      const nextFeatures = [...currentFeatures];
      nextFeatures[draftIndex] = finalizedLineFeature;
      deps.qMapLineLastClick[activeTarget] = null;
      store.dispatch(deps.setQMapDrawLineStart(activeTarget, null));
      store.dispatch(wrapTo(deps.keplerInstanceId, setFeatures(nextFeatures as any)));
      store.dispatch(wrapTo(deps.keplerInstanceId, setSelectedFeature(null)));
      store.dispatch(wrapTo(deps.keplerInstanceId, setEditorMode(EDITOR_MODES.EDIT)));
      deps.syncDrawDatasetLayerVisibility(store);
      return result;
    }

    const lastVertex = baseCoords[baseCoords.length - 1] || [];
    if (Number(lastVertex[0]) === lng && Number(lastVertex[1]) === lat) {
      return result;
    }

    const nextCoords = [...baseCoords, [lng, lat]];
    const nextDraft = {
      ...draftFeature,
      geometry: {
        ...(draftFeature.geometry || {}),
        type: 'LineString',
        coordinates: nextCoords
      }
    };
    const nextFeatures = [...currentFeatures];
    nextFeatures[draftIndex] = nextDraft;
    store.dispatch(deps.setQMapDrawLineStart(activeTarget, [lng, lat]));
    store.dispatch(wrapTo(deps.keplerInstanceId, setFeatures(nextFeatures as any)));
    store.dispatch(wrapTo(deps.keplerInstanceId, setSelectedFeature(null)));
    store.dispatch(wrapTo(deps.keplerInstanceId, setEditorMode(EDITOR_MODES.EDIT)));
    deps.syncDrawDatasetLayerVisibility(store);
    return result;
  };
}
