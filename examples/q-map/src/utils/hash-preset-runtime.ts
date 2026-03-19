import {mapStyleChange, updateMap, wrapTo} from '@kepler.gl/actions';
import {setQMapMode} from '../features/qmap-mode/actions';
import {getQMapUserModeContextFromUiState, resolveQMapModeForUser} from '../mode/qmap-mode';
import {selectQMapUiState} from '../state/qmap-selectors';
import {logQMapPresetFromHash, QMapMapPreset, QMapUiPreset} from './hash-preset';

export function resolveQMapBasemapFromPreset(rawBasemap: unknown, allowedBasemaps: Set<string>): string | null {
  const basemap = String(rawBasemap || '').trim();
  if (!basemap) {
    return null;
  }
  return allowedBasemaps.has(basemap) ? basemap : null;
}

export function getQMapViewportPatchFromPreset(rawMapPreset: QMapMapPreset | null | undefined): Record<string, number> {
  const viewport = rawMapPreset?.viewport || {};
  const next: Record<string, number> = {};
  if (Number.isFinite(viewport.latitude as number)) {
    next.latitude = Number(viewport.latitude);
  }
  if (Number.isFinite(viewport.longitude as number)) {
    next.longitude = Number(viewport.longitude);
  }
  if (Number.isFinite(viewport.zoom as number)) {
    next.zoom = Number(viewport.zoom);
  }
  if (Number.isFinite(viewport.bearing as number)) {
    next.bearing = Number(viewport.bearing);
  }
  if (Number.isFinite(viewport.pitch as number)) {
    next.pitch = Number(viewport.pitch);
  }
  return next;
}

function getUiPresetWithoutMode(rawUiPreset: QMapUiPreset): QMapUiPreset {
  const {qmapMode: _presetMode, ...uiPresetWithoutMode} = rawUiPreset;
  return uiPresetWithoutMode;
}

export function applyQMapHashPresetFromRuntimeLocation({
  dispatch,
  store,
  keplerInstanceId,
  applyQMapUiPresetActionType,
  allowedBasemaps
}: {
  dispatch: any;
  store: any;
  keplerInstanceId: string;
  applyQMapUiPresetActionType: string;
  allowedBasemaps: Set<string>;
}): void {
  const parsed = logQMapPresetFromHash();
  if (!parsed) {
    return;
  }

  const rawUiPreset = parsed.uiPreset;
  const rawMapPreset = parsed.mapPreset;
  if (!rawUiPreset && !rawMapPreset) {
    return;
  }

  const runtimeUiState = selectQMapUiState(store.getState() as any);
  const userContext = getQMapUserModeContextFromUiState(runtimeUiState);

  if (typeof rawUiPreset?.qmapMode === 'string' && rawUiPreset.qmapMode.trim()) {
    const nextMode = resolveQMapModeForUser(rawUiPreset.qmapMode, userContext);
    dispatch(wrapTo(keplerInstanceId, setQMapMode(nextMode)));
  }

  if (rawUiPreset) {
    const uiPresetWithoutMode = getUiPresetWithoutMode(rawUiPreset);
    if (Object.keys(uiPresetWithoutMode).length > 0) {
      dispatch(
        wrapTo(keplerInstanceId, {
          type: applyQMapUiPresetActionType,
          payload: uiPresetWithoutMode
        })
      );
    }
  }

  if (rawMapPreset) {
    const viewportPatch = getQMapViewportPatchFromPreset(rawMapPreset);
    if (Object.keys(viewportPatch).length > 0) {
      dispatch(wrapTo(keplerInstanceId, updateMap(viewportPatch as any, 0)));
    }
  }

  const explicitBasemap = resolveQMapBasemapFromPreset(rawMapPreset?.basemap, allowedBasemaps);
  if (explicitBasemap) {
    dispatch(
      wrapTo(keplerInstanceId, mapStyleChange(explicitBasemap) as any)
    );
  }
}
