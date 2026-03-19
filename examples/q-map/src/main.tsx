// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import React, {useCallback, useEffect, useState} from 'react';
import ReactDOM from 'react-dom/client';
import {Provider, useDispatch} from 'react-redux';
import './app.css';
import 'mapbox-gl/dist/mapbox-gl.css';
import 'maplibre-gl/dist/maplibre-gl.css';

import {MAP_CONTROLS} from '@kepler.gl/constants';
import {loadCloudMap, updateMap, wrapTo} from '@kepler.gl/actions';

import KeplerGl, {
  CustomPanelsFactory,
  FeatureActionPanelFactory,
  injectComponents,
  LoadDataModalFactory,
  LoadTileSetFactory,
  MapPopoverFactory,
  PanelToggleFactory,
  PanelHeaderFactory,
  SidebarFactory
} from '@kepler.gl/components';
import {setMapBoundary, updateAiAssistantConfig} from '@kepler.gl/ai-assistant';
import {exportMap, initApplicationConfig} from '@kepler.gl/utils';

import AutoSizer from 'react-virtualized/dist/commonjs/AutoSizer';
import WebMercatorViewport from 'viewport-mercator-project';
import CustomPanelHeaderFactory from './components/panel-header';
import CustomSidebarFactory from './components/side-bar';
import {replaceMapControl} from './components/map-control';
import QMapLoadDataModalFactory from './components/load-data-modal';
import QMapLoadTilesetFactory from './components/load-tileset';
import QMapCustomPanelsFactory from './components/custom-panels';
import QMapFeatureActionPanelFactory from './components/feature-action-panel';
import QMapMapPopoverFactory from './components/map-popover';
import QMapPanelToggleFactory from './components/panel-toggle';
import qMapTheme from './theme/q-map-theme';
import {getQMapCloudProviders} from './cloud-providers';
import {
  getQMapMapPresetFromHash,
  getQMapUiPresetFromHash,
  QMapMapPreset,
  QMapUiMapControlPreset,
  QMapUiPreset
} from './utils/hash-preset';
import {
  applyQMapHashPresetFromRuntimeLocation,
  getQMapViewportPatchFromPreset,
  resolveQMapBasemapFromPreset
} from './utils/hash-preset-runtime';
import {
  applyQMapModeToMapControls,
  getQMapModeConfig,
  getQMapUserModeContextFromUiState,
  resolveQMapModeForUser,
  resolveQMapSidePanelId
} from './mode/qmap-mode';
import {createQMapStore} from './state/create-qmap-store';
import QMAP_LOCALE_MESSAGES from './i18n/q-map-locale-messages';
import {
  describeQMapIframePostFailure,
  getQMapIframeActionUuid,
  getQMapIframeCloudMapReference,
  isQMapIframeExportEnabled,
  postQMapIframeCancelDetailed,
  postQMapIframeExportDetailed
} from './utils/iframe-export';
import {bootstrapQMapAuthFromParent} from './utils/auth-bootstrap';
import {DEFAULT_QMAP_ASSISTANT_BASE_URL} from './utils/assistant-config';
import {resolveQMapRuntimeMapboxToken} from './utils/runtime-config';

const MAPBOX_TOKEN = resolveQMapRuntimeMapboxToken() || import.meta.env.VITE_MAPBOX_TOKEN || '';
const QMAP_HAS_MAPBOX_TOKEN = Boolean(String(MAPBOX_TOKEN || '').trim());
const QMAP_AI_BASE_URL = import.meta.env.VITE_QMAP_AI_PROXY_BASE || DEFAULT_QMAP_ASSISTANT_BASE_URL;
const QMAP_ALLOWED_MAP_CONTROL_IDS = new Set<string>(Object.values(MAP_CONTROLS));
const QMAP_IFRAME_EXPORT_PROVIDER_NAME = String(import.meta.env.VITE_QMAP_IFRAME_EXPORT_PROVIDER || 'q-storage-backend')
  .trim() || 'q-storage-backend';

function hasOwnKey<T extends object>(value: T | null | undefined, key: string): boolean {
  return Boolean(value) && Object.prototype.hasOwnProperty.call(value, key);
}

function mergeQMapPresetMapControls(
  baseControls: Record<string, any> | null | undefined,
  presetMapControls?: Record<string, QMapUiMapControlPreset> | null
): Record<string, any> {
  const nextControls = {...(baseControls || {})};
  if (!presetMapControls || typeof presetMapControls !== 'object') {
    return nextControls;
  }

  Object.entries(presetMapControls).forEach(([controlId, rawControlPreset]) => {
    if (!QMAP_ALLOWED_MAP_CONTROL_IDS.has(controlId) || !rawControlPreset || typeof rawControlPreset !== 'object') {
      return;
    }

    const current = nextControls[controlId] || {};
    const next = {...current};
    if (typeof rawControlPreset.show === 'boolean') {
      next.show = rawControlPreset.show;
    }
    if (typeof rawControlPreset.active === 'boolean') {
      next.active = rawControlPreset.active;
    }
    if (next.show === false) {
      next.active = false;
    }
    nextControls[controlId] = next;
  });

  return nextControls;
}

function resolveQMapActiveSidePanelFromPreset(
  requestedPanel: unknown,
  modeConfig: ReturnType<typeof getQMapModeConfig>,
  fallbackPanel: string | null
): string | null {
  if (requestedPanel === null) {
    return null;
  }
  const panelId = String(requestedPanel || '').trim();
  if (!panelId) {
    return fallbackPanel;
  }
  return resolveQMapSidePanelId(panelId, modeConfig);
}

function applyQMapUiPresetToUiState(currentUiState: any, rawPreset: QMapUiPreset | null | undefined): any {
  if (!rawPreset || typeof rawPreset !== 'object') {
    return currentUiState;
  }

  const userContext = getQMapUserModeContextFromUiState(currentUiState);
  const nextMode = resolveQMapModeForUser(rawPreset.qmapMode ?? currentUiState?.qmapMode, userContext);
  const modeConfig = getQMapModeConfig(nextMode);
  const hasExplicitActiveSidePanel = hasOwnKey(rawPreset, 'activeSidePanel');

  const fallbackActiveSidePanel =
    currentUiState?.activeSidePanel === null
      ? null
      : resolveQMapSidePanelId(currentUiState?.activeSidePanel, modeConfig);

  const nextActiveSidePanel = hasExplicitActiveSidePanel
    ? resolveQMapActiveSidePanelFromPreset(rawPreset.activeSidePanel, modeConfig, fallbackActiveSidePanel)
    : fallbackActiveSidePanel;

  const modeControls = applyQMapModeToMapControls(currentUiState?.mapControls, modeConfig);
  const nextMapControls = mergeQMapPresetMapControls(modeControls, rawPreset.mapControls);

  const nextUiState = {
    ...currentUiState,
    qmapMode: nextMode,
    activeSidePanel: nextActiveSidePanel,
    mapControls: nextMapControls
  };

  if (typeof rawPreset.readOnly === 'boolean') {
    nextUiState.readOnly = rawPreset.readOnly;
  }
  if (typeof rawPreset.locale === 'string' && rawPreset.locale.trim()) {
    nextUiState.locale = rawPreset.locale.trim();
  }

  return nextUiState;
}

const QMAP_BASEMAP_OPTIONS = new Set([
  'no_map',
  'dark-matter',
  'positron',
  'voyager',
  'satellite',
  'dark',
  'light',
  'muted',
  'muted_night'
]);
const QMAP_DEFAULT_BASEMAP = QMAP_BASEMAP_OPTIONS.has(String(import.meta.env.VITE_QMAP_DEFAULT_BASEMAP || ''))
  ? String(import.meta.env.VITE_QMAP_DEFAULT_BASEMAP)
  : (QMAP_HAS_MAPBOX_TOKEN ? 'muted' : 'no_map');
const QMAP_DEFAULT_MAP_VIEWPORT = {
  latitude: 42.5,
  longitude: 12.5,
  zoom: 5,
  bearing: 0,
  pitch: 0
};

function buildQMapInitialMapViewport(rawMapPreset: QMapMapPreset | null | undefined) {
  return {
    ...QMAP_DEFAULT_MAP_VIEWPORT,
    ...getQMapViewportPatchFromPreset(rawMapPreset)
  };
}

const QMAP_DRAW_STRESSOR_DEFAULT_BASEMAP = QMAP_HAS_MAPBOX_TOKEN ? 'satellite' : 'no_map';
const QMAP_AI_TEMPERATURE = Number.isFinite(Number(import.meta.env.VITE_QMAP_AI_TEMPERATURE))
  ? Number(import.meta.env.VITE_QMAP_AI_TEMPERATURE)
  : undefined;
const QMAP_AI_TOP_P = Number.isFinite(Number(import.meta.env.VITE_QMAP_AI_TOP_P))
  ? Number(import.meta.env.VITE_QMAP_AI_TOP_P)
  : undefined;
const QMAP_HASH_UI_PRESET = getQMapUiPresetFromHash();
const QMAP_HASH_MAP_PRESET = getQMapMapPresetFromHash();
const QMAP_INITIAL_MAP_VIEWPORT = buildQMapInitialMapViewport(QMAP_HASH_MAP_PRESET);
const QMAP_INITIAL_USER_CONTEXT = {userType: 'user', groupSlug: null};
const QMAP_INITIAL_MODE = resolveQMapModeForUser(
  QMAP_HASH_UI_PRESET?.qmapMode || import.meta.env.VITE_QMAP_MODE,
  QMAP_INITIAL_USER_CONTEXT
);
const QMAP_INITIAL_MODE_CONFIG = getQMapModeConfig(QMAP_INITIAL_MODE);
const QMAP_INITIAL_BASEMAP =
  resolveQMapBasemapFromPreset(QMAP_HASH_MAP_PRESET?.basemap, QMAP_BASEMAP_OPTIONS) ||
  (
    QMAP_INITIAL_MODE === 'draw-stressor' || QMAP_INITIAL_MODE === 'geotoken'
      ? QMAP_DRAW_STRESSOR_DEFAULT_BASEMAP
      : QMAP_DEFAULT_BASEMAP
  );
const QMAP_INITIAL_ACTIVE_SIDE_PANEL = hasOwnKey(QMAP_HASH_UI_PRESET, 'activeSidePanel')
  ? resolveQMapActiveSidePanelFromPreset(QMAP_HASH_UI_PRESET?.activeSidePanel, QMAP_INITIAL_MODE_CONFIG, null)
  : resolveQMapSidePanelId('profile', QMAP_INITIAL_MODE_CONFIG);
const QMAP_INITIAL_MAP_CONTROLS = mergeQMapPresetMapControls(
  applyQMapModeToMapControls(undefined, QMAP_INITIAL_MODE_CONFIG),
  QMAP_HASH_UI_PRESET?.mapControls
);
const KEPLER_INSTANCE_ID = 'map';
const TOGGLE_QMAP_READ_ONLY = 'TOGGLE_QMAP_READ_ONLY';
const APPLY_QMAP_UI_PRESET = 'APPLY_QMAP_UI_PRESET';
const CLOUD_PROVIDERS = getQMapCloudProviders();

function resolveQMapCloudMapId(response: any): string {
  return (
    String(response?.id || '').trim() ||
    String(response?.info?.id || '').trim() ||
    String(response?.loadParams?.id || '').trim()
  );
}

function getQMapCloudProviderByName(providerName: string): any {
  const resolvedName = String(providerName || '').trim();
  if (!resolvedName) {
    return null;
  }
  return CLOUD_PROVIDERS.find((provider: any) => String(provider?.name || '').trim() === resolvedName) || null;
}

function getQMapIframeExportProvider(): any {
  return getQMapCloudProviderByName(QMAP_IFRAME_EXPORT_PROVIDER_NAME);
}

async function saveQMapSnapshotForIframeExport(
  snapshot: {map: any; thumbnail: Blob | null},
  existingCloudMapId?: string
) {
  const provider = getQMapIframeExportProvider();
  if (!provider) {
    throw new Error(`Cloud provider "${QMAP_IFRAME_EXPORT_PROVIDER_NAME}" is not configured.`);
  }
  if (typeof provider.uploadMap !== 'function') {
    throw new Error(`Cloud provider "${QMAP_IFRAME_EXPORT_PROVIDER_NAME}" does not support map save.`);
  }

  if (typeof provider.login === 'function') {
    await provider.login();
  }

  const normalizedExistingCloudMapId = String(existingCloudMapId || '').trim();
  const actionUuid = getQMapIframeActionUuid(window.location.hash);
  const mapMetadata = actionUuid
    ? {
        locked: true,
        lockType: 'action',
        actionUuid,
        lockSource: 'q_hive'
      }
    : undefined;

  const response = await provider.uploadMap({
    mapData: snapshot,
    options: {
      isPublic: false,
      overwrite: Boolean(normalizedExistingCloudMapId),
      ...(mapMetadata ? {mapMetadata} : {}),
      ...(normalizedExistingCloudMapId
        ? {
            mapIdToOverwrite: normalizedExistingCloudMapId
          }
        : {})
    }
  });

  return {
    providerName: String(provider?.name || '').trim(),
    mapId: resolveQMapCloudMapId(response)
  };
}

(initApplicationConfig as any)({
  disableIconLayerRemoteSvgIcons:
    String(import.meta.env.VITE_QMAP_DISABLE_REMOTE_SVG_ICONS || 'true').toLowerCase() !== 'false'
});

const {store, keplerGlGetState} = createQMapStore({
  keplerInstanceId: KEPLER_INSTANCE_ID,
  initialMapViewport: QMAP_INITIAL_MAP_VIEWPORT,
  initialBasemap: QMAP_INITIAL_BASEMAP,
  initialUiState: {
    readOnly: typeof QMAP_HASH_UI_PRESET?.readOnly === 'boolean' ? QMAP_HASH_UI_PRESET.readOnly : false,
    activeSidePanel: QMAP_INITIAL_ACTIVE_SIDE_PANEL,
    locale:
      typeof QMAP_HASH_UI_PRESET?.locale === 'string' && QMAP_HASH_UI_PRESET.locale.trim()
        ? QMAP_HASH_UI_PRESET.locale.trim()
        : 'it',
    qmapUserType: QMAP_INITIAL_USER_CONTEXT.userType,
    qmapUserGroupSlug: QMAP_INITIAL_USER_CONTEXT.groupSlug,
    qmapMode: QMAP_INITIAL_MODE,
    mapControls: QMAP_INITIAL_MAP_CONTROLS
  },
  drawStressorDefaultBasemap: QMAP_DRAW_STRESSOR_DEFAULT_BASEMAP,
  toggleReadOnlyActionType: TOGGLE_QMAP_READ_ONLY,
  applyQMapUiPresetActionType: APPLY_QMAP_UI_PRESET,
  applyQMapUiPresetToUiState
});

// Pre-configure AI assistant so the default baseUrl (api.openai.com) is never
// visible to any component.  Without this, a GET to api.openai.com can leak on
// boot before the useEffect-based updateAiAssistantConfig fires.
store.dispatch(
  updateAiAssistantConfig({
    isReady: true,
    baseUrl: QMAP_AI_BASE_URL,
    apiKey: '',
    temperature: QMAP_AI_TEMPERATURE,
    topP: QMAP_AI_TOP_P,
    mapboxToken: MAPBOX_TOKEN
  })
);

const QMapKeplerGl = (injectComponents as any)([
  [PanelHeaderFactory, CustomPanelHeaderFactory],
  [SidebarFactory, CustomSidebarFactory],
  [PanelToggleFactory, QMapPanelToggleFactory],
  [CustomPanelsFactory, QMapCustomPanelsFactory],
  [LoadDataModalFactory, QMapLoadDataModalFactory],
  [LoadTileSetFactory, QMapLoadTilesetFactory],
  [FeatureActionPanelFactory, QMapFeatureActionPanelFactory],
  [MapPopoverFactory, QMapMapPopoverFactory],
  replaceMapControl()
]) as typeof KeplerGl;
const AutoSizerComponent = AutoSizer as unknown as React.ComponentType<{
  children: (size: {height: number; width: number}) => React.ReactNode;
}>;

const App = () => {
  const dispatch = useDispatch<any>();
  const [showIframeExportButton, setShowIframeExportButton] = useState<boolean>(() => isQMapIframeExportEnabled());
  const [iframeExportError, setIframeExportError] = useState<string>('');
  const [isIframeExporting, setIsIframeExporting] = useState<boolean>(false);
  const [isIframeCloudMapLoading, setIsIframeCloudMapLoading] = useState<boolean>(false);
  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }

    const applyHashPreset = () => {
      applyQMapHashPresetFromRuntimeLocation({
        dispatch,
        store,
        keplerInstanceId: KEPLER_INSTANCE_ID,
        applyQMapUiPresetActionType: APPLY_QMAP_UI_PRESET,
        allowedBasemaps: QMAP_BASEMAP_OPTIONS
      });
    };

    applyHashPreset();
    window.addEventListener('hashchange', applyHashPreset);
    return () => {
      window.removeEventListener('hashchange', applyHashPreset);
    };
  }, [dispatch]);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }
    const syncIframeExportVisibility = () => {
      setShowIframeExportButton(isQMapIframeExportEnabled());
    };
    syncIframeExportVisibility();
    window.addEventListener('hashchange', syncIframeExportVisibility);
    return () => {
      window.removeEventListener('hashchange', syncIframeExportVisibility);
    };
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined' || !isQMapIframeExportEnabled()) {
      return;
    }
    const cloudMapReference = getQMapIframeCloudMapReference(window.location.hash);
    if (!cloudMapReference?.id) {
      return;
    }

    let cancelled = false;
    const provider =
      getQMapCloudProviderByName(cloudMapReference.provider) || getQMapIframeExportProvider();
    if (!provider) {
      setIframeExportError(`Cloud provider "${cloudMapReference.provider}" is not configured.`);
      return;
    }

    const loadCloudMapForIframe = async () => {
      setIsIframeCloudMapLoading(true);
      try {
        if (typeof provider.login === 'function') {
          await provider.login();
        }
        dispatch(
          wrapTo(
            'map',
            loadCloudMap({
              provider: provider as any,
              loadParams: {
                id: cloudMapReference.id
              },
              onSuccess: () => {
                if (cancelled) {
                  return {
                    type: '@@q-map/IFRAME_CLOUD_MAP_LOAD_CANCELLED'
                  };
                }
                setIsIframeCloudMapLoading(false);
                return {
                  type: '@@q-map/IFRAME_CLOUD_MAP_LOADED'
                };
              },
              onError: ({error}: any) => {
                if (cancelled) {
                  return {
                    type: '@@q-map/IFRAME_CLOUD_MAP_LOAD_ERROR_CANCELLED'
                  };
                }
                setIsIframeCloudMapLoading(false);
                setIframeExportError(
                  `Cloud map load failed: ${String(error?.message || error || 'Unknown error')}`
                );
                return {
                  type: '@@q-map/IFRAME_CLOUD_MAP_LOAD_ERROR'
                };
              }
            }) as any
          )
        );
      } catch (error: any) {
        if (cancelled) {
          return;
        }
        setIsIframeCloudMapLoading(false);
        setIframeExportError(
          `Cloud map load failed: ${String(error?.message || error || 'Unknown error')}`
        );
      }
    };

    void loadCloudMapForIframe();
    return () => {
      cancelled = true;
    };
  }, [dispatch]);

  const onViewStateChange = useCallback(
    (viewState: any, width: number, height: number) => {
      const nextViewport = {
        latitude: viewState?.latitude,
        longitude: viewState?.longitude,
        zoom: viewState?.zoom,
        width: viewState?.width ?? width,
        height: viewState?.height ?? height
      };
      try {
        const viewport = new WebMercatorViewport(nextViewport);
        const nw = viewport.unproject([0, 0]);
        const se = viewport.unproject([viewport.width, viewport.height]);
        dispatch(setMapBoundary(nw, se));
      } catch {
        // Ignore invalid transition states while viewport is settling.
      }
    },
    [dispatch]
  );

  const postMapStateToParent = useCallback(async () => {
    if (isIframeExporting) {
      return;
    }
    setIframeExportError('');
    setIsIframeExporting(true);

    const keplerState = (store.getState() as any)?.demo?.keplerGl?.[KEPLER_INSTANCE_ID];
    if (!keplerState) {
      setIsIframeExporting(false);
      return;
    }

    try {
      const snapshot = exportMap(keplerState);
      const map = snapshot?.map || null;
      if (!map) {
        throw new Error('Map snapshot is empty.');
      }

      const actionUuid = getQMapIframeActionUuid(window.location.hash);
      if (actionUuid) {
        const existingInfo = map?.info && typeof map.info === 'object' ? map.info : {};
        map.info = {
          ...existingInfo,
          title: `Action - ${actionUuid}`
        };
      }

      const cloudMapReference = getQMapIframeCloudMapReference(window.location.hash);
      const saved = await saveQMapSnapshotForIframeExport(
        snapshot as {map: any; thumbnail: Blob | null},
        cloudMapReference?.id
      );
      const postResult = postQMapIframeExportDetailed(map, KEPLER_INSTANCE_ID, {
        cloudProvider: saved.providerName,
        cloudMapId: saved.mapId
      });
      if (!postResult.ok) {
        throw new Error(describeQMapIframePostFailure(postResult.reason));
      }
    } catch (error: any) {
      setIframeExportError(String(error?.message || error || 'Failed to save map before export.'));
    } finally {
      setIsIframeExporting(false);
    }
  }, [isIframeExporting]);

  const cancelIframeExport = useCallback(() => {
    setIframeExportError('');
    const postResult = postQMapIframeCancelDetailed();
    if (!postResult.ok) {
      setIframeExportError(describeQMapIframePostFailure(postResult.reason));
    }
  }, []);

  return (
    <div
      style={{
        position: 'absolute',
        top: '0px',
        left: '0px',
        width: '100%',
        height: '100%'
      }}
    >
      <AutoSizerComponent>
        {({height, width}) => (
          <QMapKeplerGl
            mapboxApiAccessToken={MAPBOX_TOKEN}
            id={KEPLER_INSTANCE_ID}
            getState={keplerGlGetState}
            width={width}
            height={height}
            theme={qMapTheme}
            cloudProviders={CLOUD_PROVIDERS}
            localeMessages={QMAP_LOCALE_MESSAGES}
            onViewStateChange={(viewState: any) => onViewStateChange(viewState, width, height)}
          />
        )}
      </AutoSizerComponent>
      {showIframeExportButton ? (
        <div className="qmap-iframe-export-container">
          <div className="qmap-iframe-export-actions">
            <button
              type="button"
              className="qmap-iframe-cancel-button"
              onClick={cancelIframeExport}
              title="Chiudi senza inviare"
              aria-label="Chiudi senza inviare"
              disabled={isIframeExporting}
            >
              Annulla
            </button>
            <button
              type="button"
              className="qmap-iframe-export-button map-overlay-toggle"
              onClick={postMapStateToParent}
              title="Salva su Le mie mappe e invia le informazioni"
              aria-label="Salva su Le mie mappe e invia le informazioni"
              disabled={isIframeExporting || isIframeCloudMapLoading}
              aria-busy={isIframeExporting || isIframeCloudMapLoading ? 'true' : 'false'}
            >
              <span className="map-overlay-toggle-icon" aria-hidden="true">
                <svg viewBox="0 0 24 24" focusable="false" aria-hidden="true">
                  <path d="M12 2.75 19.5 7v10L12 21.25 4.5 17V7z"></path>
                  <path d="M12 8 15.5 10v4L12 16 8.5 14v-4z"></path>
                </svg>
              </span>
              <span className="map-overlay-toggle-label">
                {isIframeCloudMapLoading ? 'Caricamento...' : isIframeExporting ? 'Salvataggio...' : 'Invia'}
              </span>
            </button>
          </div>
          {iframeExportError ? (
            <div className="qmap-iframe-export-error" role="alert">
              {iframeExportError}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
};

const Root = () => (
  <Provider store={store}>
    <App />
  </Provider>
);

const container = document.getElementById('root');
async function startQMapApp() {
  await bootstrapQMapAuthFromParent();
  if (!container) {
    return;
  }
  const root = ReactDOM.createRoot(container);
  root.render(<Root />);
}

void startQMapApp();
