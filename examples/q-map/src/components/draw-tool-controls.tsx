import React from 'react';
import classnames from 'classnames';
import {useDispatch, useSelector} from 'react-redux';

import {EDITOR_MODES} from '@kepler.gl/constants';
import {
  setEditorMode,
  setFeatures,
  setMapControlSettings,
  setSelectedFeature,
  toggleMapControl,
  wrapTo
} from '@kepler.gl/actions';
import {MapControlButton} from '@kepler.gl/components/common/styled-components';
import ToolbarItem from '@kepler.gl/components/common/toolbar-item';
import {
  Crosshairs,
  CursorClick,
  CursorPoint,
  Polygon,
  Rectangle,
  Trash
} from '@kepler.gl/components/common/icons';
import MapControlTooltipFactory from '@kepler.gl/components/map/map-control-tooltip';
import MapControlToolbarFactory from '@kepler.gl/components/map/map-control-toolbar';

import {clearQMapDrawActiveTool, setQMapDrawActiveTool, setQMapDrawLineStart} from '../features/qmap-draw/actions';
import {
  QMAP_DRAW_DRAFT_PROPERTY,
  QMAP_DRAW_SKIP_DATASET_SYNC_FLAG,
  QMAP_MAP_DRAW_SETTING_BYPASS_EDITOR_CLICK,
  QMAP_MAP_DRAW_SETTING_DISABLE_DOUBLE_CLICK_ZOOM,
  QMAP_MAP_DRAW_SETTING_FORCE_CROSSHAIR,
  QMAP_DRAW_TOOL_PROPERTY,
  QMAP_DRAW_TARGET_PROPERTY,
  QMapDrawTarget,
  QMapDrawTool
} from '../features/qmap-draw/constants';
import {getQMapModeConfig, isQMapCustomControlEnabled, resolveQMapModeFromUiState} from '../mode/qmap-mode';
import {selectQMapEditorFeatures, selectQMapUiState} from '../state/qmap-selectors';

type DrawToolItem = {
  id: QMapDrawTool;
  label: string;
  icon: any;
};

const KEPLER_INSTANCE_ID = 'map';

const DRAW_TOOL_ITEMS: DrawToolItem[] = [
  {id: 'point', label: 'qmapDrawTools.point', icon: CursorPoint},
  {id: 'line', label: 'qmapDrawTools.line', icon: CursorClick},
  {id: 'polygon', label: 'qmapDrawTools.polygon', icon: Polygon},
  {id: 'rectangle', label: 'qmapDrawTools.rectangle', icon: Rectangle},
  {id: 'radius', label: 'qmapDrawTools.radius', icon: Crosshairs}
];

function getDrawToolItems(target: QMapDrawTarget): DrawToolItem[] {
  if (target === 'stressor') {
    return DRAW_TOOL_ITEMS.filter(item => item.id !== 'point' && item.id !== 'line');
  }
  return DRAW_TOOL_ITEMS;
}

function resolveEditorModeForTool(tool: QMapDrawTool): string {
  if (tool === 'polygon') return EDITOR_MODES.DRAW_POLYGON;
  if (tool === 'rectangle') return EDITOR_MODES.DRAW_RECTANGLE;
  if (tool === 'radius') return 'DRAW_CIRCLE';
  return EDITOR_MODES.EDIT;
}

function getControlButtonContent(target: QMapDrawTarget) {
  if (target === 'stressor') {
    return (
      <span style={{fontSize: '10px', fontWeight: 700, letterSpacing: '0.04em'}}>PE</span>
    );
  }

  return (
    <span style={{fontSize: '10px', fontWeight: 700, letterSpacing: '0.04em'}}>OP</span>
  );
}

function buildDrawControlFactory(target: QMapDrawTarget, tooltipLabel: string) {
  function QMapDrawControlFactory(MapControlTooltip: any, MapControlToolbar: any) {
    const QMapDrawControl = React.memo(({mapControls}: any) => {
      const dispatch = useDispatch<any>();
      const [isOpen, setIsOpen] = React.useState(false);
      const activeTarget = useSelector((state: any) => state?.demo?.qmapDraw?.activeTarget || null);
      const activeTool = useSelector((state: any) => state?.demo?.qmapDraw?.activeTool || null);
      const features = useSelector((state: any) => selectQMapEditorFeatures(state));
      const activeMode = useSelector((state: any) => resolveQMapModeFromUiState(selectQMapUiState(state)));
      const modeConfig = React.useMemo(() => getQMapModeConfig(activeMode), [activeMode]);
      const isControlEnabled = isQMapCustomControlEnabled('drawTools', modeConfig);

      const isVisible = Boolean(mapControls?.mapDraw?.show);
      const isControlActive = Boolean(isOpen || activeTarget === target);
      const isTargetAllowedForMode = !(activeMode === 'geotoken' && target === 'operations');

      const exitDrawModeForCurrentControl = React.useCallback(() => {
        if (activeTarget !== target) {
          return;
        }
        const cleanedFeatures = (features || []).filter((feature: any) => {
          const featureTarget = String(feature?.properties?.[QMAP_DRAW_TARGET_PROPERTY] || '').trim();
          return featureTarget !== target;
        });
        if (cleanedFeatures.length !== (features || []).length) {
          const wrappedAction: any = wrapTo(KEPLER_INSTANCE_ID, setFeatures(cleanedFeatures as any));
          if (wrappedAction?.payload?.meta?._id_) {
            wrappedAction.payload[QMAP_DRAW_SKIP_DATASET_SYNC_FLAG] = true;
          } else {
            wrappedAction[QMAP_DRAW_SKIP_DATASET_SYNC_FLAG] = true;
          }
          dispatch(wrappedAction);
        }
        dispatch(clearQMapDrawActiveTool());
        dispatch(setQMapDrawLineStart(target, null));
        if (target === 'stressor') {
          dispatch(
            wrapTo(
              KEPLER_INSTANCE_ID,
              setMapControlSettings('mapDraw', {
                [QMAP_MAP_DRAW_SETTING_BYPASS_EDITOR_CLICK]: false,
                [QMAP_MAP_DRAW_SETTING_FORCE_CROSSHAIR]: false,
                [QMAP_MAP_DRAW_SETTING_DISABLE_DOUBLE_CLICK_ZOOM]: false
              })
            )
          );
        }
        if (mapControls?.mapDraw?.active) {
          dispatch(wrapTo(KEPLER_INSTANCE_ID, toggleMapControl('mapDraw', 0)));
        }
        dispatch(wrapTo(KEPLER_INSTANCE_ID, setSelectedFeature(null)));
        dispatch(wrapTo(KEPLER_INSTANCE_ID, setEditorMode(EDITOR_MODES.EDIT)));
      }, [activeTarget, dispatch, features, mapControls, target]);

      React.useEffect(() => {
        if (!isControlEnabled && isOpen) {
          setIsOpen(false);
          exitDrawModeForCurrentControl();
        }
      }, [exitDrawModeForCurrentControl, isControlEnabled, isOpen]);

      React.useEffect(() => {
        if (target !== 'stressor') {
          return;
        }
        const isPointOrLineBehavior =
          activeMode === 'draw-stressor' &&
          Boolean(activeTarget) &&
          (activeTool === 'point' || activeTool === 'line');
        const shouldUseLineDrawingBehavior =
          activeMode === 'draw-stressor' && Boolean(activeTarget) && activeTool === 'line';
        dispatch(
          wrapTo(
            KEPLER_INSTANCE_ID,
            setMapControlSettings('mapDraw', {
              [QMAP_MAP_DRAW_SETTING_BYPASS_EDITOR_CLICK]: shouldUseLineDrawingBehavior,
              [QMAP_MAP_DRAW_SETTING_FORCE_CROSSHAIR]: isPointOrLineBehavior,
              [QMAP_MAP_DRAW_SETTING_DISABLE_DOUBLE_CLICK_ZOOM]: shouldUseLineDrawingBehavior
            })
          )
        );
      }, [activeMode, activeTarget, activeTool, dispatch, target]);

      const onSelectTool = React.useCallback(
        (tool: QMapDrawTool) => {
          const hadDraftLine = (features || []).some((feature: any) => {
            const featureTarget = String(feature?.properties?.[QMAP_DRAW_TARGET_PROPERTY] || '').trim();
            const isDraft = Boolean(feature?.properties?.[QMAP_DRAW_DRAFT_PROPERTY]);
            const featureTool = String(feature?.properties?.[QMAP_DRAW_TOOL_PROPERTY] || '').trim();
            return featureTarget === target && isDraft && featureTool === 'line';
          });
          if (tool !== 'line' && hadDraftLine) {
            const cleaned = (features || []).filter((feature: any) => {
              const featureTarget = String(feature?.properties?.[QMAP_DRAW_TARGET_PROPERTY] || '').trim();
              const isDraft = Boolean(feature?.properties?.[QMAP_DRAW_DRAFT_PROPERTY]);
              const featureTool = String(feature?.properties?.[QMAP_DRAW_TOOL_PROPERTY] || '').trim();
              return !(featureTarget === target && isDraft && featureTool === 'line');
            });
            dispatch(wrapTo(KEPLER_INSTANCE_ID, setFeatures(cleaned as any)));
            dispatch(wrapTo(KEPLER_INSTANCE_ID, setSelectedFeature(null)));
          }
          dispatch(setQMapDrawActiveTool(target, tool));
          if (tool !== 'line') {
            dispatch(setQMapDrawLineStart(target, null));
          }
          const requiresKeplerDraw = tool === 'polygon' || tool === 'rectangle' || tool === 'radius';
          if (requiresKeplerDraw && !mapControls?.mapDraw?.active) {
            dispatch(wrapTo(KEPLER_INSTANCE_ID, toggleMapControl('mapDraw', 0)));
          }
          if (!requiresKeplerDraw && mapControls?.mapDraw?.active) {
            dispatch(wrapTo(KEPLER_INSTANCE_ID, toggleMapControl('mapDraw', 0)));
          }
          dispatch(wrapTo(KEPLER_INSTANCE_ID, setEditorMode(resolveEditorModeForTool(tool))));
        },
        [dispatch, features, mapControls, target]
      );

      const onClearTarget = React.useCallback(() => {
        const activeToolForTarget = activeTarget === target ? activeTool : null;
        if (!activeToolForTarget) {
          return;
        }
        const nextFeatures = (features || []).filter((feature: any) => {
          const featureTarget = String(feature?.properties?.[QMAP_DRAW_TARGET_PROPERTY] || '').trim();
          const featureTool = String(feature?.properties?.[QMAP_DRAW_TOOL_PROPERTY] || '').trim();
          return !(featureTarget === target && featureTool === activeToolForTarget);
        });
        dispatch(setQMapDrawLineStart(target, null));
        dispatch(wrapTo(KEPLER_INSTANCE_ID, setFeatures(nextFeatures as any)));
        dispatch(wrapTo(KEPLER_INSTANCE_ID, setSelectedFeature(null)));
        dispatch(wrapTo(KEPLER_INSTANCE_ID, setEditorMode(EDITOR_MODES.EDIT)));
      }, [activeTarget, activeTool, dispatch, features, target]);

      React.useEffect(() => {
        if (!(activeTarget === target && activeTool === 'line')) {
          return;
        }

        const onKeyDown = (event: KeyboardEvent) => {
          const activeElement = document.activeElement as HTMLElement | null;
          const tagName = String(activeElement?.tagName || '').toLowerCase();
          const isTypingElement =
            tagName === 'input' ||
            tagName === 'textarea' ||
            Boolean(activeElement?.isContentEditable);
          if (isTypingElement) {
            return;
          }

          const draftIndex = (features || []).findIndex((feature: any) => {
            const featureTarget = String(feature?.properties?.[QMAP_DRAW_TARGET_PROPERTY] || '').trim();
            const isDraft = Boolean(feature?.properties?.[QMAP_DRAW_DRAFT_PROPERTY]);
            const featureTool = String(feature?.properties?.[QMAP_DRAW_TOOL_PROPERTY] || '').trim();
            return featureTarget === target && isDraft && featureTool === 'line';
          });
          if (draftIndex < 0) {
            return;
          }

          if (event.key === 'Escape') {
            event.preventDefault();
            const cleaned = (features || []).filter((_: any, idx: number) => idx !== draftIndex);
            dispatch(setQMapDrawLineStart(target, null));
            dispatch(wrapTo(KEPLER_INSTANCE_ID, setFeatures(cleaned as any)));
            dispatch(wrapTo(KEPLER_INSTANCE_ID, setSelectedFeature(null)));
            dispatch(wrapTo(KEPLER_INSTANCE_ID, setEditorMode(EDITOR_MODES.EDIT)));
            return;
          }

          if (event.key === 'Enter') {
            event.preventDefault();
            const draftFeature = features[draftIndex];
            const coords = Array.isArray(draftFeature?.geometry?.coordinates)
              ? draftFeature.geometry.coordinates
              : [];
            const dedupedFinalCoords = coords.filter((coord: any, idx: number) => {
              if (idx === 0) return true;
              const prev = coords[idx - 1] || [];
              return Number(prev[0]) !== Number(coord?.[0]) || Number(prev[1]) !== Number(coord?.[1]);
            });

            if (dedupedFinalCoords.length < 2) {
              const cleaned = (features || []).filter((_: any, idx: number) => idx !== draftIndex);
              dispatch(setQMapDrawLineStart(target, null));
              dispatch(wrapTo(KEPLER_INSTANCE_ID, setFeatures(cleaned as any)));
              dispatch(wrapTo(KEPLER_INSTANCE_ID, setSelectedFeature(null)));
              dispatch(wrapTo(KEPLER_INSTANCE_ID, setEditorMode(EDITOR_MODES.EDIT)));
              return;
            }

            const finalizedLineFeature = {
              ...draftFeature,
              geometry: {
                ...(draftFeature.geometry || {}),
                type: 'LineString',
                coordinates: dedupedFinalCoords
              },
              properties: {
                ...(draftFeature.properties || {}),
                [QMAP_DRAW_DRAFT_PROPERTY]: false
              }
            };
            const nextFeatures = [...features];
            nextFeatures[draftIndex] = finalizedLineFeature;
            dispatch(setQMapDrawLineStart(target, null));
            dispatch(wrapTo(KEPLER_INSTANCE_ID, setFeatures(nextFeatures as any)));
            dispatch(wrapTo(KEPLER_INSTANCE_ID, setSelectedFeature(null)));
            dispatch(wrapTo(KEPLER_INSTANCE_ID, setEditorMode(EDITOR_MODES.EDIT)));
            return;
          }

          if (event.key === 'Backspace') {
            event.preventDefault();
            const draftFeature = features[draftIndex];
            const coords = Array.isArray(draftFeature?.geometry?.coordinates)
              ? draftFeature.geometry.coordinates
              : [];

            if (coords.length <= 1) {
              const cleaned = (features || []).filter((_: any, idx: number) => idx !== draftIndex);
              dispatch(setQMapDrawLineStart(target, null));
              dispatch(wrapTo(KEPLER_INSTANCE_ID, setFeatures(cleaned as any)));
              dispatch(wrapTo(KEPLER_INSTANCE_ID, setSelectedFeature(null)));
              dispatch(wrapTo(KEPLER_INSTANCE_ID, setEditorMode(EDITOR_MODES.EDIT)));
              return;
            }

            const nextCoords = coords.slice(0, -1);
            const nextDraft = {
              ...draftFeature,
              geometry: {
                ...(draftFeature.geometry || {}),
                type: 'LineString',
                coordinates: nextCoords
              }
            };
            const nextFeatures = [...features];
            nextFeatures[draftIndex] = nextDraft;
            const last = nextCoords[nextCoords.length - 1];
            const nextLineStart =
              Array.isArray(last) && last.length >= 2 ? ([Number(last[0]), Number(last[1])] as [number, number]) : null;
            dispatch(setQMapDrawLineStart(target, nextLineStart));
            dispatch(wrapTo(KEPLER_INSTANCE_ID, setFeatures(nextFeatures as any)));
            dispatch(wrapTo(KEPLER_INSTANCE_ID, setSelectedFeature(null)));
            dispatch(wrapTo(KEPLER_INSTANCE_ID, setEditorMode(EDITOR_MODES.EDIT)));
          }
        };

        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
      }, [activeTarget, activeTool, dispatch, features, target]);

      if (!isVisible || !isControlEnabled || !isTargetAllowedForMode) {
        return null;
      }

      return (
        <div className="qmap-draw-control" style={{position: 'relative'}}>
          {isOpen ? (
            <MapControlToolbar show={isOpen}>
              {getDrawToolItems(target).map(item => (
                <ToolbarItem
                  key={`${target}-${item.id}`}
                  className={`draw-${item.id}`}
                  onClick={() => onSelectTool(item.id)}
                  label={item.label}
                  icon={item.icon}
                  active={activeTarget === target && activeTool === item.id}
                />
              ))}
              <ToolbarItem
                key={`${target}-clear`}
                className="draw-clear"
                onClick={onClearTarget}
                label="fieldSelector.clearAll"
                icon={Trash}
                active={false}
              />
            </MapControlToolbar>
          ) : null}

          <MapControlTooltip id={`qmap-draw-${target}`} message={tooltipLabel}>
            <MapControlButton
              className={classnames('map-control-button', `qmap-draw-${target}`, {
                isActive: isControlActive
              })}
              onClick={(e: React.MouseEvent<HTMLButtonElement>) => {
                e.preventDefault();
                setIsOpen(current => {
                  const nextOpen = !current;
                  if (!nextOpen) {
                    exitDrawModeForCurrentControl();
                  }
                  return nextOpen;
                });
              }}
              active={isControlActive}
            >
              {getControlButtonContent(target)}
            </MapControlButton>
          </MapControlTooltip>
        </div>
      );
    });

    QMapDrawControl.displayName =
      target === 'stressor' ? 'QMapDrawPerimeterControl' : 'QMapDrawOperationsControl';

    return QMapDrawControl;
  }

  QMapDrawControlFactory.deps = [MapControlTooltipFactory, MapControlToolbarFactory];
  return QMapDrawControlFactory;
}

export const QMapDrawStressorControlFactory = buildDrawControlFactory(
  'stressor',
  'qmapDrawTools.drawPerimeter'
);

export const QMapDrawOperationsControlFactory = buildDrawControlFactory(
  'operations',
  'qmapDrawTools.drawOperations'
);
