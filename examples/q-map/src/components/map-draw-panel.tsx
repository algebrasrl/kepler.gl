// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import React, {useCallback} from 'react';
import classnames from 'classnames';
import {useDispatch, useSelector} from 'react-redux';

import {EDITOR_MODES} from '@kepler.gl/constants';
import {setEditorMode, setFeatures, setSelectedFeature, wrapTo} from '@kepler.gl/actions';
import {MapControlButton} from '@kepler.gl/components/common/styled-components';
import ToolbarItem from '@kepler.gl/components/common/toolbar-item';
import {CursorClick, DrawPolygon, Polygon, Rectangle, Crosshairs, Trash} from '@kepler.gl/components/common/icons';
import MapControlTooltipFactory from '@kepler.gl/components/map/map-control-tooltip';
import MapControlToolbarFactory from '@kepler.gl/components/map/map-control-toolbar';
import {getQMapModeConfig, isQMapCustomControlEnabled, resolveQMapModeFromUiState} from '../mode/qmap-mode';
import {selectQMapUiState} from '../state/qmap-selectors';

QMapDrawPanelFactory.deps = [MapControlTooltipFactory, MapControlToolbarFactory];

const KEPLER_INSTANCE_ID = 'map';
const DRAW_CIRCLE_MODE = 'DRAW_CIRCLE';

function QMapDrawPanelFactory(MapControlTooltip: any, MapControlToolbar: any) {
  const defaultActionIcons = {
    polygon: DrawPolygon,
    cursor: CursorClick,
    innerPolygon: Polygon,
    rectangle: Rectangle,
    circle: Crosshairs,
    clear: Trash
  };

  const QMapDrawPanel = React.memo(
    ({
      editor,
      locale,
      mapControls,
      onToggleMapControl,
      onSetEditorMode,
      actionIcons = defaultActionIcons
    }: any) => {
      const dispatch = useDispatch();
      const isActive = mapControls?.mapDraw?.active;
      const activeMode = useSelector((state: any) => resolveQMapModeFromUiState(selectQMapUiState(state)));
      const modeConfig = React.useMemo(() => getQMapModeConfig(activeMode), [activeMode]);
      const isCustomDrawToolsEnabled = isQMapCustomControlEnabled('drawTools', modeConfig);
      const onToggleMenuPanel = useCallback(() => onToggleMapControl('mapDraw'), [onToggleMapControl]);

      const onClearFeatures = useCallback(() => {
        const isItalian = String(locale || '').toLowerCase().startsWith('it');
        const confirmMessage = isItalian
          ? 'Vuoi cancellare i disegni?'
          : 'Do you want to clear drawings?';
        if (!window.confirm(confirmMessage)) {
          return;
        }
        dispatch(wrapTo(KEPLER_INSTANCE_ID, setFeatures([])));
        dispatch(wrapTo(KEPLER_INSTANCE_ID, setSelectedFeature(null)));
        dispatch(wrapTo(KEPLER_INSTANCE_ID, setEditorMode(EDITOR_MODES.EDIT)));
      }, [dispatch, locale]);

      if (!mapControls?.mapDraw?.show || isCustomDrawToolsEnabled) {
        return null;
      }

      return (
        <div className="map-draw-controls" style={{position: 'relative'}}>
          {isActive ? (
            <MapControlToolbar show={isActive}>
              <ToolbarItem
                className="edit-feature"
                onClick={() => onSetEditorMode(EDITOR_MODES.EDIT)}
                label="toolbar.select"
                icon={actionIcons.cursor}
                active={editor.mode === EDITOR_MODES.EDIT}
              />
              <ToolbarItem
                className="draw-feature"
                onClick={() => onSetEditorMode(EDITOR_MODES.DRAW_POLYGON)}
                label="toolbar.polygon"
                icon={actionIcons.innerPolygon}
                active={editor.mode === EDITOR_MODES.DRAW_POLYGON}
              />
              <ToolbarItem
                className="draw-rectangle"
                onClick={() => onSetEditorMode(EDITOR_MODES.DRAW_RECTANGLE)}
                label="toolbar.rectangle"
                icon={actionIcons.rectangle}
                active={editor.mode === EDITOR_MODES.DRAW_RECTANGLE}
              />
              <ToolbarItem
                className="draw-circle"
                onClick={() => onSetEditorMode(DRAW_CIRCLE_MODE)}
                label="toolbar.radius"
                icon={actionIcons.circle}
                active={editor.mode === DRAW_CIRCLE_MODE}
              />
              <ToolbarItem
                className="clear-features"
                onClick={onClearFeatures}
                label="fieldSelector.clearAll"
                icon={actionIcons.clear}
                active={false}
              />
            </MapControlToolbar>
          ) : null}
          <MapControlTooltip id="map-draw" message="tooltip.DrawOnMap">
            <MapControlButton
              className={classnames('map-control-button', 'map-draw', {isActive})}
              onClick={(e: React.MouseEvent<HTMLButtonElement>) => {
                e.preventDefault();
                onToggleMenuPanel();
              }}
              active={isActive}
            >
              <actionIcons.polygon height="18px" />
            </MapControlButton>
          </MapControlTooltip>
        </div>
      );
    }
  );

  QMapDrawPanel.displayName = 'QMapDrawPanel';
  return QMapDrawPanel;
}

export default QMapDrawPanelFactory;
