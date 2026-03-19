import React from 'react';
import classnames from 'classnames';
import {useDispatch, useSelector} from 'react-redux';
import {wrapTo} from '@kepler.gl/actions';
import {MapControlButton} from '@kepler.gl/components/common/styled-components';
import ToolbarItem from '@kepler.gl/components/common/toolbar-item';
import MapControlTooltipFactory from '@kepler.gl/components/map/map-control-tooltip';
import MapControlToolbarFactory from '@kepler.gl/components/map/map-control-toolbar';
import {setQMapMode} from '../features/qmap-mode/actions';
import {
  getQMapModeOptionsForUser,
  QMapMode,
  getQMapUserModeContextFromUiState,
  resolveQMapModeFromUiState
} from '../mode/qmap-mode';
import {selectQMapUiState} from '../state/qmap-selectors';

const KEPLER_INSTANCE_ID = 'map';

QMapModeSelectorControlFactory.deps = [MapControlTooltipFactory, MapControlToolbarFactory];

function QMapModeSelectorControlFactory(MapControlTooltip: any, MapControlToolbar: any) {
  const QMapModeSelectorControl = React.memo(() => {
    const dispatch = useDispatch<any>();
    const [isOpen, setIsOpen] = React.useState(false);
    const uiState = useSelector((state: any) => selectQMapUiState(state) || {});
    const activeMode = React.useMemo(() => resolveQMapModeFromUiState(uiState), [uiState]);
    const userContext = React.useMemo(() => getQMapUserModeContextFromUiState(uiState), [uiState]);
    const modeOptions = React.useMemo(() => getQMapModeOptionsForUser(userContext), [userContext]);
    const modeShortLabel =
      activeMode === 'draw-stressor'
        ? 'DS'
        : activeMode === 'draw-on-map'
          ? 'DM'
          : activeMode === 'geotoken'
            ? 'GR'
          : 'KP';

    const selectMode = React.useCallback(
      (mode: QMapMode) => {
        dispatch(wrapTo(KEPLER_INSTANCE_ID, setQMapMode(mode)));
        setIsOpen(false);
      },
      [dispatch]
    );

    return (
      <div className="qmap-mode-selector-control" style={{position: 'relative'}}>
        {isOpen ? (
          <MapControlToolbar show={isOpen}>
            {modeOptions.map(item => (
              <ToolbarItem
                key={item.id}
                onClick={() => selectMode(item.id)}
                label={item.label}
                active={activeMode === item.id}
              />
            ))}
          </MapControlToolbar>
        ) : null}
        <MapControlTooltip id="qmap-mode-selector" message="qmapMode.tooltip">
          <MapControlButton
            className={classnames('map-control-button', 'qmap-mode-selector', {isActive: isOpen})}
            onClick={(event: React.MouseEvent<HTMLButtonElement>) => {
              event.preventDefault();
              setIsOpen(previous => !previous);
            }}
            active={isOpen}
          >
            <span style={{fontSize: '10px', fontWeight: 700}}>{modeShortLabel}</span>
          </MapControlButton>
        </MapControlTooltip>
      </div>
    );
  });

  QMapModeSelectorControl.displayName = 'QMapModeSelectorControl';
  return QMapModeSelectorControl;
}

export default QMapModeSelectorControlFactory;
