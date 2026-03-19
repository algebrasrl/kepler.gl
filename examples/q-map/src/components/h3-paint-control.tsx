import React from 'react';
import classnames from 'classnames';
import styled from 'styled-components';
import {useDispatch, useSelector} from 'react-redux';
import {replaceDataInMap, wrapTo} from '@kepler.gl/actions';
import {MapControlButton} from '@kepler.gl/components/common/styled-components';
import MapControlTooltipFactory from '@kepler.gl/components/map/map-control-tooltip';
import {setQMapH3PaintActive, setQMapH3PaintResolution} from '../features/h3-paint/actions';
import {H3_PAINT_RESOLUTIONS} from '../features/h3-paint/reducer';
import {
  getH3PaintDataset,
  H3_PAINT_FIELDS,
  H3_PAINT_DATASET_LABEL_PREFIX
} from '../features/h3-paint/utils';
import {getQMapModeConfig, isQMapCustomControlEnabled, resolveQMapModeFromUiState} from '../mode/qmap-mode';
import {selectQMapDatasets, selectQMapUiState} from '../state/qmap-selectors';

QMapH3PaintControlFactory.deps = [MapControlTooltipFactory];

const KEPLER_INSTANCE_ID = 'map';

const ResolutionPanel = styled.div`
  position: absolute;
  right: calc(100% + 8px);
  top: 0;
  z-index: 40;
  width: 136px;
  padding: 8px;
  border-radius: 8px;
  border: 1px solid #2b3240;
  background: #1f2937;
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.28);
`;

const ResolutionTitle = styled.div`
  color: #e5e7eb;
  font-size: 11px;
  font-weight: 700;
  margin-bottom: 6px;
`;

const ResolutionGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 4px;
`;

const ResolutionButton = styled.button<{active: boolean}>`
  border: 1px solid ${props => (props.active ? '#d300ff' : '#3d4453')};
  background: ${props => (props.active ? 'rgba(211, 0, 255, 0.16)' : '#111827')};
  color: ${props => (props.active ? '#f3e8ff' : '#d1d5db')};
  height: 28px;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 700;
  cursor: pointer;

  &:hover {
    border-color: #d300ff;
    color: #f3e8ff;
  }
`;

const InfoText = styled.div`
  margin-top: 6px;
  font-size: 10px;
  color: #9ca3af;
`;

const ClearButton = styled.button`
  margin-top: 8px;
  width: 100%;
  height: 30px;
  border-radius: 6px;
  border: 1px solid #d300ff;
  background: #111827;
  color: #f3e8ff;
  font-size: 12px;
  font-weight: 700;
  cursor: pointer;

  &:hover {
    background: #1f2937;
  }
`;

function getCanvasElement() {
  return document.querySelector('.mapboxgl-canvas, .maplibregl-canvas') as HTMLCanvasElement | null;
}

function QMapH3PaintControlFactory(MapControlTooltip: any) {
  const QMapH3PaintControl = React.memo(() => {
    const dispatch = useDispatch<any>();
    const datasets = useSelector((state: any) => selectQMapDatasets(state));
    const isPaintActive = useSelector((state: any) => Boolean(state?.demo?.h3Paint?.active));
    const resolution = useSelector((state: any) => Number(state?.demo?.h3Paint?.resolution || 7));
    const activeMode = useSelector((state: any) => resolveQMapModeFromUiState(selectQMapUiState(state)));
    const modeConfig = React.useMemo(() => getQMapModeConfig(activeMode), [activeMode]);
    const isControlEnabled = isQMapCustomControlEnabled('h3Paint', modeConfig);

    React.useEffect(() => {
      const canvas = getCanvasElement();
      if (!canvas) return;
      canvas.style.cursor = isPaintActive ? 'crosshair' : '';

      return () => {
        canvas.style.cursor = '';
      };
    }, [isPaintActive]);

    React.useEffect(() => {
      if (!isControlEnabled && isPaintActive) {
        dispatch(setQMapH3PaintActive(false));
      }
    }, [dispatch, isControlEnabled, isPaintActive]);

    const onClearCurrent = React.useCallback(() => {
      const existingDataset = getH3PaintDataset(datasets, resolution);
      if (!existingDataset?.id) return;
      const targetLabel = `${H3_PAINT_DATASET_LABEL_PREFIX}${resolution}`;
      const datasetToUse = {
        info: {
          id: existingDataset.id,
          label: existingDataset.label || targetLabel
        },
        data: {
          fields: H3_PAINT_FIELDS as any,
          rows: []
        }
      };
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
    }, [datasets, dispatch, resolution]);

    if (!isControlEnabled) {
      return null;
    }

    return (
      <div className="qmap-h3-paint-control" style={{position: 'relative'}}>
        <MapControlTooltip id="qmap-h3-paint" message="qmapHexPaint.tooltip">
          <MapControlButton
            className={classnames('map-control-button', 'qmap-h3-paint', {isActive: isPaintActive})}
            onClick={(e: React.MouseEvent<HTMLButtonElement>) => {
              e.preventDefault();
              dispatch(setQMapH3PaintActive(!isPaintActive));
            }}
            active={isPaintActive}
          >
            <span style={{fontSize: '10px', fontWeight: 700}}>H3</span>
          </MapControlButton>
        </MapControlTooltip>
        {isPaintActive ? (
          <ResolutionPanel>
            <ResolutionTitle>Hex Draw Resolution</ResolutionTitle>
            <ResolutionGrid>
              {H3_PAINT_RESOLUTIONS.map(item => (
                <ResolutionButton
                  key={item}
                  type="button"
                  active={item === resolution}
                  onClick={() => dispatch(setQMapH3PaintResolution(item))}
                >
                  {item}
                </ResolutionButton>
              ))}
            </ResolutionGrid>
            <InfoText>Dataset: {H3_PAINT_DATASET_LABEL_PREFIX}{resolution}</InfoText>
            <ClearButton type="button" onClick={onClearCurrent}>
              Clear current
            </ClearButton>
          </ResolutionPanel>
        ) : null}
      </div>
    );
  });

  QMapH3PaintControl.displayName = 'QMapH3PaintControl';
  return QMapH3PaintControl;
}

export default QMapH3PaintControlFactory;
