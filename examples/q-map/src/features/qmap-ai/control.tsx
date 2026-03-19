import React from 'react';
import classnames from 'classnames';
import {useDispatch, useSelector} from 'react-redux';
import {MapControlButton} from '@kepler.gl/components/common/styled-components';
import MapControlTooltipFactory from '@kepler.gl/components/map/map-control-tooltip';
import styled from 'styled-components';
import {closeQMapAiPanel, toggleQMapAiPanel} from './actions';
import {getQMapModeConfig, isQMapCustomControlEnabled, resolveQMapModeFromUiState} from '../../mode/qmap-mode';
import {selectQMapAiPanelState, selectQMapUiState} from '../../state/qmap-selectors';
import type {QMapRootState} from '../../state/qmap-state-types';

QMapAiControlFactory.deps = [MapControlTooltipFactory];

const PANEL_WIDTH = 320;
const PANEL_MARGIN = 12;
const CONTROL_GAP = 56;
const MIN_PANEL_WIDTH = 320;
const MIN_PANEL_HEIGHT = 360;
const DRAG_MOVE_ID = 'qmap-ai-move';
const DRAG_RESIZE_ID = 'qmap-ai-resize';
const LazyQMapAiPanel = React.lazy(() => import('./panel'));

const DragShell = styled.div<{height: number; width: number}>`
  position: fixed;
  z-index: 30;
  border-radius: 4px;
  border: 1px solid #d300ff;
  box-shadow: 0 10px 30px rgba(15, 23, 42, 0.22);
  width: ${props => props.width}px;
  max-width: calc(100vw - 24px);
  height: ${props => props.height}px;
  max-height: calc(100vh - 24px);
  overflow: hidden;

  &:hover .qmap-ai-move-handle,
  &:hover .qmap-ai-resize-corner-handle {
    opacity: 1;
    pointer-events: auto;
  }
`;

const MoveHandle = styled.div`
  position: absolute;
  top: 0;
  left: 50%;
  transform: translate(-50%, -50%);
  width: 48px;
  height: 16px;
  border-radius: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: move;
  background: ${props => props.theme.activeColor || '#8b5cf6'};
  color: #ffffff;
  opacity: 1;
  pointer-events: auto;
  transition: opacity 0.2s ease-in-out;
  z-index: 2;
`;

const ResizeCornerHandle = styled.div`
  position: absolute;
  bottom: 0;
  left: 0;
  transform: translate(-50%, 50%);
  width: 18px;
  height: 18px;
  border-radius: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: nesw-resize;
  background: ${props => props.theme.activeColor || '#8b5cf6'};
  color: #f8fafc;
  opacity: 1;
  pointer-events: auto;
  transition: opacity 0.2s ease-in-out;
  z-index: 2;
`;

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

function getInitialLeft() {
  if (typeof window === 'undefined') {
    return PANEL_MARGIN;
  }
  return Math.max(PANEL_MARGIN, window.innerWidth - PANEL_WIDTH - CONTROL_GAP);
}

function QMapAiControlFactory(MapControlTooltip: any) {
  const QMapAiControl = React.memo(() => {
    const dispatch = useDispatch();
    const isOpen = useSelector((state: QMapRootState) => Boolean(selectQMapAiPanelState(state)?.isOpen));
    const activeMode = useSelector((state: QMapRootState) => resolveQMapModeFromUiState(selectQMapUiState(state)));
    const modeConfig = React.useMemo(() => getQMapModeConfig(activeMode), [activeMode]);
    const isControlEnabled = isQMapCustomControlEnabled('ai', modeConfig);
    const [left, setLeft] = React.useState(getInitialLeft);
    const [top, setTop] = React.useState(PANEL_MARGIN);
    const [panelWidth, setPanelWidth] = React.useState(
      typeof window === 'undefined' ? PANEL_WIDTH : Math.min(PANEL_WIDTH, window.innerWidth - 2 * PANEL_MARGIN)
    );
    const [panelHeight, setPanelHeight] = React.useState(
      typeof window === 'undefined' ? 640 : Math.max(MIN_PANEL_HEIGHT, window.innerHeight - 2 * PANEL_MARGIN)
    );
    const dragStartRef = React.useRef({left: 0, top: 0, panelWidth: 0, panelHeight: 0});

    React.useEffect(() => {
      if (typeof window === 'undefined') {
        return;
      }
      const onResize = () => {
        const maxPanelWidth = Math.max(MIN_PANEL_WIDTH, window.innerWidth - 2 * PANEL_MARGIN);
        const nextWidth = clamp(panelWidth, MIN_PANEL_WIDTH, maxPanelWidth);
        const maxLeft = Math.max(PANEL_MARGIN, window.innerWidth - nextWidth - PANEL_MARGIN);
        const maxTop = Math.max(PANEL_MARGIN, window.innerHeight - MIN_PANEL_HEIGHT - PANEL_MARGIN);
        const maxHeight = Math.max(MIN_PANEL_HEIGHT, window.innerHeight - PANEL_MARGIN - top);

        setPanelWidth(nextWidth);
        setLeft(prev => clamp(prev, PANEL_MARGIN, maxLeft));
        setTop(prev => clamp(prev, PANEL_MARGIN, maxTop));
        setPanelHeight(prev => clamp(prev, MIN_PANEL_HEIGHT, maxHeight));
      };
      window.addEventListener('resize', onResize);
      return () => window.removeEventListener('resize', onResize);
    }, [panelWidth, top]);

    React.useEffect(() => {
      if (!isControlEnabled && isOpen) {
        dispatch(closeQMapAiPanel() as any);
      }
    }, [dispatch, isControlEnabled, isOpen]);

    const startMoveDrag = (event: React.MouseEvent<HTMLDivElement>) => {
      event.preventDefault();
      dragStartRef.current = {left, top, panelWidth, panelHeight};
      const originX = event.clientX;
      const originY = event.clientY;

      const onMove = (moveEvent: MouseEvent) => {
        if (typeof window === 'undefined') {
          return;
        }
        const maxLeft = Math.max(PANEL_MARGIN, window.innerWidth - panelWidth - PANEL_MARGIN);
        const maxTop = Math.max(PANEL_MARGIN, window.innerHeight - MIN_PANEL_HEIGHT - PANEL_MARGIN);
        setLeft(clamp(dragStartRef.current.left + (moveEvent.clientX - originX), PANEL_MARGIN, maxLeft));
        setTop(clamp(dragStartRef.current.top + (moveEvent.clientY - originY), PANEL_MARGIN, maxTop));
      };

      const onUp = () => {
        window.removeEventListener('mousemove', onMove);
        window.removeEventListener('mouseup', onUp);
      };

      window.addEventListener('mousemove', onMove);
      window.addEventListener('mouseup', onUp);
    };

    const startResizeCornerDrag = (event: React.MouseEvent<HTMLDivElement>) => {
      event.preventDefault();
      dragStartRef.current = {left, top, panelWidth, panelHeight};
      const startLeft = dragStartRef.current.left;
      const startWidth = dragStartRef.current.panelWidth;
      const rightEdge = startLeft + startWidth;
      const minLeftByWidth = rightEdge - MIN_PANEL_WIDTH;
      const originY = event.clientY;
      const originX = event.clientX;

      const onMove = (moveEvent: MouseEvent) => {
        if (typeof window === 'undefined') {
          return;
        }
        const dx = moveEvent.clientX - originX;
        const dy = moveEvent.clientY - originY;

        const maxHeight = Math.max(MIN_PANEL_HEIGHT, window.innerHeight - PANEL_MARGIN - top);
        setPanelHeight(clamp(dragStartRef.current.panelHeight + dy, MIN_PANEL_HEIGHT, maxHeight));

        const newLeft = clamp(startLeft + dx, PANEL_MARGIN, minLeftByWidth);
        setLeft(newLeft);
        setPanelWidth(Math.max(MIN_PANEL_WIDTH, rightEdge - newLeft));
      };

      const onUp = () => {
        window.removeEventListener('mousemove', onMove);
        window.removeEventListener('mouseup', onUp);
      };

      window.addEventListener('mousemove', onMove);
      window.addEventListener('mouseup', onUp);
    };

    if (!isControlEnabled) {
      return null;
    }

    return (
      <div className="map-qmap-ai-control" style={{position: 'relative'}}>
        <MapControlTooltip id="qmap-ai" message="qmapAi.tooltip">
          <MapControlButton
            className={classnames('map-control-button', 'qmap-ai', {isActive: isOpen})}
            onClick={(e: React.MouseEvent<HTMLButtonElement>) => {
              e.preventDefault();
              dispatch(toggleQMapAiPanel());
            }}
            active={isOpen}
          >
            <span style={{fontSize: '10px', fontWeight: 700, color: '#d300ff'}}>AI</span>
          </MapControlButton>
        </MapControlTooltip>
        {isOpen ? (
          <DragShell
            style={{
              top,
              left
            }}
            width={panelWidth}
            height={panelHeight}
          >
            <div className="qmap-custom-ai-panel">
              <React.Suspense fallback={<div style={{padding: 12, fontSize: 11}}>Loading assistant...</div>}>
                <LazyQMapAiPanel />
              </React.Suspense>
            </div>
            <MoveHandle className="qmap-ai-move-handle" onMouseDown={startMoveDrag}>
              <span style={{fontSize: '11px', fontWeight: 700, lineHeight: 1}}>⋮⋮</span>
            </MoveHandle>
            <ResizeCornerHandle className="qmap-ai-resize-corner-handle" onMouseDown={startResizeCornerDrag}>
              <span style={{fontSize: '11px', fontWeight: 700, lineHeight: 1}}>◢</span>
            </ResizeCornerHandle>
          </DragShell>
        ) : null}
      </div>
    );
  });

  QMapAiControl.displayName = 'QMapAiControl';
  return QMapAiControl;
}

export default QMapAiControlFactory;
