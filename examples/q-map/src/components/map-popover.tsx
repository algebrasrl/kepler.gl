import React, {useState, useCallback, useContext, useMemo} from 'react';
import styled from 'styled-components';
import {useDispatch, useSelector} from 'react-redux';
import {replaceDataInMap, wrapTo} from '@kepler.gl/actions';
import {LAYER_TYPES} from '@kepler.gl/constants';
import {
  MapPopoverContentFactory,
  RootContext
} from '@kepler.gl/components';
import {Pin, ArrowLeft, ArrowRight, CursorPoint, Trash} from '@kepler.gl/components/common/icons';
import {injectIntl, IntlShape} from 'react-intl';
import {FormattedMessage} from '@kepler.gl/localization';
import {parseGeoJsonRawFeature} from '@kepler.gl/layers';
import {generateHashId, idToPolygonGeo} from '@kepler.gl/common-utils';
import {LayerHoverProp, getLayerHoverPropValue} from '@kepler.gl/reducers';
import {Feature, FeatureSelectionContext} from '@kepler.gl/types';
import {
  FloatingPortal,
  flip,
  offset,
  useClientPoint,
  useFloating,
  useInteractions
} from '@floating-ui/react';
import {
  getH3PaintDataset,
  H3_PAINT_FIELDS,
  H3_PAINT_DATASET_LABEL_PREFIX,
  readH3PaintRows
} from '../features/h3-paint/utils';
import {selectQMapDatasets} from '../state/qmap-selectors';

const SELECTABLE_LAYERS: string[] = [LAYER_TYPES.hexagonId, LAYER_TYPES.geojson];
const MAX_WIDTH = 500;
const MAX_HEIGHT = 600;
const KEPLER_INSTANCE_ID = 'map';

const StyledMapPopover = styled.div`
  display: flex;
  flex-direction: column;
  max-width: ${MAX_WIDTH}px;
  max-height: ${MAX_HEIGHT}px;
  padding: 14px;
  & > * + * {
    margin-top: 6px;
  }
  ${props => props.theme.scrollBar};
  font-family: ${props => props.theme.fontFamily};
  font-size: 11px;
  font-weight: 500;
  background-color: ${props => props.theme.panelBackground};
  color: ${props => props.theme.textColor};
  z-index: 98;
  overflow-x: auto;
  box-shadow: ${props => props.theme.panelBoxShadow};
`;

const PinnedButtons = styled.div`
  display: flex;
  align-self: center;
  align-items: center;
  justify-items: center;
  & > * + * {
    margin-left: 10px;
  }
`;

const PopoverContent = styled.div`
  display: flex;
  flex-direction: column;
  & > * + * {
    margin-top: 12px;
  }
`;

const StyledIcon = styled.div`
  color: ${props => props.theme.activeColor};
  &:hover {
    cursor: pointer;
    color: ${props => props.theme.linkBtnColor};
  }
`;

const StyledAction = styled.div`
  display: flex;
  align-items: center;
  color: ${props => props.theme.textColorHl};
  svg {
    margin-right: 6px;
  }
  &:hover {
    cursor: pointer;
    color: ${props => props.theme.linkBtnColor};
  }
`;

QMapMapPopoverFactory.deps = [MapPopoverContentFactory];

export function getSelectedFeature(layerHoverProp: LayerHoverProp | null): Feature | null {
  const layer = layerHoverProp?.layer as any;
  let fieldIdx;
  let selectedFeature;
  switch (layer?.type) {
    case LAYER_TYPES.hexagonId:
      fieldIdx = layer.config?.columns?.hex_id?.fieldIdx;
      selectedFeature = idToPolygonGeo(
        {id: getLayerHoverPropValue(layerHoverProp?.data, fieldIdx)},
        {isClosed: true}
      );
      break;
    case LAYER_TYPES.geojson:
      fieldIdx = layer.config?.columns?.geojson?.fieldIdx;
      selectedFeature = parseGeoJsonRawFeature(getLayerHoverPropValue(layerHoverProp?.data, fieldIdx));
      break;
    default:
      break;
  }

  if (!selectedFeature) return null;
  return {...selectedFeature, id: generateHashId(8)} as any;
}

type MapPopoverProps = {
  x: number;
  y: number;
  frozen?: boolean;
  coordinate: [number, number] | boolean;
  layerHoverProp: LayerHoverProp | null;
  isBase?: boolean;
  zoom: number;
  container?: HTMLElement | null;
  onClose: () => void;
  onSetFeatures: (features: Feature[]) => any;
  setSelectedFeature: (feature: Feature | null, clickContext?: FeatureSelectionContext) => any;
  featureCollection?: {
    type: string;
    features: Feature[];
  };
};

type IntlProps = {
  intl: IntlShape;
};

function resolveHoveredPaintedHex(layerHoverProp: LayerHoverProp | null): {h3Id: string; resolution: number} | null {
  const label = String((layerHoverProp as any)?.layer?.config?.label || '').trim();
  if (!label.toLowerCase().startsWith(H3_PAINT_DATASET_LABEL_PREFIX.toLowerCase())) {
    return null;
  }
  const fields = Array.isArray((layerHoverProp as any)?.fields) ? ((layerHoverProp as any).fields as any[]) : [];
  const h3Idx = fields.findIndex(f => String(f?.name || '').toLowerCase() === 'h3_id');
  if (h3Idx < 0) return null;
  const resIdx = fields.findIndex(f => String(f?.name || '').toLowerCase() === 'h3_resolution');
  const h3Id = String(getLayerHoverPropValue((layerHoverProp as any)?.data, h3Idx) || '').trim();
  if (!h3Id) return null;

  const parsedFromRow = Number(getLayerHoverPropValue((layerHoverProp as any)?.data, resIdx));
  const parsedFromLabel = Number(label.split('_r').pop());
  const resolution = Number.isFinite(parsedFromRow)
    ? parsedFromRow
    : Number.isFinite(parsedFromLabel)
    ? parsedFromLabel
    : null;
  if (!Number.isFinite(Number(resolution))) return null;
  return {h3Id, resolution: Number(resolution)};
}

export default function QMapMapPopoverFactory(MapPopoverContent: any) {
  const MapPopover: React.FC<MapPopoverProps & IntlProps> = ({
    x,
    y,
    frozen,
    coordinate,
    layerHoverProp,
    isBase,
    zoom,
    container,
    onClose,
    onSetFeatures,
    setSelectedFeature,
    featureCollection
  }) => {
    const dispatch = useDispatch<any>();
    const datasets = useSelector((state: any) => selectQMapDatasets(state));
    const [horizontalPlacement, setHorizontalPlacement] = useState('start');
    const moveLeft = () => setHorizontalPlacement('end');
    const moveRight = () => setHorizontalPlacement('start');
    const rootContext = useContext(RootContext);
    const {refs, context, floatingStyles} = useFloating({
      placement: `${horizontalPlacement == 'end' ? 'left' : 'right'}-start`,
      middleware: [offset({mainAxis: 20, alignmentAxis: 20}), flip()]
    });

    const hoveredHex = useMemo(() => resolveHoveredPaintedHex(layerHoverProp), [layerHoverProp]);

    const onSetSelectedFeature = useCallback(() => {
      const clickContext = {
        mapIndex: 0,
        rightClick: true,
        position: {x, y}
      };
      const selectedFeature = getSelectedFeature(layerHoverProp);
      if (selectedFeature) {
        setSelectedFeature(selectedFeature, clickContext);
        const updatedFeatures = featureCollection ? [...featureCollection.features, selectedFeature] : [selectedFeature];
        onSetFeatures(updatedFeatures);
      }
      onClose();
    }, [onClose, onSetFeatures, x, y, setSelectedFeature, layerHoverProp, featureCollection]);

    const onDeletePaintedHex = useCallback(() => {
      if (!hoveredHex) return;
      const dataset = getH3PaintDataset(datasets, hoveredHex.resolution);
      if (!dataset?.id) return;
      const existingRows = readH3PaintRows(dataset, hoveredHex.resolution);
      const nextRows = existingRows.filter(([id]) => id !== hoveredHex.h3Id);
      if (nextRows.length === existingRows.length) return;
      dispatch(
        wrapTo(
          KEPLER_INSTANCE_ID,
          replaceDataInMap({
            datasetToReplaceId: dataset.id,
            datasetToUse: {
              info: {
                id: dataset.id,
                label: dataset.label || `${H3_PAINT_DATASET_LABEL_PREFIX}${hoveredHex.resolution}`
              },
              data: {
                fields: H3_PAINT_FIELDS as any,
                rows: nextRows
              }
            },
            options: {
              keepExistingConfig: true,
              centerMap: false,
              autoCreateLayers: false
            }
          }) as any
        )
      );
      onClose();
    }, [datasets, dispatch, hoveredHex, onClose]);

    const containerBounds = container?.getBoundingClientRect();
    const clientPoint = useClientPoint(context, {
      x: (containerBounds?.left || 0) + x,
      y: (containerBounds?.top || 0) + y
    });
    const {getFloatingProps} = useInteractions([clientPoint]);

    return (
      <FloatingPortal root={rootContext?.current}>
        <StyledMapPopover className="map-popover" ref={refs.setFloating} style={floatingStyles} {...getFloatingProps()}>
          {frozen ? (
            <PinnedButtons>
              {horizontalPlacement === 'start' && (
                <StyledIcon className="popover-arrow-left" onClick={moveLeft}>
                  <ArrowLeft />
                </StyledIcon>
              )}
              <StyledIcon className="popover-pin" onClick={onClose}>
                <Pin height="16px" />
              </StyledIcon>
              {horizontalPlacement === 'end' && (
                <StyledIcon className="popover-arrow-right" onClick={moveRight}>
                  <ArrowRight />
                </StyledIcon>
              )}
              {isBase && (
                <div className="primary-label">
                  <FormattedMessage id="mapPopover.primary" />
                </div>
              )}
            </PinnedButtons>
          ) : null}
          <PopoverContent>
            <MapPopoverContent coordinate={coordinate} zoom={zoom} layerHoverProp={layerHoverProp} />
          </PopoverContent>
          {layerHoverProp?.layer?.type && SELECTABLE_LAYERS.includes(layerHoverProp?.layer?.type) && frozen ? (
            <StyledAction className="select-geometry" onClick={onSetSelectedFeature}>
              <CursorPoint />
              Select Geometry
            </StyledAction>
          ) : null}
          {hoveredHex && frozen ? (
            <StyledAction className="delete-painted-hex" onClick={onDeletePaintedHex}>
              <Trash />
              Delete painted hex
            </StyledAction>
          ) : null}
        </StyledMapPopover>
      </FloatingPortal>
    );
  };

  return injectIntl(MapPopover);
}
