import React, {useMemo, useState} from 'react';
import {
  ActionPanelItem,
  FeatureActionPanelProps,
  PureFeatureActionPanelFactory
} from '@kepler.gl/components';
import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {addDataToMap, replaceDataInMap, wrapTo} from '@kepler.gl/actions';
import {useDispatch} from 'react-redux';
import {
  getIntersectingH3Ids as getIntersectingH3IdsFromCover,
  getPolygonsFromGeometry,
  parseGeoJsonLike,
  polygonIntersectsPolygon,
  PolygonCoords
} from '../geo';
import {
  getH3PaintDataset,
  H3_PAINT_FIELDS,
  H3_PAINT_DATASET_LABEL_PREFIX,
  readH3PaintRows
} from '../features/h3-paint/utils';

const TASSELLATION_LABEL_PREFIX = 'Tassellation_r';
const RESOLUTIONS = [4, 5, 6, 7, 8, 9, 10, 11];

function toRows(existingRows: Array<[string, number]>, ids: string[], resolution: number) {
  const seen = new Set(existingRows.map(r => r[0]));
  const merged = [...existingRows];
  ids.forEach(id => {
    if (!seen.has(id)) {
      seen.add(id);
      merged.push([id, resolution]);
    }
  });
  return merged;
}

function QMapFeatureActionPanel(props: FeatureActionPanelProps) {
  const BaseFeatureActionPanel = useMemo(() => PureFeatureActionPanelFactory(), []);
  const dispatch = useDispatch<any>();
  const [isBusy, setIsBusy] = useState(false);

  const selectedPolygons = useMemo(
    () => getPolygonsFromGeometry(props.selectedFeature?.geometry),
    [props.selectedFeature?.geometry]
  );

  const visibleGeojsonDataset = useMemo(() => {
    const layers = Array.isArray((props as any).layers) ? (props as any).layers : [];
    const datasets = (props as any).datasets || {};
    for (const layer of layers) {
      const hidden = Boolean(layer?.config?.hidden);
      if (hidden) continue;
      const dataId = Array.isArray(layer?.config?.dataId) ? layer.config.dataId[0] : layer?.config?.dataId;
      const dataset = datasets?.[dataId];
      const hasGeojson = Boolean((dataset?.fields || []).find((f: any) => String(f?.type || '') === 'geojson'));
      if (dataset && hasGeojson) return dataset;
    }
    return null;
  }, [props]);

  const selectedHex = useMemo(() => {
    const properties = (props.selectedFeature as any)?.properties || {};
    const h3Id = String(properties?.h3_id || '').trim();
    const h3ResolutionRaw = Number(properties?.h3_resolution);
    const h3Resolution = Number.isFinite(h3ResolutionRaw) ? h3ResolutionRaw : null;
    if (!h3Id || !Number.isFinite(Number(h3Resolution))) {
      return null;
    }
    const dataset = getH3PaintDataset((props as any).datasets || {}, Number(h3Resolution));
    if (!dataset?.id) {
      return null;
    }
    return {
      h3Id,
      resolution: Number(h3Resolution),
      dataset
    };
  }, [props.selectedFeature, props.datasets]);

  const onTassellate = (resolution: number) => {
    if (isBusy) return;
    const polygons = selectedPolygons;
    if (!polygons.length) return;

    setIsBusy(true);
    try {
      const targetLabel = `${TASSELLATION_LABEL_PREFIX}${resolution}`;
      const existingDataset = Object.values(props.datasets || {}).find(
        (d: any) => String(d?.label || '').toLowerCase() === targetLabel.toLowerCase()
      ) as any;

      const newIds = getIntersectingH3IdsFromCover(polygons, resolution);
      const existingRows: Array<[string, number]> = [];
      if (existingDataset?.id) {
        const idx = Array.isArray(existingDataset.allIndexes)
          ? existingDataset.allIndexes
          : Array.from({length: Number(existingDataset.length || 0)}, (_, i) => i);
        idx.forEach((rowIdx: number) => {
          const id = existingDataset.getValue('h3_id', rowIdx);
          const res = existingDataset.getValue('h3_resolution', rowIdx);
          if (id) existingRows.push([String(id), Number.isFinite(Number(res)) ? Number(res) : resolution]);
        });
      }

      const mergedRows = toRows(existingRows, newIds, resolution).map(([id, res]) => [id, res]);
      const datasetToUse = {
        info: {
          id: existingDataset?.id || `qmap_tassellation_r${resolution}`,
          label: targetLabel
        },
        data: {
          fields: [
            {name: 'h3_id', type: ALL_FIELD_TYPES.h3},
            {name: 'h3_resolution', type: ALL_FIELD_TYPES.integer}
          ],
          rows: mergedRows
        }
      };

      if (existingDataset?.id) {
        dispatch(
          wrapTo(
            'map',
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
      } else {
        dispatch(
          wrapTo(
            'map',
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
    } finally {
      setIsBusy(false);
    }
  };

  const onCreateSelectedGeometryDataset = () => {
    if (isBusy) return;
    const geometry = props.selectedFeature?.geometry;
    if (!geometry) return;
    setIsBusy(true);
    try {
      dispatch(
        wrapTo(
          'map',
          addDataToMap({
            datasets: {
              info: {
                id: `qmap_selected_geometry_${Date.now()}`,
                label: 'Selected_Geometry'
              },
              data: {
                fields: [{name: '_geojson', type: ALL_FIELD_TYPES.geojson}],
                rows: [[geometry]]
              }
            },
            options: {
              centerMap: false,
              autoCreateLayers: true
            }
          }) as any
        )
      );
    } finally {
      setIsBusy(false);
    }
  };

  const onClipVisibleGeojsonBySelection = () => {
    if (isBusy) return;
    if (!selectedPolygons.length) return;
    const dataset = visibleGeojsonDataset as any;
    if (!dataset) return;
    const geometryField =
      (dataset.fields || []).find((f: any) => String(f?.type || '') === 'geojson')?.name || '_geojson';
    const idx = Array.isArray(dataset.allIndexes)
      ? dataset.allIndexes
      : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);

    setIsBusy(true);
    try {
      const matchedRows = idx.filter((rowIdx: number) => {
        const parsed = parseGeoJsonLike(dataset.getValue(geometryField, rowIdx));
        const geometry = parsed?.type === 'Feature' ? parsed.geometry : parsed;
        const rowPolygons = getPolygonsFromGeometry(geometry);
        if (!rowPolygons.length) return false;
        return rowPolygons.some((rowPoly: PolygonCoords) =>
          selectedPolygons.some((selPoly: PolygonCoords) => polygonIntersectsPolygon(rowPoly, selPoly))
        );
      });

      if (!matchedRows.length) return;
      const fields = (dataset.fields || []).map((f: any) => ({name: f.name, type: f.type}));
      const rows = matchedRows.map((rowIdx: number) => fields.map((f: any) => dataset.getValue(f.name, rowIdx)));
      const label = `${String(dataset.label || dataset.id)}_clipped_selection`;
      const existing = Object.values((props as any).datasets || {}).find(
        (d: any) => String(d?.label || '').toLowerCase() === label.toLowerCase()
      ) as any;
      const datasetToUse = {
        info: {
          id: existing?.id || `${dataset.id}_clipped_selection`,
          label
        },
        data: {fields, rows}
      };
      if (existing?.id) {
        dispatch(
          wrapTo(
            'map',
            replaceDataInMap({
              datasetToReplaceId: existing.id,
              datasetToUse,
              options: {
                keepExistingConfig: true,
                centerMap: false,
                autoCreateLayers: false
              }
            }) as any
          )
        );
      } else {
        dispatch(
          wrapTo(
            'map',
            addDataToMap({
              datasets: datasetToUse as any,
              options: {
                centerMap: false,
                autoCreateLayers: true
              }
            }) as any
          )
        );
      }
    } finally {
      setIsBusy(false);
    }
  };

  const onDeletePaintedHex = () => {
    if (isBusy || !selectedHex?.dataset?.id) return;
    setIsBusy(true);
    try {
      const existingPaintRows = readH3PaintRows(selectedHex.dataset, selectedHex.resolution);
      const nextRows = existingPaintRows.filter(([id]) => id !== selectedHex.h3Id);
      if (nextRows.length === existingPaintRows.length) return;

      const datasetToUse = {
        info: {
          id: selectedHex.dataset.id,
          label: selectedHex.dataset.label || `${H3_PAINT_DATASET_LABEL_PREFIX}${selectedHex.resolution}`
        },
        data: {
          fields: H3_PAINT_FIELDS as any,
          rows: nextRows
        }
      };

      dispatch(
        wrapTo(
          'map',
          replaceDataInMap({
            datasetToReplaceId: selectedHex.dataset.id,
            datasetToUse,
            options: {
              keepExistingConfig: true,
              centerMap: false,
              autoCreateLayers: false
            }
          }) as any
        )
      );
    } finally {
      setIsBusy(false);
    }
  };

  return (
    <BaseFeatureActionPanel {...props}>
      <ActionPanelItem
        className="editor-quick-actions"
        label="Quick Actions"
        isDisabled={isBusy || !props.selectedFeature?.geometry}
      >
        <ActionPanelItem
          label="Crea dataset selezione"
          isSelection={true}
          isActive={false}
          onClick={onCreateSelectedGeometryDataset}
          className="layer-panel-item"
        />
        <ActionPanelItem
          label="Clip layer visibile"
          isSelection={true}
          isActive={false}
          onClick={onClipVisibleGeojsonBySelection}
          className="layer-panel-item"
          isDisabled={!visibleGeojsonDataset}
        />
      </ActionPanelItem>
      <ActionPanelItem
        className="editor-tassellation-list"
        label="Tassellazione"
        isDisabled={isBusy || !props.selectedFeature?.geometry}
      >
        {RESOLUTIONS.map(resolution => (
          <ActionPanelItem
            key={resolution}
            label={`H3 ${resolution}`}
            isSelection={true}
            isActive={false}
            onClick={() => onTassellate(resolution)}
            className="layer-panel-item"
          />
        ))}
      </ActionPanelItem>
      {selectedHex ? (
        <ActionPanelItem
          className="editor-delete-painted-hex"
          label={`Delete painted hex (${selectedHex.h3Id.slice(0, 8)}...)`}
          isDisabled={isBusy}
          onClick={onDeletePaintedHex}
        />
      ) : null}
    </BaseFeatureActionPanel>
  );
}

export default function QMapFeatureActionPanelFactory() {
  return QMapFeatureActionPanel;
}

QMapFeatureActionPanelFactory.deps = [] as any[];
