// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import React from 'react';
import styled from 'styled-components';
import {useDispatch, useSelector} from 'react-redux';
import {ADD_DATA_ID} from '@kepler.gl/constants';
import {ALL_FIELD_TYPES} from '@kepler.gl/constants';
import {addDataToMap, replaceDataInMap, wrapTo} from '@kepler.gl/actions';
import {Icons} from '@kepler.gl/components';
import {isValidCell, cellToBoundary} from 'h3-js-v4';
import {resolveQMapAuthorizationHeader} from '../utils/auth-token';
import {selectQMapAiAssistantConfig, selectQMapDatasets} from '../state/qmap-selectors';
import {resolveQMapAssistantBaseUrl} from '../utils/assistant-config';
import {
  booleanContains as turfBooleanContains,
  booleanIntersects as turfBooleanIntersects,
  booleanWithin as turfBooleanWithin
} from '@turf/turf';

const ProfilePanelWrap = styled.div`
  position: relative;
  padding: 8px 4px;
  max-height: calc(100vh - 116px);
  overflow-y: auto;
  overflow-x: hidden;
  overscroll-behavior: contain;
`;

const AddDataButtonWrap = styled.div`
  position: absolute;
  top: 0;
  right: 0;
  z-index: 2;
`;

const AddDataButton = styled.button`
  border: 0;
  border-radius: 2px;
  padding: 6px 9px;
  font-size: 11px;
  font-weight: 600;
  color: #ffffff;
  background: #f5c400;
  cursor: pointer;

  &:hover {
    background: #e0b200;
  }
`;

const ProfileTitle = styled.h3`
  margin: 0 0 12px;
  font-size: 14px;
  color: ${props => props.theme.textColorHl};
`;

const ProfileText = styled.p`
  margin: 0;
  font-size: 12px;
  color: ${props => props.theme.subtextColor};
  line-height: 1.5;
`;

const ProfileList = styled.div`
  display: grid;
  gap: 8px;
`;

const ProfileRow = styled.div`
  display: grid;
  gap: 2px;
`;

const ProfileLabel = styled.div`
  font-size: 11px;
  color: ${props => props.theme.subtextColor};
`;

const ProfileValue = styled.div`
  font-size: 12px;
  color: ${props => props.theme.textColorHl};
`;

const Divider = styled.div`
  margin: 14px 0 12px;
  border-top: 1px solid ${props => props.theme.panelBorderColorLT || 'rgba(255, 255, 255, 0.12)'};
`;

const RecentBox = styled.div`
  border: 1px solid ${props => props.theme.panelBorderColorLT || 'rgba(255, 255, 255, 0.16)'};
  border-radius: 4px;
  padding: 10px;
  background: ${props => props.theme.secondaryInputBgd || 'rgba(255, 255, 255, 0.02)'};
`;

const RecentTitle = styled.div`
  font-size: 12px;
  color: ${props => props.theme.textColorHl};
  margin-bottom: 8px;
  font-weight: 600;
`;

const RecentList = styled.ul`
  margin: 0;
  padding-left: 16px;
  display: grid;
  gap: 6px;
`;

const RecentItem = styled.li`
  font-size: 12px;
  color: ${props => props.theme.subtextColor};
  line-height: 1.35;
`;

const OpsWrap = styled.div`
  padding: 8px 4px;
  display: grid;
  gap: 12px;
  max-height: calc(100vh - 116px);
  overflow-y: auto;
  overflow-x: hidden;
  overscroll-behavior: contain;
`;

const OpsCard = styled.div`
  border: 1px solid ${props => props.theme.panelBorderColorLT || 'rgba(255, 255, 255, 0.16)'};
  border-radius: 4px;
  padding: 10px;
  background: ${props => props.theme.secondaryInputBgd || 'rgba(255, 255, 255, 0.02)'};
  display: grid;
  gap: 8px;
  min-width: 0;
  box-sizing: border-box;
`;

const OpsTitle = styled.div`
  font-size: 12px;
  font-weight: 600;
  color: ${props => props.theme.textColorHl};
`;

const OpsRow = styled.div`
  display: grid;
  gap: 6px;
  min-width: 0;
`;

const OpsLabel = styled.label`
  font-size: 11px;
  color: ${props => props.theme.subtextColor};
`;

const OpsSelect = styled.select`
  width: 100%;
  min-width: 0;
  height: 30px;
  border: 1px solid ${props => props.theme.inputBorderColor || 'rgba(255,255,255,0.2)'};
  border-radius: 2px;
  background: ${props => props.theme.inputBgd || 'rgba(0,0,0,0.2)'};
  color: ${props => props.theme.textColorHl || '#fff'};
  padding: 0 8px;
  font-size: 12px;
  box-sizing: border-box;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`;

const OpsButton = styled.button`
  width: 100%;
  min-width: 0;
  border: 0;
  border-radius: 2px;
  padding: 8px 10px;
  font-size: 12px;
  font-weight: 600;
  color: #ffffff;
  background: #f5c400;
  cursor: pointer;

  &:hover {
    background: #e0b200;
  }
`;

const OpsStatus = styled.div`
  font-size: 11px;
  color: ${props => props.theme.subtextColor};
`;

const UserFaceIcon = ({height = '18px'}: {height?: string}) => (
  <svg viewBox="0 0 24 24" width={height} height={height} fill="none" aria-hidden="true">
    <circle cx="12" cy="8" r="4" stroke="currentColor" strokeWidth="1.8" />
    <path d="M4.5 20a7.5 7.5 0 0 1 15 0" stroke="currentColor" strokeWidth="1.8" />
  </svg>
);

function parseGeoJsonLike(value: unknown): any | null {
  if (!value) return null;
  if (typeof value === 'object') return value;
  if (typeof value === 'string') {
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }
  return null;
}

function toFeature(input: any): any | null {
  if (!input) return null;
  const feature = input?.type === 'Feature' ? input : {type: 'Feature', properties: {}, geometry: input};
  if (!feature?.geometry?.type) return null;
  return feature;
}

function toFeatureFromH3Cell(value: unknown): any | null {
  const h3Cell = String(value || '').trim();
  if (!h3Cell || !isValidCell(h3Cell)) return null;
  try {
    const ring = cellToBoundary(h3Cell, true) as Array<[number, number]>;
    if (!Array.isArray(ring) || ring.length < 3) return null;
    const first = ring[0];
    const last = ring[ring.length - 1];
    const closedRing =
      first && last && first[0] === last[0] && first[1] === last[1] ? ring : [...ring, first];
    return {
      type: 'Feature',
      properties: {},
      geometry: {
        type: 'Polygon',
        coordinates: [closedRing.map(([lng, lat]) => [lng, lat])]
      }
    };
  } catch {
    return null;
  }
}

function inferFieldTypeFromValue(value: unknown): string {
  if (value === null || value === undefined) return ALL_FIELD_TYPES.string;
  if (typeof value === 'boolean') return ALL_FIELD_TYPES.boolean;
  if (typeof value === 'number') {
    return Number.isInteger(value) ? ALL_FIELD_TYPES.integer : ALL_FIELD_TYPES.real;
  }
  if (typeof value === 'object') {
    const asAny = value as any;
    if (asAny?.type === 'Feature' || asAny?.type === 'Polygon' || asAny?.type === 'MultiPolygon') {
      return ALL_FIELD_TYPES.geojson;
    }
  }
  return ALL_FIELD_TYPES.string;
}

function upsertDerivedDataset(
  dispatchFn: any,
  datasets: any,
  datasetName: string,
  rowsAsObjects: Array<Record<string, unknown>>,
  idPrefix: string
) {
  if (!rowsAsObjects.length) return;
  const existing = Object.values(datasets || {}).find(
    (d: any) => String(d?.label || '').toLowerCase() === String(datasetName).toLowerCase()
  ) as any;
  const fieldNames = Array.from(
    rowsAsObjects.reduce((acc, row) => {
      Object.keys(row || {}).forEach(k => acc.add(k));
      return acc;
    }, new Set<string>())
  );
  const fields = fieldNames.map(name => {
    const sample = rowsAsObjects.find(row => row && row[name] !== undefined && row[name] !== null)?.[name];
    return {name, type: inferFieldTypeFromValue(sample)};
  });
  const rows = rowsAsObjects.map(row => fields.map(f => row?.[f.name] ?? null));
  const datasetToUse = {
    info: {
      id: existing?.id || `${idPrefix}_${Date.now()}`,
      label: datasetName
    },
    data: {
      fields,
      rows
    }
  };
  if (existing?.id) {
    dispatchFn(
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
    dispatchFn(
      wrapTo(
        'map',
        addDataToMap({
          datasets: datasetToUse as any,
          options: {autoCreateLayers: true, centerMap: false}
        }) as any
      )
    );
  }
}

function QMapCustomPanelsFactory() {
  const ProfileInfoPanel = () => {
    const aiAssistantConfig = useSelector(selectQMapAiAssistantConfig);
    const aiBaseUrl = resolveQMapAssistantBaseUrl(aiAssistantConfig);
    const datasets = useSelector(selectQMapDatasets);
    const [loading, setLoading] = React.useState(false);
    const [error, setError] = React.useState<string | null>(null);
    const [profile, setProfile] = React.useState<{
      name: string;
      email: string;
      registeredAt: string;
      country: string;
    } | null>(null);

    React.useEffect(() => {
      let alive = true;
      const run = async () => {
        setLoading(true);
        setError(null);
        try {
          const base = aiBaseUrl.replace(/\/+$/, '');
          const authorizationHeader = resolveQMapAuthorizationHeader();
          const response = await fetch(`${base}/me`, {
            method: 'GET',
            headers: {
              accept: 'application/json',
              ...(authorizationHeader ? {Authorization: authorizationHeader} : {})
            }
          });
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }
          const payload = await response.json();
          const data = payload?.profile || payload || {};
          if (!alive) return;
          setProfile({
            name: String(data?.name || ''),
            email: String(data?.email || ''),
            registeredAt: String(data?.registeredAt || data?.registrationDate || ''),
            country: String(data?.country || '')
          });
        } catch (e: any) {
          if (!alive) return;
          setError(e?.message || 'Impossibile caricare il profilo');
        } finally {
          if (alive) setLoading(false);
        }
      };
      run();
      return () => {
        alive = false;
      };
    }, [aiBaseUrl]);

    const recentItems = React.useMemo(() => {
      const values = Object.values(datasets || {});
      return values
        .map((dataset: any) => {
          const label =
            String(
              dataset?.label ||
                dataset?.data?.label ||
                dataset?.data?.id ||
                dataset?.id ||
                ''
            ).trim();
          return label;
        })
        .filter(Boolean)
        .slice(-3)
        .reverse();
    }, [datasets]);

    if (loading) {
      return <ProfileText>Caricamento profilo...</ProfileText>;
    }
    if (error) {
      return <ProfileText>Errore profilo: {error}</ProfileText>;
    }
    if (!profile) {
      return <ProfileText>Profilo non disponibile.</ProfileText>;
    }

    return (
      <>
        <ProfileList>
          <ProfileRow>
            <ProfileLabel>Nome</ProfileLabel>
            <ProfileValue>{profile.name || '-'}</ProfileValue>
          </ProfileRow>
          <ProfileRow>
            <ProfileLabel>Email</ProfileLabel>
            <ProfileValue>{profile.email || '-'}</ProfileValue>
          </ProfileRow>
          <ProfileRow>
            <ProfileLabel>Data di registrazione</ProfileLabel>
            <ProfileValue>{profile.registeredAt || '-'}</ProfileValue>
          </ProfileRow>
          <ProfileRow>
            <ProfileLabel>Country</ProfileLabel>
            <ProfileValue>{profile.country || '-'}</ProfileValue>
          </ProfileRow>
        </ProfileList>
        <Divider />
        <RecentBox>
          <RecentTitle>Recent items</RecentTitle>
          <RecentList>
            {recentItems.length ? (
              recentItems.map(item => <RecentItem key={item}>{item}</RecentItem>)
            ) : (
              <RecentItem>Nessun dataset caricato.</RecentItem>
            )}
          </RecentList>
        </RecentBox>
      </>
    );
  };

  const ProfilePanel = ({
    activeSidePanel,
    onQuickAddData
  }: {
    activeSidePanel: string | null;
    onQuickAddData?: () => void;
  }) => {
    if (activeSidePanel === 'operations') {
      return <OperationsPanel activeSidePanel={activeSidePanel} />;
    }

    if (activeSidePanel !== 'profile') {
      return null;
    }

    return (
      <ProfilePanelWrap>
        <ProfileTitle>Profilo Utente</ProfileTitle>
        <AddDataButtonWrap>
          <AddDataButton type="button" onClick={onQuickAddData}>
            + Aggiungi dati
          </AddDataButton>
        </AddDataButtonWrap>
        <ProfileInfoPanel />
      </ProfilePanelWrap>
    );
  };

  const OperationsPanel = ({activeSidePanel}: {activeSidePanel: string | null}) => {
    const dispatch = useDispatch<any>();
    const datasets = useSelector((state: any) => selectQMapDatasets(state));
    const [status, setStatus] = React.useState<string>('');
    const datasetList = React.useMemo(
      () =>
        Object.values(datasets || {})
          .map((d: any) => ({
            id: String(d?.id || ''),
            label: String(d?.label || d?.id || ''),
            geomField: (d?.fields || []).find((f: any) => String(f?.type || '') === 'geojson')?.name || null,
            h3Field:
              (d?.fields || []).find((f: any) => String(f?.type || '') === 'h3')?.name ||
              (d?.fields || []).find((f: any) => /^(h3_id|h3__id)$/i.test(String(f?.name || '')))?.name ||
              null,
            fields: (d?.fields || []).map((f: any) => String(f?.name || ''))
          }))
          .filter((d: any) => d.id && d.label),
      [datasets]
    );

    const [clipSource, setClipSource] = React.useState('');
    const [clipBoundary, setClipBoundary] = React.useState('');
    const [clipMode, setClipMode] = React.useState<'intersects' | 'within'>('intersects');
    const [zAdmin, setZAdmin] = React.useState('');
    const [zValue, setZValue] = React.useState('');
    const [zField, setZField] = React.useState('');
    const [zAgg, setZAgg] = React.useState<'count' | 'sum' | 'avg'>('count');
    const [jLeft, setJLeft] = React.useState('');
    const [jRight, setJRight] = React.useState('');
    const [jPredicate, setJPredicate] = React.useState<'intersects' | 'within' | 'contains'>('intersects');

    React.useEffect(() => {
      if (!datasetList.length) return;
      if (!clipSource) setClipSource(datasetList[0]?.label || '');
      if (!clipBoundary) setClipBoundary(datasetList[1]?.label || datasetList[0]?.label || '');
      if (!zAdmin) setZAdmin(datasetList[0]?.label || '');
      if (!zValue) setZValue(datasetList[1]?.label || datasetList[0]?.label || '');
      if (!jLeft) setJLeft(datasetList[0]?.label || '');
      if (!jRight) setJRight(datasetList[1]?.label || datasetList[0]?.label || '');
    }, [datasetList, clipSource, clipBoundary, zAdmin, zValue, jLeft, jRight]);

    React.useEffect(() => {
      if (!zValue) return;
      const ds = datasetList.find((d: any) => d.label === zValue);
      if (!ds) return;
      const nonGeometryFields = (ds.fields || []).filter(
        (f: string) => f !== ds.geomField && f !== ds.h3Field && f !== '_geojson'
      );
      const candidate = ds.fields.find((f: string) => /population|popolaz|value|valore|sum|count/i.test(f));
      setZField(candidate || nonGeometryFields[0] || ds.fields[0] || '');
    }, [zValue, datasetList]);

    if (activeSidePanel !== 'operations') return null;

    const getDs = (label: string) => datasetList.find((d: any) => d.label === label) || null;
    const datasetHasSpatialField = (d: any) => Boolean(d?.geomField || d?.h3Field);
    const featureForRow = (dataset: any, datasetMeta: any, rowIdx: number) => {
      if (datasetMeta?.geomField) {
        const geomFeature = toFeature(parseGeoJsonLike(dataset.getValue(datasetMeta.geomField, rowIdx)));
        if (geomFeature) return geomFeature;
      }
      if (datasetMeta?.h3Field) {
        return toFeatureFromH3Cell(dataset.getValue(datasetMeta.h3Field, rowIdx));
      }
      return null;
    };

    const runClip = () => {
      const source = getDs(clipSource);
      const boundary = getDs(clipBoundary);
      if (!source || !boundary || !datasetHasSpatialField(source) || !datasetHasSpatialField(boundary)) {
        setStatus('Clip: dataset/geometria non validi');
        return;
      }
      const sourceDataset = datasets[source.id];
      const boundaryDataset = datasets[boundary.id];
      const sourceIdx = Array.isArray(sourceDataset?.allIndexes)
        ? sourceDataset.allIndexes
        : Array.from({length: Number(sourceDataset?.length || 0)}, (_, i) => i);
      const boundaryFeatures = (Array.isArray(boundaryDataset?.allIndexes)
        ? boundaryDataset.allIndexes
        : Array.from({length: Number(boundaryDataset?.length || 0)}, (_, i) => i)
      )
        .map((rowIdx: number) => featureForRow(boundaryDataset, boundary, rowIdx))
        .filter(Boolean);
      const outRows = sourceIdx
        .filter((rowIdx: number) => {
          const sf = featureForRow(sourceDataset, source, rowIdx);
          if (!sf) return false;
          return boundaryFeatures.some((bf: any) => {
            try {
              return clipMode === 'within' ? turfBooleanWithin(sf, bf) : turfBooleanIntersects(sf, bf);
            } catch {
              return false;
            }
          });
        })
        .map((rowIdx: number) => {
          const row: Record<string, unknown> = {};
          (sourceDataset.fields || []).forEach((f: any) => (row[f.name] = sourceDataset.getValue(f.name, rowIdx)));
          return row;
        });
      upsertDerivedDataset(dispatch, datasets, `${source.label}_clipped_${boundary.label}`, outRows, 'qmap_ops_clip');
      setStatus(`Clip: ${outRows.length} righe`);
    };

    const runZonal = () => {
      const admin = getDs(zAdmin);
      const value = getDs(zValue);
      if (!admin || !value || !datasetHasSpatialField(admin) || !datasetHasSpatialField(value)) {
        setStatus('Zonal: dataset/geometria non validi');
        return;
      }
      const adminDataset = datasets[admin.id];
      const valueDataset = datasets[value.id];
      const adminIdx = Array.isArray(adminDataset?.allIndexes)
        ? adminDataset.allIndexes
        : Array.from({length: Number(adminDataset?.length || 0)}, (_, i) => i);
      const valueFeatures = (Array.isArray(valueDataset?.allIndexes)
        ? valueDataset.allIndexes
        : Array.from({length: Number(valueDataset?.length || 0)}, (_, i) => i)
      )
        .map((rowIdx: number) => ({
          feature: featureForRow(valueDataset, value, rowIdx),
          metric: Number(valueDataset.getValue(zField, rowIdx))
        }))
        .filter((x: any) => Boolean(x.feature));
      const outRows = adminIdx.map((rowIdx: number) => {
        const af = featureForRow(adminDataset, admin, rowIdx);
        const matched = valueFeatures.filter((vf: any) => {
          try {
            return af && turfBooleanIntersects(af, vf.feature);
          } catch {
            return false;
          }
        });
        const nums = matched.map((m: any) => m.metric).filter((v: number) => Number.isFinite(v));
        const metric =
          zAgg === 'sum'
            ? nums.reduce((a: number, b: number) => a + b, 0)
            : zAgg === 'avg'
            ? nums.length
              ? nums.reduce((a: number, b: number) => a + b, 0) / nums.length
              : null
            : matched.length;
        const row: Record<string, unknown> = {};
        (adminDataset.fields || []).forEach((f: any) => (row[f.name] = adminDataset.getValue(f.name, rowIdx)));
        row.zonal_metric = metric;
        return row;
      });
      upsertDerivedDataset(dispatch, datasets, `${admin.label}_zonal_${value.label}`, outRows, 'qmap_ops_zonal');
      setStatus(`Zonal: ${outRows.length} righe`);
    };

    const runJoin = () => {
      const left = getDs(jLeft);
      const right = getDs(jRight);
      if (!left || !right || !datasetHasSpatialField(left) || !datasetHasSpatialField(right)) {
        setStatus('Join: dataset/geometria non validi');
        return;
      }
      const leftDataset = datasets[left.id];
      const rightDataset = datasets[right.id];
      const rightFeatures = (Array.isArray(rightDataset?.allIndexes)
        ? rightDataset.allIndexes
        : Array.from({length: Number(rightDataset?.length || 0)}, (_, i) => i)
      )
        .map((rowIdx: number) => featureForRow(rightDataset, right, rowIdx))
        .filter(Boolean);
      const outRows = (Array.isArray(leftDataset?.allIndexes)
        ? leftDataset.allIndexes
        : Array.from({length: Number(leftDataset?.length || 0)}, (_, i) => i)
      ).map((rowIdx: number) => {
        const lf = featureForRow(leftDataset, left, rowIdx);
        const count = rightFeatures.filter((rf: any) => {
          try {
            if (!lf) return false;
            if (jPredicate === 'within') return turfBooleanWithin(lf, rf);
            if (jPredicate === 'contains') return turfBooleanContains(lf, rf);
            return turfBooleanIntersects(lf, rf);
          } catch {
            return false;
          }
        }).length;
        const row: Record<string, unknown> = {};
        (leftDataset.fields || []).forEach((f: any) => (row[f.name] = leftDataset.getValue(f.name, rowIdx)));
        row.join_count = count;
        return row;
      });
      upsertDerivedDataset(dispatch, datasets, `${left.label}_join_${right.label}`, outRows, 'qmap_ops_join');
      setStatus(`Join: ${outRows.length} righe`);
    };

    return (
      <OpsWrap>
        <ProfileTitle>Operazioni</ProfileTitle>
        <OpsCard>
          <OpsTitle>Clip</OpsTitle>
          <OpsRow>
            <OpsLabel>Sorgente</OpsLabel>
            <OpsSelect value={clipSource} onChange={e => setClipSource(e.target.value)}>
              {datasetList.map((d: any) => (
                <option key={`clip-source-${d.id}`} value={d.label}>
                  {d.label}
                </option>
              ))}
            </OpsSelect>
            <OpsLabel>Boundary</OpsLabel>
            <OpsSelect value={clipBoundary} onChange={e => setClipBoundary(e.target.value)}>
              {datasetList.map((d: any) => (
                <option key={`clip-boundary-${d.id}`} value={d.label}>
                  {d.label}
                </option>
              ))}
            </OpsSelect>
            <OpsLabel>Modalità</OpsLabel>
            <OpsSelect value={clipMode} onChange={e => setClipMode(e.target.value as any)}>
              <option value="intersects">intersects</option>
              <option value="within">within</option>
            </OpsSelect>
          </OpsRow>
          <OpsButton type="button" onClick={runClip}>
            Esegui Clip
          </OpsButton>
        </OpsCard>

        <OpsCard>
          <OpsTitle>Zonal stats</OpsTitle>
          <OpsRow>
            <OpsLabel>Admin</OpsLabel>
            <OpsSelect value={zAdmin} onChange={e => setZAdmin(e.target.value)}>
              {datasetList.map((d: any) => (
                <option key={`z-admin-${d.id}`} value={d.label}>
                  {d.label}
                </option>
              ))}
            </OpsSelect>
            <OpsLabel>Valori</OpsLabel>
            <OpsSelect value={zValue} onChange={e => setZValue(e.target.value)}>
              {datasetList.map((d: any) => (
                <option key={`z-value-${d.id}`} value={d.label}>
                  {d.label}
                </option>
              ))}
            </OpsSelect>
            <OpsLabel>Campo</OpsLabel>
            <OpsSelect value={zField} onChange={e => setZField(e.target.value)}>
              {(getDs(zValue)?.fields || []).map((f: string) => (
                <option key={`z-field-${f}`} value={f}>
                  {f}
                </option>
              ))}
            </OpsSelect>
            <OpsLabel>Agg</OpsLabel>
            <OpsSelect value={zAgg} onChange={e => setZAgg(e.target.value as any)}>
              <option value="count">count</option>
              <option value="sum">sum</option>
              <option value="avg">avg</option>
            </OpsSelect>
          </OpsRow>
          <OpsButton type="button" onClick={runZonal}>
            Esegui Zonal
          </OpsButton>
        </OpsCard>

        <OpsCard>
          <OpsTitle>Spatial join</OpsTitle>
          <OpsRow>
            <OpsLabel>Left</OpsLabel>
            <OpsSelect value={jLeft} onChange={e => setJLeft(e.target.value)}>
              {datasetList.map((d: any) => (
                <option key={`j-left-${d.id}`} value={d.label}>
                  {d.label}
                </option>
              ))}
            </OpsSelect>
            <OpsLabel>Right</OpsLabel>
            <OpsSelect value={jRight} onChange={e => setJRight(e.target.value)}>
              {datasetList.map((d: any) => (
                <option key={`j-right-${d.id}`} value={d.label}>
                  {d.label}
                </option>
              ))}
            </OpsSelect>
            <OpsLabel>Predicate</OpsLabel>
            <OpsSelect value={jPredicate} onChange={e => setJPredicate(e.target.value as any)}>
              <option value="intersects">intersects</option>
              <option value="within">within</option>
              <option value="contains">contains</option>
            </OpsSelect>
          </OpsRow>
          <OpsButton type="button" onClick={runJoin}>
            Esegui Join
          </OpsButton>
        </OpsCard>
        <OpsStatus>{status}</OpsStatus>
      </OpsWrap>
    );
  };

  (ProfilePanel as any).displayName = 'QMapProfilePanel';

  (ProfilePanel as any).panels = [
    {
      id: 'profile',
      label: 'sidebar.panels.profile',
      iconComponent: (props: any) => <UserFaceIcon height={props?.height || '18px'} />
    },
    {
      id: 'operations',
      label: 'sidebar.panels.operations',
      iconComponent: (props: any) => <Icons.MagicWand height={props?.height || '18px'} />
    }
  ];

  (ProfilePanel as any).getProps = (props: any) => ({
    onQuickAddData: () => {
      const toggleModal = props?.uiStateActions?.toggleModal;
      const toggleSidePanel = props?.uiStateActions?.toggleSidePanel;
      if (typeof toggleModal === 'function') {
        toggleModal(ADD_DATA_ID);
      }
      if (typeof toggleSidePanel === 'function') {
        toggleSidePanel('layer');
      }
    }
  });

  return ProfilePanel;
}

export default QMapCustomPanelsFactory;
