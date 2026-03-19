// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import React, {useCallback, useMemo, useState} from 'react';
import JSONPretty from 'react-json-pretty';
import {AutoSizer} from 'react-virtualized';
import styled from 'styled-components';

import * as Layers from '@kepler.gl/layers';
import {getError, getApplicationConfig} from '@kepler.gl/utils';
import {MetaResponse} from '@kepler.gl/components/modals/tilesets-modals/common';
import LoadDataFooter from '@kepler.gl/components/modals/tilesets-modals/load-data-footer';
import TilesetIcon from '@kepler.gl/components/modals/tilesets-modals/tileset-icon';
import TilesetRasterForm from '@kepler.gl/components/modals/tilesets-modals/tileset-raster-form';
import TilesetWMSForm from '@kepler.gl/components/modals/tilesets-modals/tileset-wms-form';
import TilesetVectorForm from './tileset-vector-form';
import {TilesetProviderPreset} from './tileset-provider';

const WIDTH_ICON = '70px';

const LoadTilesetTabContainer = styled.div`
  color: ${props => props.theme.AZURE};
`;

const Container = styled.div`
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  grid-gap: 20px;
  background-color: ${props => props.theme.WHITE};
`;

const TilesetTypeContainer = styled.div`
  display: grid;
  grid-template-columns: repeat(4, ${WIDTH_ICON});
  column-gap: 10px;
  margin-bottom: 20px;
`;

const MetaContainer = styled.div`
  display: flex;
  max-height: 400px;
  background-color: ${({theme}) => theme.editorBackground};
`;

const MetaInnerContainer = styled.div<{width: number; height: number}>`
  position: relative;
  border: 1px solid ${props => props.theme.selectBorderColorLT};
  background-color: white;
  border-radius: 2px;
  display: inline-block;
  font: inherit;
  line-height: 1.5em;
  padding: 0.5em 3.5em 0.5em 1em;
  box-sizing: border-box;
  overflow-y: scroll;
  overflow-x: auto;
  white-space: pre-wrap;
  word-wrap: break-word;
  height: ${props => props.height}px;
  width: ${props => props.width}px;
  color: ${props => props.theme.textColorLT};
  font-size: 11px;
  font-family: ${props => props.theme.fontFamily};
  max-width: 600px;
`;

const StyledHeaderMessage = styled.div`
  color: ${props => props.theme.textColorLT};
  font-size: 14px;
`;

const ProviderContainer = styled.div`
  margin-bottom: 16px;
`;

const ProviderSelect = styled.select`
  width: 100%;
  border: 1px solid ${props => props.theme.selectBorderColorLT};
  background-color: ${props => props.theme.secondaryInputBgdLT};
  color: ${props => props.theme.textColorLT};
  height: 34px;
  border-radius: 2px;
  padding: 6px 8px;
`;

const TILE_TYPES = [
  {
    id: 'vectorTile',
    label: 'Vector Tile',
    Icon: (Layers as any).VectorTileIcon,
    Component: TilesetVectorForm
  },
  {
    id: 'rasterTile',
    label: 'Raster Tile',
    Icon: (Layers as any).RasterTileIcon,
    Component: TilesetRasterForm
  },
  {
    id: 'wms',
    label: 'WMS',
    Icon: (Layers as any).WMSLayerIcon,
    Component: TilesetWMSForm
  }
];

function isReady(response: MetaResponse) {
  return response.dataset && !response.loading && !response.error;
}

function getQMapTilesetProviders(): TilesetProviderPreset[] {
  const qCumberBase = (import.meta.env.VITE_QCUMBER_TILESET_BASE || '/api/q-cumber').replace(
    /\/+$/,
    ''
  );

  return [
    {
      id: 'q-cumber',
      label: 'Q-cumber',
      vectorTilesetUrl: `${qCumberBase}/geotoken_registry_dynamic/{z}/{x}/{y}`,
      vectorMetadataUrl: `${qCumberBase}/geotoken_registry_dynamic`
    }
  ];
}

function QMapLoadTilesetFactory() {
  const AutoSizerComponent = AutoSizer as unknown as React.ComponentType<{
    children: (size: {height: number; width: number}) => React.ReactNode;
  }>;

  const QMapLoadTileset: React.FC<{onTilesetAdded: (tilesetInfo: any, metadata?: any) => void; isAddingDatasets: boolean}> = ({
    onTilesetAdded,
    isAddingDatasets
  }) => {
    const [typeIndex, setTypeIndex] = useState<number>(0);
    const [providerId, setProviderId] = useState<string>('');
    const [response, setResponse] = useState<MetaResponse>({});

    const error = response.error;
    const loading = response.loading;
    const data = response.metadata;
    const jsonDataText = useMemo(() => JSON.stringify(data, null, 2), [data]);

    const createTileDataset = useCallback(() => {
      const {dataset, metadata} = response;
      if (dataset) {
        onTilesetAdded(dataset, metadata);
      }
    }, [onTilesetAdded, response]);

    const appConfig = getApplicationConfig() as any;
    const enableRasterTileLayer = appConfig.enableRasterTileLayer;
    const enableWMSLayer = appConfig.enableWMSLayer;

    const tileTypes = useMemo(() => {
      return TILE_TYPES.filter(tileType => {
        if (tileType.id === 'rasterTile') {
          return enableRasterTileLayer;
        }
        if (tileType.id === 'wms') {
          return enableWMSLayer;
        }
        return true;
      });
    }, [enableRasterTileLayer, enableWMSLayer]);

    const providers = useMemo(getQMapTilesetProviders, []);
    const selectedProvider = useMemo(
      () => providers.find(provider => provider.id === providerId) ?? null,
      [providerId, providers]
    );

    const CurrentForm = tileTypes[typeIndex].Component as React.ComponentType<any>;

    return (
      <LoadTilesetTabContainer>
        <Container>
          <div>
            <ProviderContainer>
              <StyledHeaderMessage>Provider</StyledHeaderMessage>
              <ProviderSelect
                value={providerId}
                onChange={event => setProviderId(event.target.value)}
                aria-label="Tileset provider"
              >
                <option value="">Direct URL</option>
                {providers.map(provider => (
                  <option key={provider.id} value={provider.id}>
                    {provider.label}
                  </option>
                ))}
              </ProviderSelect>
            </ProviderContainer>

            <StyledHeaderMessage>Tileset Type</StyledHeaderMessage>
            <TilesetTypeContainer className="tileset-type">
              {tileTypes.map((tileType, index) => (
                <TilesetIcon
                  key={tileType.label}
                  name={tileType.label}
                  Icon={<tileType.Icon height={WIDTH_ICON} />}
                  onClick={() => setTypeIndex(index)}
                  selected={typeIndex === index}
                />
              ))}
            </TilesetTypeContainer>

            <CurrentForm setResponse={setResponse} provider={selectedProvider} />
          </div>

          <MetaContainer>
            {data ? (
              <AutoSizerComponent>
                {({height, width}) => (
                  <MetaInnerContainer height={height} width={width}>
                    <JSONPretty id="json-pretty" json={jsonDataText} />
                  </MetaInnerContainer>
                )}
              </AutoSizerComponent>
            ) : null}
          </MetaContainer>
        </Container>

        <LoadDataFooter
          disabled={Boolean(error) || !isReady(response)}
          isLoading={loading || isAddingDatasets}
          onConfirm={createTileDataset}
          confirmText="tilesetSetup.addTilesetText"
          errorText={error && getError(error)}
        />
      </LoadTilesetTabContainer>
    );
  };

  return QMapLoadTileset;
}

export default QMapLoadTilesetFactory;
