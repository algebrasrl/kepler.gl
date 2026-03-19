// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import React, {useCallback, useEffect, useMemo, useState} from 'react';
import styled from 'styled-components';

import {isPMTilesUrl} from '@kepler.gl/common-utils';
import {
  DatasetType,
  PMTilesType,
  RemoteTileFormat,
  VectorTileDatasetMetadata,
  REMOTE_TILE
} from '@kepler.gl/constants';
import {TileJSON} from '@loaders.gl/mvt';
import {PMTilesMetadata} from '@loaders.gl/pmtiles';
import {getMetaUrl, parseVectorMetadata, VectorTileMetadata} from '@kepler.gl/table';
import {Merge} from '@kepler.gl/types';
import useFetchVectorTileMetadata from '@kepler.gl/components/hooks/use-fetch-vector-tile-metadata';
import {MetaResponse} from '@kepler.gl/components/modals/tilesets-modals/common';
import {TilesetProviderPreset} from './tileset-provider';

const TilesetInputContainer = styled.div`
  display: grid;
  grid-template-rows: repeat(3, 1fr);
  row-gap: 18px;
  font-size: 12px;
`;

const TilesetInputDescription = styled.div`
  text-align: center;
  color: ${props => props.theme.AZURE200};
  font-size: 11px;
`;

const InputLight = styled.input`
  width: 100%;
  min-height: 34px;
  border: 1px solid ${props => props.theme.selectBorderColorLT};
  background-color: ${props => props.theme.secondaryInputBgdLT};
  color: ${props => props.theme.textColorLT};
  border-radius: 2px;
  padding: 6px 8px;
`;

export type VectorTilesetFormData = {
  name: string;
  dataUrl: string;
  metadataUrl?: string;
};

export type VectorTileDatasetCreationAttributes = Merge<
  {
    name: string;
    type: string;
    metadata: Record<string, any>;
  },
  {
    metadata: VectorTileDatasetMetadata;
  }
>;

export function getDatasetAttributesFromVectorTile({
  name,
  dataUrl,
  metadataUrl
}: VectorTilesetFormData): VectorTileDatasetCreationAttributes {
  return {
    name,
    type: DatasetType.VECTOR_TILE,
    metadata: {
      type: REMOTE_TILE,
      remoteTileFormat: isPMTilesUrl(dataUrl) ? RemoteTileFormat.PMTILES : RemoteTileFormat.MVT,
      tilesetDataUrl: dataUrl,
      tilesetMetadataUrl: metadataUrl
    }
  };
}

type TilesetVectorFormProps = {
  setResponse: (response: MetaResponse) => void;
  provider?: TilesetProviderPreset | null;
};

const TilesetVectorForm: React.FC<TilesetVectorFormProps> = ({setResponse, provider}) => {
  const [tileName, setTileName] = useState<string>('');
  const [tileUrl, setTileUrl] = useState<string>('');
  const [metadataUrl, setMetadataUrl] = useState<string | null>('');
  const [initialFetchError, setInitialFetchError] = useState<Error | null>(null);

  const onTileNameChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    event.preventDefault();
    setTileName(event.target.value);
  }, []);

  const onTileMetaUrlChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    event.preventDefault();
    setMetadataUrl(event.target.value);
  }, []);

  const onTileUrlChange = useCallback(
    async (event: React.ChangeEvent<HTMLInputElement>) => {
      event.preventDefault();
      const newTileUrl = event.target.value;
      setTileUrl(newTileUrl);

      const usePMTiles = isPMTilesUrl(newTileUrl);
      const potentialMetadataUrl = usePMTiles ? newTileUrl : getMetaUrl(newTileUrl);
      if (!metadataUrl && potentialMetadataUrl) {
        const metadataCandidates = usePMTiles
          ? [potentialMetadataUrl]
          : [
              potentialMetadataUrl,
              potentialMetadataUrl.replace(/\/metadata\.json$/i, '')
            ].filter(Boolean);

        let matchedUrl: string | null = null;
        let lastError: Error | null = null;
        for (const candidate of metadataCandidates) {
          try {
            const resp = await fetch(candidate);
            if (resp.ok) {
              matchedUrl = candidate;
              break;
            }
            lastError = new Error(`Metadata loading failed: ${resp.status} ${resp.statusText}`);
          } catch (err) {
            lastError = err as Error;
          }
        }

        if (matchedUrl) {
          setInitialFetchError(null);
          setMetadataUrl(matchedUrl);
        } else {
          setInitialFetchError(lastError || new Error('Metadata loading failed'));
        }
      } else {
        setInitialFetchError(null);
      }

      if (!tileName) {
        setTileName(newTileUrl.split('/').pop() || newTileUrl);
      }
    },
    [metadataUrl, tileName]
  );

  const process = useMemo(
    () => (value: PMTilesMetadata | TileJSON) => parseVectorMetadata(value, {tileUrl: metadataUrl}),
    [metadataUrl]
  );

  const {
    data: metadata,
    loading,
    error: metaError
  } = useFetchVectorTileMetadata({
    metadataUrl,
    tilesetUrl: tileUrl,
    remoteTileFormat: isPMTilesUrl(metadataUrl) ? RemoteTileFormat.PMTILES : RemoteTileFormat.MVT,
    process
  });

  if (metadata && initialFetchError) {
    setInitialFetchError(null);
  }

  useEffect(() => {
    if (tileName && tileUrl) {
      if (metadata?.pmtilesType === PMTilesType.RASTER) {
        return setResponse({
          metadata,
          dataset: null,
          loading,
          error: new Error('For .pmtiles in raster format, please use the Raster Tile form.')
        });
      }

      const dataset = getDatasetAttributesFromVectorTile({
        name: tileName,
        dataUrl: tileUrl,
        metadataUrl: metadataUrl ?? undefined
      });

      setResponse({
        metadata,
        dataset,
        loading,
        error: metaError || initialFetchError
      });
    } else {
      setResponse({
        metadata,
        dataset: null,
        loading,
        error: metaError || initialFetchError
      });
    }
  }, [setResponse, metadata, loading, metaError, initialFetchError, tileUrl, tileName, metadataUrl]);

  useEffect(() => {
    if (metadata) {
      const name = (metadata as VectorTileMetadata).name;
      if (name) {
        setTileName(name);
      }
    }
  }, [metadata]);

  useEffect(() => {
    if (!provider) {
      return;
    }

    if (provider.vectorTilesetUrl) {
      setTileUrl(provider.vectorTilesetUrl);
    }
    setMetadataUrl(
      provider.vectorMetadataUrl ||
        (provider.vectorTilesetUrl ? getMetaUrl(provider.vectorTilesetUrl) : '')
    );
    setInitialFetchError(null);
    if (!tileName) {
      setTileName(provider.label);
    }
  }, [provider, tileName]);

  return (
    <TilesetInputContainer>
      <div>
        <label htmlFor="tileset-name">Name</label>
        <InputLight
          id="tileset-name"
          placeholder="Name your tileset"
          value={tileName}
          onChange={onTileNameChange}
        />
      </div>
      <div>
        <label htmlFor="tile-url">Tileset URL</label>
        <InputLight id="tile-url" placeholder="Tileset URL" value={tileUrl} onChange={onTileUrlChange} />
        <TilesetInputDescription>
          Requires &#123;x&#125;, &#123;y&#125;, &#123;z&#125; placeholders in URL or .pmtile extension.
        </TilesetInputDescription>
      </div>
      <div>
        <label htmlFor="tile-metadata">Tileset metadata URL</label>
        <InputLight
          id="tile-metadata"
          placeholder="Tileset metadata"
          value={metadataUrl ?? undefined}
          onChange={onTileMetaUrlChange}
        />
        <TilesetInputDescription>Optional, but recommended. Supports json, txt</TilesetInputDescription>
      </div>
    </TilesetInputContainer>
  );
};

export default TilesetVectorForm;
