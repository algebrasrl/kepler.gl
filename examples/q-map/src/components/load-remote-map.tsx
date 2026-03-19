import React, {useMemo, useState} from 'react';
import styled from 'styled-components';
import {FormattedMessage} from '@kepler.gl/localization';

const InputForm = styled.div`
  flex-grow: 1;
  padding: 24px;
  background-color: ${props => props.theme.panelBackgroundLT};
`;

const StyledDescription = styled.div`
  font-size: 13px;
  color: ${props => props.theme.labelColorLT};
  line-height: 18px;
  margin-bottom: 8px;
`;

const StyledInputLabel = styled.div`
  font-size: 11px;
  color: ${props => props.theme.textColorLT};
  margin-bottom: 6px;

  ul {
    margin: 6px 0 10px;
    padding-left: 16px;
  }
`;

const StyledFromGroup = styled.div`
  margin-top: 12px;
  display: flex;
  flex-direction: row;
  gap: 8px;
`;

const StyledInput = styled.input<{error?: boolean}>`
  width: 100%;
  padding: ${props => props.theme.inputPadding};
  color: ${props => (props.error ? props.theme.errorColor || 'red' : props.theme.titleColorLT)};
  height: ${props => props.theme.inputBoxHeight};
  border: 1px solid ${props => props.theme.selectBorderColorLT};
  background-color: ${props => props.theme.secondaryInputBgdLT};
  outline: 0;
  font-size: ${props => props.theme.inputFontSize};
`;

const UrlButton = styled.button`
  min-height: ${props => props.theme.inputBoxHeight};
  border: 0;
  border-radius: 2px;
  padding: 0 12px;
  color: #fff;
  background: ${props => props.theme.primaryBtnBgd || '#2c7be5'};
  cursor: pointer;

  &:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }
`;

const ErrorBlock = styled.div`
  color: ${props => props.theme.errorColor || 'red'};
  font-size: 12px;
  margin-top: 8px;
`;

function inferFilename(url: string, contentType: string | null): string {
  const lowerContentType = (contentType || '').toLowerCase();
  let outputParam: string | null = null;
  try {
    const parsed = new URL(url);
    outputParam = parsed.searchParams.get('output');
    const path = parsed.pathname.split('/').filter(Boolean).pop();
    if (path) {
      const hasExtension = /\.[a-z0-9]+$/i.test(path);
      if (hasExtension) {
        return path;
      }

      if (outputParam === 'csv' || lowerContentType.includes('csv')) return `${path}.csv`;
      if (outputParam === 'geojson' || lowerContentType.includes('geo+json')) return `${path}.geojson`;
      if (outputParam === 'json' || lowerContentType.includes('json')) return `${path}.json`;
      return `${path}.json`;
    }
  } catch {
    // noop
  }

  if (outputParam === 'csv' || lowerContentType.includes('csv')) return 'dataset.csv';
  if (outputParam === 'geojson' || lowerContentType.includes('geo+json')) return 'dataset.geojson';
  if (outputParam === 'json' || lowerContentType.includes('json')) return 'dataset.json';
  return 'dataset.data';
}

const QMapLoadRemoteMap: React.FC<any> = props => {
  const [dataUrl, setDataUrl] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const canLoad = useMemo(() => Boolean(dataUrl.trim()) && !loading, [dataUrl, loading]);

  const onLoadRemoteMap = async () => {
    const target = dataUrl.trim();
    if (!target) return;

    setLoading(true);
    setError(null);
    try {
      const resp = await fetch(target);
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status} ${resp.statusText}`);
      }

      const blob = await resp.blob();
      const filename = inferFilename(target, resp.headers.get('content-type'));
      const file = new File([blob], filename, {
        type: blob.type || resp.headers.get('content-type') || 'application/octet-stream'
      });

      props.onFileUpload?.([file]);
    } catch (err: any) {
      setError(err?.message || 'Failed to load dataset from URL');
    } finally {
      setLoading(false);
    }
  };

  return (
    <InputForm>
      <StyledDescription>
        <FormattedMessage id="loadRemoteMap.description" />
      </StyledDescription>
      <StyledInputLabel>
        <FormattedMessage id="loadRemoteMap.message" />
      </StyledInputLabel>
      <StyledInputLabel>
        <FormattedMessage id="loadRemoteMap.examples" />
        <ul>
          <li>https://your.map.url/map.json</li>
          <li>http://localhost:3003/h3/registry?output=csv&limit=5000</li>
        </ul>
      </StyledInputLabel>
      <StyledFromGroup>
        <StyledInput
          onChange={e => setDataUrl(e.target.value)}
          type="url"
          placeholder="URL"
          value={dataUrl}
          error={Boolean(error)}
        />
        <UrlButton type="button" disabled={!canLoad} onClick={onLoadRemoteMap}>
          {loading ? 'Loading...' : <FormattedMessage id="loadRemoteMap.fetch" />}
        </UrlButton>
      </StyledFromGroup>
      {error ? <ErrorBlock>{error}</ErrorBlock> : null}
    </InputForm>
  );
};

export default QMapLoadRemoteMap;
