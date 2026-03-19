import React, {useCallback, useMemo, useState} from 'react';
import styled from 'styled-components';
import {FileUpload as CoreFileUpload} from '@kepler.gl/components/common/file-uploader/file-upload';

type DuckDbModule = typeof import('@duckdb/duckdb-wasm');

const UrlImportCard = styled.div`
  border: 1px solid ${props => props.theme.selectBorderColorLT};
  border-radius: 4px;
  padding: 12px;
  margin-bottom: 12px;
  background: ${props => props.theme.panelBackgroundLT || '#fff'};
`;

const UrlRow = styled.div`
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 8px;
  align-items: center;
`;

const UrlInput = styled.input`
  width: 100%;
  min-height: 34px;
  border: 1px solid ${props => props.theme.selectBorderColorLT};
  background-color: ${props => props.theme.secondaryInputBgdLT};
  color: ${props => props.theme.textColorLT};
  border-radius: 2px;
  padding: 6px 8px;
`;

const UrlButton = styled.button`
  min-height: 34px;
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

const UrlHint = styled.div`
  margin-top: 8px;
  font-size: 11px;
  color: ${props => props.theme.subtextColorLT};
`;

const UrlError = styled.div`
  margin-top: 8px;
  font-size: 12px;
  color: ${props => props.theme.errorColor || '#d93025'};
`;

const EXTRA_FILE_EXTENSIONS = ['zip', 'gpkg'];
const EXTRA_FILE_FORMAT_NAMES = ['Shapefile ZIP', 'GeoPackage'];
const HIDDEN_UNSTABLE_EXTENSIONS = new Set(['arrow', 'parquet']);
const HIDDEN_UNSTABLE_FORMAT_NAMES = new Set(['arrow', 'parquet']);
const SPATIAL_EXTENSION_UNAVAILABLE_MESSAGE =
  'DuckDB spatial extension is unavailable in this environment. ZIP/GPKG upload is not supported in this browser session.';

let duckDbPromise: Promise<any> | null = null;
let duckDbModulePromise: Promise<DuckDbModule> | null = null;
let jsZipModulePromise: Promise<any> | null = null;
let proj4ModulePromise: Promise<any> | null = null;
let hasAttemptedSpatialInstall = false;

async function getDuckDbModule(): Promise<DuckDbModule> {
  if (!duckDbModulePromise) {
    duckDbModulePromise = import('@duckdb/duckdb-wasm');
  }
  return duckDbModulePromise;
}

async function getJsZipModule() {
  if (!jsZipModulePromise) {
    jsZipModulePromise = import('jszip');
  }
  return jsZipModulePromise;
}

async function getProj4Module() {
  if (!proj4ModulePromise) {
    proj4ModulePromise = import('proj4');
  }
  return proj4ModulePromise;
}

function mergeUnique(values: unknown[]): string[] {
  const output: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const text = String(value || '').trim();
    if (!text) continue;
    const key = text.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    output.push(text);
  }
  return output;
}

function filterHiddenUploadFormats(values: string[]): string[] {
  return values.filter(value => {
    const normalized = String(value || '').trim().toLowerCase();
    return normalized ? !HIDDEN_UNSTABLE_EXTENSIONS.has(normalized) && !HIDDEN_UNSTABLE_FORMAT_NAMES.has(normalized) : false;
  });
}

function getFileExtension(fileName: string): string {
  const match = String(fileName || '').toLowerCase().match(/\.([a-z0-9]+)$/);
  return match?.[1] || '';
}

function isPlainRecord(value: unknown): value is Record<string, any> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function isRowObjectArray(value: unknown): value is Record<string, any>[] {
  return Array.isArray(value) && (value.length === 0 || isPlainRecord(value[0]));
}

function isGeoJsonLike(value: unknown): boolean {
  if (!isPlainRecord(value)) return false;
  const type = String(value.type || '');
  return type === 'Feature' || type === 'FeatureCollection';
}

function isKeplerMapLike(value: unknown): boolean {
  if (!isPlainRecord(value)) return false;
  return Boolean(value.datasets && value.config && isPlainRecord(value.info) && value.info.app === 'kepler.gl');
}

function isJsonScalar(value: unknown): value is string | number | boolean | null {
  return value == null || typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean';
}

function toScalarRecord(value: Record<string, any>, excludeKeys: string[] = []): Record<string, any> {
  const output: Record<string, any> = {};
  const excludeSet = new Set(excludeKeys);
  for (const [key, nestedValue] of Object.entries(value || {})) {
    if (excludeSet.has(key)) continue;
    if (isJsonScalar(nestedValue)) {
      output[key] = nestedValue;
    }
  }
  return output;
}

function findWrappedRowArray(value: Record<string, any>): Record<string, any>[] | null {
  const preferredKeys = ['rows', 'data', 'items', 'results', 'records'];

  for (const key of preferredKeys) {
    if (isRowObjectArray(value[key])) {
      return value[key];
    }
  }

  for (const nestedValue of Object.values(value)) {
    if (isRowObjectArray(nestedValue)) {
      return nestedValue;
    }
  }

  return null;
}

function extractOpasRows(value: Record<string, any>): Record<string, any>[] | null {
  const meta = isPlainRecord(value.meta) ? value.meta : null;
  const payload = isPlainRecord(value.payload) ? value.payload : null;
  const payloadData = payload && isPlainRecord(payload.data) ? payload.data : null;

  if (!payloadData) return null;

  const selectedSeries = meta && isPlainRecord(meta.selected_series) ? meta.selected_series : null;
  const stationGeo = selectedSeries && isPlainRecord(selectedSeries.station_geo) ? selectedSeries.station_geo : null;
  const hasOpasHints = Boolean(
    (meta && (Object.hasOwn(meta, 'metric') || Object.hasOwn(meta, 'region') || Object.hasOwn(meta, 'generated_at_utc'))) ||
      Object.hasOwn(payloadData, 'series_data') ||
      Object.hasOwn(payloadData, 'station_id') ||
      Object.hasOwn(payloadData, 'parameter_name')
  );
  if (!hasOpasHints) return null;

  const baseRow: Record<string, any> = {
    ...toScalarRecord(payloadData, ['series_data']),
    opas_region: meta?.region ?? null,
    opas_metric: meta?.metric ?? null,
    opas_days: meta?.days ?? null,
    opas_generated_at_utc: meta?.generated_at_utc ?? null,
    station_id: payloadData?.station_id ?? selectedSeries?.station_id ?? null,
    station_name: payloadData?.station_name ?? selectedSeries?.station_name ?? null,
    parameter_id: payloadData?.parameter_id ?? null,
    parameter_name: payloadData?.parameter_name ?? selectedSeries?.parameter_name ?? null,
    parameter_unit: payloadData?.parameter_unit ?? selectedSeries?.parameter_unit ?? null,
    series_id: payloadData?.series_id ?? selectedSeries?.series_id ?? null,
    series_name: payloadData?.series_name ?? null,
    lat: stationGeo?.lat_wgs84 ?? null,
    lon: stationGeo?.lon_wgs84 ?? null,
    altitude: stationGeo?.altitude ?? null,
    municipality_name: stationGeo?.municipality_name ?? null,
    province_name: stationGeo?.province_name ?? null,
    region_name: stationGeo?.region_name ?? selectedSeries?.region_name ?? null
  };

  const seriesData = payloadData.series_data;
  if (Array.isArray(seriesData)) {
    const rows = seriesData
      .map(item => (isPlainRecord(item) ? {...baseRow, ...toScalarRecord(item)} : null))
      .filter(Boolean) as Record<string, any>[];
    if (rows.length) {
      return rows;
    }
  }

  return [baseRow];
}

async function normalizeJsonUploadFile(file: File): Promise<File> {
  if (getFileExtension(file?.name || '') !== 'json') {
    return file;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(await file.text());
  } catch {
    return file;
  }

  // Already in a format recognized by kepler processors.
  if (isRowObjectArray(parsed) || isGeoJsonLike(parsed) || isKeplerMapLike(parsed)) {
    return file;
  }

  if (!isPlainRecord(parsed)) {
    return file;
  }

  const extractedRows = findWrappedRowArray(parsed);
  if (extractedRows) {
    return new File([JSON.stringify(extractedRows)], file.name, {
      type: file.type || 'application/json'
    });
  }

  const opasRows = extractOpasRows(parsed);
  if (opasRows) {
    return new File([JSON.stringify(opasRows)], file.name, {
      type: file.type || 'application/json'
    });
  }

  return file;
}

function getErrorMessage(error: unknown): string {
  if (error && typeof error === 'object' && 'message' in error) {
    const message = String((error as any).message || '').trim();
    if (message) return message;
  }
  const message = String(error || '').trim();
  return message || 'Unknown error';
}

function buildSpatialExtensionError(loadError: unknown, installError?: unknown): Error {
  const details = [getErrorMessage(loadError), installError ? getErrorMessage(installError) : null]
    .filter(Boolean)
    .join(' | ');
  return new Error(`${SPATIAL_EXTENSION_UNAVAILABLE_MESSAGE}${details ? ` Details: ${details}` : ''}`);
}

function shouldFallbackToSingleThreadBundle(error: unknown): boolean {
  const message = getErrorMessage(error).toLowerCase();
  return (
    message.includes('thread constructor failed') ||
    message.includes('resource temporarily unavailable') ||
    message.includes('pthread') ||
    message.includes('sharedarraybuffer')
  );
}

async function instantiateDuckDbFromBundle(bundle: any, duckdb: DuckDbModule): Promise<any> {
  if (!bundle?.mainWorker || !bundle?.mainModule) {
    throw new Error('Invalid DuckDB bundle: missing worker or module.');
  }

  const workerUrl = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], {type: 'text/javascript'}));
  const worker = new Worker(workerUrl);
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);

  try {
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    return db;
  } catch (error) {
    worker.terminate();
    throw error;
  } finally {
    URL.revokeObjectURL(workerUrl);
  }
}

function isSpatialArchiveFile(file: File): boolean {
  const ext = getFileExtension(file?.name || '');
  return ext === 'zip' || ext === 'gpkg';
}

function normalizeShapefileEntryName(entryName: string): string {
  const parts = String(entryName || '').split('/');
  const fileName = parts.pop() || '';
  const normalizedFileName = fileName.replace(/\.shp\.(shp|dbf|shx|prj|cpg|qix|fix|sbn|sbx)$/i, '.$1');
  parts.push(normalizedFileName);
  return parts.join('/');
}

function buildVirtualFileName(prefix: string, entryName: string): string {
  const safeEntryName = String(entryName || 'file').replace(/[^a-zA-Z0-9._-]/g, '_');
  return `${prefix}_${safeEntryName}`;
}

async function extractShapefileZipEntries(
  file: File,
  uploadPrefix: string
): Promise<{primarySourceName: string; files: Array<{virtualName: string; file: File}>; prjText: string | null} | null> {
  if (getFileExtension(file?.name || '') !== 'zip') return null;

  try {
    const jsZip = await getJsZipModule();
    const JSZip = jsZip?.default || jsZip;
    const archive = await JSZip.loadAsync(await file.arrayBuffer());
    const entries = Object.values(archive.files || {}).filter((entry: any) => !entry?.dir);
    const normalizedEntries = entries.map((entry: any) => ({
      entry,
      normalizedName: normalizeShapefileEntryName(entry.name)
    }));
    const shapefileMain = normalizedEntries.find(item => /\.shp$/i.test(item.normalizedName));
    const prjEntry = normalizedEntries.find(item => /\.prj$/i.test(item.normalizedName));

    if (!shapefileMain) return null;

    const extractedFiles: Array<{virtualName: string; file: File}> = [];
    for (const item of normalizedEntries) {
      const content = await item.entry.async('uint8array');
      const virtualName = buildVirtualFileName(uploadPrefix, item.normalizedName);
      extractedFiles.push({
        virtualName,
        file: new File([content], virtualName, {type: 'application/octet-stream'})
      });
    }

    const primarySourceName = buildVirtualFileName(uploadPrefix, shapefileMain.normalizedName);
    const prjText = prjEntry ? String(await prjEntry.entry.async('string')).trim() || null : null;
    return {primarySourceName, files: extractedFiles, prjText};
  } catch {
    return null;
  }
}

function sqlEscapeLiteral(value: string): string {
  return String(value || '').replace(/'/g, "''");
}

function buildGeoJsonExpression(geometryColumn: string, sourceCrs: string | null): string {
  const baseGeometryExpression = geometryColumn === 'wkb_geometry' ? `ST_GeomFromWKB(${geometryColumn})` : geometryColumn;
  if (!sourceCrs || sourceCrs.toUpperCase() === 'EPSG:4326') {
    return `ST_AsGeoJSON(${baseGeometryExpression})`;
  }

  const sourceCrsLiteral = sqlEscapeLiteral(sourceCrs);
  return `ST_AsGeoJSON(ST_Transform(${baseGeometryExpression}, '${sourceCrsLiteral}', 'EPSG:4326', true))`;
}

function parseGeoJSONGeometry(value: unknown): any | null {
  if (value == null) return null;

  let parsed = value as any;
  if (typeof parsed === 'string') {
    const text = parsed.trim();
    if (!text) return null;
    try {
      parsed = JSON.parse(text);
    } catch {
      return null;
    }
  }

  if (!parsed || typeof parsed !== 'object') return null;
  if (parsed.type === 'Feature' && parsed.geometry) return parsed.geometry;
  if (parsed.type === 'FeatureCollection') return null;
  if (parsed.type && parsed.coordinates) return parsed;
  if (parsed.geometry) return parseGeoJSONGeometry(parsed.geometry);
  return null;
}

function toJsonSafeValue(value: unknown): any {
  if (typeof value === 'bigint') return value.toString();
  if (Array.isArray(value)) return value.map(item => toJsonSafeValue(item));
  if (value && typeof value === 'object') {
    if (value instanceof Date) return value.toISOString();
    const output: Record<string, any> = {};
    for (const [key, nested] of Object.entries(value as Record<string, any>)) {
      output[key] = toJsonSafeValue(nested);
    }
    return output;
  }
  return value;
}

function transformCoordinate(
  coordinate: unknown,
  project: (xy: [number, number]) => [number, number]
): unknown {
  if (!Array.isArray(coordinate)) return coordinate;

  if (coordinate.length >= 2 && typeof coordinate[0] === 'number' && typeof coordinate[1] === 'number') {
    try {
      const [x, y] = project([coordinate[0], coordinate[1]]);
      if (Number.isFinite(x) && Number.isFinite(y)) {
        return [x, y, ...coordinate.slice(2)];
      }
    } catch {
      return coordinate;
    }
    return coordinate;
  }

  return coordinate.map(item => transformCoordinate(item, project));
}

function reprojectFeatureGeometry(
  geometry: any,
  project: (xy: [number, number]) => [number, number]
): any {
  if (!geometry || typeof geometry !== 'object') return geometry;

  if (geometry.type === 'GeometryCollection' && Array.isArray(geometry.geometries)) {
    return {
      ...geometry,
      geometries: geometry.geometries.map((item: any) => reprojectFeatureGeometry(item, project))
    };
  }

  if (!('coordinates' in geometry)) return geometry;

  return {
    ...geometry,
    coordinates: transformCoordinate((geometry as any).coordinates, project)
  };
}

async function reprojectFeatureCollectionToWgs84(
  featureCollection: any,
  sourceCrsWkt: string | null
): Promise<any> {
  if (!sourceCrsWkt) return featureCollection;

  const proj4Module = await getProj4Module();
  const proj4 = proj4Module?.default || proj4Module;
  const project = (xy: [number, number]) => proj4(sourceCrsWkt, 'EPSG:4326', xy) as [number, number];

  try {
    project([0, 0]);
  } catch {
    return featureCollection;
  }

  return {
    ...featureCollection,
    features: (featureCollection?.features || []).map((feature: any) => ({
      ...feature,
      geometry: reprojectFeatureGeometry(feature?.geometry, project)
    }))
  };
}

function hasOutOfBoundsLonLatCoordinate(coordinate: unknown): boolean {
  if (!Array.isArray(coordinate)) return false;

  if (coordinate.length >= 2 && typeof coordinate[0] === 'number' && typeof coordinate[1] === 'number') {
    const lon = coordinate[0];
    const lat = coordinate[1];
    return !Number.isFinite(lon) || !Number.isFinite(lat) || Math.abs(lon) > 180 || Math.abs(lat) > 90;
  }

  return coordinate.some(item => hasOutOfBoundsLonLatCoordinate(item));
}

function hasOutOfBoundsLonLat(featureCollection: any): boolean {
  return (featureCollection?.features || []).some((feature: any) => {
    const geometry = feature?.geometry;
    if (!geometry) return false;

    if (geometry.type === 'GeometryCollection') {
      return (geometry.geometries || []).some((item: any) => hasOutOfBoundsLonLatCoordinate(item?.coordinates));
    }

    return hasOutOfBoundsLonLatCoordinate(geometry.coordinates);
  });
}

function buildFeatureCollection(rows: Record<string, any>[]) {
  const features = rows
    .map(row => {
      const geometry =
        parseGeoJSONGeometry(row?._geojson) ||
        parseGeoJSONGeometry(row?.geojson) ||
        parseGeoJSONGeometry(row?.geometry) ||
        parseGeoJSONGeometry(row?.geom) ||
        parseGeoJSONGeometry(row?.wkb_geometry);

      if (!geometry) return null;

      const properties: Record<string, any> = {};
      for (const [key, value] of Object.entries(row || {})) {
        properties[key] = toJsonSafeValue(value);
      }
      delete properties._geojson;
      delete properties.geojson;
      delete properties.geometry;
      delete properties.geom;
      delete properties.wkb_geometry;

      return {
        type: 'Feature',
        geometry,
        properties
      };
    })
    .filter(Boolean);

  if (!features.length) {
    throw new Error('No geometry features were found in the uploaded spatial file.');
  }

  return {
    type: 'FeatureCollection',
    features
  } as const;
}

async function getDuckDb(): Promise<any> {
  if (!duckDbPromise) {
    duckDbPromise = (async () => {
      const duckdb = await getDuckDbModule();
      const bundles = duckdb.getJsDelivrBundles() as any;
      const selectedBundle = await duckdb.selectBundle(bundles);

      try {
        return await instantiateDuckDbFromBundle(selectedBundle, duckdb);
      } catch (selectedError) {
        const mvpBundle = bundles?.mvp;
        const canFallback =
          mvpBundle &&
          mvpBundle.mainModule &&
          mvpBundle.mainWorker &&
          (mvpBundle.mainModule !== selectedBundle?.mainModule || mvpBundle.mainWorker !== selectedBundle?.mainWorker) &&
          shouldFallbackToSingleThreadBundle(selectedError);

        if (!canFallback) {
          throw selectedError;
        }

        try {
          return await instantiateDuckDbFromBundle(mvpBundle, duckdb);
        } catch (mvpError) {
          const selectedMessage = getErrorMessage(selectedError);
          const mvpMessage = getErrorMessage(mvpError);
          throw new Error(
            `DuckDB initialization failed on threaded and single-thread bundles. Threaded: ${selectedMessage}. Single-thread: ${mvpMessage}`
          );
        }
      }
    })().catch(error => {
      duckDbPromise = null;
      throw error;
    });
  }
  return duckDbPromise;
}

async function ensureSpatialExtension(conn: any): Promise<void> {
  try {
    await conn.query('LOAD spatial;');
    return;
  } catch (loadError) {
    if (hasAttemptedSpatialInstall) {
      throw buildSpatialExtensionError(loadError);
    }

    hasAttemptedSpatialInstall = true;
    try {
      await conn.query('INSTALL spatial;');
      await conn.query('LOAD spatial;');
      return;
    } catch (installError) {
      throw buildSpatialExtensionError(loadError, installError);
    }
  }
}

async function detectSourceCrsFromMeta(conn: any, sourceLiteral: string): Promise<string | null> {
  try {
    const meta = await conn.query(
      `SELECT
        layers[1].geometry_fields[1].crs.auth_name AS source_auth_name,
        layers[1].geometry_fields[1].crs.auth_code AS source_auth_code
      FROM ST_Read_Meta('${sourceLiteral}')`
    );
    const row = (meta.toArray?.() || [])[0] as Record<string, any> | undefined;
    const authName = String(row?.source_auth_name || '').trim().toUpperCase();
    const authCode = String(row?.source_auth_code || '').trim();

    if (authName && authCode) return `${authName}:${authCode}`;
    if (authCode) return `EPSG:${authCode}`;
    return null;
  } catch {
    return null;
  }
}

async function loadSpatialRowsWithDuckDb(file: File): Promise<{rows: Record<string, any>[]; sourcePrj: string | null}> {
  const duckdb = await getDuckDbModule();
  const db = await getDuckDb();
  const conn = await db.connect();
  const uploadPrefix = `qmap_upload_${Date.now()}_${Math.random().toString(36).slice(2)}`;
  const registeredSourceNames: string[] = [];

  try {
    const extractedShapefile = await extractShapefileZipEntries(file, uploadPrefix);
    const sourceExtension = getFileExtension(file?.name || '');
    const normalizedExtension = sourceExtension === 'zip' || sourceExtension === 'gpkg' ? sourceExtension : 'data';
    let sourceName = `${uploadPrefix}.${normalizedExtension}`;

    if (extractedShapefile) {
      for (const item of extractedShapefile.files) {
        await db.registerFileHandle(item.virtualName, item.file, duckdb.DuckDBDataProtocol.BROWSER_FILEREADER, true);
        registeredSourceNames.push(item.virtualName);
      }
      sourceName = extractedShapefile.primarySourceName;
    } else {
      await db.registerFileHandle(sourceName, file, duckdb.DuckDBDataProtocol.BROWSER_FILEREADER, true);
      registeredSourceNames.push(sourceName);
    }

    await ensureSpatialExtension(conn);

    const sourceLiteral = sqlEscapeLiteral(sourceName);
    const useGpkgSqlReprojection = sourceExtension === 'gpkg';
    const sourceCrs = useGpkgSqlReprojection ? await detectSourceCrsFromMeta(conn, sourceLiteral) : null;

    const queries = [
      `SELECT ${buildGeoJsonExpression('geom', sourceCrs)} AS _geojson, * EXCLUDE (geom) FROM ST_READ('${sourceLiteral}')`,
      `SELECT ${buildGeoJsonExpression('geometry', sourceCrs)} AS _geojson, * EXCLUDE (geometry) FROM ST_READ('${sourceLiteral}')`,
      `SELECT ${buildGeoJsonExpression('wkb_geometry', sourceCrs)} AS _geojson, * EXCLUDE (wkb_geometry) FROM ST_READ('${sourceLiteral}')`,
      `SELECT * FROM ST_READ('${sourceLiteral}')`
    ];

    let lastError: unknown = null;
    for (const query of queries) {
      try {
        const table = await conn.query(query);
        return {
          rows: (table.toArray?.() || []) as Record<string, any>[],
          sourcePrj: extractedShapefile?.prjText || null
        };
      } catch (error) {
        lastError = error;
      }
    }

    throw lastError || new Error('Could not read the spatial file with DuckDB ST_READ.');
  } finally {
    await conn.close().catch(() => { /* ignore */ });
    for (const sourceName of registeredSourceNames) {
      await db.dropFile(sourceName).catch(() => { /* ignore */ });
    }
  }
}

async function convertSpatialFileToGeoJSON(file: File): Promise<File> {
  const {rows, sourcePrj} = await loadSpatialRowsWithDuckDb(file);
  let featureCollection = buildFeatureCollection(rows);
  if (getFileExtension(file?.name || '') === 'zip' && sourcePrj) {
    featureCollection = await reprojectFeatureCollectionToWgs84(featureCollection, sourcePrj);
  }
  if (hasOutOfBoundsLonLat(featureCollection)) {
    throw new Error('Spatial file CRS is not compatible with EPSG:4326. Reproject to WGS84 or include a valid .prj definition.');
  }
  const baseName = String(file?.name || 'dataset').replace(/\.(zip|gpkg)$/i, '');
  const outputName = `${baseName || 'dataset'}.geojson`;
  return new File([JSON.stringify(featureCollection)], outputName, {type: 'application/geo+json'});
}

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

      if (outputParam === 'csv' || lowerContentType.includes('csv')) {
        return `${path}.csv`;
      }
      if (outputParam === 'geojson' || lowerContentType.includes('geo+json')) {
        return `${path}.geojson`;
      }
      if (outputParam === 'json' || lowerContentType.includes('json')) {
        return `${path}.json`;
      }
      if (lowerContentType.includes('zip')) {
        return `${path}.zip`;
      }
      if (lowerContentType.includes('gpkg') || lowerContentType.includes('geopackage')) {
        return `${path}.gpkg`;
      }

      return `${path}.json`;
    }
  } catch {
    // noop
  }

  if (outputParam === 'csv' || lowerContentType.includes('csv')) {
    return 'dataset.csv';
  }
  if (outputParam === 'geojson' || lowerContentType.includes('geo+json')) {
    return 'dataset.geojson';
  }
  if (outputParam === 'json' || lowerContentType.includes('json')) {
    return 'dataset.json';
  }
  if (lowerContentType.includes('zip')) {
    return 'dataset.zip';
  }
  if (lowerContentType.includes('gpkg') || lowerContentType.includes('geopackage')) {
    return 'dataset.gpkg';
  }
  return 'dataset.data';
}

function QMapFileUploadFactory() {
  const QMapFileUpload: React.FC<any> = props => {
    const [url, setUrl] = useState('');
    const [loading, setLoading] = useState(false);
    const [processingFiles, setProcessingFiles] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const fileExtensions = useMemo(
      () => filterHiddenUploadFormats(mergeUnique([...(props.fileExtensions || []), ...EXTRA_FILE_EXTENSIONS])),
      [props.fileExtensions]
    );
    const fileFormatNames = useMemo(
      () => filterHiddenUploadFormats(mergeUnique([...(props.fileFormatNames || []), ...EXTRA_FILE_FORMAT_NAMES])),
      [props.fileFormatNames]
    );
    const canLoad = useMemo(() => Boolean(url.trim()) && !loading && !processingFiles, [url, loading, processingFiles]);

    const onFileUpload = useCallback(
      async (files: File[]) => {
        setError(null);
        setProcessingFiles(true);
        try {
          const processedFiles: File[] = [];
          for (const file of files || []) {
            if (isSpatialArchiveFile(file)) {
              processedFiles.push(await convertSpatialFileToGeoJSON(file));
            } else {
              processedFiles.push(await normalizeJsonUploadFile(file));
            }
          }

          props.onFileUpload?.(processedFiles);
        } catch (err: any) {
          setError(err?.message || 'Failed to process uploaded spatial file.');
        } finally {
          setProcessingFiles(false);
        }
      },
      [props]
    );

    const onLoadFromUrl = async () => {
      const target = url.trim();
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

        await onFileUpload([file]);
      } catch (err: any) {
        setError(err?.message || 'Failed to load dataset from URL');
      } finally {
        setLoading(false);
      }
    };

    return (
      <div>
        <UrlImportCard>
          <UrlRow>
            <UrlInput
              type="url"
              placeholder="Paste dataset URL (GeoJSON/CSV/JSON/ZIP/GPKG)"
              value={url}
              onChange={event => setUrl(event.target.value)}
            />
            <UrlButton type="button" disabled={!canLoad} onClick={onLoadFromUrl}>
              {loading || processingFiles ? 'Loading...' : 'Load URL'}
            </UrlButton>
          </UrlRow>
          <UrlHint>
            Example: `http://localhost:3003/h3/registry?output=geojson&bbox=6,36,19,48&limit=5000`
          </UrlHint>
          {error ? <UrlError>{error}</UrlError> : null}
        </UrlImportCard>

        <CoreFileUpload
          {...props}
          fileExtensions={fileExtensions}
          fileFormatNames={fileFormatNames}
          onFileUpload={onFileUpload}
        />
      </div>
    );
  };

  return QMapFileUpload;
}

export default QMapFileUploadFactory;
