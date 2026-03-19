const unsupported = () => {
  throw new Error('GeoArrow support is disabled in q-map.');
};

export const GeoArrowLoader = {
  id: 'geoarrow-disabled-loader',
  name: 'GeoArrow Disabled Loader',
  module: 'geoarrow',
  version: '0.0.0',
  extensions: [],
  mimeTypes: [],
  test: () => false,
  testText: () => false,
  parse: unsupported,
  parseSync: unsupported,
  parseInBatches: unsupported
};

export const parseGeometryFromArrow = () => null;
export const getBinaryGeometriesFromArrow = () => [];
export const updateBoundsFromGeoArrowSamples = (bounds: any) => bounds;
