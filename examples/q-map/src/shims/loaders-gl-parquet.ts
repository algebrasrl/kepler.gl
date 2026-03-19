const unsupported = () => {
  throw new Error('Parquet support is disabled in q-map.');
};

export const ParquetWasmLoader = {
  id: 'parquet-disabled-loader',
  name: 'Parquet Disabled Loader',
  module: 'parquet',
  version: '0.0.0',
  extensions: [],
  mimeTypes: [],
  test: () => false,
  testText: () => false,
  parse: unsupported,
  parseSync: unsupported,
  parseInBatches: unsupported
};
