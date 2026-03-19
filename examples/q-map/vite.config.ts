// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import {defineConfig, loadEnv} from 'vite';
import react from '@vitejs/plugin-react';
import wasm from 'vite-plugin-wasm';
import {resolve} from 'path';

const localKeplerAliases = {
  '@kepler.gl/ai-assistant': resolve(__dirname, '../../src/ai-assistant/src'),
  '@kepler.gl/actions': resolve(__dirname, '../../src/actions/src'),
  '@kepler.gl/cloud-providers': resolve(__dirname, '../../src/cloud-providers/src'),
  '@kepler.gl/common-utils': resolve(__dirname, '../../src/common-utils/src'),
  '@kepler.gl/components': resolve(__dirname, '../../src/components/src'),
  '@kepler.gl/constants': resolve(__dirname, '../../src/constants/src'),
  '@kepler.gl/deckgl-arrow-layers': resolve(__dirname, '../../src/deckgl-arrow-layers/src'),
  '@kepler.gl/deckgl-layers': resolve(__dirname, '../../src/deckgl-layers/src'),
  '@kepler.gl/effects': resolve(__dirname, '../../src/effects/src'),
  '@kepler.gl/layers': resolve(__dirname, '../../src/layers/src'),
  '@kepler.gl/localization': resolve(__dirname, '../../src/localization/src'),
  '@kepler.gl/processors': resolve(__dirname, '../../src/processors/src'),
  '@kepler.gl/reducers': resolve(__dirname, '../../src/reducers/src'),
  '@kepler.gl/schemas': resolve(__dirname, '../../src/schemas/src'),
  '@kepler.gl/styles': resolve(__dirname, '../../src/styles/src'),
  '@kepler.gl/table': resolve(__dirname, '../../src/table/src'),
  '@kepler.gl/tasks': resolve(__dirname, '../../src/tasks/src'),
  '@kepler.gl/types': resolve(__dirname, '../../src/types/src'),
  '@kepler.gl/utils': resolve(__dirname, '../../src/utils/src')
};

const productionKeplerAliases = {
  '@kepler.gl/ai-assistant': resolve(__dirname, '../../src/ai-assistant/src'),
  '@kepler.gl/components': resolve(__dirname, '../../src/components/src')
};

const localQMapDepsAliases = {
  '@deck.gl/aggregation-layers': resolve(__dirname, './node_modules/@deck.gl/aggregation-layers'),
  '@deck.gl/core': resolve(__dirname, './node_modules/@deck.gl/core'),
  '@deck.gl/extensions': resolve(__dirname, './node_modules/@deck.gl/extensions'),
  '@deck.gl/geo-layers': resolve(__dirname, './node_modules/@deck.gl/geo-layers'),
  '@deck.gl/layers': resolve(__dirname, './node_modules/@deck.gl/layers'),
  '@deck.gl/mesh-layers': resolve(__dirname, './node_modules/@deck.gl/mesh-layers'),
  '@geoarrow/geoarrow-js': resolve(__dirname, './node_modules/@geoarrow/geoarrow-js'),
  '@loaders.gl/arrow': resolve(__dirname, './node_modules/@loaders.gl/arrow'),
  '@loaders.gl/core': resolve(__dirname, './node_modules/@loaders.gl/core'),
  '@loaders.gl/gltf': resolve(__dirname, './node_modules/@loaders.gl/gltf'),
  '@loaders.gl/images': resolve(__dirname, './node_modules/@loaders.gl/images'),
  '@loaders.gl/mvt': resolve(__dirname, './node_modules/@loaders.gl/mvt'),
  '@loaders.gl/parquet': resolve(__dirname, './node_modules/@loaders.gl/parquet'),
  '@loaders.gl/pmtiles': resolve(__dirname, './node_modules/@loaders.gl/pmtiles'),
  '@loaders.gl/wms': resolve(__dirname, './node_modules/@loaders.gl/wms'),
  '@loaders.gl/xml': resolve(__dirname, './node_modules/@loaders.gl/xml'),
  '@luma.gl/constants': resolve(__dirname, './node_modules/@luma.gl/constants'),
  '@luma.gl/core': resolve(__dirname, './node_modules/@luma.gl/core'),
  '@luma.gl/engine': resolve(__dirname, './node_modules/@luma.gl/engine'),
  '@luma.gl/experimental': resolve(__dirname, './node_modules/@luma.gl/experimental'),
  '@luma.gl/gltools': resolve(__dirname, './node_modules/@luma.gl/gltools'),
  '@luma.gl/shadertools': resolve(__dirname, './node_modules/@luma.gl/shadertools'),
  '@luma.gl/webgl': resolve(__dirname, './node_modules/@luma.gl/webgl'),
  '@math.gl/core': resolve(__dirname, './node_modules/@math.gl/core'),
  '@math.gl/web-mercator': resolve(__dirname, './node_modules/@math.gl/web-mercator'),
  'chroma-js': resolve(__dirname, './node_modules/chroma-js'),
  'd3-scale-chromatic': resolve(__dirname, './node_modules/d3-scale-chromatic'),
  'markdown-to-jsx': resolve(__dirname, './node_modules/markdown-to-jsx'),
  'react-virtualized': resolve(__dirname, './node_modules/react-virtualized')
};

const disabledLoaderAliases = {
  '@loaders.gl/arrow': resolve(__dirname, './src/shims/loaders-gl-arrow.ts'),
  '@loaders.gl/parquet': resolve(__dirname, './src/shims/loaders-gl-parquet.ts')
};

function patchLoadersMvtUnreachableCode() {
  const mvtConvertFeaturePath = '/@loaders.gl/mvt/dist/lib/vector-tiler/features/convert-feature.js';

  return {
    name: 'q-map-patch-loaders-mvt-unreachable-code',
    enforce: 'pre' as const,
    transform(code: string, id: string) {
      if (!id.includes(mvtConvertFeaturePath)) {
        return null;
      }

      const patchedCode = code.replace(
        /if \(options\.lineMetrics\)\s*\{([\s\S]*?)return;\s*convertLines\(coords, geometry, tolerance, false\);\s*\}\s*break;/m,
        `if (options.lineMetrics) {$1return;
            }
            convertLines(coords, geometry, tolerance, false);
            break;`
      );

      if (patchedCode === code) {
        return null;
      }

      return {
        code: patchedCode,
        map: null
      };
    }
  };
}

function qMapRuntimeAuthConfigPlugin(runtimeToken: string) {
  const serializedToken = JSON.stringify(String(runtimeToken || ''));
  const serializedMapboxToken = JSON.stringify(String(process.env.QMAP_MAPBOX_TOKEN || process.env.VITE_MAPBOX_TOKEN || ''));

  return {
    name: 'q-map-runtime-auth-config',
    configureServer(server: any) {
      server.middlewares.use((req: any, res: any, next: () => void) => {
        const requestUrl = String(req?.url || '').split('?')[0];
        if (requestUrl !== '/qmap-runtime-config.js') {
          next();
          return;
        }
        res.statusCode = 200;
        res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
        res.setHeader('Cache-Control', 'no-store');
        res.end(
          `window.__QMAP_AUTH_TOKEN__ = ${serializedToken};\nwindow.__QMAP_MAPBOX_TOKEN__ = ${serializedMapboxToken};\n`
        );
      });
    }
  };
}

// https://vitejs.dev/config/
export default defineConfig(({mode}) => {
  const env = loadEnv(mode, __dirname, '');
  const isProductionBuild = mode === 'production';
  const selectedKeplerAliases =
    isProductionBuild && String(process.env.VITE_QMAP_USE_LEAN_KEPLER_ALIASES || '').toLowerCase() === 'true'
      ? productionKeplerAliases
      : localKeplerAliases;
  const runtimeAuthToken = env.QMAP_AUTH_RUNTIME_TOKEN || '';

  return {
    plugins: [patchLoadersMvtUnreachableCode(), qMapRuntimeAuthConfigPlugin(runtimeAuthToken), wasm(), react()],
    server: {
      port: 8081,
      open: true,
      proxy: {
        '/api': {
          target: 'http://localhost:8000',
          changeOrigin: true,
          secure: false
        },
        '/anthropic': {
          target: 'https://api.anthropic.com',
          changeOrigin: true,
          secure: true,
          rewrite: path => path.replace(/^\/anthropic/, '/v1')
        }
      },
      fs: {
        allow: [resolve(__dirname, '../..')]
      }
    },
    build: {
      outDir: 'dist',
      sourcemap: false,
      minify: true,
      rollupOptions: {
        input: {
          main: resolve(__dirname, 'index.html')
        },
        output: {
          manualChunks(id) {
            if (
              id.includes('maplibre-gl') ||
              id.includes('mapbox-gl') ||
              id.includes('react-map-gl/mapbox') ||
              id.includes('react-map-gl/maplibre')
            ) {
              return 'map-engine';
            }
            return undefined;
          }
        }
      },
      target: 'esnext',
      commonjsOptions: {
        include: [/node_modules/],
        transformMixedEsModules: true
      }
    },
    define: {
      'process.env.NODE_ENV': JSON.stringify(process.env.NODE_ENV || 'production'),
      'process.env.MapboxAccessToken': JSON.stringify(process.env.MapboxAccessToken || ''),
      'process.env.DropboxClientId': JSON.stringify(process.env.DropboxClientId || ''),
      'process.env.MapboxExportToken': JSON.stringify(process.env.MapboxExportToken || ''),
      'process.env.CartoClientId': JSON.stringify(process.env.CartoClientId || ''),
      'process.env.FoursquareClientId': JSON.stringify(process.env.FoursquareClientId || ''),
      'process.env.FoursquareDomain': JSON.stringify(process.env.FoursquareDomain || ''),
      'process.env.FoursquareAPIURL': JSON.stringify(process.env.FoursquareAPIURL || ''),
      'process.env.FoursquareUserMapsURL': JSON.stringify(process.env.FoursquareUserMapsURL || ''),
      'process.env.OpenAIToken': JSON.stringify(process.env.OpenAIToken || ''),
      'process.env.NODE_DEBUG': JSON.stringify(false)
    },
    resolve: {
      dedupe: ['styled-components'],
      alias: {
        '@': resolve(__dirname, './src'),
        ...selectedKeplerAliases,
        ...localQMapDepsAliases,
        ...disabledLoaderAliases
      }
    },
    optimizeDeps: {
      exclude: [
        'parquet-wasm',
        '@loaders.gl/parquet',
        'apache-arrow',
        '@kepler.gl/actions',
        '@kepler.gl/ai-assistant',
        '@kepler.gl/cloud-providers',
        '@kepler.gl/common-utils',
        '@kepler.gl/components',
        '@kepler.gl/constants',
        '@kepler.gl/deckgl-arrow-layers',
        '@kepler.gl/deckgl-layers',
        '@kepler.gl/effects',
        '@kepler.gl/layers',
        '@kepler.gl/localization',
        '@kepler.gl/processors',
        '@kepler.gl/reducers',
        '@kepler.gl/schemas',
        '@kepler.gl/styles',
        '@kepler.gl/table',
        '@kepler.gl/tasks',
        '@kepler.gl/types',
        '@kepler.gl/utils'
      ],
      include: [
        'buffer',
        'react',
        'react-dom',
        'react-redux',
        'redux',
        'styled-components',
        '@deck.gl/core',
        '@deck.gl/layers',
        '@deck.gl/aggregation-layers',
        '@deck.gl/geo-layers',
        '@deck.gl/mesh-layers',
        '@deck.gl/extensions',
        '@luma.gl/core',
        '@luma.gl/engine',
        '@luma.gl/gltools',
        '@luma.gl/shadertools',
        '@luma.gl/webgl',
        '@loaders.gl/core',
        '@loaders.gl/gltf',
        '@loaders.gl/images',
        '@loaders.gl/parquet',
        '@math.gl/core',
        '@math.gl/web-mercator',
        'gl-matrix',
        'lodash.uniq'
      ],
      esbuildOptions: {
        target: 'es2020'
      }
    }
  };
});
