import assert from 'node:assert/strict';
import test from 'node:test';
import {normalizeQMapToolExecuteArgs, preprocessFlatFilterToolArgs} from '../../../src/features/qmap-ai/tool-args-normalization';

test('keeps canonical args for setQMapLayerSolidColor', () => {
  const normalized = normalizeQMapToolExecuteArgs('setQMapLayerSolidColor', {
    layerName: 'Comuni_Treviso_Area',
    fillColor: '#FF0000'
  });
  assert.equal(normalized.layerName, 'Comuni_Treviso_Area');
  assert.equal(normalized.fillColor, '#FF0000');
});

test('does not remap non-canonical args for createDatasetWithNormalizedField', () => {
  const normalized = normalizeQMapToolExecuteArgs('createDatasetWithNormalizedField', {
    datasetName: 'id:qmap_h3_join_example',
    numeratorField: 'area_ha__sum',
    denominatorField: 'qmap_h3_area_ha',
    outputFieldName: 'boschi_pct_area'
  });
  assert.equal(normalized.sourceDatasetName, undefined);
  assert.equal(normalized.numeratorFieldName, undefined);
  assert.equal(normalized.denominatorFieldName, undefined);
  assert.equal(normalized.datasetName, 'id:qmap_h3_join_example');
  assert.equal(normalized.numeratorField, 'area_ha__sum');
  assert.equal(normalized.denominatorField, 'qmap_h3_area_ha');
  assert.equal(normalized.outputFieldName, 'boschi_pct_area');
});

test('does not remap datasetRef/datasetId aliases for waitForQMapDataset', () => {
  const fromDatasetRef = normalizeQMapToolExecuteArgs('waitForQMapDataset', {
    datasetRef: 'id:qmap_clip_boschi_treviso_clipped'
  });
  assert.equal(fromDatasetRef.datasetName, undefined);
  assert.equal(fromDatasetRef.datasetRef, 'id:qmap_clip_boschi_treviso_clipped');

  const keepCanonical = normalizeQMapToolExecuteArgs('waitForQMapDataset', {
    datasetName: 'id:already_canonical',
    datasetRef: 'id:ignored_alias'
  });
  assert.equal(keepCanonical.datasetName, 'id:already_canonical');
});

test('does not remap style metric aliases for setQMapLayerColorByField', () => {
  const normalized = normalizeQMapToolExecuteArgs('setQMapLayerColorByField', {
    datasetName: 'Tassellation_Brescia_Population',
    layerNameOrId: 'Tassellation_Brescia_Population',
    metricFieldName: 'population'
  });
  assert.equal(normalized.layerName, undefined);
  assert.equal(normalized.fieldName, undefined);
  assert.equal(normalized.layerNameOrId, 'Tassellation_Brescia_Population');
  assert.equal(normalized.metricFieldName, 'population');
});

test('prefers canonical datasetRef when resolver can map aliases', () => {
  const normalized = normalizeQMapToolExecuteArgs(
    'countQMapRows',
    {
      datasetName: 'Tassellation_Brescia_r8'
    },
    {
      resolveCanonicalDatasetRef: value => (value === 'Tassellation_Brescia_r8' ? 'id:pzbzvu' : '')
    }
  );
  assert.equal(normalized.datasetName, 'id:pzbzvu');
});

test('keeps chart-tool datasetName labels when resolver is provided', () => {
  const normalized = normalizeQMapToolExecuteArgs(
    'histogramTool',
    {
      datasetName: 'population_polygons.geojson',
      variableName: 'population'
    },
    {
      resolveCanonicalDatasetRef: value => (value === 'population_polygons.geojson' ? 'id:abc123' : '')
    }
  );
  assert.equal(normalized.datasetName, 'population_polygons.geojson');
});

test('keeps styling-tool datasetName labels when resolver is provided', () => {
  const normalized = normalizeQMapToolExecuteArgs(
    'setQMapTooltipFields',
    {
      datasetName: 'population_polygons.geojson',
      fieldNames: ['name', 'population']
    },
    {
      resolveCanonicalDatasetRef: value => (value === 'population_polygons.geojson' ? 'id:abc123' : '')
    }
  );
  assert.equal(normalized.datasetName, 'population_polygons.geojson');
});

test('returns stable object for unknown tools and non-object args', () => {
  assert.deepEqual(normalizeQMapToolExecuteArgs('unknownTool', {foo: 'bar'}), {foo: 'bar'});
  assert.deepEqual(normalizeQMapToolExecuteArgs('setQMapLayerSolidColor', null), {});
});

// ── Filter parameter normalization ──

test('normalizes filters array to flat params for createDatasetFromFilter', () => {
  const normalized = normalizeQMapToolExecuteArgs('createDatasetFromFilter', {
    datasetName: 'Province',
    filters: [{field: 'name', op: 'eq', value: 'Trieste'}],
    showOnMap: true
  });
  assert.equal(normalized.fieldName, 'name');
  assert.equal(normalized.operator, 'eq');
  assert.equal(normalized.value, 'Trieste');
  assert.equal(normalized.filters, undefined);
});

test('normalizes filter object to flat params for createDatasetFromFilter', () => {
  const normalized = normalizeQMapToolExecuteArgs('createDatasetFromFilter', {
    datasetName: 'Province',
    filter: {fieldName: 'population', operator: 'gt', value: 1000}
  });
  assert.equal(normalized.fieldName, 'population');
  assert.equal(normalized.operator, 'gt');
  assert.equal(normalized.value, 1000);
  assert.equal(normalized.filter, undefined);
});

test('normalizes field→fieldName and op→operator aliases', () => {
  const normalized = normalizeQMapToolExecuteArgs('countQMapRows', {
    datasetName: 'ds',
    field: 'population',
    op: 'gte',
    value: 500
  });
  assert.equal(normalized.fieldName, 'population');
  assert.equal(normalized.operator, 'gte');
  assert.equal(normalized.field, undefined);
  assert.equal(normalized.op, undefined);
});

test('normalizes operator aliases == != > >= < <=', () => {
  const cases: Array<[string, string]> = [
    ['==', 'eq'],
    ['=', 'eq'],
    ['!=', 'neq'],
    ['<>', 'neq'],
    ['>', 'gt'],
    ['>=', 'gte'],
    ['<', 'lt'],
    ['<=', 'lte'],
    ['like', 'contains'],
    ['starts_with', 'startsWith'],
    ['ends_with', 'endsWith']
  ];
  for (const [input, expected] of cases) {
    const normalized = normalizeQMapToolExecuteArgs('createDatasetFromFilter', {
      datasetName: 'ds',
      fieldName: 'f',
      operator: input,
      value: 1
    });
    assert.equal(normalized.operator, expected, `expected ${input} → ${expected}`);
  }
});

test('preserves correct flat params without modification', () => {
  const normalized = normalizeQMapToolExecuteArgs('createDatasetFromFilter', {
    datasetName: 'Province',
    fieldName: 'name',
    operator: 'eq',
    value: 'Trieste'
  });
  assert.equal(normalized.fieldName, 'name');
  assert.equal(normalized.operator, 'eq');
  assert.equal(normalized.value, 'Trieste');
});

test('does not apply filter normalization to non-filter tools', () => {
  const normalized = normalizeQMapToolExecuteArgs('setQMapLayerSolidColor', {
    filters: [{field: 'x'}],
    layerName: 'test'
  });
  assert.ok(Array.isArray(normalized.filters), 'filters should be preserved for non-filter tools');
});

// ── preprocessFlatFilterToolArgs (Zod preprocess callback) ──

test('preprocessFlatFilterToolArgs normalizes filters array', () => {
  const result = preprocessFlatFilterToolArgs({
    datasetName: 'Province',
    filters: [{field: 'name', op: 'eq', value: 'Trieste'}],
    showOnMap: true,
    newDatasetName: 'Provincia_Piu_Piccola'
  }) as Record<string, unknown>;
  assert.equal(result.fieldName, 'name');
  assert.equal(result.operator, 'eq');
  assert.equal(result.value, 'Trieste');
  assert.equal(result.filters, undefined);
  assert.equal(result.datasetName, 'Province');
  assert.equal(result.showOnMap, true);
});

test('preprocessFlatFilterToolArgs passes through canonical args', () => {
  const result = preprocessFlatFilterToolArgs({
    datasetName: 'ds',
    fieldName: 'name',
    operator: 'eq',
    value: 'Trieste'
  }) as Record<string, unknown>;
  assert.equal(result.fieldName, 'name');
  assert.equal(result.operator, 'eq');
  assert.equal(result.value, 'Trieste');
});

test('preprocessFlatFilterToolArgs returns non-object inputs as-is', () => {
  assert.equal(preprocessFlatFilterToolArgs(null), null);
  assert.equal(preprocessFlatFilterToolArgs(undefined), undefined);
  assert.equal(preprocessFlatFilterToolArgs('string'), 'string');
  assert.deepEqual(preprocessFlatFilterToolArgs([1, 2]), [1, 2]);
});
