import {describe, it, expect} from 'vitest';
import {
  normalizeQMapToolExecuteArgs,
  preprocessFlatFilterToolArgs
} from '../../../src/features/qmap-ai/tool-args-normalization';

// ─── normalizeQMapToolExecuteArgs ───────────────────────────────────────────

describe('normalizeQMapToolExecuteArgs', () => {
  it('preserves canonical args unchanged', () => {
    const result = normalizeQMapToolExecuteArgs('setQMapLayerSolidColor', {
      layerName: 'Comuni',
      fillColor: '#FF0000'
    });
    expect(result.layerName).toBe('Comuni');
    expect(result.fillColor).toBe('#FF0000');
  });

  it('returns empty object for null/undefined args', () => {
    expect(normalizeQMapToolExecuteArgs('test', null)).toEqual({});
    expect(normalizeQMapToolExecuteArgs('test', undefined)).toEqual({});
  });

  it('returns empty object for non-object args', () => {
    expect(normalizeQMapToolExecuteArgs('test', 'string' as any)).toEqual({});
    expect(normalizeQMapToolExecuteArgs('test', 42 as any)).toEqual({});
    expect(normalizeQMapToolExecuteArgs('test', [1, 2] as any)).toEqual({});
  });

  it('passes through unknown tool args without modification', () => {
    const result = normalizeQMapToolExecuteArgs('unknownTool', {foo: 'bar', baz: 123});
    expect(result).toEqual({foo: 'bar', baz: 123});
  });

  // ── Dataset name resolution ──

  it('resolves datasetName via resolver for canonical dataset ref tools', () => {
    const result = normalizeQMapToolExecuteArgs(
      'countQMapRows',
      {datasetName: 'Tassellation_Brescia_r8'},
      {resolveCanonicalDatasetRef: value => (value === 'Tassellation_Brescia_r8' ? 'id:pzbzvu' : '')}
    );
    expect(result.datasetName).toBe('id:pzbzvu');
  });

  it('does not resolve datasetName for non-canonical tools', () => {
    const result = normalizeQMapToolExecuteArgs(
      'setQMapLayerSolidColor',
      {datasetName: 'Comuni'},
      {resolveCanonicalDatasetRef: () => 'id:resolved'}
    );
    expect(result.datasetName).toBe('Comuni');
  });

  it('fills fallback datasetName for waitForQMapDataset when missing', () => {
    const result = normalizeQMapToolExecuteArgs(
      'waitForQMapDataset',
      {},
      {resolveFallbackDatasetRef: () => 'id:fallback-dataset'}
    );
    expect(result.datasetName).toBe('id:fallback-dataset');
  });

  it('does not override existing datasetName with fallback', () => {
    const result = normalizeQMapToolExecuteArgs(
      'waitForQMapDataset',
      {datasetName: 'id:existing'},
      {resolveFallbackDatasetRef: () => 'id:fallback-dataset'}
    );
    expect(result.datasetName).toBe('id:existing');
  });

  it('fills fallback datasetName for countQMapRows when missing', () => {
    const result = normalizeQMapToolExecuteArgs(
      'countQMapRows',
      {},
      {resolveFallbackDatasetRef: () => 'id:fallback-count'}
    );
    expect(result.datasetName).toBe('id:fallback-count');
  });

  // ── Filter normalization ──

  it('normalizes filters array to flat params for createDatasetFromFilter', () => {
    const result = normalizeQMapToolExecuteArgs('createDatasetFromFilter', {
      datasetName: 'Province',
      filters: [{field: 'name', op: 'eq', value: 'Trieste'}],
      showOnMap: true
    });
    expect(result.fieldName).toBe('name');
    expect(result.operator).toBe('eq');
    expect(result.value).toBe('Trieste');
    expect(result.filters).toBeUndefined();
  });

  it('normalizes filter object to flat params for createDatasetFromFilter', () => {
    const result = normalizeQMapToolExecuteArgs('createDatasetFromFilter', {
      datasetName: 'Province',
      filter: {fieldName: 'population', operator: 'gt', value: 1000}
    });
    expect(result.fieldName).toBe('population');
    expect(result.operator).toBe('gt');
    expect(result.value).toBe(1000);
    expect(result.filter).toBeUndefined();
  });

  it('normalizes field->fieldName and op->operator aliases', () => {
    const result = normalizeQMapToolExecuteArgs('countQMapRows', {
      datasetName: 'ds',
      field: 'population',
      op: 'gte',
      value: 500
    });
    expect(result.fieldName).toBe('population');
    expect(result.operator).toBe('gte');
    expect(result.field).toBeUndefined();
    expect(result.op).toBeUndefined();
  });

  it('normalizes operator aliases for filter tools', () => {
    const cases: Array<[string, string]> = [
      ['==', 'eq'],
      ['=', 'eq'],
      ['equals', 'eq'],
      ['equal', 'eq'],
      ['!=', 'neq'],
      ['<>', 'neq'],
      ['not_eq', 'neq'],
      ['not_equal', 'neq'],
      ['>', 'gt'],
      ['greater_than', 'gt'],
      ['>=', 'gte'],
      ['greater_than_or_equal', 'gte'],
      ['<', 'lt'],
      ['less_than', 'lt'],
      ['<=', 'lte'],
      ['less_than_or_equal', 'lte'],
      ['like', 'contains'],
      ['substring', 'contains'],
      ['starts_with', 'startsWith'],
      ['ends_with', 'endsWith']
    ];
    for (const [input, expected] of cases) {
      const result = normalizeQMapToolExecuteArgs('createDatasetFromFilter', {
        datasetName: 'ds',
        fieldName: 'f',
        operator: input,
        value: 1
      });
      expect(result.operator).toBe(expected);
    }
  });

  it('preserves correct flat params without modification', () => {
    const result = normalizeQMapToolExecuteArgs('createDatasetFromFilter', {
      datasetName: 'Province',
      fieldName: 'name',
      operator: 'eq',
      value: 'Trieste'
    });
    expect(result.fieldName).toBe('name');
    expect(result.operator).toBe('eq');
    expect(result.value).toBe('Trieste');
  });

  it('does not apply filter normalization to non-filter tools', () => {
    const result = normalizeQMapToolExecuteArgs('setQMapLayerSolidColor', {
      filters: [{field: 'x'}],
      layerName: 'test'
    });
    expect(Array.isArray(result.filters)).toBe(true);
  });

  // ── Edge cases ──

  it('handles empty tool name', () => {
    const result = normalizeQMapToolExecuteArgs('', {a: 1});
    expect(result).toEqual({a: 1});
  });

  it('does not crash with deeply nested args', () => {
    const result = normalizeQMapToolExecuteArgs('test', {
      level1: {level2: {level3: {level4: 'deep'}}}
    });
    expect((result.level1 as any).level2.level3.level4).toBe('deep');
  });
});

// ─── preprocessFlatFilterToolArgs ───────────────────────────────────────────

describe('preprocessFlatFilterToolArgs', () => {
  it('normalizes filters array to flat params', () => {
    const result = preprocessFlatFilterToolArgs({
      datasetName: 'Province',
      filters: [{field: 'name', op: 'eq', value: 'Trieste'}],
      showOnMap: true,
      newDatasetName: 'Provincia_Piu_Piccola'
    }) as Record<string, unknown>;
    expect(result.fieldName).toBe('name');
    expect(result.operator).toBe('eq');
    expect(result.value).toBe('Trieste');
    expect(result.filters).toBeUndefined();
    expect(result.datasetName).toBe('Province');
    expect(result.showOnMap).toBe(true);
  });

  it('passes through canonical args unchanged', () => {
    const result = preprocessFlatFilterToolArgs({
      datasetName: 'ds',
      fieldName: 'name',
      operator: 'eq',
      value: 'Trieste'
    }) as Record<string, unknown>;
    expect(result.fieldName).toBe('name');
    expect(result.operator).toBe('eq');
    expect(result.value).toBe('Trieste');
  });

  it('returns non-object inputs as-is', () => {
    expect(preprocessFlatFilterToolArgs(null)).toBeNull();
    expect(preprocessFlatFilterToolArgs(undefined)).toBeUndefined();
    expect(preprocessFlatFilterToolArgs('string')).toBe('string');
    expect(preprocessFlatFilterToolArgs(42)).toBe(42);
  });

  it('returns arrays as-is', () => {
    const arr = [1, 2, 3];
    expect(preprocessFlatFilterToolArgs(arr)).toEqual(arr);
  });

  it('normalizes filter object shape', () => {
    const result = preprocessFlatFilterToolArgs({
      filter: {field: 'pop', op: 'gt', value: 1000}
    }) as Record<string, unknown>;
    expect(result.fieldName).toBe('pop');
    expect(result.operator).toBe('gt');
    expect(result.value).toBe(1000);
    expect(result.filter).toBeUndefined();
  });

  it('normalizes operator aliases in preprocessed args', () => {
    const result = preprocessFlatFilterToolArgs({
      fieldName: 'age',
      operator: '>=',
      value: 18
    }) as Record<string, unknown>;
    expect(result.operator).toBe('gte');
  });

  it('does not mutate the original object', () => {
    const original = {fieldName: 'x', operator: '==', value: 1};
    const originalCopy = {...original};
    preprocessFlatFilterToolArgs(original);
    // preprocessFlatFilterToolArgs creates a shallow copy, so original stays unchanged
    expect(original).toEqual(originalCopy);
  });
});
