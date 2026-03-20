import {describe, it, expect} from 'vitest';
import {
  normalizeToolResult,
  isStatelessToolEligibleForDedup,
  extractToolAutoRetryDirective,
  QMAP_TOOL_RESULT_SCHEMA,
  STATELESS_TOOL_DEDUP_ELIGIBLE_TOOLS,
  makeExecutionKey,
  rememberExecutedToolComponentKey,
  rememberExecutedFilterToolSignature,
  EXECUTED_TOOL_COMPONENT_KEYS,
  EXECUTED_FILTER_TOOL_SIGNATURES,
  EXECUTED_TOOL_COMPONENT_KEYS_MAX_SIZE,
  EXECUTED_FILTER_TOOL_SIGNATURES_MAX_SIZE,
  putStatelessToolCacheEntry,
  STATELESS_TOOL_CALL_CACHE_LIMIT
} from '../../../src/features/qmap-ai/tool-result-normalization';

// ─── normalizeToolResult ────────────────────────────────────────────────────

describe('normalizeToolResult', () => {
  it('wraps a successful result with qmapToolResult envelope', () => {
    const result = normalizeToolResult('testTool', {
      llmResult: {success: true, details: 'All good'}
    });
    expect(result.success).toBe(true);
    expect(result.details).toBe('All good');
    expect(result.qmapToolResult.schema).toBe(QMAP_TOOL_RESULT_SCHEMA);
    expect(result.qmapToolResult.toolName).toBe('testTool');
    expect(result.qmapToolResult.success).toBe(true);
    expect(result.qmapToolResult.error).toBeNull();
  });

  it('wraps a failed result with error details', () => {
    const result = normalizeToolResult('failTool', {
      llmResult: {success: false, details: 'Something went wrong'}
    });
    expect(result.success).toBe(false);
    expect(result.details).toBe('Something went wrong');
    expect(result.qmapToolResult.error).toEqual({message: 'Something went wrong'});
    expect(result.qmapToolResult.blockingErrors.length).toBeGreaterThan(0);
  });

  it('handles thrown error by setting success=false', () => {
    // When there is an existing details string in the raw result, the
    // implementation keeps it but forces success=false when thrownError is set.
    const resultWithDetails = normalizeToolResult(
      'errorTool',
      {llmResult: {success: true, details: 'was ok'}},
      new Error('Runtime crash')
    );
    expect(resultWithDetails.success).toBe(false);

    // When no details exist, the error message is used as details
    const resultNoDetails = normalizeToolResult(
      'errorTool',
      {},
      new Error('Runtime crash')
    );
    expect(resultNoDetails.success).toBe(false);
    expect(resultNoDetails.details).toContain('Runtime crash');
  });

  it('provides default details when none supplied and success=true', () => {
    const result = normalizeToolResult('noDetailsTool', {llmResult: {success: true}});
    expect(result.success).toBe(true);
    expect(result.details).toContain('noDetailsTool');
    expect(result.details).toContain('completed');
  });

  it('provides default details when none supplied and success=false', () => {
    const result = normalizeToolResult('noDetailsTool', {llmResult: {success: false}});
    expect(result.success).toBe(false);
    expect(result.details).toContain('noDetailsTool');
    expect(result.details).toContain('failed');
  });

  it('handles null/undefined raw result gracefully', () => {
    const fromNull = normalizeToolResult('nullTool', null);
    expect(fromNull.success).toBe(false);
    expect(fromNull.qmapToolResult.toolName).toBe('nullTool');

    const fromUndefined = normalizeToolResult('undefinedTool', undefined);
    expect(fromUndefined.success).toBe(false);
    expect(fromUndefined.qmapToolResult.toolName).toBe('undefinedTool');
  });

  it('merges warnings from llmResult and qmapToolResult', () => {
    const result = normalizeToolResult('warnTool', {
      llmResult: {success: true, details: 'ok', warnings: ['warn-llm']},
      qmapToolResult: {warnings: ['warn-envelope']}
    });
    expect(result.qmapToolResult.warnings).toContain('warn-llm');
    expect(result.qmapToolResult.warnings).toContain('warn-envelope');
  });

  it('deduplicates warnings', () => {
    const result = normalizeToolResult('dedupTool', {
      llmResult: {success: true, details: 'ok', warnings: ['same']},
      qmapToolResult: {warnings: ['same']}
    });
    const count = result.qmapToolResult.warnings.filter((w: string) => w === 'same').length;
    expect(count).toBe(1);
  });

  it('extracts producedDatasetRefs from llmResult', () => {
    const result = normalizeToolResult('refTool', {
      llmResult: {success: true, details: 'ok', datasetRef: 'myDataset'}
    });
    expect(result.qmapToolResult.producedDatasetRefs.length).toBeGreaterThan(0);
    expect(result.qmapToolResult.producedDatasetRefs).toContain('myDataset');
  });

  it('extracts producedDatasetRefs from datasetId with id: prefix', () => {
    const result = normalizeToolResult('idTool', {
      llmResult: {success: true, details: 'ok', datasetId: 'abc123'}
    });
    expect(result.qmapToolResult.producedDatasetRefs).toContain('id:abc123');
  });

  it('sets objectiveReached to match success by default', () => {
    const successResult = normalizeToolResult('objTool', {
      llmResult: {success: true, details: 'ok'}
    });
    expect(successResult.qmapToolResult.objectiveReached).toBe(true);

    const failResult = normalizeToolResult('objTool', {
      llmResult: {success: false, details: 'fail'}
    });
    expect(failResult.qmapToolResult.objectiveReached).toBe(false);
  });

  it('respects explicit objectiveReached override', () => {
    const result = normalizeToolResult('overrideTool', {
      llmResult: {success: true, details: 'ok', objectiveReached: false}
    });
    expect(result.qmapToolResult.objectiveReached).toBe(false);
  });
});

// ─── isStatelessToolEligibleForDedup ────────────────────────────────────────

describe('isStatelessToolEligibleForDedup', () => {
  it('returns true for eligible tools', () => {
    expect(isStatelessToolEligibleForDedup('listQMapDatasets')).toBe(true);
    expect(isStatelessToolEligibleForDedup('listQCumberProviders')).toBe(true);
    expect(isStatelessToolEligibleForDedup('previewQMapDatasetRows')).toBe(true);
    expect(isStatelessToolEligibleForDedup('countQMapRows')).toBe(true);
  });

  it('returns false for non-eligible tools', () => {
    expect(isStatelessToolEligibleForDedup('clipQMapDatasetByGeometry')).toBe(false);
    expect(isStatelessToolEligibleForDedup('setQMapLayerSolidColor')).toBe(false);
    expect(isStatelessToolEligibleForDedup('waitForQMapDataset')).toBe(false);
  });

  it('returns false for empty or undefined', () => {
    expect(isStatelessToolEligibleForDedup('')).toBe(false);
    expect(isStatelessToolEligibleForDedup(undefined as any)).toBe(false);
  });

  it('trims whitespace', () => {
    expect(isStatelessToolEligibleForDedup('  listQMapDatasets  ')).toBe(true);
  });

  it('the eligible set has expected members', () => {
    expect(STATELESS_TOOL_DEDUP_ELIGIBLE_TOOLS.has('listQMapDatasets')).toBe(true);
    expect(STATELESS_TOOL_DEDUP_ELIGIBLE_TOOLS.has('listQCumberProviders')).toBe(true);
    expect(STATELESS_TOOL_DEDUP_ELIGIBLE_TOOLS.has('listQCumberDatasets')).toBe(true);
    expect(STATELESS_TOOL_DEDUP_ELIGIBLE_TOOLS.has('getQCumberDatasetHelp')).toBe(true);
    expect(STATELESS_TOOL_DEDUP_ELIGIBLE_TOOLS.has('previewQMapDatasetRows')).toBe(true);
    expect(STATELESS_TOOL_DEDUP_ELIGIBLE_TOOLS.has('rankQMapDatasetRows')).toBe(true);
    expect(STATELESS_TOOL_DEDUP_ELIGIBLE_TOOLS.has('distinctQMapFieldValues')).toBe(true);
    expect(STATELESS_TOOL_DEDUP_ELIGIBLE_TOOLS.has('searchQMapFieldValues')).toBe(true);
    expect(STATELESS_TOOL_DEDUP_ELIGIBLE_TOOLS.has('countQMapRows')).toBe(true);
  });
});

// ─── extractToolAutoRetryDirective ──────────────────────────────────────────

describe('extractToolAutoRetryDirective', () => {
  it('returns null when llmResult is missing', () => {
    expect(extractToolAutoRetryDirective(null)).toBeNull();
    expect(extractToolAutoRetryDirective(undefined)).toBeNull();
    expect(extractToolAutoRetryDirective({})).toBeNull();
  });

  it('returns null when success is not false', () => {
    expect(
      extractToolAutoRetryDirective({llmResult: {success: true, retryWithTool: 'foo'}})
    ).toBeNull();
  });

  it('returns null when retryWithTool is missing', () => {
    expect(
      extractToolAutoRetryDirective({llmResult: {success: false, details: 'fail'}})
    ).toBeNull();
  });

  it('extracts retry directive when present', () => {
    const directive = extractToolAutoRetryDirective({
      llmResult: {
        success: false,
        retryWithTool: 'waitForQMapDataset',
        retryWithArgs: {datasetName: 'foo', timeout: 30000},
        retryReason: 'Dataset not ready'
      }
    });
    expect(directive).not.toBeNull();
    expect(directive!.retryToolName).toBe('waitForQMapDataset');
    expect(directive!.retryArgs).toEqual({datasetName: 'foo', timeout: 30000});
    expect(directive!.retryReason).toBe('Dataset not ready');
  });

  it('defaults retryArgs to empty object when not provided', () => {
    const directive = extractToolAutoRetryDirective({
      llmResult: {success: false, retryWithTool: 'retry'}
    });
    expect(directive).not.toBeNull();
    expect(directive!.retryArgs).toEqual({});
  });

  it('defaults retryReason to empty string when not provided', () => {
    const directive = extractToolAutoRetryDirective({
      llmResult: {success: false, retryWithTool: 'retry'}
    });
    expect(directive!.retryReason).toBe('');
  });
});

// ─── makeExecutionKey ───────────────────────────────────────────────────────

describe('makeExecutionKey', () => {
  it('starts with the provided prefix', () => {
    const key = makeExecutionKey('test-prefix');
    expect(key.startsWith('test-prefix:')).toBe(true);
  });

  it('produces unique keys on successive calls', () => {
    const keys = new Set(Array.from({length: 50}, () => makeExecutionKey('x')));
    expect(keys.size).toBe(50);
  });
});

// ─── putStatelessToolCacheEntry ─────────────────────────────────────────────

describe('putStatelessToolCacheEntry', () => {
  it('enforces STATELESS_TOOL_CALL_CACHE_LIMIT', () => {
    const cache = new Map<string, any>();
    for (let i = 0; i < STATELESS_TOOL_CALL_CACHE_LIMIT + 10; i++) {
      putStatelessToolCacheEntry(cache, {
        dedupHash: `hash:${i}`,
        toolName: 'listQMapDatasets',
        toolCallId: `call-${i}`,
        cachedAtMs: i,
        cachedAtRevision: 0,
        hits: 0,
        normalizedResult: {success: true}
      });
    }
    expect(cache.size).toBe(STATELESS_TOOL_CALL_CACHE_LIMIT);
    expect(cache.has('hash:0')).toBe(false);
    expect(cache.has(`hash:${STATELESS_TOOL_CALL_CACHE_LIMIT + 9}`)).toBe(true);
  });
});

// ─── Module-level bounded sets ──────────────────────────────────────────────

describe('module-level bounded sets', () => {
  it('EXECUTED_TOOL_COMPONENT_KEYS_MAX_SIZE is a reasonable value', () => {
    expect(EXECUTED_TOOL_COMPONENT_KEYS_MAX_SIZE).toBeGreaterThanOrEqual(1024);
  });

  it('EXECUTED_FILTER_TOOL_SIGNATURES_MAX_SIZE is a reasonable value', () => {
    expect(EXECUTED_FILTER_TOOL_SIGNATURES_MAX_SIZE).toBeGreaterThanOrEqual(1024);
  });

  it('rememberExecutedToolComponentKey adds to module set', () => {
    const sizeBefore = EXECUTED_TOOL_COMPONENT_KEYS.size;
    const unique = `test-key-${Date.now()}-${Math.random()}`;
    rememberExecutedToolComponentKey(unique);
    expect(EXECUTED_TOOL_COMPONENT_KEYS.has(unique)).toBe(true);
    expect(EXECUTED_TOOL_COMPONENT_KEYS.size).toBe(sizeBefore + 1);
  });

  it('rememberExecutedFilterToolSignature adds to module set', () => {
    const sizeBefore = EXECUTED_FILTER_TOOL_SIGNATURES.size;
    const unique = `test-sig-${Date.now()}-${Math.random()}`;
    rememberExecutedFilterToolSignature(unique);
    expect(EXECUTED_FILTER_TOOL_SIGNATURES.has(unique)).toBe(true);
    expect(EXECUTED_FILTER_TOOL_SIGNATURES.size).toBe(sizeBefore + 1);
  });
});
