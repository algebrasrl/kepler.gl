import {describe, it, expect} from 'vitest';
import {
  classifyToolConcurrency,
  createTurnExecutionState,
  toolRequiresDatasetSnapshot,
  getNextAllowedToolsForPhase,
  extractToolPolicyUserText,
  TURN_STATE_DISCOVERY_GATE_ALLOWLIST,
  TURN_STATE_VALIDATE_GATE_ALLOWLIST
} from '../../../src/features/qmap-ai/guardrails';

// ─── classifyToolConcurrency ────────────────────────────────────────────────

describe('classifyToolConcurrency', () => {
  it('classifies validation tools correctly', () => {
    expect(classifyToolConcurrency('waitForQMapDataset')).toBe('validation');
    expect(classifyToolConcurrency('countQMapRows')).toBe('validation');
    expect(classifyToolConcurrency('debugQMapActiveFilters')).toBe('validation');
    expect(classifyToolConcurrency('listQMapDatasets')).toBe('validation');
  });

  it('classifies mutation tools from contract flags', () => {
    // These tools have mutatesDataset=true in the contract
    expect(classifyToolConcurrency('clipQMapDatasetByGeometry')).toBe('mutation');
    expect(classifyToolConcurrency('overlayIntersection')).toBe('mutation');
    expect(classifyToolConcurrency('tassellateSelectedGeometry')).toBe('mutation');
  });

  it('classifies q-cumber query tools as mutation (loadToMap side-effect)', () => {
    expect(classifyToolConcurrency('queryQCumberTerritorialUnits')).toBe('mutation');
    expect(classifyToolConcurrency('queryQCumberDataset')).toBe('mutation');
    expect(classifyToolConcurrency('queryQCumberDatasetSpatial')).toBe('mutation');
  });

  it('classifies read-only tools as read', () => {
    expect(classifyToolConcurrency('previewQMapDatasetRows')).toBe('read');
    expect(classifyToolConcurrency('distinctQMapFieldValues')).toBe('read');
  });

  it('returns read for unknown tools (no contract match)', () => {
    expect(classifyToolConcurrency('nonExistentTool')).toBe('read');
  });
});

// ─── createTurnExecutionState ───────────────────────────────────────────────

describe('createTurnExecutionState', () => {
  it('returns initial state with discover phase', () => {
    const state = createTurnExecutionState();
    expect(state.phase).toBe('discover');
    expect(state.snapshotTakenAt).toBe(0);
    expect(state.snapshotDatasetRefs.size).toBe(0);
  });

  it('returns a new instance on each call', () => {
    const a = createTurnExecutionState();
    const b = createTurnExecutionState();
    expect(a).not.toBe(b);
    expect(a.snapshotDatasetRefs).not.toBe(b.snapshotDatasetRefs);
  });
});

// ─── toolRequiresDatasetSnapshot ────────────────────────────────────────────

describe('toolRequiresDatasetSnapshot', () => {
  it('returns false for discovery tools', () => {
    expect(toolRequiresDatasetSnapshot('listQMapDatasets')).toBe(false);
    expect(toolRequiresDatasetSnapshot('listQMapToolCategories')).toBe(false);
    expect(toolRequiresDatasetSnapshot('loadData')).toBe(false);
    expect(toolRequiresDatasetSnapshot('basemap')).toBe(false);
  });

  it('returns false for q-cumber catalog tools', () => {
    expect(toolRequiresDatasetSnapshot('listQCumberProviders')).toBe(false);
    expect(toolRequiresDatasetSnapshot('listQCumberDatasets')).toBe(false);
    expect(toolRequiresDatasetSnapshot('getQCumberDatasetHelp')).toBe(false);
  });

  it('returns true for mutation/analysis tools', () => {
    expect(toolRequiresDatasetSnapshot('clipQMapDatasetByGeometry')).toBe(true);
    expect(toolRequiresDatasetSnapshot('setQMapLayerSolidColor')).toBe(true);
    expect(toolRequiresDatasetSnapshot('previewQMapDatasetRows')).toBe(true);
  });

  it('handles whitespace in tool name', () => {
    expect(toolRequiresDatasetSnapshot('  listQMapDatasets  ')).toBe(false);
  });

  it('returns true for empty/unknown tool name', () => {
    expect(toolRequiresDatasetSnapshot('')).toBe(true);
    expect(toolRequiresDatasetSnapshot('nonExistentTool')).toBe(true);
  });
});

// ─── getNextAllowedToolsForPhase ────────────────────────────────────────────

describe('getNextAllowedToolsForPhase', () => {
  it('returns discovery tools for discover phase', () => {
    const tools = getNextAllowedToolsForPhase('discover');
    expect(tools.length).toBeGreaterThan(0);
    expect(tools).toContain('listQMapDatasets');
    expect(tools).toContain('listQMapToolCategories');
    // The list should match the TURN_STATE_DISCOVERY_GATE_ALLOWLIST
    for (const tool of tools) {
      expect(TURN_STATE_DISCOVERY_GATE_ALLOWLIST.has(tool)).toBe(true);
    }
  });

  it('returns validate tools for validate phase', () => {
    const tools = getNextAllowedToolsForPhase('validate');
    expect(tools.length).toBeGreaterThan(0);
    expect(tools).toContain('waitForQMapDataset');
    expect(tools).toContain('countQMapRows');
    for (const tool of tools) {
      expect(TURN_STATE_VALIDATE_GATE_ALLOWLIST.has(tool)).toBe(true);
    }
  });

  it('returns empty array for execute and finalize phases', () => {
    expect(getNextAllowedToolsForPhase('execute')).toEqual([]);
    expect(getNextAllowedToolsForPhase('finalize')).toEqual([]);
  });
});

// ─── extractToolPolicyUserText ──────────────────────────────────────────────

describe('extractToolPolicyUserText', () => {
  it('extracts text from singular message (OpenAssistant format)', () => {
    const text = extractToolPolicyUserText({
      message: {role: 'user', content: 'show me Italian regions'}
    });
    expect(text).toBe('show me Italian regions');
  });

  it('extracts text from messages array (OpenAI format) — last message is user', () => {
    const text = extractToolPolicyUserText({
      messages: [
        {role: 'assistant', content: 'Hello'},
        {role: 'user', content: 'show provinces of Veneto'}
      ]
    });
    expect(text).toBe('show provinces of Veneto');
  });

  it('returns empty string when last message is assistant', () => {
    const text = extractToolPolicyUserText({
      messages: [
        {role: 'user', content: 'show me data'},
        {role: 'assistant', content: 'Here is the result'}
      ]
    });
    expect(text).toBe('');
  });

  it('returns empty string when last message is tool', () => {
    const text = extractToolPolicyUserText({
      messages: [
        {role: 'user', content: 'query'},
        {role: 'tool', content: '{"result": "ok"}'}
      ]
    });
    expect(text).toBe('');
  });

  it('handles array-type content blocks', () => {
    const text = extractToolPolicyUserText({
      message: {
        role: 'user',
        content: [{text: 'Part A'}, {text: 'Part B'}]
      }
    });
    expect(text).toBe('Part A Part B');
  });

  it('returns empty for null/undefined/array input', () => {
    expect(extractToolPolicyUserText(null)).toBe('');
    expect(extractToolPolicyUserText(undefined)).toBe('');
    expect(extractToolPolicyUserText([])).toBe('');
    expect(extractToolPolicyUserText('string')).toBe('');
  });

  it('returns empty for empty messages array', () => {
    expect(extractToolPolicyUserText({messages: []})).toBe('');
  });

  it('returns empty for missing content', () => {
    expect(extractToolPolicyUserText({message: {role: 'user'}})).toBe('');
  });
});
