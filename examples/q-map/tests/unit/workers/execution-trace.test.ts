import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildExecutionSummaryLine,
  collapseRepeatedNarrativeBlocks,
  extractInvocationResultSummaries,
  extractSubRequestIdsFromText,
  stripRuntimeDiagnosticLines,
  stripContradictoryNonSuccessClaimLines,
  stripUnverifiedCompletionClaimLines,
  textAcknowledgesNonSuccessOutcome,
  textIsRuntimeDiagnosticOnly,
  textRequestsStylingObjective,
  textClaimsWorkflowCompleted,
  type QMapAssistantExecutionStats
} from '../../../src/features/qmap-ai/services/execution-tracking';

function baseStats(overrides: Partial<QMapAssistantExecutionStats> = {}): QMapAssistantExecutionStats {
  return {
    total: 4,
    completed: 4,
    failed: 0,
    blocked: 0,
    validationFailed: 0,
    fitAttempted: 1,
    fitSuccess: 1,
    fitFailed: 0,
    mutationSuccess: 1,
    mutationFailed: 0,
    status: 'success',
    ...overrides
  };
}

test('extractSubRequestIdsFromText keeps only ids different from current request id', () => {
  const text = [
    '[requestId: turn-1]',
    '[requestId: sub-1]',
    '[requestId: sub-2]',
    '[requestId: sub-1]',
    'Ho completato la richiesta.'
  ].join('\n');
  const out = extractSubRequestIdsFromText(text, 'turn-1');
  assert.deepEqual(out, ['sub-1', 'sub-2']);
});

test('stripRuntimeDiagnosticLines removes diagnostic envelope lines from body text', () => {
  const text = [
    '[requestId: turn-1]',
    '[progress] steps=2/3',
    '[validation] failed=1',
    '[executionSummary] {"requestId":"turn-1"}',
    '[guardrail] completion_claim_blocked',
    '[subRequestIds] sub-1,sub-2',
    'Contenuto finale.'
  ].join('\n');
  const out = stripRuntimeDiagnosticLines(text);
  assert.equal(out, 'Contenuto finale.');
});

test('textIsRuntimeDiagnosticOnly detects marker-only payloads', () => {
  const diagnosticOnly = ['[requestId: turn-1]', '[progress] steps=2/2', '[executionSummary] {"status":"success"}'].join(
    '\n'
  );
  assert.equal(textIsRuntimeDiagnosticOnly(diagnosticOnly), true);
  assert.equal(textIsRuntimeDiagnosticOnly('[requestId: turn-1]\nContenuto finale.'), false);
});

test('textRequestsStylingObjective detects style/color intents in user objectives', () => {
  assert.equal(textRequestsStylingObjective('colorami la tassellazione h3 ris 8 di treviso'), true);
  assert.equal(textRequestsStylingObjective('cambia palette in viridis'), true);
  assert.equal(textRequestsStylingObjective('mostrami i comuni del veneto'), false);
});

test('buildExecutionSummaryLine exposes subRequestIds when available', () => {
  const summaryLine = buildExecutionSummaryLine('turn-1', baseStats({status: 'partial', failed: 1}), [
    'turn-1',
    'sub-1',
    'sub-2',
    'sub-1'
  ]);
  assert.ok(summaryLine.startsWith('[executionSummary] '));
  const payload = JSON.parse(summaryLine.replace('[executionSummary] ', ''));
  assert.equal(payload.requestId, 'turn-1');
  assert.equal(payload.status, 'partial');
  assert.deepEqual(payload.subRequestIds, ['sub-1', 'sub-2']);
});

test('buildExecutionSummaryLine includes chatId when available', () => {
  const summaryLine = buildExecutionSummaryLine('turn-2', baseStats(), [], 'chat-session-abc');
  const payload = JSON.parse(summaryLine.replace('[executionSummary] ', ''));
  assert.equal(payload.requestId, 'turn-2');
  assert.equal(payload.chatId, 'chat-session-abc');
});

test('completion-claim helpers detect and strip unverifiable completion lines', () => {
  const text = 'Ho completato l\'analisi della copertura boschiva.\nDettagli: workflow in corso.';
  assert.equal(textClaimsWorkflowCompleted(text), true);
  const stripped = stripUnverifiedCompletionClaimLines(text);
  assert.equal(stripped, 'Dettagli: workflow in corso.');
});

test('textAcknowledgesNonSuccessOutcome detects partial/failure narratives', () => {
  assert.equal(
    textAcknowledgesNonSuccessOutcome('Workflow completato parzialmente: alcuni passaggi non sono riusciti.'),
    true
  );
  assert.equal(
    textAcknowledgesNonSuccessOutcome('Error: impossible to complete all steps due to resource limit.'),
    true
  );
  assert.equal(textAcknowledgesNonSuccessOutcome('I confini di Treviso sono stati caricati e la mappa e stata centrata.'), false);
});

test('stripContradictoryNonSuccessClaimLines removes strong success claims but keeps stats', () => {
  const text = [
    'La provincia di Brescia e stata tassellata con successo.',
    'Le celle H3 sono state colorate in base alla popolazione dei comuni.',
    '',
    'Statistiche:',
    'Celle H3 generate: 6801',
    'Risoluzione: 8'
  ].join('\n');
  const stripped = stripContradictoryNonSuccessClaimLines(text);
  assert.ok(!/tassellata con successo/i.test(stripped));
  assert.ok(!/sono state colorate/i.test(stripped));
  assert.match(stripped, /Statistiche:/);
  assert.match(stripped, /Celle H3 generate: 6801/);
});

test('stripContradictoryNonSuccessClaimLines keeps non-success explanations', () => {
  const text = 'Workflow completato parzialmente: un passaggio e fallito.';
  const stripped = stripContradictoryNonSuccessClaimLines(text);
  assert.equal(stripped, text);
});

test('extractInvocationResultSummaries prefers blockingErrors details on failed tools', () => {
  const parts = [
    {
      type: 'tool-invocation',
      toolInvocation: {
        state: 'result',
        toolName: 'populateTassellationFromAdminUnits',
        result: {
          qmapToolResult: {
            success: false,
            details: 'generic failure detail',
            blockingErrors: ['dataset id:abc not materialized yet']
          }
        }
      }
    }
  ];
  const summaries = extractInvocationResultSummaries(parts as any[]);
  assert.equal(summaries.length, 1);
  assert.equal(summaries[0].toolName, 'populateTassellationFromAdminUnits');
  assert.equal(summaries[0].success, false);
  assert.equal(summaries[0].details, 'dataset id:abc not materialized yet');
});

test('collapseRepeatedNarrativeBlocks removes repeated consecutive lines', () => {
  const text = ['Inizio con la risoluzione della provincia di Treviso.', 'Inizio con la risoluzione della provincia di Treviso.'].join(
    '\n'
  );
  const out = collapseRepeatedNarrativeBlocks(text);
  const lines = out.split('\n').map(line => line.trim()).filter(Boolean);
  assert.equal(lines.length, 1);
  assert.equal(lines[0], 'Inizio con la risoluzione della provincia di Treviso.');
});

test('collapseRepeatedNarrativeBlocks removes repeated consecutive paragraphs', () => {
  const paragraph = [': 1->2->3->4->5', 'Risoluzione provincia di Treviso (lv=7)', 'Inizio con la risoluzione della provincia di Treviso.'].join(
    '\n'
  );
  const text = [paragraph, paragraph, 'Risultato finale disponibile.'].join('\n\n');
  const out = collapseRepeatedNarrativeBlocks(text);
  const repeatedHeaderCount = (out.match(/: 1->2->3->4->5/g) || []).length;
  assert.equal(repeatedHeaderCount, 1);
  assert.match(out, /Risultato finale disponibile\./);
});
