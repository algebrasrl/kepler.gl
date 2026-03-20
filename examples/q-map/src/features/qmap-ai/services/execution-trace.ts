/**
 * Execution trace: invocation summaries, stats, guardrail text analysis.
 */
import {DATASET_VALIDATION_MUTATING_TOOLS, firstNonEmptyString} from './post-validation';


export type QMapInvocationResultSummary = {
  toolName: string;
  success: boolean | null;
  details: string;
};

export type QMapAssistantExecutionStats = {
  total: number;
  completed: number;
  failed: number;
  skipped: number;
  blocked: number;
  validationFailed: number;
  fitAttempted: number;
  fitSuccess: number;
  fitFailed: number;
  mutationSuccess: number;
  mutationFailed: number;
  status: 'idle' | 'success' | 'partial' | 'failed';
};

const REQUEST_ID_LINE_RE = /^\s*\[requestId:\s*([^\]]+)\]\s*$/i;
const DIAGNOSTIC_PREFIX_RE = /^\s*\[(requestId|progress|validation|executionSummary|guardrail|subRequestIds)\b/i;

const STYLE_RESULT_TOOL_NAMES = new Set([
  'setQMapLayerColorByField',
  'setQMapLayerColorByThresholds',
  'setQMapLayerColorByStatsThresholds',
  'setQMapLayerSolidColor'
]);

export function getToolResultSummary(result: any): any {
  if (result?.llmResult && typeof result.llmResult === 'object') {
    return result.llmResult;
  }
  if (result?.qmapToolResult && typeof result.qmapToolResult === 'object') {
    return result.qmapToolResult;
  }
  return null;
}

export function extractInvocationResultSummaries(parts: any[]): QMapInvocationResultSummary[] {
  if (!Array.isArray(parts)) return [];
  return parts
    .filter((part: any) => part?.type === 'tool-invocation' && part?.toolInvocation?.state === 'result')
    .map((part: any) => {
      const invocation = part?.toolInvocation || {};
      const toolName = firstNonEmptyString(
        invocation?.toolName,
        invocation?.name,
        invocation?.toolCall?.function?.name,
        invocation?.toolCall?.name
      );
      if (!toolName) return null;
      const rawResult = invocation?.result;
      const summary = getToolResultSummary(rawResult) || {};
      const success =
        typeof summary?.success === 'boolean'
          ? summary.success
          : typeof rawResult?.success === 'boolean'
          ? rawResult.success
          : typeof rawResult?.llmResult?.success === 'boolean'
          ? rawResult.llmResult.success
          : null;
      const blockingErrors = Array.isArray((summary as any)?.blockingErrors)
        ? ((summary as any).blockingErrors as unknown[])
            .map(value => String(value || '').trim())
            .filter(Boolean)
        : [];
      const details = firstNonEmptyString(summary?.details, rawResult?.details, rawResult?.llmResult?.details);
      const resolvedDetails = success === false && blockingErrors.length > 0 ? blockingErrors[0] : details;
      return {toolName, success, details: resolvedDetails};
    })
    .filter(Boolean) as QMapInvocationResultSummary[];
}

export function computeAssistantExecutionStats({
  runs,
  totalToolCalls,
  validationFailures
}: {
  runs: QMapInvocationResultSummary[];
  totalToolCalls: number;
  validationFailures: number;
}): QMapAssistantExecutionStats {
  const completed = runs.length;
  const isBatchSkipped = (run: QMapInvocationResultSummary) => {
    const d = String(run.details || '').toLowerCase();
    return d.includes('skipped') && d.includes('batch');
  };
  const skipped = runs.filter(run => run.success === false && isBatchSkipped(run)).length;
  const failed = runs.filter(run => run.success === false && !isBatchSkipped(run)).length;
  const blocked = runs.filter(
    run => run.success === false && !isBatchSkipped(run) && String(run.details || '').toLowerCase().includes('blocked')
  ).length;
  const fitRuns = runs.filter(run => run.toolName === 'fitQMapToDataset');
  const fitSuccess = fitRuns.filter(run => run.success === true).length;
  const fitFailed = fitRuns.filter(run => run.success === false).length;
  const mutationRuns = runs.filter(run => DATASET_VALIDATION_MUTATING_TOOLS.has(String(run.toolName || '').trim()));
  const mutationSuccess = mutationRuns.filter(run => run.success === true).length;
  const mutationFailed = mutationRuns.filter(run => run.success === false).length;
  const status: QMapAssistantExecutionStats['status'] =
    completed === 0 ? 'idle' : failed === 0 ? 'success' : failed >= completed ? 'failed' : 'partial';
  return {
    total: Math.max(0, Number(totalToolCalls || 0)),
    completed,
    failed,
    skipped,
    blocked,
    validationFailed: Math.max(0, Number(validationFailures || 0)),
    fitAttempted: fitRuns.length,
    fitSuccess,
    fitFailed,
    mutationSuccess,
    mutationFailed,
    status
  };
}

export function buildExecutionSummaryLine(
  requestId: string,
  stats: QMapAssistantExecutionStats,
  subRequestIds: string[] = [],
  chatId = ''
): string {
  if (!requestId) return '';
  const normalizedChatId = String(chatId || '').trim();
  const normalizedSubRequestIds = Array.from(
    new Set(
      (Array.isArray(subRequestIds) ? subRequestIds : [])
        .map(value => String(value || '').trim())
        .filter(Boolean)
        .filter(value => value !== requestId)
    )
  );
  return `[executionSummary] ${JSON.stringify({
    requestId,
    ...(normalizedChatId ? {chatId: normalizedChatId} : {}),
    ...(normalizedSubRequestIds.length > 0 ? {subRequestIds: normalizedSubRequestIds} : {}),
    status: stats.status,
    steps: {
      total: stats.total,
      completed: stats.completed,
      failed: stats.failed,
      skipped: stats.skipped,
      blocked: stats.blocked
    },
    validationFailed: stats.validationFailed,
    fit: {
      attempted: stats.fitAttempted,
      success: stats.fitSuccess,
      failed: stats.fitFailed
    },
    mutations: {
      success: stats.mutationSuccess,
      failed: stats.mutationFailed
    }
  })}`;
}

export function extractSubRequestIdsFromText(text: string, turnRequestId: string): string[] {
  const turnId = String(turnRequestId || '').trim();
  const ids = String(text || '')
    .split('\n')
    .map(line => {
      const match = line.match(REQUEST_ID_LINE_RE);
      if (!match) return '';
      return String(match[1] || '').trim();
    })
    .filter(Boolean)
    .filter(id => id !== turnId);
  return Array.from(new Set(ids));
}

export function stripRuntimeDiagnosticLines(text: string): string {
  return String(text || '')
    .split('\n')
    .filter(line => !DIAGNOSTIC_PREFIX_RE.test(String(line || '').trim()))
    .join('\n')
    .trim();
}

export function textIsRuntimeDiagnosticOnly(text: string): boolean {
  const raw = String(text || '');
  if (!raw.trim()) return false;
  const lines = raw.split('\n').map(line => String(line || '').trim());
  const hasDiagnosticLine = lines.some(line => DIAGNOSTIC_PREFIX_RE.test(line));
  if (!hasDiagnosticLine) return false;
  return !stripRuntimeDiagnosticLines(raw);
}

function normalizeComparableText(text: string): string {
  return String(text || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
}

export function collapseRepeatedNarrativeBlocks(text: string): string {
  const raw = String(text || '');
  if (!raw.trim()) return '';

  const dedupedLines: string[] = [];
  let previousNormalizedLine = '';
  for (const line of raw.split('\n')) {
    const normalizedLine = normalizeComparableText(line);
    if (!normalizedLine) {
      dedupedLines.push('');
      previousNormalizedLine = '';
      continue;
    }
    if (normalizedLine === previousNormalizedLine) {
      continue;
    }
    dedupedLines.push(String(line || '').trimEnd());
    previousNormalizedLine = normalizedLine;
  }

  const compactLinesText = dedupedLines.join('\n').replace(/\n{3,}/g, '\n\n').trim();
  if (!compactLinesText) return '';

  const paragraphs = compactLinesText
    .split(/\n{2,}/)
    .map(paragraph => paragraph.trim())
    .filter(Boolean);
  if (paragraphs.length <= 1) return compactLinesText;

  const dedupedParagraphs: string[] = [];
  let previousNormalizedParagraph = '';
  for (const paragraph of paragraphs) {
    const normalizedParagraph = normalizeComparableText(paragraph.replace(/\n+/g, ' '));
    if (!normalizedParagraph) continue;
    if (normalizedParagraph === previousNormalizedParagraph) {
      continue;
    }
    dedupedParagraphs.push(paragraph);
    previousNormalizedParagraph = normalizedParagraph;
  }
  return dedupedParagraphs.join('\n\n').trim();
}

function isStyleResultRun(run: QMapInvocationResultSummary): boolean {
  const toolName = String(run?.toolName || '').trim();
  return STYLE_RESULT_TOOL_NAMES.has(toolName);
}

export function countSuccessfulStyleRuns(runs: QMapInvocationResultSummary[]): number {
  return runs.filter(run => isStyleResultRun(run) && run.success === true).length;
}

export function countFailedStyleRuns(runs: QMapInvocationResultSummary[]): number {
  return runs.filter(run => isStyleResultRun(run) && run.success === false).length;
}

export function textClaimsCentering(text: string): boolean {
  const normalized = String(text || '').trim();
  if (!normalized) return false;
  const hasPositiveCentering = /(\bcentrat\w*\b|\binquadr\w*\b|\bzoom\w*\b|\bmap\s+centered\b|\bmap\s+centred\b)/i.test(
    normalized
  );
  if (!hasPositiveCentering) return false;
  const hasNegativeCentering = /(\bnon\b[^\n.]{0,36}(centrat|inquadr|zoom|fit)|centratura non confermata|not centered)/i.test(
    normalized
  );
  return !hasNegativeCentering;
}

export function textClaimsStyling(text: string): boolean {
  const normalized = String(text || '').trim();
  if (!normalized) return false;
  const hasStyleClaim =
    /(\bstile\s*:|\bcolorazion\w*\b|\bcoroplet\w*\b|\bpalette\b|\bquantil\w*\b|\bquantize\b|\bcolor\s+scale\b|\bscala\s+(color|cromatic|log))/i.test(
      normalized
    ) && /(\bapplicat\w*\b|\bimpostat\w*\b|\baggiornat\w*\b|\bsettat\w*\b|\bconfigurat\w*\b|\bstile\s*:)/i.test(normalized);
  if (!hasStyleClaim) return false;
  const hasNegativeClaim =
    /(\bnon\b[^\n.]{0,42}(stile|color|quantil|palette)|\bfallit\w*\b[^\n.]{0,42}(stile|color|quantil)|not applied)/i.test(
      normalized
    );
  return !hasNegativeClaim;
}

export function textRequestsStylingObjective(text: string): boolean {
  const normalized = String(text || '').trim();
  if (!normalized) return false;
  return /(\bcolora\w*\b|\bpalette\b|\bcoroplet\w*\b|\bstile\b|\bscala\s+(color|cromatic)|\bcolor\s+scale\b|\bchoropleth\b)/i.test(
    normalized
  );
}

export function stripUnverifiedCenteringClaimLines(text: string): string {
  return String(text || '')
    .split('\n')
    .filter(line => {
      const raw = String(line || '').trim();
      if (!raw) return true;
      if (/\b(non|impossibile|non conferm|not)\b/i.test(raw)) return true;
      return !/(\bcentrat\w*\b|\binquadr\w*\b|\bzoom\w*\b|\bmap\s+centered\b|\bmap\s+centred\b)/i.test(raw);
    })
    .join('\n')
    .trim();
}

export function stripUnverifiedStylingClaimLines(text: string): string {
  return String(text || '')
    .split('\n')
    .filter(line => {
      const raw = String(line || '').trim();
      if (!raw) return true;
      if (/\b(non|impossibile|non conferm|not|failed|fallit)\b/i.test(raw)) return true;
      const hasStyleClaim = /(\bstile\s*:|\bcolorazion\w*\b|\bcoroplet\w*\b|\bpalette\b|\bquantil\w*\b|\bquantize\b|\bcolor\s+scale\b)/i.test(
        raw
      );
      if (!hasStyleClaim) return true;
      return !/(\bapplicat\w*\b|\bimpostat\w*\b|\baggiornat\w*\b|\bsettat\w*\b|\bconfigurat\w*\b|\bstile\s*:)/i.test(
        raw
      );
    })
    .join('\n')
    .trim();
}

export function textClaimsWorkflowCompleted(text: string): boolean {
  const normalized = String(text || '').trim();
  if (!normalized) return false;
  const hasPositiveCompletion = /(\bho\s+completat\w*\b|\boperazion\w*\s+completat\w*\b|\bworkflow\s+completat\w*\b|\banalisi\s+completat\w*\b)/i.test(
    normalized
  );
  if (!hasPositiveCompletion) return false;
  const hasNegativeCompletion = /(\bnon\b[^\n.]{0,42}(completat|riuscit)|\bparzial\w*\b|\bincomplet\w*\b|\bfailed\b)/i.test(
    normalized
  );
  return !hasNegativeCompletion;
}

export function textAcknowledgesNonSuccessOutcome(text: string): boolean {
  const normalized = String(text || '').trim();
  if (!normalized) return false;
  return /(\bworkflow\s+completat\w*\s+parzial\w*\b|\bworkflow\s+non\s+completat\w*\b|\bparzial\w*\b|\bnon\s+completat\w*\b|\bfallit\w*\b|\bfailed\b|\berror\w*\b|\berrore\w*\b|\bnon\s+riuscit\w*\b|\blimite\s+rilevat\w*\b)/i.test(
    normalized
  );
}

function lineHasStrongSuccessClaim(raw: string): boolean {
  if (!raw) return false;
  return /(\bcon\s+successo\b|\b(?:ho|abbiamo)\s+completat\w*\b|\b(?:operazion\w*|workflow|analisi)\s+completat\w*\b|\b(?:e|è|sono)\s+stat\w*[^\n.]{0,64}\b(?:tassellat\w*|colorat\w*|aggiornat\w*|caricat\w*|visualizzat\w*|mostrat\w*|applicat\w*)\b|\b(?:mappa|dataset|celle|layer|provincia|comuni?)\b[^\n.]{0,96}\b(?:tassellat\w*|colorat\w*|aggiornat\w*|caricat\w*|visualizzat\w*|mostrat\w*|applicat\w*)\b)/i.test(
    raw
  );
}

export function stripContradictoryNonSuccessClaimLines(text: string): string {
  return String(text || '')
    .split('\n')
    .filter(line => {
      const raw = String(line || '').trim();
      if (!raw) return true;
      if (textAcknowledgesNonSuccessOutcome(raw)) return true;
      return !lineHasStrongSuccessClaim(raw);
    })
    .join('\n')
    .trim();
}

export function stripUnverifiedCompletionClaimLines(text: string): string {
  return String(text || '')
    .split('\n')
    .filter(line => {
      const raw = String(line || '').trim();
      if (!raw) return true;
      if (/\b(non|parzial|incomplet|failed|fallit)\b/i.test(raw)) return true;
      return !/(\bho\s+completat\w*\b|\boperazion\w*\s+completat\w*\b|\bworkflow\s+completat\w*\b|\banalisi\s+completat\w*\b)/i.test(
        raw
      );
    })
    .join('\n')
    .trim();
}
