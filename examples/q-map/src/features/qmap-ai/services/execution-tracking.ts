/**
 * Barrel re-export for execution tracking modules.
 *
 * Actual implementations:
 *   - services/post-validation.ts — mutation tool set, dataset name resolution
 *   - services/tool-component-runtime.ts — skip/complete guards for React components
 *   - services/execution-trace.ts — invocation summaries, stats, text analysis
 */
export {
  DATASET_VALIDATION_MUTATING_TOOLS,
  shouldRunDatasetPostValidation,
  resolveDatasetNameForPostValidation,
  resolveValidationTimeoutMs
} from './post-validation';

export {
  type ToolComponentGuardDeps,
  shouldSkipToolComponentRun,
  markToolComponentRunCompleted,
  shouldSkipToolComponentByExecutionKey,
  rememberToolComponentExecutionKey
} from './tool-component-runtime';

export {
  type QMapInvocationResultSummary,
  type QMapAssistantExecutionStats,
  getToolResultSummary,
  extractInvocationResultSummaries,
  computeAssistantExecutionStats,
  buildExecutionSummaryLine,
  extractSubRequestIdsFromText,
  stripRuntimeDiagnosticLines,
  textIsRuntimeDiagnosticOnly,
  collapseRepeatedNarrativeBlocks,
  countSuccessfulStyleRuns,
  countFailedStyleRuns,
  textClaimsCentering,
  textClaimsStyling,
  textRequestsStylingObjective,
  stripUnverifiedCenteringClaimLines,
  stripUnverifiedStylingClaimLines,
  textClaimsWorkflowCompleted,
  textAcknowledgesNonSuccessOutcome,
  stripContradictoryNonSuccessClaimLines,
  stripUnverifiedCompletionClaimLines
} from './execution-trace';
