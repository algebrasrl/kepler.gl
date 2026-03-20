/**
 * Barrel re-export for service modules.
 *
 * Actual implementations:
 *   - services/execution-tracking.ts — barrel for post-validation, tool-component-runtime, execution-trace
 *   - services/qcumber-api.ts — q-cumber proxy client
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

export {
  qcumberListProviders,
  qcumberListDatasets,
  qcumberGetDatasetHelp,
  qcumberQuery
} from './qcumber-api';
