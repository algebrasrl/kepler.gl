/**
 * 5-stage tool execution pipeline.
 *
 * Replaces wrapToolsWithNormalizedResultEnvelope (607 LOC, 10 interleaved passes)
 * with a linear pipeline:
 *   1. preprocess — arg normalization (via z.preprocess on each tool schema)
 *   2. policy gate — contract check + phase gate + unknown args
 *   3. dedup — stateless/mutation/non-actionable caches
 *   4. execute — concurrency control + actual tool execution + auto-retry
 *   5. postprocess — result normalization + cache persistence + lineage + phase metadata
 */
import {normalizeQMapToolExecuteArgs} from '../tool-args-normalization';
import {
  makeExecutionKey,
  normalizeToolResult,
  extractToolAutoRetryDirective
} from '../tool-result-normalization';
import {classifyToolConcurrency, getNextAllowedToolsForPhase} from '../guardrails';
import {
  resolveDatasetNameForPostValidation,
  shouldRunDatasetPostValidation
} from '../services/execution-tracking';
import {normalizeToolDetails} from '../dataset-utils';
import {runPolicyGate, type PolicyGateInput} from './policy-gate';
import {checkDedupCaches, persistToCache} from './dedup-cache';
import type {
  WrapToolRuntimeOptions,
  QMapToolExecutionEvent
} from '../tool-schema-utils';

export type ToolPipelineOptions = WrapToolRuntimeOptions;

/** Max calls per tool name per turn before the circuit breaker blocks execution. */
const TOOL_CALL_CIRCUIT_BREAKER_MAX = 3;
/** Max total tool calls per turn (all tools combined) before hard abort. */
const TOOL_CALL_TURN_HARD_CAP = 15;
/** Max tool calls per single LLM response (batch cap). Allows legitimate batches
 *  (e.g. showOnly + fit + tooltip = 3) but blocks pathological ones (4+ wait, 16 count). */
const TOOL_CALL_RESPONSE_BATCH_CAP = 3;

/**
 * Wraps a tool registry through the 5-stage pipeline.
 * Returns a new registry where each tool.execute() is wrapped.
 */
export function wrapToolsWithPipeline(
  tools: Record<string, any>,
  options: ToolPipelineOptions = {}
): Record<string, any> {
  const rawTools = tools || {};
  const {
    shouldAllowTool,
    onToolEvent,
    resolveCanonicalDatasetRef,
    resolveFallbackDatasetRef,
    onNormalizedToolResult,
    mutationIdempotencyCache,
    nonActionableFailureCache,
    statelessToolCallCache,
    mutationRevisionRef,
    turnExecutionStateRef,
    mutationMutex,
    toolCallCounter,
    responseBatchTracker
  } = options;

  return Object.fromEntries(
    Object.entries(tools).map(([toolName, tool]) => {
      if (!tool || typeof tool !== 'object' || typeof tool.execute !== 'function') {
        return [toolName, tool];
      }
      const originalExecute = tool.execute.bind(tool);

      return [
        toolName,
        {
          ...tool,
          execute: async (args: any, context: any) => {
            // ─── Shared state ─────────────────────────────────────────────
            const retryDepth = Math.max(0, Number(context?.__qmapAutoRetryDepth || 0));
            const toolCallId = String(context?.toolCallId || makeExecutionKey(`tool:${toolName}`));
            const isInternalValidationRun = Boolean(context?.__qmapInternalValidation);
            const safeArgs =
              args && typeof args === 'object' && !Array.isArray(args)
                ? (args as Record<string, unknown>)
                : {};
            const safeContext =
              context && typeof context === 'object' && !Array.isArray(context)
                ? (context as Record<string, unknown>)
                : {};
            const bypassStateMachine = Boolean((safeContext as any)?.__qmapBypassTurnStateMachine);
            const currentMutationRevision = Math.max(0, Number(mutationRevisionRef?.current || 0));

            // ─── Stage 1: preprocess (arg normalization) ──────────────────
            const normalizedArgs = normalizeQMapToolExecuteArgs(toolName, safeArgs, {
              resolveCanonicalDatasetRef,
              resolveFallbackDatasetRef
            });

            // ─── Stage 2: policy gate ─────────────────────────────────────
            const policyResult = runPolicyGate({
              toolName,
              normalizedArgs,
              context: safeContext,
              isInternalValidationRun,
              shouldAllowTool
            });

            if (!policyResult.allow) {
              if (!isInternalValidationRun) {
                onToolEvent?.({
                  phase: 'blocked',
                  toolName,
                  toolCallId,
                  success: false,
                  details: policyResult.result.details
                });
              }
              emitNormalizedResult(onNormalizedToolResult, toolName, policyResult.result);
              return finalizeResult(policyResult.result, turnExecutionStateRef, toolName);
            }

            // ─── Stage 3: dedup cache ─────────────────────────────────────
            const dedupResult = checkDedupCaches({
              toolName,
              normalizedArgs,
              toolCallId,
              isInternalValidationRun,
              bypassStateMachine,
              currentMutationRevision,
              mutationIdempotencyCache,
              nonActionableFailureCache,
              statelessToolCallCache
            });

            if (dedupResult.hit) {
              if (!isInternalValidationRun) {
                onToolEvent?.({
                  phase: 'finish',
                  toolName,
                  toolCallId,
                  success: Boolean(dedupResult.result.success),
                  details: dedupResult.result.details,
                  requiresDatasetValidation: false
                });
              }
              emitNormalizedResult(onNormalizedToolResult, toolName, dedupResult.result);
              return finalizeResult(dedupResult.result, turnExecutionStateRef, toolName);
            }

            // ─── Stage 3b: single-tool-per-response enforcement ───────────
            // Providers that ignore parallel_tool_calls=false (e.g. Gemini)
            // emit N tool calls per response. Execute only the first; skip
            // the rest so the model re-plans after seeing one result.
            if (responseBatchTracker && !isInternalValidationRun) {
              const batch = responseBatchTracker.current;
              batch.callsInBatch += 1;
              if (batch.callsInBatch > TOOL_CALL_RESPONSE_BATCH_CAP) {
                const skippedResult = normalizeToolResult(toolName, {
                  llmResult: {
                    success: false,
                    details:
                      `Skipped: max ${TOOL_CALL_RESPONSE_BATCH_CAP} tool calls per model response. ` +
                      `This call was #${batch.callsInBatch} in the batch. ` +
                      `Wait for results, then call remaining tools in a new response.`
                  }
                });
                if (!isInternalValidationRun) {
                  onToolEvent?.({
                    phase: 'blocked',
                    toolName,
                    toolCallId,
                    success: false,
                    details: skippedResult.details
                  });
                }
                emitNormalizedResult(onNormalizedToolResult, toolName, skippedResult);
                return finalizeResult(skippedResult, turnExecutionStateRef, toolName);
              }
            }

            // ─── Stage 3c: circuit breakers ────────────────────────────────
            // Local-only tools (countQMapRows, waitForQMapDataset) bypass the
            // backend, so backend guardrails cannot stop loops. Two caps:
            //   1. Per-tool: max TOOL_CALL_CIRCUIT_BREAKER_MAX per tool name
            //   2. Total: max TOOL_CALL_TURN_HARD_CAP across all tools
            // This prevents providers that ignore parallel_tool_calls=false
            // from burning through dozens of batched calls per turn.
            if (toolCallCounter && !isInternalValidationRun) {
              const totalKey = '__total__';
              const totalCount = (toolCallCounter.get(totalKey) || 0) + 1;
              toolCallCounter.set(totalKey, totalCount);
              if (totalCount > TOOL_CALL_TURN_HARD_CAP) {
                const blockedResult = normalizeToolResult(toolName, {
                  llmResult: {
                    success: false,
                    details:
                      `Turn hard cap reached: ${totalCount} total tool calls this turn ` +
                      `(max ${TOOL_CALL_TURN_HARD_CAP}). Finalize your response now.`
                  }
                });
                onToolEvent?.({
                  phase: 'blocked',
                  toolName,
                  toolCallId,
                  success: false,
                  details: blockedResult.details
                });
                emitNormalizedResult(onNormalizedToolResult, toolName, blockedResult);
                return finalizeResult(blockedResult, turnExecutionStateRef, toolName);
              }
              const count = (toolCallCounter.get(toolName) || 0) + 1;
              toolCallCounter.set(toolName, count);
              if (count > TOOL_CALL_CIRCUIT_BREAKER_MAX) {
                const blockedResult = normalizeToolResult(toolName, {
                  llmResult: {
                    success: false,
                    details:
                      `Circuit breaker: "${toolName}" called ${count} times this turn ` +
                      `(max ${TOOL_CALL_CIRCUIT_BREAKER_MAX}). Stop repeating and proceed ` +
                      'with available evidence or call a different tool.'
                  }
                });
                if (!isInternalValidationRun) {
                  onToolEvent?.({
                    phase: 'blocked',
                    toolName,
                    toolCallId,
                    success: false,
                    details: blockedResult.details
                  });
                }
                emitNormalizedResult(onNormalizedToolResult, toolName, blockedResult);
                return finalizeResult(blockedResult, turnExecutionStateRef, toolName);
              }
            }

            // ─── Stage 4: execute with concurrency control ────────────────
            const concurrencyClass = classifyToolConcurrency(toolName);
            let releaseMutationLock: (() => void) | null = null;

            // Full serialization: ALL tool calls go through the mutex so they
            // execute one at a time in FIFO order. This prevents the LLM from
            // batch-planning tool chains where downstream tools reference
            // dataset IDs that don't exist yet. parallel_tool_calls=false is
            // not reliably supported by all providers (e.g. Gemini/OpenRouter),
            // so frontend serialization is the only reliable enforcement.
            if (mutationMutex && !isInternalValidationRun) {
              releaseMutationLock = await mutationMutex.acquire();
            }

            if (!isInternalValidationRun) {
              onToolEvent?.({phase: 'start', toolName, toolCallId});
            }

            try {
              const rawResult = await originalExecute(normalizedArgs, context);

              // ─── Auto-retry ─────────────────────────────────────────────
              const retryDirective = retryDepth < 1 ? extractToolAutoRetryDirective(rawResult) : null;
              if (retryDirective && retryDirective.retryToolName !== toolName) {
                const retryTarget = rawTools?.[retryDirective.retryToolName];
                if (retryTarget && typeof retryTarget.execute === 'function') {
                  const retryResult = await executeAutoRetry({
                    retryTarget,
                    retryDirective,
                    normalizedArgs,
                    context,
                    retryDepth,
                    toolName,
                    rawResult
                  });
                  const normalizedRetryResult = normalizeToolResult(toolName, retryResult);

                  // ─── Stage 5: postprocess ─────────────────────────────
                  const persisted = persistToCache({
                    toolName,
                    normalizedArgs,
                    toolCallId,
                    result: normalizedRetryResult,
                    currentMutationRevision,
                    isInternalValidationRun,
                    bypassStateMachine,
                    mutationIdempotencyCache,
                    nonActionableFailureCache,
                    statelessToolCallCache,
                    mutationRevisionRef
                  });
                  emitToolFinishEvent(
                    onToolEvent, isInternalValidationRun, toolName, toolCallId,
                    persisted, normalizedArgs, resolveCanonicalDatasetRef
                  );
                  emitNormalizedResult(onNormalizedToolResult, toolName, persisted);
                  return finalizeResult(persisted, turnExecutionStateRef, toolName);
                }
              }

              // ─── Stage 5: postprocess (normal path) ─────────────────────
              const normalizedResult = normalizeToolResult(toolName, rawResult);
              const persisted = persistToCache({
                toolName,
                normalizedArgs,
                toolCallId,
                result: normalizedResult,
                currentMutationRevision,
                isInternalValidationRun,
                bypassStateMachine,
                mutationIdempotencyCache,
                nonActionableFailureCache,
                statelessToolCallCache,
                mutationRevisionRef
              });
              emitToolFinishEvent(
                onToolEvent, isInternalValidationRun, toolName, toolCallId,
                persisted, normalizedArgs, resolveCanonicalDatasetRef
              );
              emitNormalizedResult(onNormalizedToolResult, toolName, persisted);
              return finalizeResult(persisted, turnExecutionStateRef, toolName);
            } catch (error) {
              const normalizedError = normalizeToolResult(toolName, {}, error);
              if (!isInternalValidationRun) {
                onToolEvent?.({
                  phase: 'finish',
                  toolName,
                  toolCallId,
                  success: false,
                  details: normalizedError.details,
                  requiresDatasetValidation: false
                });
              }
              emitNormalizedResult(onNormalizedToolResult, toolName, normalizedError);
              return finalizeResult(normalizedError, turnExecutionStateRef, toolName);
            } finally {
              if (releaseMutationLock) releaseMutationLock();
            }
          }
        }
      ];
    })
  );
}

// ─── Internal helpers ──────────────────────────────────────────────────────────

function emitNormalizedResult(
  onNormalizedToolResult: ((toolName: string, result: Record<string, unknown>) => void) | undefined,
  toolName: string,
  result: any
): void {
  if (!result || typeof result !== 'object' || Array.isArray(result) || typeof onNormalizedToolResult !== 'function') {
    return;
  }
  try {
    onNormalizedToolResult(toolName, result as Record<string, unknown>);
  } catch {
    // no-op: lineage/audit side-effects must not break tool execution
  }
}

function finalizeResult(
  result: any,
  turnExecutionStateRef: any,
  toolName: string
): any {
  if (!turnExecutionStateRef || !result || typeof result !== 'object') return result;
  const currentPhase = turnExecutionStateRef.current?.phase || 'execute';
  const cls = classifyToolConcurrency(toolName);
  const nextAllowed = getNextAllowedToolsForPhase(currentPhase);
  if (result.llmResult && typeof result.llmResult === 'object') {
    result.llmResult.executionPhase = currentPhase;
    result.llmResult.concurrencyClass = cls;
    if (nextAllowed.length > 0) {
      result.llmResult.nextAllowedTools = nextAllowed;
    }
  }
  return result;
}

function emitToolFinishEvent(
  onToolEvent: ((event: QMapToolExecutionEvent) => void) | undefined,
  isInternalValidationRun: boolean,
  toolName: string,
  toolCallId: string,
  result: any,
  normalizedArgs: Record<string, unknown>,
  resolveCanonicalDatasetRef?: (ref: string) => string
): void {
  if (isInternalValidationRun) return;
  const validationDatasetName = resolveDatasetNameForPostValidation(
    toolName,
    normalizedArgs,
    result as Record<string, unknown>
  );
  const canonicalValidationDatasetName = String(
    resolveCanonicalDatasetRef?.(validationDatasetName) || validationDatasetName
  ).trim();
  onToolEvent?.({
    phase: 'finish',
    toolName,
    toolCallId,
    success: Boolean(result?.success),
    details: result?.details || '',
    requiresDatasetValidation: shouldRunDatasetPostValidation(toolName),
    datasetName: canonicalValidationDatasetName
  });
}

async function executeAutoRetry({
  retryTarget,
  retryDirective,
  normalizedArgs,
  context,
  retryDepth,
  toolName,
  rawResult
}: {
  retryTarget: any;
  retryDirective: {retryToolName: string; retryArgs: Record<string, unknown>; retryReason: string};
  normalizedArgs: Record<string, unknown>;
  context: any;
  retryDepth: number;
  toolName: string;
  rawResult: any;
}): Promise<any> {
  const retryExecute = retryTarget.execute.bind(retryTarget);
  const retryArgs =
    retryDirective.retryArgs && Object.keys(retryDirective.retryArgs).length
      ? retryDirective.retryArgs
      : normalizedArgs;

  try {
    const retryRawResult = await retryExecute(retryArgs, {
      ...(context || {}),
      __qmapAutoRetryDepth: retryDepth + 1,
      __qmapAutoRetryFromTool: toolName
    });

    const firstDetails =
      normalizeToolDetails(rawResult?.llmResult?.details) || normalizeToolDetails(rawResult?.details);
    const retryDetails =
      normalizeToolDetails(retryRawResult?.llmResult?.details) ||
      normalizeToolDetails(retryRawResult?.details);
    const mergedResult =
      retryRawResult && typeof retryRawResult === 'object' && !Array.isArray(retryRawResult)
        ? {...retryRawResult}
        : {};
    const mergedLlm =
      mergedResult.llmResult && typeof mergedResult.llmResult === 'object' && !Array.isArray(mergedResult.llmResult)
        ? {...mergedResult.llmResult}
        : {};
    mergedLlm.autoRetry = {
      attempted: true,
      fromTool: toolName,
      toTool: retryDirective.retryToolName,
      reason: retryDirective.retryReason || 'runtime-auto-retry'
    };
    mergedLlm.details = [firstDetails, `Auto-retry executed via "${retryDirective.retryToolName}".`, retryDetails]
      .filter(Boolean)
      .join(' ');
    mergedResult.llmResult = mergedLlm;
    return mergedResult;
  } catch (retryError) {
    const firstDetails =
      normalizeToolDetails(rawResult?.llmResult?.details) || normalizeToolDetails(rawResult?.details);
    const retryErrorDetails =
      retryError instanceof Error ? retryError.message : normalizeToolDetails(String(retryError || ''));
    return {
      llmResult: {
        success: false,
        details: [firstDetails, `Auto-retry "${retryDirective.retryToolName}" failed: ${retryErrorDetails}`]
          .filter(Boolean)
          .join(' '),
        autoRetry: {
          attempted: true,
          fromTool: toolName,
          toTool: retryDirective.retryToolName,
          reason: retryDirective.retryReason || 'runtime-auto-retry',
          success: false
        }
      }
    };
  }
}