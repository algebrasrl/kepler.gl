/**
 * Stage 2 — Policy gate: phase gate + contract check + unknown args.
 *
 * Returns either `{allow: true}` or a blocked result with details.
 */
import {getQMapToolContract, getQMapToolContractUnknownArgKeys} from '../tool-contract';
import {normalizeToolResult} from '../tool-result-normalization';
import type {QMapToolExecutionPolicyDecision} from '../tool-schema-utils';

export type PolicyGateInput = {
  toolName: string;
  normalizedArgs: Record<string, unknown>;
  context: Record<string, unknown>;
  isInternalValidationRun: boolean;
  shouldAllowTool?: (
    toolName: string,
    args: Record<string, unknown>,
    context: Record<string, unknown>
  ) => QMapToolExecutionPolicyDecision;
};

export type PolicyGateResult =
  | {allow: true}
  | {allow: false; result: any; gateType: string};

export function runPolicyGate(input: PolicyGateInput): PolicyGateResult {
  const {toolName, normalizedArgs, context, isInternalValidationRun, shouldAllowTool} = input;

  // ─── Contract check ─────────────────────────────────────────────────────────
  const toolContract = getQMapToolContract(toolName);
  if (!toolContract) {
    return {
      allow: false,
      gateType: 'contract_missing',
      result: normalizeToolResult(toolName, {
        llmResult: {
          success: false,
          details:
            `Tool "${toolName}" is missing shared args/response contract metadata. ` +
            'Execution blocked until contract registry is updated.'
        }
      })
    };
  }

  // ─── Unknown arg keys ───────────────────────────────────────────────────────
  const {unknownArgKeys, allowedArgKeys} = getQMapToolContractUnknownArgKeys(toolName, normalizedArgs);
  if (unknownArgKeys.length) {
    return {
      allow: false,
      gateType: 'unknown_args',
      result: normalizeToolResult(toolName, {
        llmResult: {
          success: false,
          details:
            `Tool "${toolName}" received unknown argument keys: ${unknownArgKeys.join(', ')}. ` +
            (allowedArgKeys.length
              ? `Allowed keys: ${allowedArgKeys.join(', ')}.`
              : 'No additional argument keys are allowed by contract.')
        }
      })
    };
  }

  // ─── Runtime policy (phase gate, snapshot, ambiguous refs) ──────────────────
  const policyDecision = isInternalValidationRun
    ? {allow: true as const}
    : shouldAllowTool?.(toolName, normalizedArgs, context) || {allow: true as const};

  if (!policyDecision.allow) {
    return {
      allow: false,
      gateType: (policyDecision as any).gateType || 'policy',
      result: normalizeToolResult(toolName, {
        llmResult: {
          success: false,
          status: (policyDecision as any).gateType === 'phase' ? 'deferred' : 'blocked',
          details:
            (policyDecision as any).details ||
            `Tool "${toolName}" blocked by deterministic turn tool policy.`
        }
      })
    };
  }

  return {allow: true};
}
