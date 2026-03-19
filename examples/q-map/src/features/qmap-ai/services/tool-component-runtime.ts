/**
 * Tool component runtime: skip/complete guards for React tool side-effect components.
 */

type ToolRunRef = {current: boolean};

/** Common guard-function deps injected by the main assistant component into every tool factory. */
export type ToolComponentGuardDeps = {
  executedToolComponentKeys: Set<string>;
  rememberExecutedToolComponentKey: (value: unknown) => void;
  shouldSkipToolComponentRun: (args: any) => boolean;
  markToolComponentRunCompleted: (args: any) => void;
};

export function shouldSkipToolComponentRun({
  hasRunRef,
  executionKey,
  executedToolComponentKeys
}: {
  hasRunRef: ToolRunRef;
  executionKey: unknown;
  executedToolComponentKeys: Set<string>;
}): boolean {
  if (hasRunRef.current) return true;
  const normalizedExecutionKey = String(executionKey || '').trim();
  if (!normalizedExecutionKey) return false;
  if (executedToolComponentKeys.has(normalizedExecutionKey)) {
    hasRunRef.current = true;
    return true;
  }
  return false;
}

export function markToolComponentRunCompleted({
  hasRunRef,
  executionKey,
  rememberExecutedToolComponentKey
}: {
  hasRunRef: ToolRunRef;
  executionKey: unknown;
  rememberExecutedToolComponentKey: (value: unknown) => void;
}) {
  hasRunRef.current = true;
  const normalizedExecutionKey = String(executionKey || '').trim();
  if (normalizedExecutionKey) {
    rememberExecutedToolComponentKey(normalizedExecutionKey);
  }
}

export function shouldSkipToolComponentByExecutionKey({
  executionKey,
  executedToolComponentKeys
}: {
  executionKey: unknown;
  executedToolComponentKeys: Set<string>;
}): boolean {
  const normalizedExecutionKey = String(executionKey || '').trim();
  if (!normalizedExecutionKey) return false;
  return executedToolComponentKeys.has(normalizedExecutionKey);
}

export function rememberToolComponentExecutionKey({
  executionKey,
  rememberExecutedToolComponentKey
}: {
  executionKey: unknown;
  rememberExecutedToolComponentKey: (value: unknown) => void;
}) {
  const normalizedExecutionKey = String(executionKey || '').trim();
  if (normalizedExecutionKey) {
    rememberExecutedToolComponentKey(normalizedExecutionKey);
  }
}

