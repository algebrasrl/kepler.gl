import {useRef, useCallback} from 'react';
import {
  shouldSkipToolComponentRun,
  markToolComponentRunCompleted
} from '../services/execution-tracking';

/**
 * Encapsulates the run-once guard pattern shared by all tool components:
 *   const hasRunRef
 *   shouldSkipToolComponentRun(...)
 *   markToolComponentRunCompleted(...)
 *
 * Usage inside a tool component:
 *
 *   const {shouldSkip, abort, complete} = useToolExecution({
 *     executionKey, executedToolComponentKeys, rememberExecutedToolComponentKey
 *   });
 *   useEffect(() => {
 *     if (shouldSkip()) return;
 *     if (!layer) { abort(); return; }
 *     complete();
 *     dispatch(...)
 *   }, [...deps, shouldSkip, abort, complete]);
 *   return null;
 */
export function useToolExecution({
  executionKey,
  executedToolComponentKeys,
  rememberExecutedToolComponentKey
}: {
  executionKey: unknown;
  executedToolComponentKeys: Set<string>;
  rememberExecutedToolComponentKey: (value: unknown) => void;
}) {
  const hasRunRef = useRef(false);

  const shouldSkip = useCallback(
    () => shouldSkipToolComponentRun({hasRunRef, executionKey, executedToolComponentKeys}),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [executionKey, executedToolComponentKeys]
  );

  const abort = useCallback(() => {
    hasRunRef.current = true;
  }, []);

  const complete = useCallback(
    () => markToolComponentRunCompleted({hasRunRef, executionKey, rememberExecutedToolComponentKey}),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [executionKey, rememberExecutedToolComponentKey]
  );

  return {shouldSkip, abort, complete};
}
