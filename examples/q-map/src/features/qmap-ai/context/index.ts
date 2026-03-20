/**
 * Barrel re-export for context modules.
 *
 * Actual implementations:
 *   - context/tool-context.ts — QMapToolContext interface
 *   - context/tool-context-provider.ts — buildQMapToolContext factory
 */
export type {QMapToolContext} from './tool-context';

export {buildQMapToolContext, type BuildToolContextInput} from './tool-context-provider';
