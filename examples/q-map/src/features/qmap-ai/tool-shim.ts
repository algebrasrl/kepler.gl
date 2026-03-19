/**
 * Local replacement for @openassistant/utils extendedTool().
 *
 * extendedTool was literally `function(n) { return n; }` — a no-op identity.
 * This shim preserves that behavior with proper typing so tool builders
 * can pass extra properties (component, context, onToolCompleted) that the
 * Vercel AI SDK tool() function doesn't accept.
 *
 * Tool builders will migrate to AI SDK tool() directly in a later phase
 * once the component/context properties are removed from tool definitions.
 */
export function extendedTool<T extends {description: string; parameters: any; execute: (...args: any[]) => any}>(
  config: T
): T {
  return config;
}
