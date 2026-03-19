/**
 * Stage 1 — Schema preprocess: normalize tool args BEFORE Zod validation.
 *
 * This runs inside z.preprocess() on each tool schema, ensuring hallucinated
 * arg shapes ({filters:[…]}, {filter:{…}}, operator aliases) are fixed before
 * Zod rejects them. Also resolves canonical dataset refs and fallback refs.
 */
import {normalizeQMapToolExecuteArgs} from '../tool-args-normalization';

export type SchemaPreprocessOptions = {
  resolveCanonicalDatasetRef?: (datasetCandidate: string) => string;
  resolveFallbackDatasetRef?: () => string;
};

/**
 * Creates a z.preprocess() callback that normalizes tool args.
 * Use as: `z.preprocess(createSchemaPreprocess(toolName, opts), z.object({...}))`
 */
export function createSchemaPreprocess(
  toolName: string,
  options: SchemaPreprocessOptions = {}
): (raw: unknown) => unknown {
  return (raw: unknown) => {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return raw;
    return normalizeQMapToolExecuteArgs(toolName, raw, options);
  };
}
