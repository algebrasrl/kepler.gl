import type {QMapChartToolState} from './chart-tools';
import {getQMapToolManifest} from './tool-manifest';
import {extendedTool} from './tool-shim';
import {z} from 'zod';

export type QMapToolCategoryDescriptor = {
  key: string;
  label: string;
  description: string;
  toolNames: string[];
};

export type QMapToolCategorySnapshot = {
  totalAvailableTools: number;
  categories: Array<
    QMapToolCategoryDescriptor & {
      availableTools: string[];
      knownButUnavailableTools: Array<{name: string; reason: string}>;
    }
  >;
  uncategorizedTools: string[];
};

export const QMAP_MANUAL_TOOL_DESCRIPTIONS: Record<string, string> = {
  listQMapToolCategories:
    'List functional categories of q-map tools currently available in this runtime.',
  listQMapToolsByCategory:
    'List tools in a selected functional category to narrow deterministic tool selection.'
};

export function getQMapToolCategoryDescriptors(): QMapToolCategoryDescriptor[] {
  const manifest = getQMapToolManifest();
  return manifest.categories.map(category => ({
    key: category.key,
    label: category.label,
    description: category.description,
    toolNames: [...category.tools]
  }));
}

export function getQMapToolCategoryMap(): Map<string, QMapToolCategoryDescriptor> {
  const descriptors = getQMapToolCategoryDescriptors();
  return new Map(descriptors.map(descriptor => [descriptor.key, descriptor] as const));
}

export function buildQMapToolCategorySnapshot(
  toolRegistry: Record<string, any>,
  chartStates: QMapChartToolState[],
  extraAvailableToolNames: string[] = []
): QMapToolCategorySnapshot {
  const categoryDescriptors = getQMapToolCategoryDescriptors();
  const availableToolNames = Array.from(
    new Set([...Object.keys(toolRegistry || {}), ...(extraAvailableToolNames || [])].filter(Boolean))
  );
  const availableSet = new Set(availableToolNames);
  const chartStateByKey = new Map(
    (chartStates || []).map(state => [String(state?.key || ''), state] as const).filter(entry => Boolean(entry[0]))
  );

  const categories = categoryDescriptors.map(descriptor => {
    const availableTools = descriptor.toolNames.filter(name => availableSet.has(name));
    const knownButUnavailableTools = descriptor.toolNames
      .filter(name => !availableSet.has(name))
      .map(name => {
        const chartState = chartStateByKey.get(name);
        if (chartState) {
          return {name, reason: chartState.reason || 'not available in current runtime policy'};
        }
        return {name, reason: 'not available in current runtime'};
      });
    return {
      ...descriptor,
      availableTools,
      knownButUnavailableTools
    };
  });

  const categorizedTools = new Set(categories.flatMap(category => category.availableTools));
  const uncategorizedTools = availableToolNames
    .filter(toolName => !categorizedTools.has(toolName))
    .sort((a, b) => a.localeCompare(b));

  return {
    totalAvailableTools: availableToolNames.length,
    categories,
    uncategorizedTools
  };
}

export function createQMapToolCategoryIntrospectionTools({
  toolRegistry,
  chartStates,
  baseToolsRaw
}: {
  toolRegistry: Record<string, any>;
  chartStates: QMapChartToolState[];
  baseToolsRaw: Record<string, any>;
}) {
  const getToolCategorySnapshot = () =>
    buildQMapToolCategorySnapshot(
      toolRegistry,
      chartStates,
      Object.keys(QMAP_MANUAL_TOOL_DESCRIPTIONS)
    );

  const listQMapToolCategories = extendedTool({
    description:
      'List q-map tool categories (functional classes) with available counts to guide deterministic tool routing.',
    parameters: z.object({
      includeToolNames: z.boolean().optional().describe('Include available tool names per category (default true).'),
      includeUncategorized: z.boolean().optional().describe('Include available uncategorized tools (default false).')
    }),
    execute: async ({includeToolNames, includeUncategorized}) => {
      const includeNames = includeToolNames !== false;
      const includeUncat = includeUncategorized === true;
      const snapshot = getToolCategorySnapshot();
      const categories = snapshot.categories.map(category => ({
        key: category.key,
        label: category.label,
        description: category.description,
        availableCount: category.availableTools.length,
        unavailableCount: category.knownButUnavailableTools.length,
        availableTools: includeNames ? category.availableTools : undefined,
        knownButUnavailableTools:
          category.key === 'chart-visualization' && category.knownButUnavailableTools.length
            ? category.knownButUnavailableTools
            : undefined
      }));
      const categorySummary = categories
        .map(category => `${category.key}:${category.availableCount}`)
        .join(', ');
      return {
        llmResult: {
          success: true,
          totalAvailableTools: snapshot.totalAvailableTools,
          categories,
          uncategorizedTools: includeUncat ? snapshot.uncategorizedTools : undefined,
          details: `Tool categories ready (${snapshot.totalAvailableTools} tools): ${categorySummary}.`
        }
      };
    }
  });

  const listQMapToolsByCategory = extendedTool({
    description:
      'List tools for one q-map functional category. Use this to reduce tool ambiguity before executing a workflow.',
    parameters: z.object({
      categoryKey: z.string().describe('Category key from listQMapToolCategories (e.g. discovery, geospatial-analysis).'),
      includeDescriptions: z
        .boolean()
        .optional()
        .describe('Include tool descriptions when available (default true).'),
      includeUnavailable: z
        .boolean()
        .optional()
        .describe('Include known-but-unavailable tools for the category (default false).'),
      limit: z.number().min(1).max(200).optional().describe('Max number of available tools returned (default 120).')
    }),
    execute: async ({categoryKey, includeDescriptions, includeUnavailable, limit}) => {
      const normalizedKey = String(categoryKey || '')
        .trim()
        .toLowerCase();
      const snapshot = getToolCategorySnapshot();
      const category = snapshot.categories.find(item => item.key === normalizedKey);
      if (!category) {
        return {
          llmResult: {
            success: false,
            categoryKey: normalizedKey,
            availableCategories: snapshot.categories.map(item => item.key),
            details: `Unknown tool category "${String(categoryKey)}". Available: ${snapshot.categories
              .map(item => item.key)
              .join(', ')}.`
          }
        };
      }

      const maxTools = Math.max(1, Number(limit || 120));
      const withDescriptions = includeDescriptions !== false;
      const toolRows = category.availableTools.slice(0, maxTools).map(name => {
        const description = withDescriptions
          ? String(
              toolRegistry?.[name]?.description ||
                baseToolsRaw?.[name]?.description ||
                QMAP_MANUAL_TOOL_DESCRIPTIONS[name] ||
                ''
            ).trim()
          : '';
        return {
          name,
          description: description || undefined
        };
      });
      const unavailableRows = includeUnavailable
        ? category.knownButUnavailableTools
        : undefined;
      return {
        llmResult: {
          success: true,
          category: {
            key: category.key,
            label: category.label,
            description: category.description
          },
          availableCount: category.availableTools.length,
          tools: toolRows,
          unavailableTools: unavailableRows,
          details: `Category "${category.key}" has ${category.availableTools.length} available tools${
            includeUnavailable ? ` and ${category.knownButUnavailableTools.length} unavailable tools` : ''
          }.`
        }
      };
    }
  });

  return {
    listQMapToolCategories,
    listQMapToolsByCategory
  };
}
