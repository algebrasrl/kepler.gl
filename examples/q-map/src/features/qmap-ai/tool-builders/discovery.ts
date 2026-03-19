import {extendedTool} from '../tool-shim';
import {z} from 'zod';
import type {QMapChartToolState} from '../chart-tools';

export function createListQMapChartTools({
  mode,
  timeSeriesEligibility,
  getMergedStates
}: {
  mode: string;
  timeSeriesEligibility: any;
  getMergedStates: () => QMapChartToolState[];
}) {
  return extendedTool({
    description:
      'List chart rendering tools currently available/enabled in q-map runtime (mode safe/full/timeseries-safe with env overrides).',
    parameters: z.object({}),
    execute: async () => {
      const mergedStates = getMergedStates();
      const enabled = mergedStates.filter(item => item.enabled).map(item => item.key);
      return {
        llmResult: {
          success: true,
          mode,
          timeSeriesEligibility,
          chartTools: mergedStates,
          enabledChartTools: enabled,
          details: enabled.length
            ? `Enabled chart tools (${mode} mode): ${enabled.join(', ')}.`
            : `No chart tools enabled in ${mode} mode.`
        }
      };
    }
  });
}
