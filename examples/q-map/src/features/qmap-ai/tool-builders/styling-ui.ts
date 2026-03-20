import {toggleSidePanel, wrapTo} from '@kepler.gl/actions';
import {z} from 'zod';

import type {QMapToolContext} from '../context/tool-context';

export function createOpenQMapPanelTool(ctx: QMapToolContext) {
  const {dispatch, getCurrentUiState} = ctx;
  return {
    description:
      'Open a q-map side panel tab (UI navigation only; this does NOT run analysis or generate charts). Supported canonical panel ids: layer, filter, interaction, map, profile, operations. Use panelId=null to close.',
    parameters: z.object({
      panelId: z
        .union([z.string(), z.null()])
        .describe('Panel id or alias (e.g. layers/filter/interaction/mapstyle/profile/operations/analytics). Use null or "close" to close side panel.')
    }),
    execute: async ({panelId}: any) => {
      const raw = panelId === null ? '' : String(panelId || '').trim().toLowerCase();
      const aliasMap: Record<string, string> = {
        layer: 'layer',
        layers: 'layer',
        dataset: 'layer',
        datasets: 'layer',
        filter: 'filter',
        filters: 'filter',
        interaction: 'interaction',
        interactions: 'interaction',
        map: 'map',
        basemap: 'map',
        mapstyle: 'map',
        'map-style': 'map',
        style: 'map',
        profile: 'profile',
        profilo: 'profile',
        operations: 'operations',
        operation: 'operations',
        operazioni: 'operations',
        operationi: 'operations',
        analytics: 'operations',
        analysis: 'operations',
        analisi: 'operations',
        charts: 'operations',
        grafici: 'operations'
      };
      const shouldClose =
        panelId === null || raw === '' || raw === 'close' || raw === 'none' || raw === 'hide';
      const resolvedPanel = shouldClose ? null : aliasMap[raw] || raw;
      const supported = ['layer', 'filter', 'interaction', 'map', 'profile', 'operations'];
      if (resolvedPanel && !supported.includes(resolvedPanel)) {
        return {
          llmResult: {
            success: false,
            details: `Unsupported panel "${String(panelId)}". Supported: ${supported.join(', ')}.`
          }
        };
      }

      dispatch(wrapTo('map', toggleSidePanel(resolvedPanel as any) as any));
      const active = getCurrentUiState()?.activeSidePanel ?? null;
      const humanPanel = resolvedPanel === 'operations' ? 'analytics' : resolvedPanel;
      return {
        llmResult: {
          success: true,
          panel: resolvedPanel,
          activeSidePanel: active,
          details: resolvedPanel
            ? `Opened side panel "${humanPanel}".`
            : 'Closed side panel.'
        }
      };
    }
  };
}
