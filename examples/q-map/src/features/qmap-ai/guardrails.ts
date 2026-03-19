export type QMapTurnExecutionPhase = 'discover' | 'execute' | 'validate' | 'finalize';

export type QMapTurnExecutionState = {
  phase: QMapTurnExecutionPhase;
  snapshotTakenAt: number;
  snapshotDatasetRefs: Set<string>;
};

import {getQMapToolContract} from './tool-contract';

export const TURN_STATE_DISCOVERY_GATE_ALLOWLIST = new Set([
  'listQMapDatasets',
  'listQMapToolCategories',
  'listQMapToolsByCategory',
  'listQMapChartTools',
  'listQMapCloudMaps',
  'loadQMapCloudMap',
  'loadCloudMapAndWait',
  'loadData',
  'basemap',
  'openQMapPanel'
]);

const TURN_STATE_SNAPSHOT_OPTIONAL_TOOLS = new Set([
  ...TURN_STATE_DISCOVERY_GATE_ALLOWLIST,
  'listQCumberProviders',
  'listQCumberDatasets',
  'getQCumberDatasetHelp'
]);

export const TURN_STATE_VALIDATE_GATE_ALLOWLIST = new Set([
  'waitForQMapDataset',
  'countQMapRows',
  'debugQMapActiveFilters',
  'listQMapDatasets'
]);

export const TURN_DATASET_SNAPSHOT_TTL_MS = Math.max(
  30000,
  Number(import.meta.env.VITE_QMAP_AI_TURN_SNAPSHOT_TTL_MS || 180000) || 180000
);

export function createTurnExecutionState(): QMapTurnExecutionState {
  return {
    phase: 'discover',
    snapshotTakenAt: 0,
    snapshotDatasetRefs: new Set<string>()
  };
}

export function toolRequiresDatasetSnapshot(toolName: string): boolean {
  return !TURN_STATE_SNAPSHOT_OPTIONAL_TOOLS.has(String(toolName || '').trim());
}

// q-cumber query tools can create datasets via loadToMap=true, so they must be
// serialized as mutations even though the contract says mutatesDataset=false.
const QCUMBER_QUERY_MUTATION_TOOLS = new Set([
  'queryQCumberTerritorialUnits',
  'queryQCumberDataset',
  'queryQCumberDatasetSpatial'
]);

export function classifyToolConcurrency(toolName: string): 'read' | 'mutation' | 'validation' {
  if (TURN_STATE_VALIDATE_GATE_ALLOWLIST.has(toolName)) return 'validation';
  const contract = getQMapToolContract(toolName);
  if (contract?.flags.mutatesDataset) return 'mutation';
  if (QCUMBER_QUERY_MUTATION_TOOLS.has(toolName)) return 'mutation';
  return 'read';
}

export function getNextAllowedToolsForPhase(phase: QMapTurnExecutionPhase): string[] {
  switch (phase) {
    case 'discover':
      return [...TURN_STATE_DISCOVERY_GATE_ALLOWLIST];
    case 'validate':
      return [...TURN_STATE_VALIDATE_GATE_ALLOWLIST];
    default:
      return [];
  }
}

function extractTextFromMessage(message: Record<string, unknown>): string {
  const role = String(message.role || '').toLowerCase();
  if (role !== 'user') return '';
  const content = message.content;
  if (typeof content === 'string') {
    return content.trim();
  }
  if (Array.isArray(content)) {
    return content
      .map(item => {
        if (typeof item === 'string') return item;
        if (item && typeof item === 'object' && !Array.isArray(item)) {
          const text = (item as Record<string, unknown>).text;
          if (typeof text === 'string') return text;
        }
        return '';
      })
      .filter(Boolean)
      .join(' ')
      .trim();
  }
  return '';
}

export function extractToolPolicyUserText(requestBody: unknown): string {
  const payload =
    requestBody && typeof requestBody === 'object' && !Array.isArray(requestBody)
      ? (requestBody as Record<string, unknown>)
      : null;
  if (!payload) return '';

  // Singular message (OpenAssistant SDK format)
  if (payload.message && typeof payload.message === 'object' && !Array.isArray(payload.message)) {
    const text = extractTextFromMessage(payload.message as Record<string, unknown>);
    if (text) return text;
  }

  // Plural messages array (OpenAI-compatible format) — only detect new user turn
  // when the LAST message in the array is role=user. Sub-requests have the last
  // message as role=tool or role=assistant, so they won't trigger a reset.
  if (Array.isArray(payload.messages) && payload.messages.length > 0) {
    const last = payload.messages[payload.messages.length - 1];
    if (last && typeof last === 'object' && !Array.isArray(last)) {
      const text = extractTextFromMessage(last as Record<string, unknown>);
      if (text) return text;
    }
  }

  return '';
}
