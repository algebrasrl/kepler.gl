export const QMAP_DRAW_TARGETS = ['stressor', 'operations'] as const;
export type QMapDrawTarget = (typeof QMAP_DRAW_TARGETS)[number];

export const QMAP_DRAW_TOOLS = ['point', 'line', 'polygon', 'rectangle', 'radius'] as const;
export type QMapDrawTool = (typeof QMAP_DRAW_TOOLS)[number];

export const QMAP_DRAW_TARGET_PROPERTY = 'qmap_draw_target';
export const QMAP_DRAW_TOOL_PROPERTY = 'qmap_draw_tool';
export const QMAP_DRAW_DRAFT_PROPERTY = 'qmap_draw_draft';
export const QMAP_MAP_DRAW_SETTING_FORCE_CROSSHAIR = 'forceCrosshair';
export const QMAP_MAP_DRAW_SETTING_DISABLE_DOUBLE_CLICK_ZOOM = 'disableDoubleClickZoom';
export const QMAP_MAP_DRAW_SETTING_BYPASS_EDITOR_CLICK = 'bypassEditorClick';
export const QMAP_DRAW_SKIP_DATASET_SYNC_FLAG = 'qmapDrawSkipDatasetSync';

export type QMapDrawDatasetConfig = {
  id: string;
  label: string;
  targetLabel: string;
  toolLabel: string;
};

const QMAP_DRAW_DATASET_PREFIX: Record<QMapDrawTarget, string> = {
  stressor: 'stressor_perimeter',
  operations: 'stressor_operations'
};

export function getQMapDrawDatasetConfig(
  target: QMapDrawTarget,
  tool: QMapDrawTool
): QMapDrawDatasetConfig {
  const prefix = QMAP_DRAW_DATASET_PREFIX[target];
  const datasetName = `${prefix}__${tool}`;
  return {
    id: datasetName,
    label: datasetName,
    targetLabel: target,
    toolLabel: tool
  };
}

export function isQMapDrawTarget(value: unknown): value is QMapDrawTarget {
  return QMAP_DRAW_TARGETS.includes(String(value || '') as QMapDrawTarget);
}

export function isQMapDrawTool(value: unknown): value is QMapDrawTool {
  return QMAP_DRAW_TOOLS.includes(String(value || '') as QMapDrawTool);
}
