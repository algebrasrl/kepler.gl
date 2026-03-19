import {QMapDrawTarget, QMapDrawTool} from './constants';

export const QMAP_DRAW_SET_ACTIVE_TOOL = 'QMAP_DRAW_SET_ACTIVE_TOOL';
export const QMAP_DRAW_SET_LINE_START = 'QMAP_DRAW_SET_LINE_START';
export const QMAP_DRAW_CLEAR_ACTIVE_TOOL = 'QMAP_DRAW_CLEAR_ACTIVE_TOOL';

export function setQMapDrawActiveTool(target: QMapDrawTarget, tool: QMapDrawTool) {
  return {
    type: QMAP_DRAW_SET_ACTIVE_TOOL,
    payload: {
      target,
      tool
    }
  };
}

export function setQMapDrawLineStart(target: QMapDrawTarget, coordinate: [number, number] | null) {
  return {
    type: QMAP_DRAW_SET_LINE_START,
    payload: {
      target,
      coordinate: Array.isArray(coordinate) ? [Number(coordinate[0]), Number(coordinate[1])] : null
    }
  };
}

export function clearQMapDrawActiveTool() {
  return {
    type: QMAP_DRAW_CLEAR_ACTIVE_TOOL
  };
}
