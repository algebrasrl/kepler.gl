import {QMAP_DRAW_CLEAR_ACTIVE_TOOL, QMAP_DRAW_SET_ACTIVE_TOOL, QMAP_DRAW_SET_LINE_START} from './actions';
import {
  isQMapDrawTarget,
  isQMapDrawTool,
  QMapDrawTarget,
  QMapDrawTool
} from './constants';

type QMapDrawState = {
  activeTarget: QMapDrawTarget | null;
  activeTool: QMapDrawTool | null;
  lineStartByTarget: Record<QMapDrawTarget, [number, number] | null>;
};

const initialState: QMapDrawState = {
  activeTarget: null,
  activeTool: null,
  lineStartByTarget: {
    stressor: null,
    operations: null
  }
};

export default function qMapDrawReducer(state = initialState, action: any): QMapDrawState {
  switch (action.type) {
    case QMAP_DRAW_SET_ACTIVE_TOOL: {
      const target = action?.payload?.target;
      const tool = action?.payload?.tool;
      if (!isQMapDrawTarget(target) || !isQMapDrawTool(tool)) {
        return state;
      }
      return {
        ...state,
        activeTarget: target,
        activeTool: tool,
        lineStartByTarget:
          tool === 'line'
            ? state.lineStartByTarget
            : {
                ...state.lineStartByTarget,
                [target]: null
              }
      };
    }
    case QMAP_DRAW_SET_LINE_START: {
      const target = action?.payload?.target;
      if (!isQMapDrawTarget(target)) {
        return state;
      }
      const coordinate = action?.payload?.coordinate;
      const nextCoordinate =
        Array.isArray(coordinate) && coordinate.length >= 2 &&
        Number.isFinite(Number(coordinate[0])) &&
        Number.isFinite(Number(coordinate[1]))
          ? ([Number(coordinate[0]), Number(coordinate[1])] as [number, number])
          : null;
      return {
        ...state,
        lineStartByTarget: {
          ...state.lineStartByTarget,
          [target]: nextCoordinate
        }
      };
    }
    case QMAP_DRAW_CLEAR_ACTIVE_TOOL:
      return {
        ...state,
        activeTarget: null,
        activeTool: null,
        lineStartByTarget: {
          stressor: null,
          operations: null
        }
      };
    default:
      return state;
  }
}
