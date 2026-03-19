import {QMAP_H3_PAINT_SET_ACTIVE, QMAP_H3_PAINT_SET_RESOLUTION} from './actions';

export const H3_PAINT_RESOLUTIONS = [3, 4, 5, 6, 7, 8, 9, 10, 11];
export const H3_PAINT_DEFAULT_RESOLUTION = 7;

type H3PaintState = {
  active: boolean;
  resolution: number;
};

const initialState: H3PaintState = {
  active: false,
  resolution: H3_PAINT_DEFAULT_RESOLUTION
};

export default function qMapH3PaintReducer(state = initialState, action: any): H3PaintState {
  switch (action.type) {
    case QMAP_H3_PAINT_SET_ACTIVE:
      return {...state, active: Boolean(action.payload)};
    case QMAP_H3_PAINT_SET_RESOLUTION: {
      const next = Number(action.payload);
      if (!H3_PAINT_RESOLUTIONS.includes(next)) {
        return state;
      }
      return {...state, resolution: next};
    }
    default:
      return state;
  }
}
