import {QMapMode} from '../../mode/qmap-mode';

export const QMAP_MODE_SET_MODE = 'QMAP_MODE_SET_MODE';

export function setQMapMode(mode: QMapMode) {
  return {type: QMAP_MODE_SET_MODE, payload: mode};
}
