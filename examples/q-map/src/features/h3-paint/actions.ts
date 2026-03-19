export const QMAP_H3_PAINT_SET_ACTIVE = 'QMAP_H3_PAINT_SET_ACTIVE';
export const QMAP_H3_PAINT_SET_RESOLUTION = 'QMAP_H3_PAINT_SET_RESOLUTION';

export function setQMapH3PaintActive(active: boolean) {
  return {type: QMAP_H3_PAINT_SET_ACTIVE, payload: Boolean(active)};
}

export function setQMapH3PaintResolution(resolution: number) {
  return {type: QMAP_H3_PAINT_SET_RESOLUTION, payload: Number(resolution)};
}
