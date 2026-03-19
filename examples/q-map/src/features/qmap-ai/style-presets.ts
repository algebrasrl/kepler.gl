export type QMapStylePresetName = 'comuni_population';

export type QMapStylePreset = {
  colorField: string;
  mode: 'linear' | 'quantize' | 'quantile';
  classes: number;
  palette: 'redGreen' | 'greenRed' | 'blueRed' | 'viridis' | 'magma' | 'yellowRed' | 'yellowBlue';
  applyToStroke: boolean;
  fillOpacity: number;
  strokeOpacity: number;
  isolateLayer: boolean;
};

export const QMAP_STYLE_PRESETS: Record<QMapStylePresetName, QMapStylePreset> = {
  comuni_population: {
    colorField: 'population',
    mode: 'quantile',
    classes: 7,
    palette: 'viridis',
    applyToStroke: false,
    fillOpacity: 0.9,
    strokeOpacity: 0,
    isolateLayer: true
  }
};

export function getQMapStylePreset(name: string): QMapStylePreset | null {
  const key = String(name || '').trim().toLowerCase() as QMapStylePresetName;
  return QMAP_STYLE_PRESETS[key] || null;
}

