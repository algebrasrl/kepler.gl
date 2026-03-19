// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import React from 'react';
import {MapControlFactory} from '@kepler.gl/components';
import QMapModeSelectorControlFactory from './mode-selector-control';
import QMapDrawPanelFactory from './map-draw-panel';
import {QMapDrawOperationsControlFactory, QMapDrawStressorControlFactory} from './draw-tool-controls';
import QMapH3PaintControlFactory from './h3-paint-control';
import QMapAiControlFactory from '../features/qmap-ai/control';

CustomMapControlFactory.deps = [
  QMapModeSelectorControlFactory,
  QMapDrawPanelFactory,
  QMapDrawStressorControlFactory,
  QMapDrawOperationsControlFactory,
  QMapH3PaintControlFactory,
  QMapAiControlFactory,
  ...MapControlFactory.deps
];

function CustomMapControlFactory(
  QMapModeSelectorControl: any,
  QMapDrawPanel: any,
  QMapDrawStressorControl: any,
  QMapDrawOperationsControl: any,
  QMapH3PaintControl: any,
  QMapAiControl: any,
  ...deps: any[]
) {
  const MapControl = (MapControlFactory as any)(...deps);
  const defaultActionComponents = [...(MapControl.defaultActionComponents ?? [])].filter(
    (component: any) =>
      component?.displayName !== 'MapDrawPanel' &&
      component?.name !== 'MapDrawPanel'
  );

  const actionComponents = [
    QMapModeSelectorControl,
    ...defaultActionComponents,
    QMapDrawPanel,
    QMapDrawStressorControl,
    QMapDrawOperationsControl,
    QMapH3PaintControl,
    QMapAiControl
  ];

  const CustomMapControl = (props: any) => (
    <MapControl {...props} actionComponents={actionComponents} />
  );

  return React.memo(CustomMapControl);
}

export function replaceMapControl() {
  return [MapControlFactory, CustomMapControlFactory];
}
