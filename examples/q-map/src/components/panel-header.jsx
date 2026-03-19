// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import React from 'react';
import {PanelHeaderFactory, appInjector} from '@kepler.gl/components';
import logoFull from '../assets/logo-full.svg';

const QMapLogo = ({appWebsite}) => (
  <div className="side-panel-logo" style={{display: 'flex', alignItems: 'center'}}>
    <a
      className="logo__link"
      href={appWebsite || '#'}
      target={appWebsite ? '_blank' : undefined}
      rel={appWebsite ? 'noopener noreferrer' : undefined}
      style={{display: 'inline-flex'}}
    >
      <img
        className="side-panel-logo__logo"
        src={logoFull}
        alt="Q Map"
        style={{height: 32, width: 'auto', marginTop: 4, marginBottom: 12}}
      />
    </a>
  </div>
);

export function CustomPanelHeaderFactory() {
  const PanelHeader = appInjector.get(PanelHeaderFactory);
  PanelHeader.defaultProps = {
    ...PanelHeader.defaultProps,
    logoComponent: QMapLogo
  };
  return PanelHeader;
}

export default CustomPanelHeaderFactory;
