// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import React from 'react';
import styled from 'styled-components';
import {SidebarFactory, Icons} from '@kepler.gl/components';

const StyledCloseButton = styled.div`
  align-items: center;
  justify-content: center;
  background-color: ${props => props.theme.sideBarCloseBtnBgd};
  color: ${props => props.theme.sideBarCloseBtnColor};
  display: flex;
  height: 20px;
  width: 20px;
  border-radius: 1px;
  position: absolute;
  right: -8px;
  top: ${props => props.theme.sidePanel.margin.top}px;

  &:hover {
    cursor: pointer;
    background-color: ${props => props.theme.sideBarCloseBtnBgdHover};
  }
`;

const CloseButtonFactory = () => {
  const CloseButton = ({onClick, isOpen}) => (
    <StyledCloseButton className="side-bar__close" onClick={onClick}>
      <Icons.ArrowRight height="12px" style={{transform: `rotate(${isOpen ? 180 : 0}deg)`}} />
    </StyledCloseButton>
  );
  return CloseButton;
};

function CustomSidebarFactory(CloseButton) {
  return SidebarFactory(CloseButton);
}

CustomSidebarFactory.deps = [CloseButtonFactory];

export default CustomSidebarFactory;
