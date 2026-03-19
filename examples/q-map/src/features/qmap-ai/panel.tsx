import React from 'react';
import styled from 'styled-components';
import QMapAiAssistantComponent from './qmap-ai-assistant-component';

const QMAP_FONT_STACK = 'ff-clan-web-pro, "Helvetica Neue", Helvetica, sans-serif';

const PanelRoot = styled.div`
  display: flex;
  flex-direction: column;
  width: 100%;
  height: 100%;
  background: #ffffff;
  border: 1px solid ${props => props.theme.panelBorderColor || '#334155'};
  box-shadow: 0 10px 30px rgba(15, 23, 42, 0.18);
  font-family: ${QMAP_FONT_STACK};
`;

const Header = styled.div.attrs({
  className: 'side-side-panel__header side-panel__panel-header'
})`
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
  border-bottom: 1px solid ${props => props.theme.panelBorderColor || '#334155'};
  background: ${props => props.theme.sidePanelHeaderBg || '#232629'};
`;

const Title = styled.div`
  font-size: 13px;
  font-weight: 500;
  letter-spacing: 0.43px;
  color: ${props => props.theme.textColorHl || '#f8fafc'};
`;

const Content = styled.div`
  flex: 1;
  min-height: 0;
  overflow: hidden;

  .ai-assistant-component {
    height: 100%;
  }
`;

const QMapAiPanel: React.FC = () => {
  return (
    <PanelRoot>
      <Header>
        <Title>Assistente</Title>
      </Header>
      <Content>
        <QMapAiAssistantComponent />
      </Content>
    </PanelRoot>
  );
};

export default QMapAiPanel;
