import React from 'react';
import {useSelector} from 'react-redux';
import {PanelToggleFactory} from '@kepler.gl/components';
import {
  filterQMapSidePanels,
  getQMapModeConfig,
  isQMapSidePanelEnabled,
  resolveQMapModeFromUiState,
  resolveQMapSidePanelId
} from '../mode/qmap-mode';
import {selectQMapUiState} from '../state/qmap-selectors';

function QMapPanelToggleFactory(...deps: any[]) {
  const BasePanelToggle = (PanelToggleFactory as any)(...deps);

  const QMapPanelToggle = (props: any) => {
    const activeMode = useSelector((state: any) => resolveQMapModeFromUiState(selectQMapUiState(state)));
    const modeConfig = React.useMemo(() => getQMapModeConfig(activeMode), [activeMode]);
    const panels = Array.isArray(props?.panels) ? props.panels : [];
    const filteredPanels = filterQMapSidePanels(panels, modeConfig);
    const profile = filteredPanels.find((p: any) => p?.id === 'profile');
    const rest = filteredPanels.filter((p: any) => p?.id !== 'profile');
    const ordered = profile ? [profile, ...rest] : filteredPanels;
    const activePanel = props?.activePanel;
    const togglePanel = props?.togglePanel;

    React.useEffect(() => {
      if (!isQMapSidePanelEnabled(activePanel, modeConfig) && typeof togglePanel === 'function') {
        const fallbackPanel = resolveQMapSidePanelId(activePanel, modeConfig);
        if (fallbackPanel !== activePanel) {
          togglePanel(fallbackPanel);
        }
      }
    }, [activePanel, modeConfig, togglePanel]);

    return <BasePanelToggle {...props} panels={ordered} />;
  };

  return QMapPanelToggle;
}

QMapPanelToggleFactory.deps = (PanelToggleFactory as any).deps;

export default QMapPanelToggleFactory;
