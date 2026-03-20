import React, {useEffect} from 'react';
import {useDispatch, useSelector, useStore} from 'react-redux';
import styled from 'styled-components';
import {MessageModel} from '@openassistant/core';
import {AiAssistant} from '@openassistant/ui';
import {buildQMapToolContext} from './context/tool-context-provider';
import {useToolRegistry} from './hooks/use-tool-registry';
import {wrapToolsWithPipeline} from './middleware/tool-pipeline';
import '@openassistant/echarts/dist/index.css';
import '@openassistant/ui/dist/index.css';
import {fitBounds, layerConfigChange, setSelectedFeature, wrapTo} from '@kepler.gl/actions';
import {updateAiAssistantMessages} from '@kepler.gl/ai-assistant';
import {
  type QMapBounds,
  type StatelessToolCallCacheEntry,
  type QMapToolExecutionEvent
} from './tool-schema-utils';
import {
  syncDatasetLineageFromCurrentDatasets,
  resolveCanonicalDatasetRefWithLineage,
  extractProducedDatasetRefsFromNormalizedResult,
  updateDatasetLineageFromToolResult,
  findDatasetCandidatesByName,
  SAFE_COLOR_RANGE,
  resolveDatasetByName,
  hasValidBounds,
  resolveGeojsonFieldName,
  parseCoordinateValue,
  getDatasetIndexes,
  type QMapRuntimeStep,
  type QMapRankContext
} from './dataset-utils';
import {makeExecutionKey} from './tool-result-normalization';
import {resolveDatasetPointFieldPair} from './merge-utils';
import {
  QMAP_CONTEXT_HEADER,
  Q_ASSISTANT_SESSION_HEADER,
  Q_ASSISTANT_CHAT_HEADER,
  Q_ASSISTANT_RUNTIME_POLICY_HEADER,
  QMAP_CONTEXT_HEADER_ENABLED,
  parseAssistantRequestBody,
  buildQMapContextHeaderValue,
  buildRuntimeDatasetHints
} from './context-header';
import {
  buildExecutionSummaryLine,
  collapseRepeatedNarrativeBlocks,
  countFailedStyleRuns,
  countSuccessfulStyleRuns,
  computeAssistantExecutionStats,
  extractSubRequestIdsFromText,
  extractInvocationResultSummaries,
  getToolResultSummary,
  stripRuntimeDiagnosticLines,
  stripUnverifiedCompletionClaimLines,
  stripUnverifiedStylingClaimLines,
  stripUnverifiedCenteringClaimLines,
  stripContradictoryNonSuccessClaimLines,
  textIsRuntimeDiagnosticOnly,
  textAcknowledgesNonSuccessOutcome,
  textClaimsWorkflowCompleted,
  textClaimsStyling,
  textRequestsStylingObjective,
  textClaimsCentering
} from './services/execution-tracking';
import {
  createTurnExecutionState,
  extractToolPolicyUserText,
  toolRequiresDatasetSnapshot,
  TURN_DATASET_SNAPSHOT_TTL_MS,
  TURN_STATE_VALIDATE_GATE_ALLOWLIST,
  type QMapTurnExecutionState
} from './guardrails';
import {AsyncMutex} from './middleware/cache';
import {buildQMapSystemPrompt} from './system-prompt';
import {buildQMapAiModePromptOverlay} from './mode-policy';
import {resolveQMapAuthorizationHeader} from '../../utils/auth-token';
import {
  selectQMapAiAssistantConfig,
  selectQMapAiAssistantState,
  selectQMapUiState,
  selectQMapVisState
} from '../../state/qmap-selectors';
import type {QMapRootState} from '../../state/qmap-state-types';
import {resolveQMapAssistantBaseUrl} from '../../utils/assistant-config';
import {resolveValidationTimeoutMs} from './services/execution-tracking';
import {type MutationIdempotencyCacheEntry} from './middleware/cache';
import {parseGeoJsonLike} from '../../geo';
import {resolveQMapModeFromUiState} from '../../mode/qmap-mode';

const StyledAssistant = styled.div`
  height: 100%;
  padding-bottom: 4px;

  * {
    font-size: 11px;
  }
`;

const RuntimeProgressPanel = styled.div`
  margin-top: 6px;
  border: 1px solid #d9dee8;
  border-radius: 6px;
  background: #f8fafc;
  padding: 6px 8px;
  color: #1f2937;
  font-size: 11px;
  line-height: 1.35;
`;

const QMAP_CLIP_MAX_LOCAL_PAIR_EVAL = Math.max(
  50000,
  Number(import.meta.env.VITE_QMAP_AI_CLIP_MAX_LOCAL_PAIR_EVAL || 750000) || 750000
);
const QMAP_ZONAL_MAX_LOCAL_PAIR_EVAL = Math.max(
  50000,
  Number(import.meta.env.VITE_QMAP_AI_ZONAL_MAX_LOCAL_PAIR_EVAL || 600000) || 600000
);
const QMAP_DEFAULT_CHUNK_SIZE = 250;
const QMAP_FIT_MERGE_WINDOW_MS = 120;
const QMAP_AUTO_HIDE_SOURCE_LAYERS =
  String(import.meta.env.VITE_QMAP_AI_AUTO_HIDE_SOURCE_LAYERS || 'true').toLowerCase() !== 'false';
const WAIT_DATASET_RETRY_TRACKER = new Map<
  string,
  {
    failedAttempts: number;
    lastFailureAt: number;
  }
>();
const WAIT_DATASET_RETRY_TTL_MS = 5 * 60 * 1000;

let pendingFitDispatch: any = null;
let pendingFitBounds: QMapBounds | null = null;
let pendingFitTimer: ReturnType<typeof setTimeout> | null = null;

function mergeBounds(a: QMapBounds | null, b: QMapBounds): QMapBounds {
  if (!a) return {...b};
  return {
    minLng: Math.min(a.minLng, b.minLng),
    minLat: Math.min(a.minLat, b.minLat),
    maxLng: Math.max(a.maxLng, b.maxLng),
    maxLat: Math.max(a.maxLat, b.maxLat)
  };
}

function scheduleMergedMapFit(dispatchFn: any, bounds: QMapBounds) {
  if (!hasValidBounds(bounds)) return;
  pendingFitDispatch = dispatchFn;
  pendingFitBounds = mergeBounds(pendingFitBounds, bounds);
  if (pendingFitTimer) return;
  pendingFitTimer = setTimeout(() => {
    const nextDispatch = pendingFitDispatch;
    const nextBounds = pendingFitBounds;
    pendingFitDispatch = null;
    pendingFitBounds = null;
    pendingFitTimer = null;
    if (!nextDispatch || !nextBounds || !hasValidBounds(nextBounds)) return;
    nextDispatch(wrapTo('map', fitBounds([nextBounds.minLng, nextBounds.minLat, nextBounds.maxLng, nextBounds.maxLat])));
  }, QMAP_FIT_MERGE_WINDOW_MS);
}


class AssistantErrorBoundary extends React.Component<
  {children: React.ReactNode},
  {hasError: boolean}
> {
  constructor(props: {children: React.ReactNode}) {
    super(props);
    this.state = {hasError: false};
  }

  static getDerivedStateFromError() {
    return {hasError: true};
  }

  componentDidCatch() {
    // Prevent crashing the entire app when a tool render side-effect throws.
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{padding: 12, color: '#111', fontSize: 11}}>
          AI tool error detected. Reset the chat and retry with a smaller step.
        </div>
      );
    }
    return this.props.children;
  }
}

function WordCloudToolComponent(props: any): JSX.Element | null {
  const words = Array.isArray(props?.words) ? props.words : [];
  if (!words.length) return null;
  return (
    <div style={{padding: 8}}>
      <div style={{fontSize: 11, fontWeight: 600, marginBottom: 6}}>
        {String(props?.title || 'Word Cloud')}
      </div>
      <div style={{display: 'flex', flexWrap: 'wrap', gap: 8, lineHeight: 1.1}}>
        {words.map((w: any, idx: number) => (
          <span
            key={`${String(w?.text || '')}-${idx}`}
            style={{
              fontSize: Number(w?.size || 12),
              fontWeight: Number(w?.value || 0) > 8 ? 700 : 500,
              color: String(w?.color || '#334155')
            }}
            title={`${String(w?.text || '')}: ${Number(w?.value || 0)}`}
          >
            {String(w?.text || '')}
          </span>
        ))}
      </div>
    </div>
  );
}

function CategoryBarsToolComponent(props: any): JSX.Element | null {
  const items = Array.isArray(props?.items) ? props.items : [];
  if (!items.length) return null;
  const maxValue = Math.max(...items.map((it: any) => Number(it?.value || 0)), 1);
  return (
    <div style={{padding: 8}}>
      <div style={{fontSize: 11, fontWeight: 600, marginBottom: 8}}>
        {String(props?.title || 'Category Bars')}
      </div>
      <div style={{display: 'grid', gap: 6}}>
        {items.map((it: any, idx: number) => {
          const label = String(it?.label || '');
          const value = Number(it?.value || 0);
          const widthPct = Math.max(2, Math.round((value / maxValue) * 100));
          return (
            <div key={`${label}-${idx}`} style={{display: 'grid', gridTemplateColumns: '1fr auto', gap: 6}}>
              <div style={{minWidth: 120}}>
                <div style={{fontSize: 10, color: '#0f172a', marginBottom: 2}} title={label}>
                  {label}
                </div>
                <div style={{height: 8, background: '#e2e8f0', borderRadius: 999}}>
                  <div
                    style={{
                      height: 8,
                      width: `${widthPct}%`,
                      background: '#0ea5e9',
                      borderRadius: 999
                    }}
                  />
                </div>
              </div>
              <div style={{fontSize: 10, color: '#334155', alignSelf: 'center'}}>{value}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

const ASSISTANT_AVATAR = '/assistant.png';
type RootState = QMapRootState;
const QMAP_AI_SESSION_STORAGE_KEY = 'qmap_ai_session_id';


function buildFailureNextStepHint(failedDetails: string, objectiveText: string): string {
  const details = String(failedDetails || '').toLowerCase();
  const objective = String(objectiveText || '').toLowerCase();
  if (/ambiguous administrative match|matched multiple levels|livelli multipli|ambiguous name/.test(details)) {
    return 'Prossimo passo consigliato: specifica il livello amministrativo (es. "provincia di Treviso" oppure "comune di Treviso").';
  }
  if (/denominator field .* not found|field .* not found/.test(details)) {
    return 'Prossimo passo consigliato: verifica i campi disponibili del dataset e riallinea numeratore/denominatore prima della colorazione.';
  }
  if (/timeout|limite computazionale|too many intersections|resource limit/.test(details)) {
    return 'Prossimo passo consigliato: riduci la risoluzione H3 (es. r7) oppure limita l\'area di studio, poi rilancia.';
  }
  if (/dataset .* not found|not materialized yet|waitforqmapdataset/.test(details)) {
    return 'Prossimo passo consigliato: attendi la materializzazione del dataset con wait/count e usa datasetRef canonico (id:<datasetId>).';
  }
  if (/(percentuale|percentage|pct|%)/.test(objective)) {
    return 'Prossimo passo consigliato: calcola esplicitamente la metrica normalizzata (percentuale) e riapplica lo stile sul campo derivato.';
  }
  if (/(colora|palette|coroplet|choropleth|scala colore|stile)/.test(objective)) {
    return 'Prossimo passo consigliato: appena il dataset è valido, applica/riprova lo stile (palette + quantili) sul campo corretto.';
  }
  return 'Prossimo passo consigliato: riporta il limite emerso e rilancia un singolo passo correttivo, poi ricontrolla il risultato.';
}

function buildSuccessNextStepHint(objectiveText: string): string {
  const objective = String(objectiveText || '').toLowerCase();
  if (/(boschi|forest)/.test(objective) && /(percentuale|percentage|pct|%)/.test(objective)) {
    return 'Se vuoi proseguire l\'analisi: confronta classifica per percentuale boschi vs area assoluta e verifica le celle estreme.';
  }
  if (/(tassell|tessell|h3)/.test(objective)) {
    return 'Se vuoi proseguire l\'analisi: confronta rapidamente la stessa metrica su r6/r7/r8 per verificare stabilità spaziale.';
  }
  if (/(colora|palette|coroplet|choropleth|scala colore|stile)/.test(objective)) {
    return 'Se vuoi proseguire l\'analisi: prova una palette alternativa o una classificazione diversa (quantili/log) per migliorare leggibilità.';
  }
  return 'Se vuoi proseguire l\'analisi: chiedi un confronto tra aree top/bottom o una classifica sintetica della metrica corrente.';
}

function isSterileCompletionText(text: string): boolean {
  const normalized = String(text || '').trim();
  if (!normalized) return true;
  return (
    /^workflow completato con successo\.?$/i.test(normalized) ||
    /^workflow completato con successo\.\s*dettagli operativi disponibili nel blocco \[executionsummary\]\.?$/i.test(
      normalized
    )
  );
}

function isLowInformationAssistantText(text: string): boolean {
  const normalized = String(text || '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!normalized) return true;
  const lexicalTokens = normalized.match(/[a-z0-9]{3,}/gi) || [];
  return lexicalTokens.length === 0;
}

function buildObjectiveCoverageLine(objectiveText: string): string {
  const objective = String(objectiveText || '').toLowerCase();
  const tags: string[] = [];
  if (/(report|riepilog|output)/.test(objective)) tags.push('report');
  if (/(giurisdizion|ente|amministrativ|comun|provinc)/.test(objective)) tags.push('giurisdizioni');
  if (/(superament|exceed|sogli)/.test(objective)) tags.push('superamenti');
  if (/(priorit|ordin|rank)/.test(objective)) tags.push('priorita');
  if (/(verificabil|consisten|valid|controll)/.test(objective)) tags.push('output verificabile');
  if (!tags.length) return '';
  return `Copertura obiettivo: ${Array.from(new Set(tags)).join(', ')}.`;
}






export default function QMapAiAssistantComponent() {
  const dispatch = useDispatch<any>();
  const store = useStore<RootState>();
  const visState = useSelector(selectQMapVisState);
  const uiLocale = useSelector((state: RootState) => selectQMapUiState(state)?.locale || 'en');
  const activeMode = useSelector((state: RootState) => resolveQMapModeFromUiState(selectQMapUiState(state)));
  const aiAssistant = useSelector(selectQMapAiAssistantState);
  const aiAssistantConfig = useSelector(selectQMapAiAssistantConfig);
  const assistantBaseUrl = resolveQMapAssistantBaseUrl(aiAssistantConfig);
  const getCurrentVisState = () => selectQMapVisState(store.getState());
  const getCurrentUiState = () => selectQMapUiState(store.getState());
  const turnExecutionStateRef = React.useRef<QMapTurnExecutionState>(createTurnExecutionState());
  const datasetLineageRef = React.useRef<Map<string, string>>(new Map());
  const latestProducedDatasetRefRef = React.useRef('');
  const toolsRef = React.useRef<Record<string, any>>({});
  const validatedToolCallIdsRef = React.useRef<Set<string>>(new Set());
  const pendingValidationCountRef = React.useRef(0);
  const mutationIdempotencyCacheRef = React.useRef<Map<string, MutationIdempotencyCacheEntry>>(new Map());
  const statelessToolCallCacheRef = React.useRef<Map<string, StatelessToolCallCacheEntry>>(new Map());
  const toolMutationRevisionRef = React.useRef(0);
  const mutationMutexRef = React.useRef(new AsyncMutex());
  const toolCallCounterRef = React.useRef<Map<string, number>>(new Map());
  const responseBatchTrackerRef = React.useRef({batchId: 0, callsInBatch: 0});
  const nonActionableFailureCacheRef = React.useRef<Map<string, {toolName: string; details: string; failedAtMs: number}>>(
    new Map()
  );
  const runtimeStepsRef = React.useRef<QMapRuntimeStep[]>([]);
  const lastRankContextRef = React.useRef<QMapRankContext | null>(null);
  const [runtimeSteps, setRuntimeSteps] = React.useState<QMapRuntimeStep[]>([]);
  const [runtimeProgressState, setRuntimeProgressState] = React.useState<{
    active: boolean;
    policySummary: string;
    requestId: string;
    chatId: string;
  }>({
    active: false,
    policySummary: '',
    requestId: '',
    chatId: ''
  });

  // Guardrail: repair malformed color ranges that can crash layer rendering/UI.
  const repairedLayerIdsRef = React.useRef<Set<string>>(new Set());
  useEffect(() => {
    const layers = (visState?.layers || []) as any[];
    layers.forEach((layer: any) => {
      const layerId = String(layer?.id || '');
      if (!layerId) return;
      const visConfig = layer?.config?.visConfig || {};
      const invalidColorRange =
        visConfig?.colorRange && !Array.isArray(visConfig?.colorRange?.colors);
      const invalidStrokeRange =
        visConfig?.strokeColorRange && !Array.isArray(visConfig?.strokeColorRange?.colors);
      if (!invalidColorRange && !invalidStrokeRange) return;
      if (repairedLayerIdsRef.current.has(layerId)) return;

      repairedLayerIdsRef.current.add(layerId);
      const nextVisConfig = {
        ...visConfig,
        ...(invalidColorRange ? {colorRange: SAFE_COLOR_RANGE} : {}),
        ...(invalidStrokeRange ? {strokeColorRange: SAFE_COLOR_RANGE} : {})
      };
      dispatch(wrapTo('map', layerConfigChange(layer, {visConfig: nextVisConfig})));
    });
  }, [dispatch, visState?.layers]);

  const toolContext = buildQMapToolContext({
    dispatch,
    store,
    visState,
    aiAssistant,
    aiAssistantConfig,
    activeMode,
    lastRankContextRef,
    scheduleMergedMapFit,
    WordCloudToolComponent,
    CategoryBarsToolComponent
  });
  const modeScopedToolsWithCategoryIntrospection = useToolRegistry(toolContext);

  const upsertRuntimeStep = React.useCallback((step: QMapRuntimeStep) => {
    setRuntimeSteps(prev => {
      const next = [...prev];
      const existingIndex = next.findIndex(item => item.toolCallId === step.toolCallId);
      if (existingIndex >= 0) {
        next[existingIndex] = {...next[existingIndex], ...step};
      } else {
        next.push(step);
      }
      const compact = next.slice(-16);
      runtimeStepsRef.current = compact;
      return compact;
    });
  }, []);

  const runDatasetPostValidation = React.useCallback(
    async ({toolCallId, toolName, datasetName}: {toolCallId: string; toolName: string; datasetName: string}) => {
      const targetDataset = String(datasetName || '').trim();
      const validationStepId = `${toolCallId}:validate`;
      const validationToolLabel = `${toolName} [validate]`;
      if (!targetDataset) {
        upsertRuntimeStep({
          toolCallId: validationStepId,
          toolName: validationToolLabel,
          status: 'failed',
          details: 'Cannot resolve target dataset name for hard validation.'
        });
        upsertRuntimeStep({
          toolCallId,
          toolName,
          status: 'failed',
          details: 'Hard validation failed: missing target dataset name.'
        });
        return;
      }

      pendingValidationCountRef.current += 1;
      setRuntimeProgressState(prev => ({...prev, active: true}));
      upsertRuntimeStep({
        toolCallId: validationStepId,
        toolName: validationToolLabel,
        status: 'running',
        details: `Validating dataset "${targetDataset}" with waitForQMapDataset + countQMapRows.`
      });
      try {
        const waitTool = toolsRef.current?.waitForQMapDataset as any;
        const countTool = toolsRef.current?.countQMapRows as any;
        if (!waitTool || typeof waitTool.execute !== 'function' || !countTool || typeof countTool.execute !== 'function') {
          throw new Error('Validation tools unavailable in runtime.');
        }
        const waitTimeoutMs = resolveValidationTimeoutMs(toolName);
        const waitResult = await waitTool.execute(
          {
            datasetName: targetDataset,
            timeoutMs: waitTimeoutMs
          },
          {
            toolCallId: `${validationStepId}:wait`,
            __qmapInternalValidation: true
          }
        );
        const waitSummary = getToolResultSummary(waitResult) || {};
        if (waitSummary?.success !== true) {
          const waitDetails = String(waitSummary?.details || `waitForQMapDataset failed for "${targetDataset}".`);
          upsertRuntimeStep({
            toolCallId: validationStepId,
            toolName: validationToolLabel,
            status: 'failed',
            details: waitDetails
          });
          upsertRuntimeStep({
            toolCallId,
            toolName,
            status: 'failed',
            details: `Hard validation failed: ${waitDetails}`
          });
          return;
        }

        const countResult = await countTool.execute(
          {
            datasetName: targetDataset
          },
          {
            toolCallId: `${validationStepId}:count`,
            __qmapInternalValidation: true
          }
        );
        const countSummary = getToolResultSummary(countResult) || {};
        if (countSummary?.success !== true) {
          const countDetails = String(countSummary?.details || `countQMapRows failed for "${targetDataset}".`);
          upsertRuntimeStep({
            toolCallId: validationStepId,
            toolName: validationToolLabel,
            status: 'failed',
            details: countDetails
          });
          upsertRuntimeStep({
            toolCallId,
            toolName,
            status: 'failed',
            details: `Hard validation failed: ${countDetails}`
          });
          return;
        }

        const rowCount = Number(countSummary?.count ?? 0);
        if (!Number.isFinite(rowCount) || rowCount <= 0) {
          const emptyDetails = `Hard validation failed: dataset "${targetDataset}" has 0 rows after mutation.`;
          upsertRuntimeStep({
            toolCallId: validationStepId,
            toolName: validationToolLabel,
            status: 'failed',
            details: emptyDetails
          });
          upsertRuntimeStep({
            toolCallId,
            toolName,
            status: 'failed',
            details: emptyDetails
          });
          return;
        }

        const successDetails = `Hard validation passed: dataset "${targetDataset}" has ${rowCount} rows.`;
        upsertRuntimeStep({
          toolCallId: validationStepId,
          toolName: validationToolLabel,
          status: 'success',
          details: successDetails
        });
        upsertRuntimeStep({
          toolCallId,
          toolName,
          status: 'success',
          details: successDetails
        });
      } catch (error) {
        const errorDetails =
          error instanceof Error
            ? error.message
            : `Hard validation failed unexpectedly for dataset "${targetDataset}".`;
        upsertRuntimeStep({
          toolCallId: validationStepId,
          toolName: validationToolLabel,
          status: 'failed',
          details: errorDetails
        });
        upsertRuntimeStep({
          toolCallId,
          toolName,
          status: 'failed',
          details: `Hard validation failed: ${errorDetails}`
        });
      } finally {
        pendingValidationCountRef.current = Math.max(0, pendingValidationCountRef.current - 1);
        if (pendingValidationCountRef.current === 0) {
          const turnState = turnExecutionStateRef.current;
          const datasets = Object.values(getCurrentVisState()?.datasets || {}) as any[];
          turnState.phase = 'execute';
          turnState.snapshotTakenAt = Date.now();
          turnState.snapshotDatasetRefs = new Set(
            datasets.map((dataset: any) => String(dataset?.id || '').trim()).filter(Boolean).map(id => `id:${id}`)
          );
          setRuntimeProgressState(prev => ({...prev, active: false}));
        }
      }
    },
    [getCurrentVisState, upsertRuntimeStep]
  );

  const handleRuntimeToolEvent = React.useCallback((event: QMapToolExecutionEvent) => {
    const turnState = turnExecutionStateRef.current;
    if (event.phase === 'finish' && event.success === true && event.toolName === 'listQMapDatasets') {
      const datasets = Object.values(getCurrentVisState()?.datasets || {}) as any[];
      turnState.phase = 'execute';
      turnState.snapshotTakenAt = Date.now();
      turnState.snapshotDatasetRefs = new Set(
        datasets.map((dataset: any) => String(dataset?.id || '').trim()).filter(Boolean).map(id => `id:${id}`)
      );
    }
    const status =
      event.phase === 'start'
        ? 'running'
        : event.phase === 'blocked'
        ? 'blocked'
        : event.success
        ? 'success'
        : 'failed';
    const details = String(event.details || '').trim() || `Tool "${event.toolName}" ${status}.`;
    upsertRuntimeStep({
      toolCallId: event.toolCallId,
      toolName: event.toolName,
      status,
      details
    });
    if (event.phase === 'start' && !runtimeProgressState.active) {
      setRuntimeProgressState(prev => ({...prev, active: true}));
    }
    if (
      event.phase === 'finish' &&
      event.success === true &&
      event.requiresDatasetValidation &&
      !validatedToolCallIdsRef.current.has(event.toolCallId)
    ) {
      turnState.phase = 'validate';
      validatedToolCallIdsRef.current.add(event.toolCallId);
      void runDatasetPostValidation({
        toolCallId: event.toolCallId,
        toolName: event.toolName,
        datasetName: String(event.datasetName || '')
      });
    }
  }, [getCurrentVisState, runDatasetPostValidation, runtimeProgressState.active, upsertRuntimeStep]);

  const tools = wrapToolsWithPipeline(modeScopedToolsWithCategoryIntrospection, {
    shouldAllowTool: (toolName, args, context) => {
      const normalizedToolName = String(toolName || '').trim();
      const isInternalValidationRun = Boolean((context as any)?.__qmapInternalValidation);
      const bypassStateMachine = Boolean((context as any)?.__qmapBypassTurnStateMachine);
      if (bypassStateMachine) {
        return {allow: true};
      }

      const turnState = turnExecutionStateRef.current;
      const requiresSnapshot = toolRequiresDatasetSnapshot(normalizedToolName);
      if (turnState.phase === 'discover' && requiresSnapshot) {
        return {
          allow: false,
          gateType: 'phase',
          details:
            'Hard-enforce turn state: discovery step is mandatory. ' +
            'Call listQMapDatasets first to capture the current map snapshot, then continue with operational tools.'
        };
      }
      if (requiresSnapshot && turnState.snapshotTakenAt > 0) {
        const snapshotAgeMs = Date.now() - Number(turnState.snapshotTakenAt || 0);
        if (snapshotAgeMs > TURN_DATASET_SNAPSHOT_TTL_MS && normalizedToolName !== 'listQMapDatasets') {
          return {
            allow: false,
            gateType: 'snapshot_expired',
            details:
              `Hard-enforce turn state: dataset snapshot expired after ${Math.round(snapshotAgeMs / 1000)}s. ` +
              'Call listQMapDatasets again before continuing.'
          };
        }
      }
      if (
        turnState.phase === 'validate' &&
        pendingValidationCountRef.current > 0 &&
        !TURN_STATE_VALIDATE_GATE_ALLOWLIST.has(normalizedToolName)
      ) {
        return {
          allow: false,
          gateType: 'phase',
          details:
            'Hard-enforce turn state: validation is in progress for a dataset mutation. ' +
            'Wait for validation to complete before issuing new operational tools.'
        };
      }
      if (!isInternalValidationRun && normalizedToolName === 'waitForQMapDataset') {
        const targetDataset = String((args as any)?.datasetName || '').trim();
        if (targetDataset && !targetDataset.toLowerCase().startsWith('id:')) {
          const candidates = findDatasetCandidatesByName(getCurrentVisState()?.datasets || {}, targetDataset);
          if (candidates.length > 1) {
            return {
              allow: false,
              gateType: 'ambiguous_ref',
              details:
                `Hard-enforce datasetRef: waitForQMapDataset received ambiguous datasetName "${targetDataset}". ` +
                'Use canonical datasetRef (id:<datasetId>) from listQMapDatasets.'
            };
          }
          if (candidates.length === 0) {
            return {
              allow: false,
              gateType: 'ambiguous_ref',
              details:
                `Hard-enforce datasetRef: waitForQMapDataset requires canonical datasetRef when target is not materialized yet ("${targetDataset}"). ` +
                'Use id:<datasetId> from the mutation tool result.'
            };
          }
        }
      }
      return {allow: true};
    },
    onToolEvent: handleRuntimeToolEvent,
    resolveCanonicalDatasetRef: datasetCandidate =>
      resolveCanonicalDatasetRefWithLineage(
        datasetLineageRef.current,
        (getCurrentVisState()?.datasets || {}) as Record<string, unknown>,
        datasetCandidate
      ),
    resolveFallbackDatasetRef: () => {
      const currentDatasets = (getCurrentVisState()?.datasets || {}) as Record<string, unknown>;
      return (
        resolveCanonicalDatasetRefWithLineage(
          datasetLineageRef.current,
          currentDatasets,
          latestProducedDatasetRefRef.current
        ) || String(latestProducedDatasetRefRef.current || '').trim()
      );
    },
    onNormalizedToolResult: (_toolName, normalizedResult) => {
      const currentDatasets = (getCurrentVisState()?.datasets || {}) as Record<string, unknown>;
      syncDatasetLineageFromCurrentDatasets(datasetLineageRef.current, currentDatasets);
      updateDatasetLineageFromToolResult(datasetLineageRef.current, currentDatasets, normalizedResult);
      const producedRefs = extractProducedDatasetRefsFromNormalizedResult(normalizedResult);
      const canonicalProducedRefs = producedRefs
        .map(ref => resolveCanonicalDatasetRefWithLineage(datasetLineageRef.current, currentDatasets, ref))
        .filter(ref => Boolean(String(ref || '').trim()));
      const latestProducedDatasetRef =
        canonicalProducedRefs[canonicalProducedRefs.length - 1] || '';
      if (latestProducedDatasetRef) {
        latestProducedDatasetRefRef.current = String(latestProducedDatasetRef || '').trim();
      }
    },
    mutationIdempotencyCache: mutationIdempotencyCacheRef.current,
    nonActionableFailureCache: nonActionableFailureCacheRef.current,
    statelessToolCallCache: statelessToolCallCacheRef.current,
    mutationRevisionRef: toolMutationRevisionRef,
    turnExecutionStateRef,
    mutationMutex: mutationMutexRef.current,
    toolCallCounter: toolCallCounterRef.current,
    responseBatchTracker: responseBatchTrackerRef
  });

  React.useEffect(() => {
    toolsRef.current = tools;
  }, [tools]);

  const toolQueueRef = React.useRef<Promise<unknown>>(Promise.resolve());
  const [e2eToolRuns, setE2eToolRuns] = React.useState<
    Array<{
      id: string;
      toolName: string;
      args: Record<string, unknown>;
      result: any;
      additionalData: Record<string, unknown> | null;
      component: React.ElementType | null;
    }>
  >([]);
  const e2eEnabled =
    typeof window !== 'undefined' && Boolean((window as any).__QMAP_E2E_TOOLS__);

  React.useEffect(() => {
    if (!e2eEnabled) return;
    const globalObj = globalThis as typeof globalThis & {
      __qmapRunTool?: (toolName: string, args?: Record<string, unknown>) => Promise<any>;
      __qmapRunToolWithStateMachine?: (toolName: string, args?: Record<string, unknown>) => Promise<any>;
      __qmapGetDatasets?: () => any;
      __qmapGetLayers?: () => any;
      __qmapSelectFirstFeature?: (datasetName: string) => any;
    };

    const enqueue = <T,>(fn: () => Promise<T>) => {
      const next = toolQueueRef.current.then(fn, fn);
      toolQueueRef.current = next.catch(() => undefined);
      return next;
    };

    globalObj.__qmapRunTool = async (toolName: string, args?: Record<string, unknown>) =>
      enqueue(async () => {
        const tool = toolsRef.current?.[toolName as keyof typeof toolsRef.current] as any;
        if (!tool || typeof tool.execute !== 'function') {
          throw new Error(`Unknown q-map tool: ${toolName}`);
        }
        const toolCallId = makeExecutionKey(`e2e-${toolName}`);
        const result = await tool.execute(args || {}, {
          toolCallId,
          context: tool.context,
          __qmapBypassTurnStateMachine: true
        });
        if (typeof tool.onToolCompleted === 'function') {
          try {
            tool.onToolCompleted(toolCallId, result?.additionalData);
          } catch {
            // swallow tool cache errors in e2e runner
          }
        }
        setE2eToolRuns(prev => [
          ...prev,
          {
            id: toolCallId,
            toolName,
            args: args || {},
            result: getToolResultSummary(result),
            additionalData: (result?.additionalData as Record<string, unknown>) || null,
            component: tool.component || null
          }
        ]);
        return {
          toolName,
          toolCallId,
          result: getToolResultSummary(result),
          additionalData: result?.additionalData || null
        };
      });

    globalObj.__qmapRunToolWithStateMachine = async (toolName: string, args?: Record<string, unknown>) =>
      enqueue(async () => {
        const tool = toolsRef.current?.[toolName as keyof typeof toolsRef.current] as any;
        if (!tool || typeof tool.execute !== 'function') {
          throw new Error(`Unknown q-map tool: ${toolName}`);
        }
        const toolCallId = makeExecutionKey(`e2e-sm-${toolName}`);
        const result = await tool.execute(args || {}, {
          toolCallId,
          context: tool.context
        });
        // Return llmResult directly so e2e tests can inspect phase metadata
        const llmResult = result?.llmResult && typeof result.llmResult === 'object'
          ? result.llmResult
          : getToolResultSummary(result);
        return {
          toolName,
          toolCallId,
          result: llmResult,
          additionalData: result?.additionalData || null
        };
      });

    globalObj.__qmapGetDatasets = () => {
      const currentVisState = getCurrentVisState();
      return Object.values(currentVisState?.datasets || {}).map((dataset: any) => ({
        id: dataset?.id || '',
        name: dataset?.label || dataset?.id || '',
        rowCount: Number(dataset?.length || 0),
        fields: (dataset?.fields || []).map((f: any) => f?.name).filter(Boolean)
      }));
    };

    globalObj.__qmapGetLayers = () => {
      const currentVisState = getCurrentVisState();
      return (currentVisState?.layers || []).map((layer: any) => ({
        id: layer?.id || '',
        name: layer?.config?.label || layer?.id || '',
        datasetId: layer?.config?.dataId || '',
        type: layer?.type || ''
      }));
    };

    globalObj.__qmapSelectFirstFeature = (datasetName: string) => {
      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {success: false, details: `Dataset "${datasetName}" not found.`};
      }
      const geometryField = resolveGeojsonFieldName(dataset, null);
      const idx = getDatasetIndexes(dataset);
      if (!idx.length) {
        return {success: false, details: `Dataset "${dataset.label || dataset.id}" has no rows.`};
      }

      let feature: any = null;
      if (geometryField) {
        const raw = dataset.getValue(geometryField, idx[0]);
        const parsed = parseGeoJsonLike(raw);
        if (parsed?.type === 'FeatureCollection') {
          feature = Array.isArray(parsed.features) ? parsed.features[0] : null;
        } else if (parsed?.type === 'Feature') {
          feature = parsed;
        } else if (parsed?.type) {
          feature = {type: 'Feature', properties: {}, geometry: parsed};
        }
      } else {
        const pointFieldPair = resolveDatasetPointFieldPair(dataset);
        if (pointFieldPair.latField && pointFieldPair.lonField) {
          for (const rowIdx of idx) {
            const lat = parseCoordinateValue(dataset.getValue(pointFieldPair.latField, rowIdx));
            const lon = parseCoordinateValue(dataset.getValue(pointFieldPair.lonField, rowIdx));
            if (lat === null || lon === null) continue;
            if (Math.abs(lat) > 90 || Math.abs(lon) > 180) continue;
            feature = {
              type: 'Feature',
              properties: {},
              geometry: {type: 'Point', coordinates: [lon, lat]}
            };
            break;
          }
        }
      }
      if (!feature?.geometry) {
        return {
          success: false,
          details:
            `Dataset "${dataset.label || dataset.id}" has no usable geometry.` +
            ' Expected geojson field or valid lat/lon columns.'
        };
      }
      dispatch(wrapTo('map', setSelectedFeature(feature)));
      return {
        success: true,
        dataset: dataset.label || dataset.id,
        geometryField: geometryField || 'lat/lon->point'
      };
    };

    return () => {
      delete globalObj.__qmapRunTool;
      delete globalObj.__qmapGetDatasets;
      delete globalObj.__qmapGetLayers;
      delete globalObj.__qmapSelectFirstFeature;
    };
  }, [dispatch, e2eEnabled, getCurrentVisState]);

  const strictMode = String(import.meta.env.VITE_QMAP_AI_STRICT_MODE || 'true').toLowerCase() !== 'false';
  const runtimeHints = React.useMemo(() => buildRuntimeDatasetHints(visState), [visState?.datasets, visState?.layers]);
  const qMapSessionId = React.useMemo(() => {
    if (typeof window === 'undefined') return '';
    try {
      const existing = String(window.sessionStorage.getItem(QMAP_AI_SESSION_STORAGE_KEY) || '').trim();
      if (existing) return existing;
      const generated =
        typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
          ? crypto.randomUUID()
          : `qmap-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
      window.sessionStorage.setItem(QMAP_AI_SESSION_STORAGE_KEY, generated);
      return generated;
    } catch (error) {
      console.warn('Failed to initialize q-map AI session id', error);
      return '';
    }
  }, []);
  const requestHeaders = React.useMemo<Record<string, string>>(() => {
    const headers: Record<string, string> = {};
    const authorizationHeader = resolveQMapAuthorizationHeader();
    if (authorizationHeader) {
      headers.Authorization = authorizationHeader;
    }
    if (qMapSessionId) {
      headers[Q_ASSISTANT_SESSION_HEADER] = qMapSessionId;
    }
    if (!QMAP_CONTEXT_HEADER_ENABLED) return headers;
    const contextHeader = buildQMapContextHeaderValue(visState, qMapSessionId);
    if (!contextHeader) return headers;
    headers[QMAP_CONTEXT_HEADER] = contextHeader;
    return headers;
  }, [qMapSessionId, visState?.datasets, visState?.layers, visState?.filters, visState?.mapState]);

  // Use a fixed OpenAI-compatible provider on the client and proxy through q-assistant.
  // q-assistant overrides the model/provider upstream; client values are placeholders only.
  // Keep placeholder model aligned with project default to avoid confusing traces/audits.
  const ASSISTANT_PROVIDER = 'openai';
  const ASSISTANT_MODEL = 'google/gemini-3-flash-preview';
  const ASSISTANT_API_KEY_PLACEHOLDER = 'server';

  const assistantProps = {
    name: 'q-hive-ai-assistant',
    description: 'A q-hive AI Assistant',
    version: '0.0.2',
    modelProvider: ASSISTANT_PROVIDER,
    model: ASSISTANT_MODEL,
    apiKey: ASSISTANT_API_KEY_PLACEHOLDER,
    baseUrl: assistantBaseUrl,
    headers: requestHeaders,
    tools
  };
  const modePromptOverlay = React.useMemo(
    () =>
      buildQMapAiModePromptOverlay({
        mode: activeMode,
        locale: uiLocale,
        availableToolNames: Object.keys(tools || {}).sort()
      }),
    [activeMode, uiLocale, tools]
  );
  const instructions = React.useMemo(
    () =>
      [buildQMapSystemPrompt({locale: uiLocale, strictMode}), modePromptOverlay, runtimeHints]
        .filter(Boolean)
        .join('\n\n'),
    [uiLocale, strictMode, modePromptOverlay, runtimeHints]
  );

  const pendingRequestIdRef = React.useRef<string | null>(null);
  const pendingChatIdRef = React.useRef<string | null>(null);
  const lastAppliedRequestIdRef = React.useRef<string | null>(null);
  const turnObjectiveTextRef = React.useRef<string>('');

  React.useEffect(() => {
    const globalObj = globalThis as typeof globalThis & {
      fetch: typeof fetch;
    };
    if (typeof globalObj.fetch !== 'function') return;

    const originalFetch = globalObj.fetch.bind(globalObj);
    const normalizedBaseUrl = assistantBaseUrl.replace(/\/+$/, '');

    const readRequestUrl = (input: RequestInfo | URL): string => {
      if (typeof input === 'string') return input;
      if (input instanceof URL) return input.toString();
      if (typeof Request !== 'undefined' && input instanceof Request) return input.url;
      return '';
    };

    const withAssistantHeaders = (
      input: RequestInfo | URL,
      init?: RequestInit
    ): {input: RequestInfo | URL; init?: RequestInit} => {
      const assistantAuthHeader = resolveQMapAuthorizationHeader();
      const baseHeaders = new Headers(
        typeof Request !== 'undefined' && input instanceof Request ? input.headers : undefined
      );
      const initHeaders = new Headers(init?.headers || undefined);
      initHeaders.forEach((value, key) => baseHeaders.set(key, value));

      if (assistantAuthHeader) {
        baseHeaders.set('Authorization', assistantAuthHeader);
      } else {
        baseHeaders.delete('Authorization');
      }
      if (qMapSessionId) {
        baseHeaders.set(Q_ASSISTANT_SESSION_HEADER, qMapSessionId);
      }
      if (QMAP_CONTEXT_HEADER_ENABLED) {
        const contextHeader = buildQMapContextHeaderValue(getCurrentVisState(), qMapSessionId);
        if (contextHeader) {
          baseHeaders.set(QMAP_CONTEXT_HEADER, contextHeader);
        } else {
          baseHeaders.delete(QMAP_CONTEXT_HEADER);
        }
      }

      if (typeof Request !== 'undefined' && input instanceof Request) {
        return {input: new Request(input, {headers: baseHeaders}), init};
      }
      return {
        input,
        init: {
          ...(init || {}),
          headers: baseHeaders
        }
      };
    };

    const patchedFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      try {
        const requestUrl = readRequestUrl(input);
        const isAssistantPath = requestUrl.includes('/chat/completions') || requestUrl.endsWith('/chat');
        const isAssistantCall =
          isAssistantPath &&
          (requestUrl.startsWith(normalizedBaseUrl) || requestUrl.startsWith('/') || requestUrl.startsWith('http'));
        if (isAssistantCall) {
          pendingRequestIdRef.current = null;
          pendingChatIdRef.current = null;
          const datasets = Object.values(getCurrentVisState()?.datasets || {}) as any[];
          turnExecutionStateRef.current = {
            phase: 'execute',
            snapshotTakenAt: Date.now(),
            snapshotDatasetRefs: new Set(
              datasets.map((dataset: any) => String(dataset?.id || '').trim()).filter(Boolean).map(id => `id:${id}`)
            )
          };
          validatedToolCallIdsRef.current.clear();
          pendingValidationCountRef.current = 0;
          mutationIdempotencyCacheRef.current.clear();
          statelessToolCallCacheRef.current.clear();
          toolMutationRevisionRef.current = 0;
          nonActionableFailureCacheRef.current.clear();
          // toolCallCounterRef is NOT cleared here — it must persist across
          // sub-requests to enforce the per-tool circuit breaker. It is only
          // cleared when a new user message arrives (see userText check below).
          datasetLineageRef.current.clear();
          latestProducedDatasetRefRef.current = '';
          syncDatasetLineageFromCurrentDatasets(
            datasetLineageRef.current,
            (getCurrentVisState()?.datasets || {}) as Record<string, unknown>
          );
          runtimeStepsRef.current = [];
          setRuntimeSteps([]);
          const parsedRequestBody = await parseAssistantRequestBody(input, init);
          const userText = extractToolPolicyUserText(parsedRequestBody);
          turnObjectiveTextRef.current = userText;
          if (userText) {
            // New user message — reset circuit breaker counter
            toolCallCounterRef.current.clear();
            setRuntimeProgressState({
              active: true,
              policySummary: '',
              requestId: '',
              chatId: ''
            });
          } else {
            setRuntimeProgressState({
              active: false,
              policySummary: '',
              requestId: '',
              chatId: ''
            });
          }
        }
      } catch (error) {
        console.warn('Failed to initialize assistant turn context from request body', error);
      }

      const requestUrl = readRequestUrl(input);
      const isAssistantPath = requestUrl.includes('/chat/completions') || requestUrl.endsWith('/chat');
      const isAssistantCall =
        isAssistantPath &&
        (requestUrl.startsWith(normalizedBaseUrl) || requestUrl.startsWith('/') || requestUrl.startsWith('http'));
      const fetchArgs = isAssistantCall ? withAssistantHeaders(input, init) : {input, init};
      const response = await originalFetch(fetchArgs.input, fetchArgs.init);

      try {
        const responseUrl = readRequestUrl(fetchArgs.input);
        const isAssistantPath = responseUrl.includes('/chat/completions') || responseUrl.endsWith('/chat');
        const isAssistantCall =
          isAssistantPath &&
          (responseUrl.startsWith(normalizedBaseUrl) ||
            responseUrl.startsWith('/') ||
            responseUrl.startsWith('http'));
        if (isAssistantCall) {
          // Reset batch tracker so the next batch of tool calls starts fresh.
          // Only the first tool call per LLM response will execute; the rest
          // are skipped by the pipeline's batch truncation gate.
          responseBatchTrackerRef.current = {
            batchId: responseBatchTrackerRef.current.batchId + 1,
            callsInBatch: 0
          };
          const requestId = response.headers.get('x-q-assistant-request-id');
          const chatId = response.headers.get(Q_ASSISTANT_CHAT_HEADER);
          const runtimePolicySummary = response.headers.get(Q_ASSISTANT_RUNTIME_POLICY_HEADER);
          if (requestId) {
            pendingRequestIdRef.current = requestId;
            setRuntimeProgressState(prev => ({...prev, requestId}));
          }
          if (chatId) {
            pendingChatIdRef.current = chatId;
            setRuntimeProgressState(prev => ({...prev, chatId}));
          }
          if (runtimePolicySummary) {
            setRuntimeProgressState(prev => ({
              ...prev,
              active: true,
              policySummary: runtimePolicySummary
            }));
          }
        }
      } catch (error) {
        console.warn('Failed to capture q-assistant trace headers from response', error);
      }
      return response;
    }) as typeof fetch;
    globalObj.fetch = patchedFetch;

    return () => {
      if (globalObj.fetch === patchedFetch) {
        globalObj.fetch = originalFetch;
      }
    };
  }, [activeMode, assistantBaseUrl, getCurrentVisState]);

  const onMessagesUpdated = (messages: MessageModel[]) => {
    setRuntimeProgressState(prev => ({...prev, active: pendingValidationCountRef.current > 0}));
    const incomingDiagnostics = messages.map(message => {
      if (message?.direction !== 'incoming') return {hasToolPart: false, diagnosticOnly: false};
      const parts = (message as any)?.messageContent?.parts;
      if (!Array.isArray(parts) || parts.length === 0) return {hasToolPart: false, diagnosticOnly: false};
      const hasToolPart = parts.some((part: any) => part?.type === 'tool-invocation');
      if (hasToolPart) return {hasToolPart: true, diagnosticOnly: false};
      const text = parts
        .filter((part: any) => part?.type === 'text')
        .map((part: any) => String(part?.text || '').trim())
        .filter(Boolean)
        .join('\n');
      if (!text) return {hasToolPart: false, diagnosticOnly: false};
      return {hasToolPart: false, diagnosticOnly: textIsRuntimeDiagnosticOnly(text)};
    });
    const hasNonDiagnosticIncoming = messages.some((message, index) => {
      if (message?.direction !== 'incoming') return false;
      const meta = incomingDiagnostics[index];
      return !meta?.diagnosticOnly;
    });
    const normalizedMessages = messages.filter((message, index) => {
      if (message?.direction !== 'incoming') return true;
      const meta = incomingDiagnostics[index];
      if (!meta) return true;
      if (meta.hasToolPart) return true;
      if (!meta.diagnosticOnly) return true;
      return !hasNonDiagnosticIncoming;
    });
    const turnState = turnExecutionStateRef.current;
    const requestId = pendingRequestIdRef.current;
    const assistantIndex = (() => {
      for (let i = normalizedMessages.length - 1; i >= 0; i -= 1) {
        if (normalizedMessages[i]?.direction === 'incoming') return i;
      }
      return -1;
    })();
    if (assistantIndex >= 0 && requestId) {
      const targetMessage = normalizedMessages[assistantIndex];
      const parts = targetMessage?.messageContent?.parts;
      const marker = `[requestId: ${requestId}]`;
      const toolParts = Array.isArray(parts) ? parts.filter(part => part?.type === 'tool-invocation') : [];
      const invocationRuns = extractInvocationResultSummaries(parts || []);
      const failedRuns = invocationRuns.filter(run => run.success === false);
      const latestFailedRun = failedRuns.length > 0 ? failedRuns[failedRuns.length - 1] : null;
      const finishedCount = toolParts.filter((part: any) => part?.toolInvocation?.state === 'result').length;
      const isBatchSkippedResult = (result: any) => {
        const d = String(result?.details || result?.llmResult?.details || '').toLowerCase();
        return d.includes('skipped') && d.includes('batch');
      };
      const failedCount = toolParts.filter((part: any) => {
        if (part?.toolInvocation?.state !== 'result') return false;
        const result = part?.toolInvocation?.result;
        const isFailed = result?.success === false || result?.llmResult?.success === false;
        return isFailed && !isBatchSkippedResult(result);
      }).length;
      const progressLine =
        toolParts.length > 0
          ? `[progress] steps=${finishedCount}/${toolParts.length}${failedCount > 0 ? ` failed=${failedCount}` : ''}`
          : '';
      const validationFailures = runtimeStepsRef.current.filter(
        step => step.status === 'failed' && step.toolName.includes('[validate]')
      ).length;
      const validationLine = validationFailures > 0 ? `[validation] failed=${validationFailures}` : '';
      let executionStats = computeAssistantExecutionStats({
        runs: invocationRuns,
        totalToolCalls: toolParts.length,
        validationFailures
      });
      const styleSuccess = countSuccessfulStyleRuns(invocationRuns);
      const styleFailed = countFailedStyleRuns(invocationRuns);
      const stylingRequiredByObjective = textRequestsStylingObjective(turnObjectiveTextRef.current);
      const stylingObjectiveNotMet = stylingRequiredByObjective && styleSuccess <= 0;
      if (stylingObjectiveNotMet && executionStats.status === 'success') {
        executionStats = {
          ...executionStats,
          status: 'partial'
        };
      }
      const allTextParts = Array.isArray(parts)
        ? parts
            .filter(part => part?.type === 'text')
            .map(part => String((part as any)?.text || ''))
            .filter(Boolean)
        : [];
      const combinedTextParts = allTextParts.join('\n');
      const subRequestIds = Array.isArray(parts) ? extractSubRequestIdsFromText(combinedTextParts, requestId) : [];
      const chatId = String(pendingChatIdRef.current || runtimeProgressState.chatId || qMapSessionId || '').trim();
      const summaryLine = buildExecutionSummaryLine(requestId, executionStats, subRequestIds, chatId);
      if (pendingValidationCountRef.current === 0 && toolParts.length > 0 && finishedCount >= toolParts.length) {
        turnState.phase = 'finalize';
      }
      if (Array.isArray(parts)) {
        const textIndexes = parts
          .map((part, index) => ({part, index}))
          .filter(entry => entry.part?.type === 'text')
          .map(entry => entry.index);
        const textIndex = (() => {
          for (let i = textIndexes.length - 1; i >= 0; i -= 1) {
            const idx = textIndexes[i];
            const candidate = String((parts[idx] as any)?.text || '');
            if (String(stripRuntimeDiagnosticLines(candidate) || '').trim()) {
              return idx;
            }
          }
          return textIndexes.length > 0 ? textIndexes[textIndexes.length - 1] : -1;
        })();
        if (textIndex >= 0) {
          const currentText = String((parts[textIndex] as any)?.text || '');
          let strippedText = collapseRepeatedNarrativeBlocks(stripRuntimeDiagnosticLines(currentText));
          const guardrailLines: string[] = [];
          const failureHint = buildFailureNextStepHint(String(latestFailedRun?.details || ''), turnObjectiveTextRef.current);
          const successHint = buildSuccessNextStepHint(turnObjectiveTextRef.current);
          const objectiveCoverageLine = buildObjectiveCoverageLine(turnObjectiveTextRef.current);
          const completionClaimBlocked = textClaimsWorkflowCompleted(strippedText) && executionStats.status !== 'success';
          if (completionClaimBlocked) {
            guardrailLines.push(
              `[guardrail] completion_claim_blocked: execution_status=${executionStats.status} failed_steps=${executionStats.failed}`
            );
            strippedText = stripUnverifiedCompletionClaimLines(strippedText);
            if (!strippedText) {
              strippedText =
                executionStats.status === 'failed'
                  ? `Workflow non completato: tutti i passaggi tool sono falliti. ${failureHint}`
                  : `Workflow completato solo parzialmente: alcuni passaggi tool sono falliti. ${failureHint}`;
            }
          }
          const centeringClaimBlocked = textClaimsCentering(strippedText) && executionStats.fitSuccess <= 0;
          if (centeringClaimBlocked) {
            guardrailLines.push(
              `[guardrail] centering_claim_blocked: fitQMapToDataset_success=0 fitQMapToDataset_failed=${executionStats.fitFailed}`
            );
            strippedText = stripUnverifiedCenteringClaimLines(strippedText);
            if (!strippedText) {
              strippedText =
                'Centratura mappa non verificata dai risultati tool. Verifica fitQMapToDataset prima di confermare.';
            }
          }
          const stylingClaimBlocked = (textClaimsStyling(strippedText) && styleSuccess <= 0) || stylingObjectiveNotMet;
          if (stylingClaimBlocked) {
            guardrailLines.push(
              `[guardrail] styling_claim_blocked: style_success=${styleSuccess} style_failed=${styleFailed} required_by_objective=${stylingRequiredByObjective}`
            );
            strippedText = stripUnverifiedStylingClaimLines(strippedText);
            if (stylingObjectiveNotMet && !String(strippedText || '').trim()) {
              strippedText =
                `Workflow completato parzialmente: la richiesta richiedeva una colorazione/stile, ma nessun tool di stile risulta applicato con successo. ${failureHint}`;
            } else if (!strippedText) {
              strippedText =
                `Applicazione stile non verificata dai risultati tool. ${failureHint}`;
            }
          }
          const workflowCompleted =
            pendingValidationCountRef.current === 0 && toolParts.length > 0 && finishedCount >= toolParts.length;
          if (!String(strippedText || '').trim() && workflowCompleted) {
            if (executionStats.status === 'success') {
              strippedText =
                `Workflow completato con successo. Dettagli operativi disponibili nel blocco [executionSummary]. ${successHint}`;
            } else if (executionStats.status === 'failed') {
              const failedToolDetail = latestFailedRun
                ? ` Ultimo errore: ${latestFailedRun.toolName} - ${String(latestFailedRun.details || 'errore non specificato.')}`
                : '';
              strippedText =
                'Workflow non completato: i passaggi operativi sono falliti.' +
                failedToolDetail +
                ` ${failureHint}`;
            } else {
              const failedToolDetail = latestFailedRun
                ? ` Limite rilevato: ${latestFailedRun.toolName} - ${String(latestFailedRun.details || 'errore non specificato.')}`
                : '';
              strippedText =
                'Workflow completato parzialmente: alcuni passaggi non sono riusciti.' +
                failedToolDetail +
                ` ${failureHint}`;
            }
          }
          if (workflowCompleted && executionStats.status === 'success' && isLowInformationAssistantText(strippedText)) {
            const completionDetail = objectiveCoverageLine || 'Dettagli operativi disponibili nel blocco [executionSummary].';
            strippedText = `Workflow completato con successo. ${completionDetail} ${successHint}`.trim();
          }
          if (workflowCompleted && executionStats.status === 'success' && isSterileCompletionText(strippedText)) {
            strippedText = `${strippedText} ${successHint}`.trim();
          }
          if (workflowCompleted && executionStats.status !== 'success') {
            const sanitizedNonSuccessText = stripContradictoryNonSuccessClaimLines(strippedText);
            if (sanitizedNonSuccessText !== strippedText) {
              guardrailLines.push(
                `[guardrail] contradictory_success_claim_blocked: execution_status=${executionStats.status} failed_steps=${executionStats.failed}`
              );
              strippedText = sanitizedNonSuccessText;
            }
          }
          if (workflowCompleted && executionStats.status !== 'success' && !textAcknowledgesNonSuccessOutcome(strippedText)) {
            guardrailLines.push(
              `[guardrail] non_success_outcome_annotation: execution_status=${executionStats.status} failed_steps=${executionStats.failed}`
            );
            const outcomeNote =
              executionStats.status === 'failed'
                ? `Nota audit: esecuzione non riuscita, i passaggi tool non sono stati completati con successo. ${failureHint}`
                : `Nota audit: esecuzione parziale, almeno un passaggio tool risulta fallito. ${failureHint}`;
            strippedText = [outcomeNote, strippedText].filter(Boolean).join('\n\n').trim();
          }
          if (
            workflowCompleted &&
            executionStats.status !== 'success' &&
            !/prossimo passo consigliato:/i.test(strippedText) &&
            /(workflow completato parzialmente|workflow non completato|nota audit:)/i.test(strippedText)
          ) {
            strippedText = `${strippedText} ${failureHint}`.trim();
          }
          strippedText = collapseRepeatedNarrativeBlocks(strippedText);
          const guardrailLine = guardrailLines.join('\n');
          const prefix = [marker, progressLine, validationLine, summaryLine, guardrailLine]
            .filter(Boolean)
            .join('\n');
          (parts[textIndex] as any).text = prefix ? `${prefix}\n${strippedText}`.trim() : strippedText;
          for (let i = textIndexes.length - 1; i >= 0; i -= 1) {
            const idx = textIndexes[i];
            if (idx !== textIndex) {
              parts.splice(idx, 1);
            }
          }
        } else {
          const prefix = [marker, progressLine, validationLine, summaryLine].filter(Boolean).join('\n');
          if (prefix) parts.unshift({type: 'text', text: prefix});
        }
      } else if (targetMessage?.messageContent) {
        const prefix = [marker, progressLine, validationLine, summaryLine].filter(Boolean).join('\n');
        targetMessage.messageContent.parts = prefix ? [{type: 'text', text: prefix}] : [];
      } else {
        const prefix = [marker, progressLine, validationLine, summaryLine].filter(Boolean).join('\n');
        targetMessage.messageContent = {parts: prefix ? [{type: 'text', text: prefix}] : []};
      }
      lastAppliedRequestIdRef.current = requestId;
    }
    dispatch(updateAiAssistantMessages(normalizedMessages));
  };

  return (
    <StyledAssistant className="ai-assistant-component">
      <AssistantErrorBoundary>
        <AiAssistant
          {...assistantProps}
          instructions={instructions}
          welcomeMessage={null}
          theme="light"
          assistantAvatar={ASSISTANT_AVATAR}
          temperature={aiAssistant?.config?.temperature || 0}
          topP={aiAssistant?.config?.topP}
          initialMessages={aiAssistant?.messages || []}
          onMessagesUpdated={onMessagesUpdated}
          fontSize="text-tiny"
        />
        {runtimeProgressState.active || runtimeSteps.length ? (
          <RuntimeProgressPanel data-testid="qmap-runtime-progress">
            <div>
              <strong>Runtime status</strong>
              {runtimeProgressState.requestId ? ` | requestId: ${runtimeProgressState.requestId}` : ''}
              {runtimeProgressState.chatId ? ` | chatId: ${runtimeProgressState.chatId}` : ''}
            </div>
            {runtimeProgressState.policySummary ? <div>{runtimeProgressState.policySummary}</div> : null}
            {runtimeSteps.length ? (
              <div>
                {runtimeSteps.map(step => {
                  const statusLabel =
                    step.status === 'running'
                      ? 'running'
                      : step.status === 'success'
                      ? 'done'
                      : step.status === 'blocked'
                      ? 'blocked'
                      : 'failed';
                  const details = step.details ? ` | ${step.details}` : '';
                  return (
                    <div key={step.toolCallId}>
                      {step.toolName}: {statusLabel}
                      {details}
                    </div>
                  );
                })}
              </div>
            ) : (
              <div>Waiting for first tool call...</div>
            )}
          </RuntimeProgressPanel>
        ) : null}
        {e2eEnabled ? (
          <div style={{display: 'none'}}>
            {e2eToolRuns.map(run => {
              const Component = run.component as React.ElementType | null;
              if (!Component) return null;
              const props = {
                ...(run.args || {}),
                ...(run.additionalData || {})
              } as Record<string, unknown>;
              return <Component key={run.id} {...props} />;
            })}
          </div>
        ) : null}
      </AssistantErrorBoundary>
    </StyledAssistant>
  );
}
