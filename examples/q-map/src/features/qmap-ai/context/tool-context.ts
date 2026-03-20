import type {MutableRefObject} from 'react';
import type {
  QMapDatasetOps,
  QMapNumericOps,
  QMapColorOps,
  QMapGeometryOps,
  QMapMergeOps,
  QMapUpsertOps,
  QMapToolRuntimeHelpers,
  QMapFieldClassification,
  QMapMcpCloud,
  QMapZodSchemas,
  QMapThirdPartyTurf,
  QMapThirdPartyH3,
  QMapThirdPartyProj4,
  QMapWorkerOps,
  QMapH3PaintOps,
  QMapToolComponents,
  QMapKeplerActions,
  QMapKeplerConstants,
  QMapConfigConstants
} from './types';

/**
 * Consolidated dependency context for all q-map AI tool builder factories.
 *
 * Replaces the 15-25 ad-hoc parameters previously passed to each tool factory.
 * Each tool builder receives a single `QMapToolContext` and destructures what it needs.
 *
 * Composed from focused sub-interfaces defined in ./types.ts.
 * All function properties are typed as `any` to match the existing loose typing across
 * tool builders. This can be tightened incrementally in later phases.
 */
export interface QMapToolContext
  extends QMapDatasetOps,
    QMapNumericOps,
    QMapColorOps,
    QMapGeometryOps,
    QMapMergeOps,
    QMapUpsertOps,
    QMapToolRuntimeHelpers,
    QMapFieldClassification,
    QMapMcpCloud,
    QMapZodSchemas,
    QMapThirdPartyTurf,
    QMapThirdPartyH3,
    QMapThirdPartyProj4,
    QMapWorkerOps,
    QMapH3PaintOps,
    QMapToolComponents,
    QMapKeplerActions,
    QMapKeplerConstants,
    QMapConfigConstants {
  // ─── Runtime state (unique, not grouped) ─────────────────────────────────
  dispatch: any;
  getCurrentVisState: () => any;
  getCurrentUiState: () => any;
  assistantBaseUrl: string;
  visState: any;
  aiAssistant: any;
  activeMode: any;

  // ─── Refs ────────────────────────────────────────────────────────────────
  lastRankContextRef: MutableRefObject<any>;

  // ─── Inter-tool dependencies (populated during registry build) ──────────
  clipQMapDatasetByGeometry?: any;
  setQMapLayerColorByThresholds?: any;

  // Allow additional properties for forward compatibility
  [key: string]: any;
}
