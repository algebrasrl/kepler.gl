import {z} from 'zod';
import type {QMapToolContext} from '../context/tool-context';
import regulatoryThresholds from '../data/regulatory-thresholds.json';

// ─── Regulatory threshold types (shared with regulatory-compliance.ts) ───────

type ThresholdLimit = {
  id: string;
  description: string;
  averaging: string;
  value: number;
  maxExceedances?: number | null;
  percentileEquivalent?: number;
  note?: string;
  reference: string;
};

type ThresholdEntry = {
  parameter: string;
  unit: string;
  limits: ThresholdLimit[];
  who?: Array<{averaging: string; value: number; reference: string; note?: string; id?: string}>;
  eu_2030?: Array<{averaging: string; value: number; maxExceedances?: number; reference: string}>;
};

const thresholdData = regulatoryThresholds.thresholds as ThresholdEntry[];

function findThresholds(parameterName: string): ThresholdEntry | null {
  const normalized = parameterName.trim().toUpperCase().replace(/[.\s-]+/g, '');
  const aliases: Record<string, string> = {
    PM10: 'PM10',
    PM25: 'PM2.5',
    'PM2.5': 'PM2.5',
    NO2: 'NO2',
    O3: 'O3',
    OZONO: 'O3',
    OZONE: 'O3',
    SO2: 'SO2',
    CO: 'CO',
    BENZENE: 'Benzene',
    C6H6: 'Benzene',
    PB: 'Pb',
    PIOMBO: 'Pb',
    LEAD: 'Pb',
    AS: 'As',
    ARSENICO: 'As',
    ARSENIC: 'As',
    CD: 'Cd',
    CADMIO: 'Cd',
    CADMIUM: 'Cd',
    NI: 'Ni',
    NICHEL: 'Ni',
    NICKEL: 'Ni',
    BAP: 'BaP',
    'BENZO(A)PIRENE': 'BaP',
    'BENZO[A]PYRENE': 'BaP',
    'BENZO[A]PIRENE': 'BaP'
  };
  const canonical = aliases[normalized] || parameterName.trim();
  return thresholdData.find(t => t.parameter.toUpperCase() === canonical.toUpperCase()) || null;
}

// ─── Haversine distance (km) ─────────────────────────────────────────────────

const haversineKm = (lat1: number, lon1: number, lat2: number, lon2: number): number => {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) * Math.cos((lat2 * Math.PI) / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
};

// ─── Coordinate extraction helpers ───────────────────────────────────────────

function extractPointCoords(
  geojson: any,
  turfCentroid: any
): {lat: number; lon: number} | null {
  if (!geojson) return null;
  const geom = geojson.geometry || geojson;
  if (!geom || !geom.type) return null;

  if (geom.type === 'Point' && Array.isArray(geom.coordinates)) {
    const [lon, lat] = geom.coordinates;
    if (Number.isFinite(lon) && Number.isFinite(lat)) return {lat, lon};
    return null;
  }

  // For Polygon / MultiPolygon / other geometry types, use turfCentroid
  try {
    const feature = geojson.type === 'Feature' ? geojson : {type: 'Feature', geometry: geom, properties: {}};
    const centroid = turfCentroid(feature);
    if (centroid?.geometry?.coordinates) {
      const [lon, lat] = centroid.geometry.coordinates;
      if (Number.isFinite(lon) && Number.isFinite(lat)) return {lat, lon};
    }
  } catch {
    // ignore
  }
  return null;
}

// ─── Row cap ─────────────────────────────────────────────────────────────────

const MAX_ROWS = 50_000;

// ─── Tool builder ────────────────────────────────────────────────────────────

export function createAssessPopulationExposureTool(ctx: QMapToolContext) {
  const {
    getCurrentVisState,
    resolveDatasetByName,
    resolveDatasetFieldName,
    getDatasetInfoByLabel,
    upsertDerivedDatasetRows,
    turfCentroid
  } = ctx;

  return {
    description:
      '[PREFERRED for exposure assessment] Estimate population exposure to air pollution by joining measurement stations with nearby administrative boundaries (municipalities). ' +
      'For each station, finds municipalities within a buffer radius and aggregates exposed population. ' +
      'Returns per-station exposure summary with population count and compliance status.',
    parameters: z.object({
      stationDatasetName: z.string().describe('Dataset with measurement points (e.g. opas-measurements loaded on map)'),
      boundaryDatasetName: z.string().describe('Dataset with admin boundaries + population (e.g. kontur municipalities)'),
      valueField: z.string().optional().describe('Measurement field. Default: measure_value'),
      populationField: z.string().optional().describe('Population field. Default: population'),
      nameField: z.string().optional().describe('Boundary name field. Default: name'),
      radiusKm: z.number().min(1).max(50).optional().describe('Buffer radius in km. Default: 10'),
      parameterName: z.string().optional().describe('Pollutant name for regulatory check (e.g. PM10)'),
      showOnMap: z.boolean().optional().describe('Create derived dataset with exposure data. Default: false')
    }),
    execute: async (rawArgs: any) => {
      const {stationDatasetName, boundaryDatasetName} = rawArgs;
      const valueFieldArg: string = rawArgs.valueField || 'measure_value';
      const populationFieldArg: string = rawArgs.populationField || 'population';
      const nameFieldArg: string = rawArgs.nameField || 'name';
      const radiusKm: number = Math.max(1, Math.min(50, Number(rawArgs.radiusKm || 10)));
      const parameterName: string | undefined = rawArgs.parameterName;
      const showOnMap: boolean = rawArgs.showOnMap === true;

      // 1. Resolve both datasets
      const currentVisState = getCurrentVisState();
      const datasets = currentVisState?.datasets || {};

      const stationDataset = resolveDatasetByName(datasets, stationDatasetName);
      if (!stationDataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Station dataset "${stationDatasetName}" not found. Load measurement data first.`
          }
        };
      }

      const boundaryDataset = resolveDatasetByName(datasets, boundaryDatasetName);
      if (!boundaryDataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Boundary dataset "${boundaryDatasetName}" not found. Load administrative boundary data first.`
          }
        };
      }

      // 2. Resolve all field names
      const resolvedValueField = resolveDatasetFieldName(stationDataset, valueFieldArg);
      if (!resolvedValueField) {
        return {
          llmResult: {
            success: false,
            details: `Value field "${valueFieldArg}" not found in station dataset "${stationDatasetName}".`
          }
        };
      }

      const resolvedPopField = resolveDatasetFieldName(boundaryDataset, populationFieldArg);
      if (!resolvedPopField) {
        return {
          llmResult: {
            success: false,
            details: `Population field "${populationFieldArg}" not found in boundary dataset "${boundaryDatasetName}".`
          }
        };
      }

      const resolvedNameField = resolveDatasetFieldName(boundaryDataset, nameFieldArg);
      if (!resolvedNameField) {
        return {
          llmResult: {
            success: false,
            details: `Name field "${nameFieldArg}" not found in boundary dataset "${boundaryDatasetName}".`
          }
        };
      }

      // Resolve _geojson field for both datasets
      const stationGeojsonField = resolveDatasetFieldName(stationDataset, '_geojson');
      if (!stationGeojsonField) {
        return {
          llmResult: {
            success: false,
            details: `Station dataset "${stationDatasetName}" has no _geojson field. Cannot extract point coordinates.`
          }
        };
      }

      const boundaryGeojsonField = resolveDatasetFieldName(boundaryDataset, '_geojson');
      if (!boundaryGeojsonField) {
        return {
          llmResult: {
            success: false,
            details: `Boundary dataset "${boundaryDatasetName}" has no _geojson field. Cannot compute centroids.`
          }
        };
      }

      // Station name field (optional — try station_name, then name, then fall back to index)
      const stationNameField =
        resolveDatasetFieldName(stationDataset, 'station_name') ||
        resolveDatasetFieldName(stationDataset, 'name') ||
        null;

      // Build indexes
      const stationIdx = Array.isArray(stationDataset.allIndexes)
        ? stationDataset.allIndexes
        : Array.from({length: Number(stationDataset.length || 0)}, (_, i) => i);

      const boundaryIdx = Array.isArray(boundaryDataset.allIndexes)
        ? boundaryDataset.allIndexes
        : Array.from({length: Number(boundaryDataset.length || 0)}, (_, i) => i);

      const cappedStationIdx = stationIdx.slice(0, MAX_ROWS);
      const cappedBoundaryIdx = boundaryIdx.slice(0, MAX_ROWS);

      // 3. Pre-compute boundary centroids + population + name
      type BoundaryInfo = {lat: number; lon: number; population: number; name: string; rowIdx: number};
      const boundaries: BoundaryInfo[] = [];

      for (const rowIdx of cappedBoundaryIdx) {
        const geojson = boundaryDataset.getValue(boundaryGeojsonField, rowIdx);
        const coords = extractPointCoords(geojson, turfCentroid);
        if (!coords) continue;

        const pop = Number(boundaryDataset.getValue(resolvedPopField, rowIdx));
        const bName = String(boundaryDataset.getValue(resolvedNameField, rowIdx) || `boundary_${rowIdx}`);
        boundaries.push({
          lat: coords.lat,
          lon: coords.lon,
          population: Number.isFinite(pop) ? pop : 0,
          name: bName,
          rowIdx
        });
      }

      if (boundaries.length === 0) {
        return {
          llmResult: {
            success: false,
            details: `No valid boundary geometries found in "${boundaryDatasetName}".`
          }
        };
      }

      // 4. Regulatory threshold lookup (if parameterName provided)
      let thresholdEntry: ThresholdEntry | null = null;
      let annualLimit: ThresholdLimit | null = null;
      if (parameterName) {
        thresholdEntry = findThresholds(parameterName);
        if (thresholdEntry) {
          // Use the annual limit for per-station compliance check (most relevant for exposure)
          annualLimit =
            thresholdEntry.limits.find(l => l.averaging === 'annual') ||
            thresholdEntry.limits.find(l => l.averaging === 'daily') ||
            thresholdEntry.limits[0] || null;
        }
      }

      // 5. For each station: compute exposure
      type StationExposure = {
        station: string;
        measureValue: number;
        unit: string;
        exposedPopulation: number;
        municipalitiesInRadius: string[];
        municipalityCount: number;
        exceedsLimit: boolean | null;
        limitValue: number | null;
        limitReference: string | null;
      };

      const stationResults: StationExposure[] = [];

      for (const rowIdx of cappedStationIdx) {
        const geojson = stationDataset.getValue(stationGeojsonField, rowIdx);
        const stationCoords = extractPointCoords(geojson, turfCentroid);
        if (!stationCoords) continue;

        const measureValue = Number(stationDataset.getValue(resolvedValueField, rowIdx));
        if (!Number.isFinite(measureValue)) continue;

        const stationLabel = stationNameField
          ? String(stationDataset.getValue(stationNameField, rowIdx) || `station_${rowIdx}`)
          : `station_${rowIdx}`;

        let exposedPopulation = 0;
        const municipalitiesInRadius: string[] = [];

        for (const boundary of boundaries) {
          const dist = haversineKm(stationCoords.lat, stationCoords.lon, boundary.lat, boundary.lon);
          if (dist <= radiusKm) {
            exposedPopulation += boundary.population;
            municipalitiesInRadius.push(boundary.name);
          }
        }

        // Compliance check
        let exceedsLimit: boolean | null = null;
        let limitValue: number | null = null;
        let limitReference: string | null = null;

        if (annualLimit) {
          limitValue = annualLimit.value;
          limitReference = annualLimit.reference;
          exceedsLimit = measureValue > annualLimit.value;
        }

        stationResults.push({
          station: stationLabel,
          measureValue: Math.round(measureValue * 100) / 100,
          unit: thresholdEntry?.unit || 'unknown',
          exposedPopulation,
          municipalitiesInRadius,
          municipalityCount: municipalitiesInRadius.length,
          exceedsLimit,
          limitValue,
          limitReference
        });
      }

      if (stationResults.length === 0) {
        return {
          llmResult: {
            success: false,
            details: `No valid station points with measurement values found in "${stationDatasetName}".`
          }
        };
      }

      // Sort by exposed population descending
      stationResults.sort((a, b) => b.exposedPopulation - a.exposedPopulation);

      const totalExposedPopulation = stationResults.reduce((sum, s) => sum + s.exposedPopulation, 0);
      const stationsExceedingLimit = stationResults.filter(s => s.exceedsLimit === true).length;

      // 6. showOnMap: create derived dataset
      let outLabel: string | null = null;
      let outDatasetId: string | null = null;

      if (showOnMap && stationResults.length > 0) {
        const targetName = `${stationDataset.label || stationDataset.id}_exposure`;
        const info = getDatasetInfoByLabel(datasets, targetName, 'qmap_exposure');
        outLabel = info.label;
        outDatasetId = info.datasetId;

        const outRows: Array<Record<string, unknown>> = [];
        for (const result of stationResults) {
          // Find the matching station row to copy geometry
          const matchIdx = cappedStationIdx.find((ri: number) => {
            const label = stationNameField
              ? String(stationDataset.getValue(stationNameField, ri) || `station_${ri}`)
              : `station_${ri}`;
            return label === result.station;
          });
          if (matchIdx === undefined) continue;

          const row: Record<string, unknown> = {};
          (stationDataset.fields || []).forEach((f: any) => {
            row[f.name] = stationDataset.getValue(f.name, matchIdx);
          });
          row.exposed_population = result.exposedPopulation;
          row.municipality_count = result.municipalityCount;
          row.municipalities = result.municipalitiesInRadius.join(', ');
          row.exceeds_limit = result.exceedsLimit;
          row.limit_value = result.limitValue;
          outRows.push(row);
        }

        if (outRows.length) {
          upsertDerivedDatasetRows(ctx.dispatch, datasets, outLabel, outRows, 'qmap_exposure', true);
        }
      }

      // 7. Build llmResult
      const top20 = stationResults.slice(0, 20);

      return {
        llmResult: {
          success: true,
          totalStationsAnalyzed: stationResults.length,
          totalExposedPopulation,
          stationsExceedingLimit,
          radiusKm,
          boundariesUsed: boundaries.length,
          ...(parameterName && thresholdEntry
            ? {parameter: thresholdEntry.parameter, unit: thresholdEntry.unit}
            : {}),
          ...(annualLimit
            ? {limitApplied: {value: annualLimit.value, averaging: annualLimit.averaging, reference: annualLimit.reference}}
            : {}),
          ...(showOnMap && outLabel ? {outputDataset: outLabel, outputDatasetId: outDatasetId} : {}),
          stations: top20,
          details:
            `Population exposure assessment: ${stationResults.length} stations analyzed with ${radiusKm} km buffer radius. ` +
            `${boundaries.length} administrative boundaries evaluated. ` +
            `Total aggregated exposed population: ${totalExposedPopulation.toLocaleString()}.` +
            (stationsExceedingLimit > 0
              ? ` ${stationsExceedingLimit} station(s) exceed regulatory limit.`
              : parameterName
                ? ' All stations within regulatory limits.'
                : '')
        }
      };
    }
  };
}
