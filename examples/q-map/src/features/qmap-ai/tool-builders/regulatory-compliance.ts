import {extendedTool} from '../tool-shim';
import {z} from 'zod';
import type {QMapToolContext} from '../context/tool-context';
import regulatoryThresholds from '../data/regulatory-thresholds.json';

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

type CompactLimit = {averaging: string; value: number; maxExceedances?: number; reference: string; note?: string; id?: string};

type ThresholdEntry = {
  parameter: string;
  unit: string;
  limits: ThresholdLimit[];
  who?: Array<CompactLimit & {note?: string}>;
  eu_2030?: Array<CompactLimit>;
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

// ---------------------------------------------------------------------------
// Tool: checkRegulatoryCompliance
// ---------------------------------------------------------------------------

export function createCheckRegulatoryComplianceTool(ctx: QMapToolContext) {
  const {getCurrentVisState, resolveDatasetByName, resolveDatasetFieldName} = ctx;

  return extendedTool({
    description:
      '[PREFERRED for regulatory compliance] Check air quality measurements against D.Lgs. 155/2010 limits and WHO AQG 2021. ' +
      'Returns per-station exceedance counts, compliance rate %. Use this when the question mentions "limiti di legge", "superamenti", "normativa", "155/2010", "conforme".',
    parameters: z.object({
      datasetName: z.string().describe('Dataset with measurement data'),
      parameterName: z
        .string()
        .describe('Pollutant name: PM10, PM2.5, NO2, O3, SO2, CO, Benzene'),
      valueField: z.string().optional().describe('Numeric field with measured values. Default: measure_value'),
      stationField: z
        .string()
        .optional()
        .describe('Field identifying stations (for per-station summary). Default: station_name'),
      includeEu2030: z
        .boolean()
        .optional()
        .describe('Also check against EU 2030 revised limits. Default false'),
      includeWho: z
        .boolean()
        .optional()
        .describe('Also check against WHO AQG 2021 guidelines. Default false')
    }),
    execute: async (rawArgs: any) => {
      const {datasetName, parameterName} = rawArgs;
      const includeEu2030 = rawArgs.includeEu2030 === true;
      const includeWho = rawArgs.includeWho === true;

      const thresholdEntry = findThresholds(parameterName);
      if (!thresholdEntry) {
        const available = thresholdData.map(t => t.parameter).join(', ');
        return {
          llmResult: {
            success: false,
            details: `No regulatory thresholds found for "${parameterName}". Available: ${available}.`
          }
        };
      }

      const currentVisState = getCurrentVisState();
      const dataset = resolveDatasetByName(currentVisState?.datasets || {}, datasetName);
      if (!dataset?.id) {
        return {
          llmResult: {
            success: false,
            details: `Dataset "${datasetName}" not found. Load measurement data first.`
          }
        };
      }

      const valueFieldName = resolveDatasetFieldName(dataset, rawArgs.valueField || 'measure_value');
      if (!valueFieldName) {
        return {
          llmResult: {
            success: false,
            details: `Value field "${rawArgs.valueField || 'measure_value'}" not found in dataset.`
          }
        };
      }

      const stationFieldName = resolveDatasetFieldName(
        dataset,
        rawArgs.stationField || 'station_name'
      );

      const idx = Array.isArray(dataset.allIndexes)
        ? dataset.allIndexes
        : Array.from({length: Number(dataset.length || 0)}, (_, i) => i);

      // Collect values, optionally grouped by station
      const stationValues: Record<string, number[]> = {};
      let totalRows = 0;
      let validRows = 0;

      for (const rowIdx of idx) {
        totalRows++;
        const v = Number(dataset.getValue(valueFieldName, rowIdx));
        if (!Number.isFinite(v)) continue;
        validRows++;

        const station = stationFieldName
          ? String(dataset.getValue(stationFieldName, rowIdx) || 'unknown')
          : '_all';
        if (!stationValues[station]) stationValues[station] = [];
        stationValues[station].push(v);
      }

      if (validRows === 0) {
        return {
          llmResult: {
            success: false,
            details: `No valid numeric values found in field "${valueFieldName}".`
          }
        };
      }

      // Check each applicable limit
      const applicableLimits = thresholdEntry.limits.filter(
        lim => lim.averaging === 'hourly' || lim.averaging === 'daily' || lim.averaging === 'annual'
      );

      type StationCompliance = {
        station: string;
        sampleCount: number;
        min: number;
        max: number;
        mean: number;
        exceedances: Array<{
          limitId: string;
          limitValue: number;
          averaging: string;
          unit: string;
          exceedanceCount: number;
          exceedancePct: number;
          maxExceedancesAllowed: number | null;
          compliant: boolean;
          reference: string;
          regime: string;
        }>;
        overallCompliant: boolean;
      };

      const stationResults: StationCompliance[] = [];
      let totalExceedances = 0;
      let nonCompliantStations = 0;

      const allLimits = [
        ...applicableLimits.map(l => ({...l, regime: 'D.Lgs. 155/2010' as string})),
        ...(includeEu2030
          ? (thresholdEntry.eu_2030 || []).map(l => ({
              id: `eu2030_${l.averaging}`,
              description: `EU 2030 target (${l.averaging})`,
              averaging: l.averaging,
              value: l.value,
              maxExceedances: l.maxExceedances ?? null,
              reference: l.reference,
              regime: 'EU 2030' as string
            }))
          : []),
        ...(includeWho
          ? ((thresholdEntry as any).who || [])
              .filter((l: any) => l.value !== undefined)
              .map((l: any) => ({
                id: l.id || `who_${l.averaging}`,
                description: `WHO AQG 2021 (${l.averaging})`,
                averaging: l.averaging,
                value: l.value,
                maxExceedances: l.maxExceedances ?? null,
                reference: l.reference || 'WHO AQG 2021',
                regime: 'WHO 2021' as string
              }))
          : [])
      ];

      for (const [station, values] of Object.entries(stationValues)) {
        const sorted = [...values].sort((a, b) => a - b);
        const mean = sorted.reduce((s, v) => s + v, 0) / sorted.length;
        const min = sorted[0];
        const max = sorted[sorted.length - 1];

        const exceedances: StationCompliance['exceedances'] = [];
        let stationCompliant = true;

        for (const lim of allLimits) {
          const over = values.filter(v => v > lim.value).length;
          const pct = (over / values.length) * 100;
          const allowed = lim.maxExceedances ?? null;
          const compliant = allowed !== null ? over <= allowed : over === 0;

          if (!compliant) stationCompliant = false;
          if (over > 0) totalExceedances += over;

          exceedances.push({
            limitId: lim.id,
            limitValue: lim.value,
            averaging: lim.averaging,
            unit: thresholdEntry.unit,
            exceedanceCount: over,
            exceedancePct: Math.round(pct * 10) / 10,
            maxExceedancesAllowed: allowed,
            compliant,
            reference: lim.reference,
            regime: lim.regime
          });
        }

        if (!stationCompliant) nonCompliantStations++;

        stationResults.push({
          station,
          sampleCount: values.length,
          min: Math.round(min * 100) / 100,
          max: Math.round(max * 100) / 100,
          mean: Math.round(mean * 100) / 100,
          exceedances,
          overallCompliant: stationCompliant
        });
      }

      // Sort: non-compliant first, then by max exceedance count
      stationResults.sort((a, b) => {
        if (a.overallCompliant !== b.overallCompliant) return a.overallCompliant ? 1 : -1;
        const aMax = Math.max(...a.exceedances.map(e => e.exceedanceCount));
        const bMax = Math.max(...b.exceedances.map(e => e.exceedanceCount));
        return bMax - aMax;
      });

      const topStations = stationResults.slice(0, 20);

      return {
        llmResult: {
          success: true,
          parameter: thresholdEntry.parameter,
          unit: thresholdEntry.unit,
          source: (regulatoryThresholds as any).sources?.italy || 'D.Lgs. 155/2010',
          totalStations: stationResults.length,
          totalMeasurements: validRows,
          totalExceedances,
          nonCompliantStations,
          complianceRate:
            Math.round(
              ((stationResults.length - nonCompliantStations) / stationResults.length) * 1000
            ) / 10,
          applicableLimits: allLimits.map(l => ({
            id: l.id,
            value: l.value,
            unit: thresholdEntry.unit,
            averaging: l.averaging,
            maxExceedances: l.maxExceedances,
            regime: l.regime
          })),
          stations: topStations,
          details:
            `Compliance check for ${thresholdEntry.parameter} (${(regulatoryThresholds as any).sources?.italy || 'D.Lgs. 155/2010'}): ` +
            `${stationResults.length} stations, ${validRows} measurements. ` +
            `${nonCompliantStations} non-compliant station(s), ${totalExceedances} total exceedance(s). ` +
            `Compliance rate: ${Math.round(((stationResults.length - nonCompliantStations) / stationResults.length) * 1000) / 10}%.`
        }
      };
    }
  });
}

// ---------------------------------------------------------------------------
// Tool: listRegulatoryThresholds (read-only, no dataset needed)
// ---------------------------------------------------------------------------

export function createListRegulatoryThresholdsTool() {
  return extendedTool({
    description:
      '[PREFERRED for air quality limits] List D.Lgs. 155/2010 + WHO AQG 2021 regulatory thresholds for 12 pollutants. No dataset required. Use this when the question asks about "limiti", "soglie normative", "qualità dell\'aria", "155/2010".',
    parameters: z.object({
      parameterName: z
        .string()
        .optional()
        .describe('Filter by pollutant (PM10, PM2.5, NO2, O3, SO2, CO, Benzene). Omit for all.')
    }),
    execute: async (rawArgs: any) => {
      const parameterName = rawArgs?.parameterName;
      if (parameterName) {
        const entry = findThresholds(parameterName);
        if (!entry) {
          const available = thresholdData.map(t => t.parameter).join(', ');
          return {
            llmResult: {
              success: false,
              details: `No thresholds for "${parameterName}". Available: ${available}.`
            }
          };
        }
        return {
          llmResult: {
            success: true,
            sources: (regulatoryThresholds as any).sources,
            parameter: entry.parameter,
            unit: entry.unit,
            limits: entry.limits,
            who: (entry as any).who || [],
            eu_2030: entry.eu_2030,
            details: `Regulatory thresholds for ${entry.parameter} (${entry.unit}).`
          }
        };
      }

      const summary = thresholdData.map(t => ({
        parameter: t.parameter,
        unit: t.unit,
        limitsCount: t.limits.length,
        limits: t.limits.map(l => ({
          averaging: l.averaging,
          value: l.value,
          maxExceedances: l.maxExceedances
        })),
        hasWho: ((t as any).who || []).filter((w: any) => w.value !== undefined).length > 0,
        hasEu2030: (t.eu_2030 || []).length > 0
      }));

      return {
        llmResult: {
          success: true,
          sources: (regulatoryThresholds as any).sources,
          parameters: summary,
          details: `${summary.length} pollutants with regulatory thresholds (D.Lgs. 155/2010 + WHO AQG 2021).`
        }
      };
    }
  });
}
