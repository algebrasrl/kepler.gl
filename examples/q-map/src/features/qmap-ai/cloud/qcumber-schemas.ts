/**
 * Zod schemas and enum normalization for q-cumber query parameters.
 */
import {z} from 'zod';

export function normalizeQcumberEnumToken(value: unknown): string {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

export function buildOptionalQcumberEnumSchema(
  values: readonly string[],
  aliases: Record<string, string>,
  fieldLabel: string
) {
  const allowed = new Set(values);
  const allowedList = values.join(', ');
  return z.any().optional().transform((value, ctx): string | undefined => {
    if (value === undefined || value === null) return undefined;
    const raw = String(value || '').trim();
    if (!raw) return undefined;
    const token = normalizeQcumberEnumToken(raw);
    const normalized = aliases[token] || (allowed.has(token) ? token : '');
    if (normalized) return normalized;
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `Invalid ${fieldLabel}. Use one of: ${allowedList}.`
    });
    return z.NEVER;
  });
}

export const NON_EMPTY_STRING_SCHEMA = z.string().trim().min(1);
export const OPTIONAL_NON_EMPTY_STRING_SCHEMA = NON_EMPTY_STRING_SCHEMA.optional();

export const QCUMBER_ORDER_DIRECTION_SCHEMA = buildOptionalQcumberEnumSchema(
  ['asc', 'desc'],
  {
    ascending: 'asc',
    ascend: 'asc',
    crescente: 'asc',
    ascendente: 'asc',
    descending: 'desc',
    descend: 'desc',
    decrescente: 'desc',
    discendente: 'desc'
  },
  'orderDirection'
);
export const QCUMBER_EXPECTED_ADMIN_TYPE_VALUES = [
  'country',
  'region',
  'province',
  'municipality',
  'stato',
  'regione',
  'provincia',
  'comune'
] as const;
export const QCUMBER_EXPECTED_ADMIN_TYPE_ALIASES: Record<string, string> = {
  countries: 'country',
  stato: 'country',
  stati: 'country',
  country_stato: 'country',
  stato_country: 'country',
  regions: 'region',
  regione: 'region',
  regioni: 'region',
  region_regione: 'region',
  regione_region: 'region',
  provinces: 'province',
  provincia: 'province',
  province_it: 'province',
  province_provincia: 'province',
  provincia_province: 'province',
  municipalities: 'municipality',
  municipalitys: 'municipality',
  comune: 'municipality',
  comuni: 'municipality',
  municipality_comune: 'municipality',
  comune_municipality: 'municipality',
  city: 'municipality',
  citta: 'municipality'
};
export const QCUMBER_EXPECTED_ADMIN_TYPE_SCHEMA = buildOptionalQcumberEnumSchema(
  QCUMBER_EXPECTED_ADMIN_TYPE_VALUES,
  QCUMBER_EXPECTED_ADMIN_TYPE_ALIASES,
  'expectedAdminType'
);

export const QCUMBER_FILTER_OP_VALUES = [
  'eq',
  'ne',
  'gt',
  'gte',
  'lt',
  'lte',
  'in',
  'contains',
  'startswith',
  'endswith',
  'is_null',
  'not_null'
] as const;
export const QCUMBER_FILTER_OP_INPUT_VALUES = [...QCUMBER_FILTER_OP_VALUES, 'neq', 'starts_with', 'ends_with'] as const;
export const QCUMBER_FILTER_OPS = new Set<string>(QCUMBER_FILTER_OP_VALUES);
export const QCUMBER_INLINE_NUMERIC_PATTERN = /^-?(?:0|[1-9]\d*)(?:\.\d+)?$/;

export function normalizeQcumberFilterOp(raw: unknown): string {
  const rawOp = String(raw || 'eq')
    .trim()
    .toLowerCase();
  if (rawOp === 'neq') return 'ne';
  if (rawOp === 'starts_with') return 'startswith';
  if (rawOp === 'ends_with') return 'endswith';
  return rawOp || 'eq';
}

function parseQcumberInlineScalar(raw: unknown): unknown {
  if (raw === undefined) return undefined;
  if (raw === null || typeof raw === 'number' || typeof raw === 'boolean') return raw;
  if (typeof raw !== 'string') return undefined;
  const token = raw.trim();
  if (!token) return undefined;
  const lower = token.toLowerCase();
  if (lower === 'null') return null;
  if (lower === 'true') return true;
  if (lower === 'false') return false;
  if (QCUMBER_INLINE_NUMERIC_PATTERN.test(token)) {
    const parsed = Number(token);
    if (Number.isFinite(parsed)) return parsed;
  }
  if (
    (token.startsWith('"') && token.endsWith('"') && token.length >= 2) ||
    (token.startsWith("'") && token.endsWith("'") && token.length >= 2)
  ) {
    return token.slice(1, -1);
  }
  return token;
}

function parseQcumberInlineValues(raw: unknown): unknown[] {
  if (Array.isArray(raw)) return raw;
  if (typeof raw !== 'string') return [];
  const token = raw.trim();
  if (!token) return [];
  if (token.startsWith('[') && token.endsWith(']')) {
    try {
      const parsed = JSON.parse(token);
      if (Array.isArray(parsed)) {
        return parsed
          .map(item => parseQcumberInlineScalar(item))
          .filter((item): item is unknown => item !== undefined);
      }
    } catch {
      // Fallback to separator parsing below.
    }
  }
  const separator = token.includes('|') ? '|' : token.includes(';') ? ';' : ',';
  return token
    .split(separator)
    .map(item => parseQcumberInlineScalar(item))
    .filter((item): item is unknown => item !== undefined);
}

function normalizeQcumberFilterInput(raw: unknown): unknown {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return raw;
  const next: Record<string, unknown> = {...(raw as Record<string, unknown>)};
  const rawOp = typeof next.op === 'string' ? next.op.trim() : '';
  const hasExplicitValue = next.value !== undefined;
  const hasExplicitValues = Array.isArray(next.values);
  if (!rawOp) return next;

  const split = rawOp.match(/^([a-z_]+)\s*[,;|]\s*(.+)$/i);
  if (!split) return next;

  const opToken = normalizeQcumberFilterOp(split[1]);
  if (!QCUMBER_FILTER_OPS.has(opToken)) return next;

  next.op = opToken;
  if (hasExplicitValue || hasExplicitValues) return next;

  const tailToken = String(split[2] || '').trim();
  if (!tailToken) return next;
  const keyed = tailToken.match(/^(value|values)\s*[:=]\s*(.+)$/i);
  const key = keyed ? keyed[1].toLowerCase() : '';
  const rawValueToken = keyed ? keyed[2] : tailToken;

  if (opToken === 'in' || key === 'values') {
    const parsedValues = parseQcumberInlineValues(rawValueToken);
    if (parsedValues.length) next.values = parsedValues;
    return next;
  }

  const parsedScalar = parseQcumberInlineScalar(rawValueToken);
  if (parsedScalar !== undefined) next.value = parsedScalar;
  return next;
}

const QCUMBER_SCALAR_FILTER_VALUE_SCHEMA = z.union([z.string(), z.number(), z.boolean(), z.null()]);
export const QCUMBER_FILTER_SCHEMA = z
  .preprocess(
    raw => normalizeQcumberFilterInput(raw),
    z
      .object({
        field: NON_EMPTY_STRING_SCHEMA,
        op: z.enum(QCUMBER_FILTER_OP_INPUT_VALUES).optional(),
        value: QCUMBER_SCALAR_FILTER_VALUE_SCHEMA.optional(),
        values: z.array(QCUMBER_SCALAR_FILTER_VALUE_SCHEMA).optional()
      })
      .strict()
      .superRefine((payload, ctx) => {
        const op = normalizeQcumberFilterOp(payload?.op);
        const hasValue = payload?.value !== undefined;
        const valuesArray = Array.isArray(payload?.values) ? payload.values : [];
        const hasValuesArray = Array.isArray(payload?.values);
        const hasValues = valuesArray.length > 0;
        if (op === 'is_null' || op === 'not_null') {
          if (hasValue || hasValuesArray) {
            ctx.addIssue({
              code: z.ZodIssueCode.custom,
              message: `Operator "${op}" does not accept value/values.`
            });
          }
          return;
        }
        if (op === 'in') {
          if (!hasValues) {
            ctx.addIssue({
              code: z.ZodIssueCode.custom,
              message: 'Operator "in" requires non-empty "values".'
            });
          }
          if (hasValue) {
            ctx.addIssue({
              code: z.ZodIssueCode.custom,
              message: 'Operator "in" must use "values", not "value".'
            });
          }
          return;
        }
        if (!hasValue) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: `Operator "${op}" requires "value".`
          });
        }
        if (hasValuesArray) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: `Operator "${op}" does not accept "values".`
          });
        }
      })
  );
