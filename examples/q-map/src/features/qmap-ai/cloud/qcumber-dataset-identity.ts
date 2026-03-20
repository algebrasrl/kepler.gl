/**
 * Dataset identity derivation, row normalization, and provider ID resolution helpers.
 */

export function withUniqueMapDatasetIdentity(dataset: any, executionKey?: string) {
  if (!dataset || typeof dataset !== 'object' || !executionKey) {
    return dataset;
  }
  const info = dataset.info;
  if (!info || typeof info !== 'object') {
    return dataset;
  }
  const baseId = String((info as any).id || 'q-cumber-query').trim() || 'q-cumber-query';
  const baseLabel = String((info as any).label || baseId).trim() || baseId;
  const safeKey = String(executionKey);
  const keyParts = safeKey.split('-').filter(Boolean);
  const shortKey = keyParts.length ? keyParts[keyParts.length - 1] : safeKey.slice(-8);
  return {
    ...dataset,
    info: {
      ...(info as Record<string, unknown>),
      id: `${baseId}-${safeKey}`,
      label: `${baseLabel} [${shortKey}]`
    }
  };
}

export function deriveDatasetIdentity(dataset: any, executionKey?: string) {
  const withIdentity = withUniqueMapDatasetIdentity(dataset, executionKey);
  const info = withIdentity?.info || {};
  const id = String(info?.id || '').trim();
  const label = String(info?.label || id || '').trim();
  return {
    id,
    label,
    ref: id ? `id:${id}` : ''
  };
}

export function rebuildQcumberMapDatasetRows(dataset: any, rows: any[], fields: any[]): any {
  if (!dataset || typeof dataset !== 'object') {
    return dataset;
  }
  const data = dataset?.data && typeof dataset.data === 'object' ? dataset.data : {};
  const existingFields = Array.isArray(data?.fields) ? data.fields : [];
  const resolvedFieldNames = (
    existingFields.length
      ? existingFields.map((field: any) => String(field?.name || '').trim()).filter(Boolean)
      : (Array.isArray(fields) ? fields : []).map((field: any) => String(field || '').trim()).filter(Boolean)
  ) as string[];
  const normalizedFieldDefs = resolvedFieldNames.map(fieldName => {
    const exact = existingFields.find((field: any) => String(field?.name || '').trim() === fieldName);
    if (exact) return exact;
    const ci = existingFields.find(
      (field: any) => String(field?.name || '').trim().toLowerCase() === fieldName.toLowerCase()
    );
    if (ci) return ci;
    return {name: fieldName, type: fieldName === '_geojson' ? 'geojson' : 'string'};
  });
  const normalizedRows = (Array.isArray(rows) ? rows : []).map((row: any) => {
    const rowObject = row && typeof row === 'object' && !Array.isArray(row) ? row : {};
    return resolvedFieldNames.map(fieldName => {
      if (Object.prototype.hasOwnProperty.call(rowObject, fieldName)) {
        return rowObject[fieldName];
      }
      const ciEntry = Object.entries(rowObject).find(([key]) => key.toLowerCase() === fieldName.toLowerCase());
      return ciEntry ? ciEntry[1] : null;
    });
  });
  return {
    ...dataset,
    data: {
      ...data,
      fields: normalizedFieldDefs,
      rows: normalizedRows
    }
  };
}

export function resolveQCumberProviderId(rawProviderId?: string): string {
  const raw = String(rawProviderId || '').trim();
  return raw;
}

export function isInvalidProviderIdLiteral(rawProviderId?: string): boolean {
  const normalized = String(rawProviderId || '')
    .trim()
    .toLowerCase();
  return (
    normalized === '[object]' ||
    normalized === '[object object]' ||
    normalized === 'object object' ||
    normalized === 'null' ||
    normalized === 'undefined'
  );
}

export function normalizeDatasetToken(value: unknown): string {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}
