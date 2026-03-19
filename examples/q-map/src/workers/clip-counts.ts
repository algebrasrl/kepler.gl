export type ClipPropertyCounts = {
  distinctValueCounts: Record<string, number>;
  propertyValueMatchCounts: Record<string, Record<string, number>>;
};

type ClipPropertyCountsOptions = {
  includeDistinctCounts: boolean;
  includeValueCountFields: boolean;
};

export function computeClipPropertyCountsFromPropertyRows(
  propertyRows: Array<Record<string, unknown> | null | undefined>,
  options: ClipPropertyCountsOptions
): ClipPropertyCounts {
  const includeDistinctCounts = options?.includeDistinctCounts === true;
  const includeValueCountFields = options?.includeValueCountFields === true;
  if (!includeDistinctCounts && !includeValueCountFields) {
    return {distinctValueCounts: {}, propertyValueMatchCounts: {}};
  }

  const distinctSetsByField = new Map<string, Set<string>>();
  const valueCountsByField = new Map<string, Map<string, number>>();
  for (const rawProperties of propertyRows) {
    const properties = rawProperties && typeof rawProperties === 'object' ? rawProperties : {};
    Object.entries(properties).forEach(([fieldName, rawValue]) => {
      if (rawValue === null || rawValue === undefined || rawValue === '') return;
      const key = String(fieldName || '').trim();
      if (!key) return;
      const valueKey = String(rawValue);
      if (includeDistinctCounts) {
        const currentSet = distinctSetsByField.get(key) || new Set<string>();
        currentSet.add(valueKey);
        distinctSetsByField.set(key, currentSet);
      }
      if (includeValueCountFields) {
        const currentCounts = valueCountsByField.get(key) || new Map<string, number>();
        currentCounts.set(valueKey, Number(currentCounts.get(valueKey) || 0) + 1);
        valueCountsByField.set(key, currentCounts);
      }
    });
  }

  const distinctValueCounts = includeDistinctCounts
    ? Array.from(distinctSetsByField.entries()).reduce(
        (acc, [fieldName, values]) => {
          acc[fieldName] = values.size;
          return acc;
        },
        {} as Record<string, number>
      )
    : {};
  const propertyValueMatchCounts = includeValueCountFields
    ? Array.from(valueCountsByField.entries()).reduce(
        (acc, [fieldName, valueCounts]) => {
          acc[fieldName] = Array.from(valueCounts.entries()).reduce(
            (inner, [value, count]) => {
              inner[value] = count;
              return inner;
            },
            {} as Record<string, number>
          );
          return acc;
        },
        {} as Record<string, Record<string, number>>
      )
    : {};

  return {distinctValueCounts, propertyValueMatchCounts};
}
