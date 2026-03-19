export type QMapToolRegistry = Record<string, any>;
export type QMapToolGroups = Record<string, QMapToolRegistry>;

export function buildQMapToolsWithoutCategoryIntrospection({
  baseToolsWithChartPolicy,
  qMapCloudTools,
  customToolGroups,
  strict = true
}: {
  baseToolsWithChartPolicy: QMapToolRegistry;
  qMapCloudTools: QMapToolRegistry;
  customToolGroups: QMapToolGroups;
  strict?: boolean;
}): QMapToolRegistry {
  const mergedCustomTools: QMapToolRegistry = {};
  const duplicates: Array<{toolName: string; firstGroup: string; secondGroup: string}> = [];
  const seenByGroup = new Map<string, string>();

  Object.entries(customToolGroups || {}).forEach(([groupName, groupTools]) => {
    Object.entries(groupTools || {}).forEach(([toolName, tool]) => {
      if (!tool || typeof tool !== 'object') {
        return;
      }
      const previousGroup = seenByGroup.get(toolName);
      if (previousGroup) {
        duplicates.push({toolName, firstGroup: previousGroup, secondGroup: groupName});
      } else {
        seenByGroup.set(toolName, groupName);
      }
      mergedCustomTools[toolName] = tool;
    });
  });

  if (strict && duplicates.length) {
    const details = duplicates
      .map(item => `"${item.toolName}" (${item.firstGroup} -> ${item.secondGroup})`)
      .join(', ');
    throw new Error(`Duplicate q-map tool registrations across groups: ${details}`);
  }

  return {
    ...baseToolsWithChartPolicy,
    ...qMapCloudTools,
    ...mergedCustomTools
  };
}
