import manifestJson from './tool-manifest.json';

export const QMAP_TOOL_MANIFEST_SCHEMA = 'qmap.tool_manifest.v1';

export type QMapToolManifestCategory = {
  key: string;
  label: string;
  description: string;
  tags: string[];
  policy: Record<string, string>;
  tools: string[];
};

export type QMapToolManifest = {
  schema: string;
  version: string;
  categories: QMapToolManifestCategory[];
  groups: {
    baseAllowlist: string[];
  };
};

function normalizeStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return Array.from(
    new Set(
      value
        .map(item => String(item || '').trim())
        .filter(Boolean)
    )
  );
}

function normalizeManifestCategory(raw: unknown): QMapToolManifestCategory | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const row = raw as Record<string, unknown>;
  const key = String(row.key || '').trim();
  if (!key) return null;
  return {
    key,
    label: String(row.label || key).trim() || key,
    description: String(row.description || '').trim(),
    tags: normalizeStringList(row.tags),
    policy: row.policy && typeof row.policy === 'object' && !Array.isArray(row.policy)
      ? Object.fromEntries(
          Object.entries(row.policy as Record<string, unknown>)
            .map(([policyKey, policyValue]) => [String(policyKey || '').trim(), String(policyValue || '').trim()])
            .filter(([policyKey, policyValue]) => Boolean(policyKey) && Boolean(policyValue))
        )
      : {},
    tools: normalizeStringList(row.tools)
  };
}

function normalizeManifest(raw: unknown): QMapToolManifest {
  const root = raw && typeof raw === 'object' && !Array.isArray(raw) ? (raw as Record<string, unknown>) : {};
  const categories = Array.isArray(root.categories)
    ? root.categories.map(normalizeManifestCategory).filter(Boolean) as QMapToolManifestCategory[]
    : [];

  const groups = root.groups && typeof root.groups === 'object' && !Array.isArray(root.groups)
    ? (root.groups as Record<string, unknown>)
    : {};

  return {
    schema: String(root.schema || '').trim() || QMAP_TOOL_MANIFEST_SCHEMA,
    version: String(root.version || '').trim() || '0',
    categories,
    groups: {
      baseAllowlist: normalizeStringList(groups.baseAllowlist)
    }
  };
}

const TOOL_MANIFEST = normalizeManifest(manifestJson as unknown);

export function getQMapToolManifest(): QMapToolManifest {
  if (TOOL_MANIFEST.schema !== QMAP_TOOL_MANIFEST_SCHEMA) {
    console.warn(
      `Unexpected q-map tool manifest schema "${TOOL_MANIFEST.schema}" (expected "${QMAP_TOOL_MANIFEST_SCHEMA}").`
    );
  }
  return TOOL_MANIFEST;
}

export function getQMapAllManifestToolNames(): string[] {
  const manifest = getQMapToolManifest();
  const names = manifest.categories.flatMap(category => category.tools);
  return Array.from(new Set(names)).sort((a, b) => a.localeCompare(b));
}

export function getQMapEvalToolNamesFromManifest(): string[] {
  return getQMapAllManifestToolNames();
}

export function getQMapBaseToolAllowlistSet(): Set<string> {
  const manifest = getQMapToolManifest();
  return new Set(manifest.groups.baseAllowlist);
}
