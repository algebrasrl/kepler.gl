#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {validateEvalReportContract} from './lib/eval-report-contract.mjs';

const QMAP_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function resolveQMapPath(target) {
  return path.isAbsolute(target) ? target : path.resolve(QMAP_ROOT, target);
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function readJsonIfExists(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return readJson(filePath);
}

function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim().length > 0;
}

function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

function validateStringArray(value, fieldLabel, errors) {
  if (!Array.isArray(value)) {
    errors.push(`${fieldLabel} must be an array`);
    return;
  }
  for (let i = 0; i < value.length; i += 1) {
    if (!isNonEmptyString(value[i])) {
      errors.push(`${fieldLabel}[${i}] must be a non-empty string`);
    }
  }
}

function validateToolArgumentExpectations(value, fieldLabel, errors) {
  if (!Array.isArray(value)) {
    errors.push(`${fieldLabel} must be an array`);
    return;
  }
  for (let i = 0; i < value.length; i += 1) {
    const row = value[i];
    const label = `${fieldLabel}[${i}]`;
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      errors.push(`${label} must be an object`);
      continue;
    }
    const hasTool = isNonEmptyString(row.tool);
    const hasToolsAny = Array.isArray(row.tools_any) && row.tools_any.length > 0;
    if (hasTool && hasToolsAny) {
      errors.push(`${label} must define either .tool or .tools_any, not both`);
    } else if (!hasTool && !hasToolsAny) {
      errors.push(`${label} must define .tool or .tools_any`);
    }
    if (row.tools_any !== undefined) {
      validateStringArray(row.tools_any, `${label}.tools_any`, errors);
    }
    if (row.required_keys_all !== undefined) {
      validateStringArray(row.required_keys_all, `${label}.required_keys_all`, errors);
    }
    if (row.required_keys_any !== undefined) {
      validateStringArray(row.required_keys_any, `${label}.required_keys_any`, errors);
    }
    if (row.forbidden_keys !== undefined) {
      validateStringArray(row.forbidden_keys, `${label}.forbidden_keys`, errors);
    }
    if (row.required_key_values !== undefined) {
      if (!row.required_key_values || typeof row.required_key_values !== 'object' || Array.isArray(row.required_key_values)) {
        errors.push(`${label}.required_key_values must be an object`);
      } else {
        for (const [key, expectedValue] of Object.entries(row.required_key_values)) {
          if (!isNonEmptyString(key)) {
            errors.push(`${label}.required_key_values contains an empty key`);
            continue;
          }
          const allowedType =
            expectedValue === null ||
            typeof expectedValue === 'string' ||
            typeof expectedValue === 'number' ||
            typeof expectedValue === 'boolean';
          if (!allowedType) {
            errors.push(`${label}.required_key_values.${key} must be string|number|boolean|null`);
          }
        }
      }
    }
    const hasConstraint =
      (Array.isArray(row.required_keys_all) && row.required_keys_all.length > 0) ||
      (Array.isArray(row.required_keys_any) && row.required_keys_any.length > 0) ||
      (Array.isArray(row.forbidden_keys) && row.forbidden_keys.length > 0) ||
      (row.required_key_values && typeof row.required_key_values === 'object' && !Array.isArray(row.required_key_values) && Object.keys(row.required_key_values).length > 0);
    if (!hasConstraint) {
      errors.push(`${label} must define at least one key constraint`);
    }
  }
}

function validateMaxToolCallsByName(value, fieldLabel, errors) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    errors.push(`${fieldLabel} must be an object`);
    return;
  }
  for (const [toolName, rawLimit] of Object.entries(value)) {
    if (!isNonEmptyString(toolName)) {
      errors.push(`${fieldLabel} contains an empty tool name`);
      continue;
    }
    const limit = Number(rawLimit);
    if (!Number.isInteger(limit) || limit < 0) {
      errors.push(`${fieldLabel}.${toolName} must be a non-negative integer`);
    }
  }
}

function validateMetricGateBlock(value, fieldLabel, errors, {required = true} = {}) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    errors.push(`${fieldLabel} must be an object`);
    return;
  }
  const gateFields = [
    'min_pass_rate',
    'min_avg_case_score',
    'min_p25_case_score',
    'min_min_case_score'
  ];
  for (const field of gateFields) {
    if (!required && value[field] === undefined) continue;
    if (!isFiniteNumber(value[field])) {
      errors.push(`${fieldLabel}.${field} must be a number`);
    }
  }
}

function validateCase(caseDef, sourceLabel, index, errors) {
  const caseLabel = `${sourceLabel}[${index}]`;
  if (!caseDef || typeof caseDef !== 'object' || Array.isArray(caseDef)) {
    errors.push(`${caseLabel} must be an object`);
    return;
  }

  const requiredStringFields = ['id', 'area', 'kpi_id', 'user_prompt'];
  for (const field of requiredStringFields) {
    if (!isNonEmptyString(caseDef[field])) {
      errors.push(`${caseLabel}.${field} must be a non-empty string`);
    }
  }

  if (caseDef.criticality !== undefined) {
    const criticality = String(caseDef.criticality).trim();
    if (!['critical', 'standard'].includes(criticality)) {
      errors.push(`${caseLabel}.criticality must be "critical" or "standard"`);
    }
  }
  if (
    caseDef.deterministic_constraints !== undefined &&
    typeof caseDef.deterministic_constraints !== 'boolean'
  ) {
    errors.push(`${caseLabel}.deterministic_constraints must be a boolean`);
  }
  if (caseDef.expected_response_mode !== undefined) {
    const expectedResponseMode = String(caseDef.expected_response_mode).trim();
    if (!['clarification', 'limitation'].includes(expectedResponseMode)) {
      errors.push(`${caseLabel}.expected_response_mode must be "clarification" or "limitation"`);
    }
  }
  if (caseDef.response_mode_markers_any !== undefined) {
    validateStringArray(caseDef.response_mode_markers_any, `${caseLabel}.response_mode_markers_any`, errors);
  }
  if (
    caseDef.require_grounded_final_answer !== undefined &&
    typeof caseDef.require_grounded_final_answer !== 'boolean'
  ) {
    errors.push(`${caseLabel}.require_grounded_final_answer must be a boolean`);
  }
  if (caseDef.grounded_required_tools_all !== undefined) {
    validateStringArray(caseDef.grounded_required_tools_all, `${caseLabel}.grounded_required_tools_all`, errors);
  }
  if (caseDef.mock_tool_results !== undefined) {
    if (
      !caseDef.mock_tool_results ||
      typeof caseDef.mock_tool_results !== 'object' ||
      Array.isArray(caseDef.mock_tool_results)
    ) {
      errors.push(`${caseLabel}.mock_tool_results must be an object`);
    } else {
      for (const [toolName, override] of Object.entries(caseDef.mock_tool_results)) {
        if (!isNonEmptyString(toolName)) {
          errors.push(`${caseLabel}.mock_tool_results contains an empty tool name`);
          continue;
        }
        if (!override || typeof override !== 'object' || Array.isArray(override)) {
          errors.push(`${caseLabel}.mock_tool_results.${toolName} must be an object`);
        }
      }
    }
  }

  const toolArrayFields = [
    'required_tools_any',
    'required_tools_all',
    'expected_tools_any',
    'forbidden_tools'
  ];
  for (const field of toolArrayFields) {
    if (caseDef[field] !== undefined) {
      validateStringArray(caseDef[field], `${caseLabel}.${field}`, errors);
    }
  }

  if (caseDef.expected_keywords_any !== undefined) {
    validateStringArray(caseDef.expected_keywords_any, `${caseLabel}.expected_keywords_any`, errors);
  }
  if (caseDef.expected_tool_arguments !== undefined) {
    validateToolArgumentExpectations(
      caseDef.expected_tool_arguments,
      `${caseLabel}.expected_tool_arguments`,
      errors
    );
  }
  if (caseDef.max_tool_calls_by_name !== undefined) {
    validateMaxToolCallsByName(caseDef.max_tool_calls_by_name, `${caseLabel}.max_tool_calls_by_name`, errors);
  }

  const requiredAny = Array.isArray(caseDef.required_tools_any) ? caseDef.required_tools_any : [];
  const requiredAll = Array.isArray(caseDef.required_tools_all) ? caseDef.required_tools_all : [];
  if (!requiredAny.length && !requiredAll.length) {
    errors.push(
      `${caseLabel} must define at least one required tool list (required_tools_any or required_tools_all)`
    );
  }

  const numericFields = ['min_case_score', 'min_tool_precision', 'min_tool_argument_score'];
  for (const field of numericFields) {
    if (caseDef[field] !== undefined && !isFiniteNumber(caseDef[field])) {
      errors.push(`${caseLabel}.${field} must be a finite number`);
    }
  }
  if (caseDef.max_extra_tool_calls !== undefined) {
    const n = Number(caseDef.max_extra_tool_calls);
    if (!Number.isInteger(n) || n < 0) {
      errors.push(`${caseLabel}.max_extra_tool_calls must be a non-negative integer`);
    }
  }
}

function validateMatrix(matrix, errors) {
  if (!matrix || typeof matrix !== 'object' || Array.isArray(matrix)) {
    errors.push('architecture-matrix root must be an object');
    return {areas: []};
  }

  if (!isFiniteNumber(matrix.version)) {
    errors.push('architecture-matrix.version must be a finite number');
  }
  if (!isNonEmptyString(matrix.generatedAt)) {
    errors.push('architecture-matrix.generatedAt must be a non-empty string');
  }

  const policy = matrix.evaluationPolicy;
  if (!policy || typeof policy !== 'object' || Array.isArray(policy)) {
    errors.push('architecture-matrix.evaluationPolicy must be an object');
  } else {
    validateMetricGateBlock(policy.runGates, 'architecture-matrix.evaluationPolicy.runGates', errors, {
      required: true
    });
    validateMetricGateBlock(
      policy.areaDefaults,
      'architecture-matrix.evaluationPolicy.areaDefaults',
      errors,
      {required: true}
    );
    if (
      !policy.criticalityDefaults ||
      typeof policy.criticalityDefaults !== 'object' ||
      Array.isArray(policy.criticalityDefaults)
    ) {
      errors.push('architecture-matrix.evaluationPolicy.criticalityDefaults must be an object');
    }
  }

  if (!Array.isArray(matrix.areas) || matrix.areas.length === 0) {
    errors.push('architecture-matrix.areas must be a non-empty array');
    return {areas: []};
  }

  const areaIds = new Set();
  for (let areaIndex = 0; areaIndex < matrix.areas.length; areaIndex += 1) {
    const area = matrix.areas[areaIndex];
    const areaLabel = `architecture-matrix.areas[${areaIndex}]`;
    if (!area || typeof area !== 'object' || Array.isArray(area)) {
      errors.push(`${areaLabel} must be an object`);
      continue;
    }

    if (!isNonEmptyString(area.id)) errors.push(`${areaLabel}.id must be a non-empty string`);
    if (!isNonEmptyString(area.label)) errors.push(`${areaLabel}.label must be a non-empty string`);
    if (isNonEmptyString(area.id)) {
      if (areaIds.has(area.id)) errors.push(`duplicate area id "${area.id}" in architecture-matrix`);
      areaIds.add(area.id);
    }

    if (!Array.isArray(area.kpis) || area.kpis.length === 0) {
      errors.push(`${areaLabel}.kpis must be a non-empty array`);
    } else {
      const kpiIds = new Set();
      for (let kpiIndex = 0; kpiIndex < area.kpis.length; kpiIndex += 1) {
        const kpi = area.kpis[kpiIndex];
        const kpiLabel = `${areaLabel}.kpis[${kpiIndex}]`;
        if (!kpi || typeof kpi !== 'object' || Array.isArray(kpi)) {
          errors.push(`${kpiLabel} must be an object`);
          continue;
        }
        if (!isNonEmptyString(kpi.id)) errors.push(`${kpiLabel}.id must be a non-empty string`);
        if (!isNonEmptyString(kpi.metric)) errors.push(`${kpiLabel}.metric must be a non-empty string`);
        if (!isNonEmptyString(kpi.target)) errors.push(`${kpiLabel}.target must be a non-empty string`);
        if (isNonEmptyString(kpi.id)) {
          if (kpiIds.has(kpi.id)) {
            errors.push(`duplicate kpi id "${kpi.id}" inside area "${area.id || areaIndex}"`);
          }
          kpiIds.add(kpi.id);
        }
      }
    }

    if (!Array.isArray(area.caseIds) || area.caseIds.length === 0) {
      errors.push(`${areaLabel}.caseIds must be a non-empty array`);
    } else {
      validateStringArray(area.caseIds, `${areaLabel}.caseIds`, errors);
    }

    if (area.gates !== undefined) {
      validateMetricGateBlock(area.gates, `${areaLabel}.gates`, errors, {required: false});
    }
  }

  return {areas: matrix.areas};
}

function validateReportCollection(resultsDir, errors) {
  if (!fs.existsSync(resultsDir)) {
    return {reportCount: 0};
  }

  const names = fs.readdirSync(resultsDir).filter(name => /^report-.*\.json$/i.test(name));
  for (const name of names) {
    const fullPath = path.join(resultsDir, name);
    const label = `results/${name}`;
    let report = null;
    try {
      report = readJson(fullPath);
    } catch (error) {
      errors.push(`${label} must be valid JSON (${String(error?.message || error)})`);
      continue;
    }

    const contractErrors = validateEvalReportContract(report, label);
    for (const error of contractErrors) {
      errors.push(error);
    }
  }

  return {reportCount: names.length};
}

function main() {
  const samplePath = resolveQMapPath('tests/ai-eval/cases.sample.json');
  const functionalPath = resolveQMapPath('tests/ai-eval/cases.functional.json');
  const adversarialPath = resolveQMapPath('tests/ai-eval/cases.adversarial.json');
  const matrixPath = resolveQMapPath('tests/ai-eval/architecture-matrix.json');
  const resultsDir = resolveQMapPath('tests/ai-eval/results');
  const errors = [];

  const sampleCases = readJson(samplePath);
  const functionalCases = readJson(functionalPath);
  const adversarialCases = readJsonIfExists(adversarialPath);
  const matrix = readJson(matrixPath);
  const {areas} = validateMatrix(matrix, errors);

  if (!Array.isArray(sampleCases) || sampleCases.length === 0) {
    errors.push('cases.sample.json must be a non-empty array');
  }
  if (!Array.isArray(functionalCases) || functionalCases.length === 0) {
    errors.push('cases.functional.json must be a non-empty array');
  }
  if (adversarialCases !== null && (!Array.isArray(adversarialCases) || adversarialCases.length === 0)) {
    errors.push('cases.adversarial.json must be a non-empty array when present');
  }

  const allCases = [];
  if (Array.isArray(sampleCases)) {
    sampleCases.forEach((caseDef, index) => {
      validateCase(caseDef, 'cases.sample', index, errors);
      allCases.push({caseDef, source: 'cases.sample', index});
    });
  }
  if (Array.isArray(functionalCases)) {
    functionalCases.forEach((caseDef, index) => {
      validateCase(caseDef, 'cases.functional', index, errors);
      allCases.push({caseDef, source: 'cases.functional', index});
    });
  }
  if (Array.isArray(adversarialCases)) {
    adversarialCases.forEach((caseDef, index) => {
      validateCase(caseDef, 'cases.adversarial', index, errors);
      allCases.push({caseDef, source: 'cases.adversarial', index});
    });
  }

  const byCaseId = new Map();
  for (const item of allCases) {
    const caseId = String(item.caseDef?.id || '').trim();
    if (!caseId) continue;
    if (byCaseId.has(caseId)) {
      const first = byCaseId.get(caseId);
      errors.push(
        `duplicate case id "${caseId}" in ${first.source}[${first.index}] and ${item.source}[${item.index}]`
      );
    } else {
      byCaseId.set(caseId, {source: item.source, index: item.index, caseDef: item.caseDef});
    }
  }

  const areaToKpis = new Map();
  for (const area of areas) {
    const areaId = String(area?.id || '').trim();
    if (!areaId) continue;
    const kpiSet = new Set(
      (Array.isArray(area?.kpis) ? area.kpis : [])
        .map(kpi => String(kpi?.id || '').trim())
        .filter(Boolean)
    );
    areaToKpis.set(areaId, kpiSet);
  }

  for (const [caseId, row] of byCaseId.entries()) {
    const caseDef = row.caseDef;
    const areaId = String(caseDef?.area || '').trim();
    const kpiId = String(caseDef?.kpi_id || '').trim();
    if (areaId && !areaToKpis.has(areaId)) {
      errors.push(`case "${caseId}" references unknown area "${areaId}"`);
      continue;
    }
    if (areaId && kpiId && !areaToKpis.get(areaId).has(kpiId)) {
      errors.push(`case "${caseId}" references unknown kpi "${kpiId}" for area "${areaId}"`);
    }
  }

  const referencedCaseIds = new Set();
  for (const area of areas) {
    const areaId = String(area?.id || '').trim() || '<missing-area-id>';
    const caseIds = Array.isArray(area?.caseIds) ? area.caseIds : [];
    for (let i = 0; i < caseIds.length; i += 1) {
      const caseId = String(caseIds[i] || '').trim();
      if (!caseId) continue;
      referencedCaseIds.add(caseId);
      if (!byCaseId.has(caseId)) {
        errors.push(`matrix area "${areaId}" references missing case id "${caseId}"`);
      }
    }
  }

  for (const caseId of byCaseId.keys()) {
    if (!referencedCaseIds.has(caseId)) {
      errors.push(`case "${caseId}" is not referenced by architecture-matrix caseIds`);
    }
  }

  const {reportCount} = validateReportCollection(resultsDir, errors);

  if (errors.length) {
    for (const error of errors) {
      console.error(`[ai-eval-schema] FAIL: ${error}`);
    }
    process.exit(1);
  }

  const kpiCount = areas.reduce(
    (sum, area) => sum + (Array.isArray(area?.kpis) ? area.kpis.length : 0),
    0
  );
  console.log(
    `[ai-eval-schema] OK: sampleCases=${Array.isArray(sampleCases) ? sampleCases.length : 0} functionalCases=${Array.isArray(functionalCases) ? functionalCases.length : 0} adversarialCases=${Array.isArray(adversarialCases) ? adversarialCases.length : 0} matrixAreas=${areas.length} matrixKpis=${kpiCount} reportContracts=${reportCount}`
  );
}

main();
