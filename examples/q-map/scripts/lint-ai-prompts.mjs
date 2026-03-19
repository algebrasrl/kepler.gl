#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const QMAP_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function resolveQMapPath(target) {
  return path.isAbsolute(target) ? target : path.resolve(QMAP_ROOT, target);
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function parseArgs(argv) {
  const out = {
    samplePath: 'tests/ai-eval/cases.sample.json',
    functionalPath: 'tests/ai-eval/cases.functional.json',
    adversarialPath: 'tests/ai-eval/cases.adversarial.json',
    resultsDir: 'tests/ai-eval/results',
    maxReports: 30,
    minHistoryRuns: 3,
    minKeywordHitRate: 0.7,
    enforceKeywordInPrompt: false,
    strictHistory: false,
    json: false
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const val = argv[i + 1];
    if (arg === '--sample' && val) out.samplePath = val;
    if (arg === '--functional' && val) out.functionalPath = val;
    if (arg === '--adversarial' && val) out.adversarialPath = val;
    if (arg === '--results-dir' && val) out.resultsDir = val;
    if (arg === '--max-reports' && val) out.maxReports = Math.max(1, Number(val) || 30);
    if (arg === '--min-history-runs' && val) out.minHistoryRuns = Math.max(1, Number(val) || 3);
    if (arg === '--min-keyword-hit-rate' && val) out.minKeywordHitRate = Number(val);
    if (arg === '--enforce-keyword-in-prompt') out.enforceKeywordInPrompt = true;
    if (arg === '--strict-history') out.strictHistory = true;
    if (arg === '--json') out.json = true;
  }
  return out;
}

function normalize(text) {
  return String(text || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenize(text) {
  const normalized = normalize(text);
  return new Set(normalized.split(' ').filter(Boolean));
}

function normalizedContainsKeyword(text, keyword) {
  const haystack = normalize(text);
  const needle = normalize(keyword);
  if (!haystack || !needle) return false;
  return haystack.includes(needle);
}

function addFinding(findings, severity, code, message, caseId = null) {
  findings.push({severity, code, message, caseId});
}

function collectCases(sampleCases, functionalCases, adversarialCases) {
  const cases = [];
  for (const row of sampleCases) cases.push({...row, __source: 'sample'});
  for (const row of functionalCases) cases.push({...row, __source: 'functional'});
  for (const row of adversarialCases) cases.push({...row, __source: 'adversarial'});
  return cases;
}

function getToolSet(caseDef) {
  const fields = ['required_tools_any', 'required_tools_all', 'expected_tools_any', 'forbidden_tools'];
  const values = [];
  for (const field of fields) {
    const list = Array.isArray(caseDef?.[field]) ? caseDef[field] : [];
    for (const item of list) values.push(String(item || '').trim());
  }
  return new Set(values.filter(Boolean).map(item => item.toLowerCase()));
}

function hasAny(set, candidates) {
  for (const item of candidates) {
    if (set.has(String(item).toLowerCase())) return true;
  }
  return false;
}

function lintStaticCases(cases, findings, opts) {
  const byId = new Map();
  const promptByNormalized = new Map();

  for (const caseDef of cases) {
    const caseId = String(caseDef?.id || '').trim();
    const source = caseDef.__source;
    if (!caseId) {
      addFinding(findings, 'error', 'missing_case_id', `${source}: missing case id`);
      continue;
    }

    if (byId.has(caseId)) {
      addFinding(
        findings,
        'error',
        'duplicate_case_id',
        `duplicate case id "${caseId}" in ${source} and ${byId.get(caseId)}`,
        caseId
      );
    } else {
      byId.set(caseId, source);
    }

    const prompt = String(caseDef?.user_prompt || '').trim();
    if (!prompt) {
      addFinding(findings, 'error', 'missing_user_prompt', 'missing user_prompt', caseId);
    } else {
      const normalizedPrompt = normalize(prompt);
      if (promptByNormalized.has(normalizedPrompt)) {
        addFinding(
          findings,
          'warning',
          'duplicate_prompt_text',
          `user_prompt duplicates case "${promptByNormalized.get(normalizedPrompt)}" after normalization`,
          caseId
        );
      } else {
        promptByNormalized.set(normalizedPrompt, caseId);
      }
    }

    const requiredAny = new Set(
      (Array.isArray(caseDef?.required_tools_any) ? caseDef.required_tools_any : [])
        .map(v => String(v || '').trim().toLowerCase())
        .filter(Boolean)
    );
    const requiredAll = new Set(
      (Array.isArray(caseDef?.required_tools_all) ? caseDef.required_tools_all : [])
        .map(v => String(v || '').trim().toLowerCase())
        .filter(Boolean)
    );
    const expectedAny = new Set(
      (Array.isArray(caseDef?.expected_tools_any) ? caseDef.expected_tools_any : [])
        .map(v => String(v || '').trim().toLowerCase())
        .filter(Boolean)
    );
    const forbidden = new Set(
      (Array.isArray(caseDef?.forbidden_tools) ? caseDef.forbidden_tools : [])
        .map(v => String(v || '').trim().toLowerCase())
        .filter(Boolean)
    );
    const keywords = (Array.isArray(caseDef?.expected_keywords_any) ? caseDef.expected_keywords_any : [])
      .map(v => String(v || '').trim())
      .filter(Boolean);

    if (requiredAny.size === 0 && requiredAll.size === 0) {
      addFinding(
        findings,
        'error',
        'missing_required_tools',
        'case defines no required tools (required_tools_any|required_tools_all)',
        caseId
      );
    }

    const overlapRequiredForbidden = [...new Set([...requiredAny, ...requiredAll])].filter(tool =>
      forbidden.has(tool)
    );
    if (overlapRequiredForbidden.length) {
      addFinding(
        findings,
        'error',
        'required_forbidden_overlap',
        `required and forbidden tool overlap: ${overlapRequiredForbidden.join(', ')}`,
        caseId
      );
    }

    const overlapExpectedForbidden = [...expectedAny].filter(tool => forbidden.has(tool));
    if (overlapExpectedForbidden.length) {
      addFinding(
        findings,
        'warning',
        'expected_forbidden_overlap',
        `expected and forbidden tool overlap: ${overlapExpectedForbidden.join(', ')}`,
        caseId
      );
    }

    const normalizedKeywordMap = new Map();
    for (const keyword of keywords) {
      const normalizedKeyword = normalize(keyword);
      if (!normalizedKeyword) continue;
      if (normalizedKeywordMap.has(normalizedKeyword)) {
        addFinding(
          findings,
          'warning',
          'duplicate_expected_keyword',
          `duplicate expected keyword "${keyword}"`,
          caseId
        );
      } else {
        normalizedKeywordMap.set(normalizedKeyword, keyword);
      }
    }

    if (keywords.length === 0) {
      addFinding(findings, 'warning', 'missing_expected_keywords', 'no expected_keywords_any', caseId);
    } else if (keywords.length > 5) {
      addFinding(
        findings,
        'warning',
        'too_many_expected_keywords',
        `expected_keywords_any has ${keywords.length} entries (recommended <= 5)`,
        caseId
      );
    }

    const promptTokens = tokenize(prompt);
    if (opts.enforceKeywordInPrompt) {
      for (const keyword of normalizedKeywordMap.keys()) {
        const keywordTokens = keyword.split(' ').filter(Boolean);
        const allInPrompt = keywordTokens.every(token => promptTokens.has(token));
        if (!allInPrompt) {
          addFinding(
            findings,
            'warning',
            'keyword_not_in_prompt',
            `expected keyword "${keyword}" not present in prompt text`,
            caseId
          );
        }
      }
    }

    const promptLower = normalize(prompt);
    const toolSet = getToolSet(caseDef);
    const rankingIntent =
      /\b(classific\w*|priorit\w*|superament\w*|ranking)\b/.test(promptLower) ||
      /\bordin(a|are|ato|ata|ati|ate)\b/.test(promptLower) ||
      /\btop(?:\s*\d+)?\b/.test(promptLower);
    const hasRankingEvidenceTools =
      hasAny(toolSet, ['rankqmapdatasetrows']) ||
      hasAny(toolSet, ['queryqcumberterritorialunits', 'queryqcumberdataset', 'queryqcumberdatasetspatial']);
    if (rankingIntent && !hasRankingEvidenceTools) {
      addFinding(
        findings,
        'warning',
        'ranking_without_rank_tool',
        'ranking/priority intent detected without rankQMapDatasetRows in tool sets',
        caseId
      );
    }
    const cloudIntent = /\b(cloud)\b/.test(promptLower);
    if (cloudIntent && !hasAny(toolSet, ['listqmapcloudmaps', 'loadqmapcloudmap', 'loadcloudmapandwait'])) {
      addFinding(
        findings,
        'warning',
        'cloud_intent_without_cloud_tools',
        'cloud/reliability intent detected without cloud tool routing',
        caseId
      );
    }
    const h3Intent = /\b(h3|tassell|griglia|esagon)\w*/.test(promptLower);
    if (
      h3Intent &&
      !hasAny(toolSet, [
        'tassellateselectedgeometry',
        'tassellatedatasetlayer',
        'aggregatedatasettoh3',
        'joinqmapdatasetsonh3',
        'paintqmaph3cell',
        'paintqmaph3cells',
        'paintqmaph3ring'
      ])
    ) {
      addFinding(
        findings,
        'warning',
        'h3_intent_without_h3_tools',
        'H3 intent detected without H3 tool routing',
        caseId
      );
    }
  }

  return byId;
}

function collectRecentReports(resultsDir, maxReports, minCreatedAtMs = 0) {
  if (!fs.existsSync(resultsDir)) return [];
  const files = fs
    .readdirSync(resultsDir)
    .filter(name => /^report-.*\.json$/i.test(name))
    .map(name => path.join(resultsDir, name));

  const reports = [];
  for (const filePath of files) {
    try {
      const payload = readJson(filePath);
      if (payload?.invalidBaseline === true) {
        continue;
      }
      const createdAt = Date.parse(String(payload?.createdAt || ''));
      if (Number.isFinite(createdAt) && createdAt < minCreatedAtMs) {
        continue;
      }
      reports.push({
        filePath,
        createdAt: Number.isFinite(createdAt) ? createdAt : 0,
        payload
      });
    } catch {
      // ignore invalid historical report files
    }
  }
  reports.sort((a, b) => b.createdAt - a.createdAt);
  return reports.slice(0, Math.max(1, maxReports));
}

function lintHistory(cases, reports, findings, opts) {
  const caseById = new Map(cases.map(item => [String(item?.id || '').trim(), item]));
  const statsByCase = new Map();

  for (const report of reports) {
    const rows = Array.isArray(report?.payload?.cases) ? report.payload.cases : [];
    for (const row of rows) {
      const gateFailures = Array.isArray(row?.gateFailures) ? row.gateFailures : [];
      const hasTransportAbortTag = gateFailures.some(value =>
        String(value || '')
          .toLowerCase()
          .includes('transport-error-abort')
      );
      const hasTransportError = String(row?.transportError || '').trim().length > 0;
      const status = Number(row?.status);
      if (hasTransportAbortTag || hasTransportError || (Number.isFinite(status) && status <= 0)) {
        // Transport/preflight failures do not reflect prompt quality and should not skew keyword stability history.
        continue;
      }

      const caseId = String(row?.id || '').trim();
      if (!caseById.has(caseId)) continue;
      if (!statsByCase.has(caseId)) {
        statsByCase.set(caseId, {runs: 0, hitsByKeyword: new Map()});
      }
      const stat = statsByCase.get(caseId);
      stat.runs += 1;

      const matched = new Set(
        (Array.isArray(row?.matchedKeywords) ? row.matchedKeywords : [])
          .map(keyword => normalize(keyword))
          .filter(Boolean)
      );
      const content = String(row?.content || '');
      const expected = (Array.isArray(caseById.get(caseId)?.expected_keywords_any)
        ? caseById.get(caseId).expected_keywords_any
        : [])
        .map(keyword => normalize(keyword))
        .filter(Boolean);
      for (const keyword of expected) {
        if (!stat.hitsByKeyword.has(keyword)) stat.hitsByKeyword.set(keyword, 0);
        if (matched.has(keyword) || normalizedContainsKeyword(content, keyword)) {
          stat.hitsByKeyword.set(keyword, stat.hitsByKeyword.get(keyword) + 1);
        }
      }
    }
  }

  for (const [caseId, stat] of statsByCase.entries()) {
    if (stat.runs < opts.minHistoryRuns) continue;
    const expectedKeywordCount = stat.hitsByKeyword.size;
    const allowedUnstableKeywords = expectedKeywordCount > 1 ? 1 : 0;
    const unstableKeywords = [];
    for (const [keyword, hits] of stat.hitsByKeyword.entries()) {
      const hitRate = hits / stat.runs;
      if (hitRate < opts.minKeywordHitRate) {
        unstableKeywords.push({keyword, hitRate});
      }
    }
    unstableKeywords
      .sort((a, b) => a.hitRate - b.hitRate || a.keyword.localeCompare(b.keyword))
      .slice(allowedUnstableKeywords)
      .forEach(({keyword, hitRate}) => {
        addFinding(
          findings,
          opts.strictHistory ? 'error' : 'warning',
          'unstable_keyword_history',
          `keyword "${keyword}" hitRate=${hitRate.toFixed(3)} below threshold=${opts.minKeywordHitRate.toFixed(
            3
          )} across ${stat.runs} reports`,
          caseId
        );
      });
  }
}

function summarize(findings, reportCount, caseCount) {
  const errors = findings.filter(f => f.severity === 'error');
  const warnings = findings.filter(f => f.severity === 'warning');
  return {
    caseCount,
    reportCount,
    errorCount: errors.length,
    warningCount: warnings.length,
    findings
  };
}

function printHuman(summary) {
  for (const finding of summary.findings) {
    const target = finding.caseId ? ` case=${finding.caseId}` : '';
    console.log(`[prompt-lint][${finding.severity}] ${finding.code}${target} ${finding.message}`);
  }
  console.log(
    `[prompt-lint] summary: cases=${summary.caseCount} reports=${summary.reportCount} errors=${summary.errorCount} warnings=${summary.warningCount}`
  );
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  const samplePath = resolveQMapPath(opts.samplePath);
  const functionalPath = resolveQMapPath(opts.functionalPath);
  const adversarialPath = resolveQMapPath(opts.adversarialPath);
  const resultsDir = resolveQMapPath(opts.resultsDir);

  const sampleCases = readJson(samplePath);
  const functionalCases = readJson(functionalPath);
  const adversarialCases = fs.existsSync(adversarialPath) ? readJson(adversarialPath) : [];
  const cases = collectCases(
    Array.isArray(sampleCases) ? sampleCases : [],
    Array.isArray(functionalCases) ? functionalCases : [],
    Array.isArray(adversarialCases) ? adversarialCases : []
  );
  const findings = [];

  lintStaticCases(cases, findings, opts);
  const sampleMtime = fs.existsSync(samplePath) ? fs.statSync(samplePath).mtimeMs : 0;
  const functionalMtime = fs.existsSync(functionalPath) ? fs.statSync(functionalPath).mtimeMs : 0;
  const adversarialMtime = fs.existsSync(adversarialPath) ? fs.statSync(adversarialPath).mtimeMs : 0;
  const minReportCreatedAtMs = Math.max(sampleMtime, functionalMtime, adversarialMtime);
  const reports = collectRecentReports(resultsDir, opts.maxReports, minReportCreatedAtMs);
  lintHistory(cases, reports, findings, opts);

  const summary = summarize(findings, reports.length, cases.length);
  if (opts.json) {
    console.log(JSON.stringify(summary, null, 2));
  } else {
    printHuman(summary);
  }

  if (summary.errorCount > 0) {
    process.exitCode = 1;
  }
}

main();
