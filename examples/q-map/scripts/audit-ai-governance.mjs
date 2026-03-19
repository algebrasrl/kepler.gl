#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

const REQUIRED_FILES = [
  "docs/ai-governance/policy.md",
  "docs/ai-governance/risk-register.yaml",
  "docs/ai-governance/control-matrix.md",
  "docs/ai-governance/incident-runbook.md",
];

function fail(message) {
  console.error(`[ai-governance] FAIL: ${message}`);
  process.exit(2);
}

function readRequiredFile(repoRoot, relPath) {
  const fullPath = path.resolve(repoRoot, relPath);
  if (!fs.existsSync(fullPath)) {
    fail(`missing required file: ${relPath}`);
  }
  const raw = fs.readFileSync(fullPath, "utf8");
  if (!raw.trim()) {
    fail(`empty required file: ${relPath}`);
  }
  return raw;
}

function uniqueSorted(values) {
  return Array.from(new Set(values)).sort((a, b) => a.localeCompare(b));
}

function extractAllMatches(input, regex, captureIndex = 1) {
  const globalRegex = regex.global
    ? regex
    : new RegExp(regex.source, `${regex.flags}g`);
  const out = [];
  for (const match of input.matchAll(globalRegex)) {
    const value = String(match[captureIndex] || "").trim();
    if (value) out.push(value);
  }
  return out;
}

function parsePolicyControls(policyText) {
  const controls = extractAllMatches(policyText, /^\-\s*`(GOV-\d{3})`/gm);
  if (!controls.length) {
    fail("policy.md must define control ids as bullet lines '- `GOV-XXX` ...'");
  }
  return uniqueSorted(controls);
}

function parseRiskRegister(riskText) {
  const riskIds = uniqueSorted(extractAllMatches(riskText, /^\s*-\s*id:\s*(RISK-\d{3})\s*$/gm));
  if (!riskIds.length) {
    fail("risk-register.yaml must define at least one risk id ('- id: RISK-XXX').");
  }

  const referencedControls = uniqueSorted(
    extractAllMatches(riskText, /controls:\s*\[([^\]]*)\]/g, 1)
      .flatMap(raw => raw.split(",").map(part => part.trim()))
      .map(token => token.replace(/^["']|["']$/g, ""))
      .filter(token => /^GOV-\d{3}$/.test(token))
  );
  if (!referencedControls.length) {
    fail("risk-register.yaml must reference controls via 'controls: [GOV-...]'.");
  }

  const reviewDateRaw = extractAllMatches(riskText, /^last_reviewed_on:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*$/m)[0];
  if (!reviewDateRaw) {
    fail("risk-register.yaml must include 'last_reviewed_on: YYYY-MM-DD'.");
  }

  return {riskIds, referencedControls, reviewDateRaw};
}

function parseControlMatrix(controlMatrixText) {
  const matrixControls = uniqueSorted(extractAllMatches(controlMatrixText, /`(GOV-\d{3})`/g));
  if (!matrixControls.length) {
    fail("control-matrix.md must reference control ids (`GOV-XXX`).");
  }
  return matrixControls;
}

function validateRunbook(runbookText) {
  const requiredMarkers = [
    "kill switch",
    "containment",
    "postmortem",
    "regression case",
  ];
  const normalized = runbookText.toLowerCase();
  for (const marker of requiredMarkers) {
    if (!normalized.includes(marker)) {
      fail(`incident-runbook.md missing required section/marker: '${marker}'.`);
    }
  }
}

function parseDateYmd(rawDate) {
  const parsed = new Date(`${rawDate}T00:00:00Z`);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function main() {
  const repoRoot = process.cwd();

  for (const filePath of REQUIRED_FILES) {
    readRequiredFile(repoRoot, filePath);
  }

  const policyText = readRequiredFile(repoRoot, "docs/ai-governance/policy.md");
  const riskText = readRequiredFile(repoRoot, "docs/ai-governance/risk-register.yaml");
  const matrixText = readRequiredFile(repoRoot, "docs/ai-governance/control-matrix.md");
  const runbookText = readRequiredFile(repoRoot, "docs/ai-governance/incident-runbook.md");

  const policyControls = parsePolicyControls(policyText);
  const {riskIds, referencedControls, reviewDateRaw} = parseRiskRegister(riskText);
  const matrixControls = parseControlMatrix(matrixText);
  validateRunbook(runbookText);

  const unknownRiskControls = referencedControls.filter(control => !policyControls.includes(control));
  if (unknownRiskControls.length) {
    fail(`risk-register references unknown controls: ${unknownRiskControls.join(", ")}`);
  }

  const missingMatrixControls = policyControls.filter(control => !matrixControls.includes(control));
  if (missingMatrixControls.length) {
    fail(`control-matrix missing policy controls: ${missingMatrixControls.join(", ")}`);
  }

  const reviewDate = parseDateYmd(reviewDateRaw);
  if (!reviewDate) {
    fail(`invalid last_reviewed_on date in risk-register.yaml: ${reviewDateRaw}`);
  }
  const ageDays = Math.floor((Date.now() - reviewDate.getTime()) / (1000 * 60 * 60 * 24));
  if (ageDays > 180) {
    fail(`risk-register last_reviewed_on is stale (${ageDays} days > 180).`);
  }

  console.log(
    `[ai-governance] OK: controls=${policyControls.length} risks=${riskIds.length} reviewAgeDays=${ageDays}`
  );
}

main();
