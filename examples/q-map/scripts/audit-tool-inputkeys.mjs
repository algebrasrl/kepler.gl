#!/usr/bin/env node
// audit-tool-inputkeys.mjs
// Verifica che le chiavi in expected_tool_arguments dei casi ai-eval
// corrispondano a inputKeys reali dichiarati nei contratti.

import fs from 'fs';
import path from 'path';
import {fileURLToPath} from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const base = path.resolve(__dirname, '..');

const contracts = JSON.parse(
  fs.readFileSync(`${base}/artifacts/tool-contracts/qmap-tool-contracts.json`, 'utf8')
);
const functional = JSON.parse(
  fs.readFileSync(`${base}/tests/ai-eval/cases.functional.json`, 'utf8')
);
const adversarial = JSON.parse(
  fs.readFileSync(`${base}/tests/ai-eval/cases.adversarial.json`, 'utf8')
);

const allCases = [...functional, ...adversarial];
const contractTools = contracts.tools;

const errors = [];
let checked = 0;

for (const c of allCases) {
  for (const entry of c.expected_tool_arguments ?? []) {
    const toolNames = entry.tools_any ?? (entry.tool ? [entry.tool] : []);
    const requiredKeys = [
      ...(entry.required_keys_all ?? []),
      ...(entry.required_keys_any ?? []),
    ];
    if (requiredKeys.length === 0) continue;

    for (const toolName of toolNames) {
      const contract = contractTools[toolName];
      if (!contract) continue; // tool non in contratto → già rilevato da altro audit
      const inputKeys = contract.inputKeys ?? [];
      if (inputKeys.length === 0) continue; // nessuna info → skip (non penalizzare)

      for (const key of requiredKeys) {
        checked++;
        if (!inputKeys.includes(key)) {
          errors.push({caseId: c.id, tool: toolName, ghostKey: key, inputKeys});
        }
      }
    }
  }
}

if (errors.length > 0) {
  console.error(`[tool-arg-audit] FAIL: ${errors.length} ghost arg key(s) found`);
  for (const e of errors) {
    console.error(`  case=${e.caseId} tool=${e.tool} ghostKey=${e.ghostKey}`);
    console.error(`    known inputKeys: [${e.inputKeys.join(', ')}]`);
  }
  process.exit(1);
}

console.log(`[tool-arg-audit] OK: checked=${checked} ghost_keys=0`);
