#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

function parseArgs(argv) {
  const out = {
    logPath: 'backends/logs/q-assistant/chat-audit/session-default.jsonl',
    ids: [],
    profile: 'h3-clc-regions',
    json: false
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (arg === '--log' && next) {
      out.logPath = next;
      i += 1;
      continue;
    }
    if (arg === '--ids' && next) {
      out.ids.push(
        ...next
          .split(',')
          .map(v => v.trim())
          .filter(Boolean)
      );
      i += 1;
      continue;
    }
    if (arg === '--id' && next) {
      out.ids.push(next.trim());
      i += 1;
      continue;
    }
    if (arg === '--profile' && next) {
      out.profile = next.trim();
      i += 1;
      continue;
    }
    if (arg === '--json') {
      out.json = true;
      continue;
    }
  }

  out.ids = [...new Set(out.ids)];
  return out;
}

function parseJsonl(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  return raw
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .map((line, idx) => {
      try {
        return JSON.parse(line);
      } catch (err) {
        throw new Error(`Invalid JSON at line ${idx + 1}: ${String(err?.message || err)}`);
      }
    });
}

function getCall(entry) {
  const call = entry?.responseToolCalls?.[0];
  if (!call || !call.function) return null;
  return {
    name: call.function.name || null,
    args: call.function.parsedArguments || {}
  };
}

function getPrevResult(entry) {
  const r = entry?.requestToolResults;
  if (!Array.isArray(r) || r.length === 0) return null;
  const last = r[r.length - 1];
  return {
    toolName: last.toolName || null,
    success: typeof last.success === 'boolean' ? last.success : null,
    details: typeof last.details === 'string' ? last.details : ''
  };
}

function normalizeInt(str) {
  const onlyDigits = String(str || '').replace(/\D+/g, '');
  if (!onlyDigits) return null;
  return Number.parseInt(onlyDigits, 10);
}

function numberSetFromText(text) {
  const out = new Set();
  const src = String(text || '');
  const matches = src.match(/\d[\d.,]*/g) || [];
  for (const m of matches) {
    const n = normalizeInt(m);
    if (Number.isFinite(n)) out.add(n);
  }
  return out;
}

function mergeSets(sets) {
  const out = new Set();
  for (const s of sets) {
    for (const v of s) out.add(v);
  }
  return out;
}

function uniqueStrings(values) {
  return [...new Set(values.filter(Boolean))];
}

function evaluateH3ClcRegions(timeline) {
  const gates = [];
  const callNames = timeline.map(t => t.callName).filter(Boolean);

  {
    const requiredOrder = [
      'listQCumberProviders',
      'listQCumberDatasets',
      'queryQCumberTerritorialUnits',
      'tassellateDatasetLayer',
      'queryQCumberDatasetSpatial',
      'aggregateDatasetToH3',
      'joinQMapDatasetsOnH3'
    ];
    let cursor = 0;
    const found = [];
    for (const name of callNames) {
      if (name === requiredOrder[cursor]) {
        found.push(name);
        cursor += 1;
        if (cursor >= requiredOrder.length) break;
      }
    }
    gates.push({
      id: 'G1',
      title: 'Sequenza minima corretta',
      pass: cursor === requiredOrder.length,
      evidence: `found=${found.join(' -> ') || '-'}`
    });
  }

  {
    const issues = [];
    for (let i = 1; i < timeline.length; i += 1) {
      const prevCall = timeline[i - 1].callName;
      if (!prevCall) continue;
      const prevResultName = timeline[i].prevResultName;
      const prevResultSuccess = timeline[i].prevResultSuccess;
      if (!prevResultName) {
        issues.push(`${timeline[i].requestId}: missing previous tool result for ${prevCall}`);
        continue;
      }
      if (prevResultName !== prevCall) {
        issues.push(`${timeline[i].requestId}: expected prev result ${prevCall}, got ${prevResultName}`);
        continue;
      }
      if (prevResultSuccess !== true) {
        issues.push(`${timeline[i].requestId}: ${prevCall} result success=${String(prevResultSuccess)}`);
      }
    }
    gates.push({
      id: 'G2',
      title: 'Esito step-by-step',
      pass: issues.length === 0,
      evidence: issues.length ? issues.slice(0, 4).join(' | ') : 'all checked transitions success=true'
    });
  }

  {
    const createOrUpdateTools = new Set([
      'tassellateSelectedGeometry',
      'tassellateDatasetLayer',
      'aggregateDatasetToH3',
      'joinQMapDatasetsOnH3',
      'populateTassellationFromAdminUnits',
      'populateTassellationFromAdminUnitsAreaWeighted',
      'populateTassellationFromAdminUnitsDiscrete',
      'createDatasetFromFilter',
      'createDatasetFromCurrentFilters',
      'createDatasetWithGeometryArea',
      'reprojectQMapDatasetCrs',
      'clipQMapDatasetByGeometry',
      'clipDatasetByBoundary',
      'overlayDifference',
      'spatialJoinByPredicate',
      'zonalStatsByAdmin',
      'bufferAndSummarize',
      'nearestFeatureJoin',
      'adjacencyGraphFromPolygons'
    ]);
    const missing = [];
    for (let i = 0; i < timeline.length; i += 1) {
      const tool = timeline[i].callName;
      if (!createOrUpdateTools.has(tool)) continue;
      const tail = timeline.slice(i + 1);
      const hasWait = tail.some(t => t.callName === 'waitForQMapDataset');
      const hasCount = tail.some(t => t.callName === 'countQMapRows');
      if (!hasWait || !hasCount) {
        missing.push(
          `${timeline[i].requestId}:${tool} wait=${hasWait ? 'yes' : 'no'} count=${hasCount ? 'yes' : 'no'}`
        );
      }
    }
    gates.push({
      id: 'G3',
      title: 'Post-create obbligatorio (wait + count)',
      pass: missing.length === 0,
      evidence: missing.length ? missing.slice(0, 6).join(' | ') : 'all create/update steps have wait+count'
    });
  }

  {
    const observedNumbers = mergeSets(timeline.map(t => numberSetFromText(t.prevResultDetails)));
    const labeledMetrics = [];
    const metricPatterns = [
      /Celle H3[^:\n]*:\s*([0-9][0-9.,]*)/gi,
      /Record CLC[^:\n]*:\s*([0-9][0-9.,]*)/gi,
      /Celle visualizzate[^:\n]*:\s*([0-9][0-9.,]*)/gi,
      /Copertura[^:\n]*:\s*([0-9][0-9.,]*)/gi
    ];
    for (const row of timeline) {
      if (!row.responseText) continue;
      for (const pattern of metricPatterns) {
        let match;
        while ((match = pattern.exec(row.responseText)) !== null) {
          const parsed = normalizeInt(match[1]);
          if (Number.isFinite(parsed)) {
            labeledMetrics.push({requestId: row.requestId, value: parsed, raw: match[0]});
          }
        }
      }
    }
    const mismatches = labeledMetrics.filter(m => !observedNumbers.has(m.value));
    gates.push({
      id: 'G4',
      title: 'Coerenza statistiche finali',
      pass: mismatches.length === 0,
      evidence: mismatches.length
        ? mismatches.slice(0, 4).map(m => `${m.requestId}:${m.raw}`).join(' | ')
        : 'final labeled metrics match observed tool-detail numbers'
    });
  }

  {
    const colorResults = timeline.filter(t => t.prevResultName === 'setQMapLayerColorByField');
    let maxDistinct = null;
    let hasSuccessWithDistinctGt1 = false;
    for (const row of colorResults) {
      if (row.prevResultSuccess !== true) continue;
      const m = /distinct\s*=\s*(\d+)/i.exec(row.prevResultDetails || '');
      if (!m) continue;
      const distinct = Number.parseInt(m[1], 10);
      if (!Number.isFinite(distinct)) continue;
      maxDistinct = maxDistinct === null ? distinct : Math.max(maxDistinct, distinct);
      if (distinct > 1) hasSuccessWithDistinctGt1 = true;
    }
    gates.push({
      id: 'G5',
      title: 'Colorazione valida (distinct > 1)',
      pass: hasSuccessWithDistinctGt1,
      evidence:
        maxDistinct === null ? 'no successful color step with parseable distinct metric' : `maxDistinct=${maxDistinct}`
    });
  }

  {
    const clipCalls = timeline.filter(t => t.callName === 'clipQMapDatasetByGeometry');
    const modes = uniqueStrings(clipCalls.map(c => String(c.callArgs?.mode || 'intersects')));
    const allWithin = clipCalls.length > 0 && modes.every(m => m === 'within');
    gates.push({
      id: 'G6',
      title: 'Clipping coerente (within per celle contenute)',
      pass: clipCalls.length === 0 ? true : allWithin,
      evidence: clipCalls.length ? `modes=${modes.join(',')}` : 'not-applicable (no clip call in scope)'
    });
  }

  {
    const clipCalls = timeline.filter(t => t.callName === 'clipQMapDatasetByGeometry');
    const lastClip = clipCalls.length ? clipCalls[clipCalls.length - 1] : null;
    let finalDataset = null;
    if (lastClip) {
      finalDataset = String(lastClip.callArgs?.newDatasetName || '').trim() || null;
    }
    const lastClipIdx = lastClip ? timeline.findIndex(t => t.requestId === lastClip.requestId) : -1;
    const tail = lastClipIdx >= 0 ? timeline.slice(lastClipIdx + 1) : timeline;
    const showOnly = tail.find(t => t.callName === 'showOnlyQMapLayer');
    const showOnlyTarget = String(showOnly?.callArgs?.layerNameOrId || '').trim();
    const styled = tail.find(
      t => t.callName === 'setQMapLayerColorByField' && t.callArgs?.datasetName === showOnlyTarget
    );
    const pass = !lastClip || (!!showOnly && !!styled && (!finalDataset || showOnlyTarget === finalDataset));
    const evidence = !lastClip
      ? 'not-applicable (no clip call in scope)'
      : `finalDataset=${finalDataset || '-'} showOnly=${showOnlyTarget || '-'} styled=${styled ? 'yes' : 'no'}`;
    gates.push({
      id: 'G7',
      title: 'Output finale pulito (showOnly + stile su layer finale)',
      pass,
      evidence
    });
  }

  return gates;
}

function buildTimeline(entries) {
  return entries.map(entry => {
    const call = getCall(entry);
    const prev = getPrevResult(entry);
    const qualityMetrics = entry?.qualityMetrics && typeof entry.qualityMetrics === 'object' ? entry.qualityMetrics : {};
    return {
      ts: entry.ts,
      requestId: entry.requestId,
      status: entry.status,
      callName: call?.name || null,
      callArgs: call?.args || {},
      prevResultName: prev?.toolName || null,
      prevResultSuccess: prev?.success ?? null,
      prevResultDetails: prev?.details || '',
      responseText: typeof entry.responseText === 'string' ? entry.responseText : '',
      qualityMetrics
    };
  });
}

function summarizeQualityMetrics(timeline) {
  const rows = timeline
    .map(t => t.qualityMetrics)
    .filter(m => m && typeof m === 'object' && Object.keys(m).length > 0);
  if (!rows.length) {
    return null;
  }
  const countTrue = key => rows.filter(m => m[key] === true).length;
  const avgScore =
    rows.reduce((acc, m) => acc + (Number.isFinite(Number(m.workflowScore)) ? Number(m.workflowScore) : 0), 0) /
    rows.length;
  return {
    entries: rows.length,
    postCreateWaitCountOkRate: Number((countTrue('postCreateWaitCountOk') / rows.length).toFixed(3)),
    finalLayerIsolatedAfterCountRate: Number((countTrue('finalLayerIsolatedAfterCount') / rows.length).toFixed(3)),
    pendingIsolationAfterCountRate: Number((countTrue('pendingIsolationAfterCount') / rows.length).toFixed(3)),
    avgWorkflowScore: Number(avgScore.toFixed(2))
  };
}

function formatTextReport({profile, logPath, requestedIds, foundIds, gates, timeline}) {
  const lines = [];
  lines.push(`Audit profile: ${profile}`);
  lines.push(`Log file: ${logPath}`);
  lines.push(`Entries matched: ${timeline.length}/${requestedIds.length || timeline.length}`);
  if (requestedIds.length) {
    const missing = requestedIds.filter(id => !foundIds.includes(id));
    lines.push(`Missing requestIds: ${missing.length ? missing.join(', ') : '-'}`);
  }
  lines.push('');
  lines.push('Gate results:');
  for (const gate of gates) {
    lines.push(`${gate.pass ? 'PASS' : 'FAIL'} ${gate.id} ${gate.title}`);
    lines.push(`  ${gate.evidence}`);
  }
  const quality = summarizeQualityMetrics(timeline);
  if (quality) {
    lines.push('');
    lines.push('Quality metrics (from audit events):');
    lines.push(`  entries=${quality.entries}`);
    lines.push(`  postCreateWaitCountOkRate=${quality.postCreateWaitCountOkRate}`);
    lines.push(`  finalLayerIsolatedAfterCountRate=${quality.finalLayerIsolatedAfterCountRate}`);
    lines.push(`  pendingIsolationAfterCountRate=${quality.pendingIsolationAfterCountRate}`);
    lines.push(`  avgWorkflowScore=${quality.avgWorkflowScore}`);
  }
  return lines.join('\n');
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.profile !== 'h3-clc-regions') {
    throw new Error(`Unsupported profile "${args.profile}". Supported: h3-clc-regions`);
  }

  const cwd = process.cwd();
  const logPath = path.isAbsolute(args.logPath) ? args.logPath : path.resolve(cwd, args.logPath);
  const allEntries = parseJsonl(logPath);

  let selected = allEntries;
  if (args.ids.length) {
    const set = new Set(args.ids);
    selected = allEntries.filter(entry => set.has(entry.requestId));
  }

  selected.sort((a, b) => String(a.ts).localeCompare(String(b.ts)));

  const timeline = buildTimeline(selected);
  const foundIds = selected.map(e => e.requestId);
  const gates = evaluateH3ClcRegions(timeline);
  const passed = gates.filter(g => g.pass).length;

  const report = {
    profile: args.profile,
    logPath,
    requestedIds: args.ids,
    foundIds,
    summary: {
      totalGates: gates.length,
      passed,
      failed: gates.length - passed
    },
    gates,
    qualityMetrics: summarizeQualityMetrics(timeline),
    timeline: timeline.map(row => ({
      ts: row.ts,
      requestId: row.requestId,
      callName: row.callName,
      prevResultName: row.prevResultName,
      prevResultSuccess: row.prevResultSuccess,
      qualityMetrics: row.qualityMetrics || null
    }))
  };

  if (args.json) {
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    return;
  }

  process.stdout.write(
    `${formatTextReport({
      profile: args.profile,
      logPath,
      requestedIds: args.ids,
      foundIds,
      gates,
      timeline
    })}\n`
  );
}

main();
