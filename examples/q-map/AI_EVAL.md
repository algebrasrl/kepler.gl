# q-map AI Eval Workflow

Obiettivo: migliorare prompt + backend/tool quality con run ripetibili, metriche oggettive e confronto tra iterazioni.

## Cosa include

- suite di query campione: `tests/ai-eval/cases.sample.json`
- suite di prompt funzionali: `tests/ai-eval/cases.functional.json`
- suite held-out/adversarial: `tests/ai-eval/cases.adversarial.json`
  - campo prompt utente: `user_prompt` (obbligatorio)
- matrice architetturale KPI->casi: `tests/ai-eval/architecture-matrix.json`
- runner automatico: `scripts/run-ai-eval.mjs`
  - include un messaggio `system` per-case con vincoli deterministici (`required_tools_*`, `forbidden_tools`) per ridurre flakiness di routing
  - timeout rete configurabile (`QMAP_AI_EVAL_REQUEST_TIMEOUT_MS` / `--request-timeout-ms`, default `120000`)
- contratto tool condiviso FE/BE/eval (artifact canonico): `artifacts/tool-contracts/qmap-tool-contracts.json`
  - mirror backend runtime/package: `backends/q-assistant/src/q_assistant/qmap-tool-contracts.json`
  - audit: `scripts/audit-tool-contracts.mjs`
  - sync da manifest: `scripts/generate-tool-contracts.mjs`
  - i tool di controllo workflow (`waitForQMapDataset`, `countQMapRows`) hanno arg schema stretti anche nell'eval catalog, cosi i KPI sugli argomenti non premiano chiamate vuote o ambigue
- audit matrice/coprertura: `scripts/audit-ai-matrix.mjs`
- audit soglie minime per-case (anti-fragile): `scripts/audit-ai-eval-thresholds.mjs`
- report per run:
  - JSON: `tests/ai-eval/results/report-<runId>.json`
  - Markdown: `tests/ai-eval/results/report-<runId>.md`
  - ogni case report ora include `requestIds` backend per collegare la run agli eventi `chat-audit`
  - ogni case report include anche `deterministicConstraintsApplied` per distinguere benchmark guidato vs held-out/adversarial
- opzione branch per esperimento (`--create-branch`)

## Metriche

Per ogni caso:

- `toolRecall`: tool richiesti trovati / tool richiesti
- `toolPrecision`: tool utili trovati / tool chiamati
- `toolArgumentScore`: copertura delle aspettative sugli argomenti dei tool nei casi che definiscono `expected_tool_arguments`
  - la copertura non e piu limitata ai soli casi q-cumber/cloud iniziali: il benchmark funzionale critico usa anche aspettative su `mapId`, `datasetName`, `fieldName`, `resolution`, `expectedAdminType`
  - ogni regola puo puntare a un singolo `tool` o a un gruppo alternativo `tools_any` quando piu tool condividono la stessa aspettativa sugli argomenti
- `keywordScore`: copertura keyword attese nella risposta testuale
  - il runner tollera un anchor mancante nelle liste `expected_keywords_any`, cosi keyword equivalenti o rami corretti alternativi non producono penalita lessicali inutili
- `caseScore`: score composito pesato
- `pass`: vero se HTTP ok, nessun tool proibito, tutti i tool richiesti presenti

Metriche aggregate run:

- pass rate
- media tool recall
- media tool precision
- media tool argument score sui casi che lo valutano
- media keyword score
- media case score
- `minCaseScore`
- `p25CaseScore`
- `avgDurationMs`
- `p95DurationMs`
- `maxDurationMs`
- `totalTokens`, `avgTotalTokens`, `p95TotalTokens`, `maxTotalTokens`
- `totalEstimatedPromptTokens`, `avgEstimatedPromptTokens`, `p95EstimatedPromptTokens`, `maxEstimatedPromptTokens`
- `usageCoverageRate`, `estimateCoverageRate`, `tokenBudgetCoverageRate`
- `avgPromptBudgetUtilizationRatio`, `maxPromptBudgetUtilizationRatio`
- `tokenBudgetWarnCases`, `tokenBudgetCompactCases`, `tokenBudgetHardCases`
- media chiamate extra (`avgExtraToolCalls`)
- `transportErrorCount`
- `transportErrorRate`

Segnali di revisione consigliati (cross-run, non tutti ancora merge-blocking):

- `groundedFinalAnswerRate`: quota di casi in cui i claim finali critici restano supportati da evidenza tool dello stesso turno
- `falseSuccessClaimRate`: quota di casi con testo finale che dichiara completamento non corroborato da validazione/materializzazione effettiva
- `escalationComplianceRate`: quota di casi ambigui o high-impact in cui il sistema limita l output o chiede un solo chiarimento invece di improvvisare
- `guidedVsHeldoutGap`: differenza tra KPI del benchmark funzionale guidato e slice held-out/adversarial, utile per intercettare saturazione del benchmark

Questi segnali servono a evitare un anti-pattern comune: benchmark guidato perfetto ma comportamento reale ancora fragile. Quando il funzionale e saturo, il delta guided-vs-held-out e la qualita del fallimento diventano piu informativi del solo `passRate`.

Segnali gia cablati nei report:

- `falseSuccessClaimRate`: `run-ai-eval` lo aggrega dai `qAssistant.qualityMetrics.falseSuccessClaimCount` restituiti dal backend
- `escalationComplianceRate`: `run-ai-eval` lo aggrega sui casi che dichiarano esplicitamente `expected_response_mode`, partendo dal primo set di casi `clarification`; i casi possono irrigidire il controllo con `response_mode_markers_any`
- per aumentare l utilita del KPI, preferire piu casi di clarification stabili sullo stesso workflow critico piuttosto che un solo caso molto generico
- per i casi `limitation` stabili, preferire failure deterministiche del mock harness (`mock_tool_results`) invece di affidarsi a condizioni runtime casuali; cosi accuratezza e precisione del fail-closed restano ripetibili nel loop
- questo vale anche per i workflow cloud/backend: meglio simulare timeout o fallback non validato in modo deterministico che affidarsi a un upstream instabile per misurare truthfulness del fail-closed
- `groundedFinalAnswerRate`: `run-ai-eval` lo aggrega sui casi che dichiarano `require_grounded_final_answer=true`, richiedendo testo finale, assenza di false-success e validazione post-create nei workflow con mutazione dataset; i casi possono anche dichiarare `grounded_required_tools_all` per richiedere evidence minima esplicita
- per i workflow qualità più sensibili, i case contract possono anche fissare `max_extra_tool_calls=0` così la precisione resta merge-visible e non dipende solo dalla media report-level
- `guidedVsHeldoutGap`: ogni report guided/held-out prova a confrontarsi con l ultimo report compatibile disponibile nella history locale (`tests/ai-eval/results`)

Gate supportati:

- gate per-case (in `cases.*.json`):
  - `criticality`: `critical|standard`
  - `min_case_score`
  - `min_tool_precision`
  - `min_tool_argument_score`
  - `max_extra_tool_calls`
  - `expected_tool_arguments` per verificare la presenza di chiavi semantiche importanti negli argomenti dei tool
    - ogni regola usa `tool` oppure `tools_any`
  - floor minimo `min_case_score` per criticality (audit statico su `cases.functional.json`)
- gate run-level:
  - da `tests/ai-eval/architecture-matrix.json` (`evaluationPolicy.runGates`)
  - override CLI/env (`--min-*`, `QMAP_AI_EVAL_MIN_*`)
- gate per-area:
  - baseline da `tests/ai-eval/architecture-matrix.json` (`evaluationPolicy.areaDefaults`)
  - override per-area nella matrice (`areas[].gates`)
  - override globale CLI/env (`--min-area-*`, `QMAP_AI_EVAL_MIN_AREA_*`)
- gate operativi run-level:
  - audit dedicato: `scripts/audit-ai-operational-kpis.mjs`
  - controlla sul report funzionale piu recente `avgDurationMs`, `p95DurationMs`, `maxDurationMs`, `transportErrorRate`, `transport.aborted`
- gate costo/token budget run-level:
  - audit dedicato: `scripts/audit-ai-cost-kpis.mjs`
  - controlla sul report funzionale piu recente copertura usage/token-budget, token per caso, utilizzo del prompt budget e, se configurati i prezzi via env, costo stimato input/output
- gate response-quality run-level:
  - audit dedicato: `scripts/audit-ai-response-quality.mjs`
  - controlla sul report funzionale piu recente `falseSuccessClaimRate`, `groundedFinalAnswerRate`, `escalationComplianceRate` e la presenza minima di casi valutati per groundedness/escalation
- gate response-quality trace-backed:
  - audit dedicato: `scripts/audit-ai-trace-quality.mjs`
  - rilegge i casi grounded/escalation del report funzionale piu recente sui `chat-audit` reali (`requestIds` -> `responseText` + `requestToolResults` + `qualityMetrics`) e blocca mismatch o evidenza mancante
- gate di affidabilita multi-attempt:
  - audit dedicato: `scripts/audit-ai-passk-reliability.mjs`
  - legge le ultime `k` run adversarial e misura `pass^k` per-case, oltre a `criticalPassAtK`

Best practice operative per q-map:

- non leggere gli score in isolamento: combinare metriche automatiche, trace evidence e review umana periodica dei casi critici
- espandere piu velocemente i casi held-out/adversarial quando il funzionale si avvicina a `1.0` stabile
- trattare i cambi prompt/guardrail come cambi di policy: ogni modifica deve mostrare beneficio su groundedness, tool discipline o failure quality, non solo su keyword/micro-score
- privilegiare metriche misurabili da log/report rispetto a criteri vaghi o puramente lessicali

## Esecuzione

Da `examples/q-map`:

```bash
yarn ai:eval
yarn ai:eval:direct
yarn ai:eval:functional
yarn ai:eval:functional:direct
yarn ai:eval:adversarial
yarn ai:eval:adversarial:direct
yarn ai:eval:matrix
yarn ai:eval:threshold-audit
yarn ai:eval:operational-audit
yarn ai:eval:cost-audit
yarn ai:eval:response-quality-audit
yarn ai:eval:trace-quality-audit
yarn ai:eval:passk-audit
yarn ai:tool-contract:audit
```

Note operative:
- `quality-gate` include `ai-eval-functional` prima degli audit di varianza, cosi i controlli di merge/release validano davvero il comportamento runtime.
- `quality-gate` include anche `ai:eval:operational-audit` via `make ai-operational-audit`, cosi regressioni di latenza/transport falliscono prima del merge.
- `quality-gate` include anche `ai:eval:cost-audit` via `make ai-cost-audit`, cosi regressioni di token budget / costo stimato vengono bloccate insieme alla parte operativa.
- `quality-gate` include anche `ai:eval:response-quality-audit` via `make ai-response-quality-audit`, cosi false-success, groundedness ed escalation smettono di essere solo segnali da report e diventano vincoli merge-blocking sul funzionale.
- `quality-gate` include anche `ai:eval:trace-quality-audit` via `make ai-trace-quality-audit`, cosi groundedness/escalation devono restare veri anche nei `chat-audit` e non solo nel report-level grading.
- `ai:eval:response-quality-audit` prova a usare l ultimo report funzionale compatibile con i nuovi summary fields; se la history locale non ne contiene ancora uno, fallisce chiedendo di rigenerare `ai-eval-functional`.
- `ai:eval:trace-quality-audit` prova a usare l ultimo report funzionale compatibile con i case-level checks trace-backed; se la history locale non ne contiene ancora uno, fallisce chiedendo di rigenerare `ai-eval-functional`.
- per loop locali senza bearer token sul gateway, usa i target/script `*-direct` contro `http://localhost:3004`; il default `EVAL_BASE_URL=http://localhost:8000/api/q-assistant` resta quello corretto per il percorso edge/Kong.
- per fare girare l intero ciclo (`ai-eval*`, `quality-gate`, `loop`) sul backend diretto senza cambiare target, usa `EVAL_TRANSPORT=direct`.
- una parte del protocollo `create/update -> wait -> count` non dipende piu solo dal prompt: il backend ora pota i tool prematuri e forza il prossimo step di validazione durante la finestra post-create/post-wait.
- `prompt-lint` e allineato al runner: nella history keyword tollera un singolo anchor instabile per-case quando `expected_keywords_any` contiene piu segnali alternativi, evitando warning storici che il benchmark funzionale gia considera non bloccanti.
- `ai-trace-grade-audit` usa i `requestIds` dell'ultimo report funzionale per validare le trace `chat-audit` dei casi critici.
- `ai:eval:adversarial` disabilita i vincoli deterministici per-case e serve come slice held-out/generalization separata dal benchmark funzionale guidato.
- `ai:eval:passk-audit` usa la storia delle run `adversarial` per misurare affidabilita su tentativi ripetuti; se la storia non basta ancora, risponde `SKIP`.
- se `ai-eval-functional` resta vicino a `1.0` per piu run consecutive, usare come leading indicators il gap rispetto a `ai:eval:adversarial`, le trace critiche e i casi con failure-quality debole.
- `make clean-loop` preserva i report in `tests/ai-eval/results`; per azzerare la finestra storica usa `make clean-loop-hard`.
- `generate-tool-contracts` e `audit-tool-contracts` risolvono i path dal root di `examples/q-map`, quindi funzionano anche se lanciati dal root del monorepo.

Run su branch dedicato:

```bash
yarn ai:eval:branch --run-id prompt-v3
```

Varianti utili:

```bash
node scripts/run-ai-eval.mjs \
  --base-url http://localhost:3004 \
  --model google/gemini-3-flash-preview \
  --cases tests/ai-eval/cases.sample.json \
  --min-avg-case-score 0.82 \
  --min-p25-case-score 0.72 \
  --run-id kontur-routing-fix
```

## Flusso consigliato

1. Crea branch esperimento per modifica prompt/backend.
2. Applica modifica.
3. Esegui `yarn ai:eval`.
4. Confronta report con baseline precedente.
5. Accetta solo modifiche con miglioramento o regressione nulla sulle metriche principali.
6. Prima del merge, usa `make quality-gate`: il gate deve coprire sia audit statici sia `ai-eval-functional`.

## Note

- Questa suite valuta soprattutto decisione tool + qualità risposta in modo veloce.
- La matrice architetturale assicura che i casi coprano le aree/kpi del sistema in modo esplicito.
- Il benchmark funzionale guidato non deve essere usato da solo come prova di robustezza: per q-map contano anche groundedness, qualita del fallimento e differenziale rispetto alle slice held-out.
- Per UX/integrazione completa continua a usare anche Playwright (`tests/e2e/ai-mode-policy.spec.ts` e `tests/e2e/tools.spec.ts`).
- Aggiorna `cases.sample.json` con query reali del tuo dominio (giurisdizioni, eventi, tassellazioni, stili).
- Per il processo continuo prompt-driven usa il runbook `SYSTEM_ENGINEERING_LOOP.md`.
