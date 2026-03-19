# q-map System Engineering Loop (Prompt-Driven)

Obiettivo: migliorare q-map in modo continuo e misurabile partendo da prompt funzionali reali.

## Principio operativo

Ogni prompt funzionale e una specifica testabile.
Il sistema evolve con un loop chiuso:

1. Raccolta prompt reali (audit/chat/incidenti).
2. Formalizzazione in casi valutabili.
3. Esecuzione test automatizzati.
4. Refactor/fix mirati.
5. Re-test e confronto con baseline.
6. Aggiornamento prompt/docs/guardrail.

## Artefatti di controllo

- Catalogo casi base: `tests/ai-eval/cases.sample.json`
- Catalogo casi funzionali: `tests/ai-eval/cases.functional.json`
- Catalogo casi held-out/adversarial: `tests/ai-eval/cases.adversarial.json`
- Matrice aree->KPI->casi: `tests/ai-eval/architecture-matrix.json`
- Contratto tool condiviso FE/BE/eval (artifact canonico): `artifacts/tool-contracts/qmap-tool-contracts.json`
- Mirror backend runtime/package: `backends/q-assistant/src/q_assistant/qmap-tool-contracts.json`
- Runner eval: `scripts/run-ai-eval.mjs`
- Report run: `tests/ai-eval/results/report-<runId>.json|md`
- Test integrazione: `tests/e2e/tools.spec.ts`

## Comandi standard

Da `examples/q-map`:

```bash
yarn ai:eval
yarn ai:eval:direct
yarn ai:eval:functional
yarn ai:eval:functional:direct
yarn test:unit:workers
yarn test:e2e tests/e2e/tools.spec.ts --grep "q-map tools functional coverage"
```

Helper equivalenti via `Makefile`:

```bash
make clean-loop
make ai-eval
make ai-eval-direct
make ai-eval-functional
make ai-eval-functional-direct
make ai-eval-adversarial
make ai-eval-adversarial-direct
make ai-operational-audit
make ai-response-quality-audit
make ai-trace-quality-audit
make ai-trace-grade-audit
make ai-passk-audit
make ai-matrix-audit
make ai-threshold-audit
make tool-contract-audit
make ai-eval-all
make tool-coverage-audit
make ai-governance-audit
make quality-gate
make loop
```

Start-clean consigliato prima di un nuovo ciclo:

```bash
make clean-loop
make loop RUN_ID=<baseline-tag>
```

Note operative:
- `make clean-loop` preserva lo storico in `tests/ai-eval/results` per non rompere i gate di varianza.
- Usa `make clean-loop-hard` solo quando vuoi azzerare intenzionalmente la baseline locale.
- `make ai-eval-adversarial` esegue casi held-out senza vincoli deterministici per-case; serve a misurare generalizzazione, non solo aderenza al benchmark guidato.
- `make ai-passk-audit` legge le ultime run adversarial e misura `pass^k` per-case: un caso conta affidabile solo se riesce almeno una volta nella finestra di tentativi ripetuti.
- se il gateway locale su `:8000` richiede bearer token, usa i target `*-direct` contro `:3004` per il loop locale; il path gateway resta il default per run edge/Kong o CI con auth configurata.
- se vuoi mantenere invariati i target standard ma instradare tutto il ciclo sul backend diretto, usa `EVAL_TRANSPORT=direct` con `make ai-eval-functional`, `make quality-gate` o `make loop`.

## Gate qualità consigliati

- `passRate` eval funzionale >= `0.90`
- `avgCaseScore` non in calo rispetto alla baseline accettata
- `p25CaseScore` non in calo rispetto alla baseline (evita code deboli nascoste dalla media)
- `minCaseScore` sopra soglia minima di sicurezza definita in matrice/policy run
- nessuna regressione su gate per-area (passRate/score) definiti in `architecture-matrix.json`
- nessuna regressione sui trace-grade dei casi critici: trace presenti, nessun false success claim, workflow trace score sopra soglia minima
- nessuna regressione operativa sul report funzionale più recente: `avgDurationMs`, `p95DurationMs`, `maxDurationMs`, `transportErrorRate`, `transport.aborted`
- nessuna regressione di response quality sul report funzionale più recente: `falseSuccessClaimRate=0`, `groundedFinalAnswerRate` ed `escalationComplianceRate` sopra soglia, con copertura minima di casi valutati
- nessuna regressione di response quality trace-backed: groundedness/escalation devono restare corroborate dai `chat-audit` reali dei casi annotati, non solo dal grading report-level
- `pass^k` sui casi held-out/adversarial sopra soglia minima sulla finestra recente, cosi la stabilita non dipende da una singola run fortunata
- gate per-case su casi critici (`criticality=critical`) per `min_case_score`, `min_tool_precision`, `max_extra_tool_calls`
- floor statico anti-fragile su `min_case_score` per `criticality` (`make ai-threshold-audit`)
- zero tool proibiti nei casi critici
- changelog aggiornato per ogni modifica tecnica q-map (`make changelog-audit` verde)
- baseline governance AI valida (`make ai-governance-audit` verde)
- `quality-gate` deve includere `ai-eval-functional`, non solo audit statici, per allinearsi ai controlli di governance basati su comportamento runtime
- `tsc --noEmit` verde
- `test:unit:workers` verde
- almeno un run `tools.spec.ts` verde prima di merge su refactor geospaziali
- `tool-coverage-audit` verde (nessun tool runtime scoperto senza copertura e2e)

## Regole di evoluzione

- Un prompt reale nuovo entra in backlog solo se riproducibile.
- Ogni bug corretto deve generare almeno un caso nel catalogo funzionale.
- Se cambiano le semantics di un tool, aggiornare nello stesso PR:
  - prompt di sistema
  - docs (`AGENT.md`, `README.md`, `DOCUMENTATION.md`)
  - test di regressione pertinenti

## Cadenza

- Daily: triage prompt falliti + aggiornamento backlog.
- Weekly: run completo eval funzionale + review trend metriche.
- Release gate: nessuna regressione su casi critici (H3/clip/join/coverage).
