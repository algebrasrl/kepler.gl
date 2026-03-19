# q-map Autopoietic Framework

Obiettivo: descrivere la struttura sistemistica autopoietica con cui q-map si auto-migliora in modo continuo, misurabile e governato.

## 1) Principio architetturale

Il sistema e autopoietico perche produce e aggiorna i propri meccanismi operativi usando gli stessi output del suo funzionamento:

- i prompt reali diventano nuovi casi di test
- i fallimenti di routing diventano nuove regole/guardrail
- i risultati dei run aggiornano priorita e backlog tecnico

In pratica, il sistema mantiene e rigenera la propria qualita attraverso un loop chiuso `osserva -> formalizza -> valida -> correggi -> ri-valida`.

## 2) Livelli della struttura

### Livello A - Operativo runtime (assistant + tools)

- runtime tools q-map: `src/features/qmap-ai/qmap-ai-assistant-component.tsx`
- policy/mode gating: `src/features/qmap-ai/mode-policy.ts`
- prompt operativo: `src/features/qmap-ai/system-prompt.ts`

Scopo: eseguire task geospaziali in modo deterministico e difensivo.

### Livello B - Valutazione e misura

- casi base: `tests/ai-eval/cases.sample.json`
- casi funzionali: `tests/ai-eval/cases.functional.json`
- matrice aree/KPI/casi: `tests/ai-eval/architecture-matrix.json`
- runner: `scripts/run-ai-eval.mjs`
- audit matrice: `scripts/audit-ai-matrix.mjs`
- audit copertura tool: `scripts/audit-tool-coverage.mjs`

Scopo: misurare in modo ripetibile comportamento, precisione tool e robustezza.

### Livello C - Gate di integrazione

- typecheck: `yarn run tsc --noEmit`
- worker tests: `yarn test:unit:workers`
- e2e tools: `yarn test:e2e tests/e2e/tools.spec.ts --grep "..."`
- backend tests: `make -C backends test-backends`
- quality gate aggregato: `make quality-gate`

Scopo: impedire che miglioramenti locali degradino stabilita complessiva.

### Livello D - Governance e memoria

- runbook loop: `SYSTEM_ENGINEERING_LOOP.md`
- workflow eval: `AI_EVAL.md`
- log di evoluzione: `CHANGELOG.md`
- linee guida operative: `AGENT.md`

Scopo: conservare conoscenza, decisioni e disciplina di rilascio.

## 3) Ciclo autopoietico standard

1. Input dal campo
- prompt reali, incidenti, chat audit, failure report.

2. Formalizzazione
- ogni problema riproducibile viene modellato come caso in `cases.functional.json`.

3. Mappatura architetturale
- il caso e collegato a una `area` e a un `kpi_id` in `architecture-matrix.json`.

4. Esecuzione deterministica
- `make clean-loop`
- `make loop RUN_ID=<tag>`

5. Diagnosi
- analisi report `tests/ai-eval/results/report-<runId>.json|md`
- focus su `failed`, `minCaseScore`, `p25CaseScore`, `extraToolCalls`.

6. Correzione mirata
- fix su runtime/prompt/policy/test con impatto esplicito su KPI.

7. Validazione finale
- rerun loop completo
- merge solo con gate verdi e nessuna regressione critica.

## 4) Invarianti di sistema

- nessun tool runtime senza copertura (audit tool coverage verde)
- nessun case id non mappato in matrice (matrix audit verde)
- ogni change tecnico in `examples/q-map` aggiorna `CHANGELOG.md`
- i casi critici hanno soglie piu restrittive (`criticality=critical`)

Queste invarianti sono la "struttura che preserva struttura": il nucleo autopoietico del progetto.

## 5) Dominio ambientale: come entra nel loop

Per analisi geolocalizzate su perimetri tematici, giurisdizionali e regolatori:

- i casi funzionali rappresentano workflow di compliance reale
- la matrice rende espliciti i KPI di affidabilita domain-specific
- i gate bloccano regressioni silenziose su routing, geoprocessing e reporting

In breve: il dominio non e solo "contenuto", e parte della struttura di controllo.

## 6) Definizione pratica di auto-miglioramento

Un ciclo e considerato migliorativo quando:

- mantiene `passRate` sopra soglia
- non degrada `p25CaseScore`/`minCaseScore`
- riduce failure ripetitivi o chiamate tool extra non utili
- aumenta coerenza tra intent utente, tool usati e output verificabile

Se una modifica non migliora queste metriche (o le peggiora), il sistema la rigetta.
