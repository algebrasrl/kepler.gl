# Q-Map AI Quality Test (Essenziale)

Questa suite è corta ma copre i comportamenti più importanti.

## 1) Discovery + map selection (data-agnostic)
1. `che mappe hai?`
2. `carica la mappa migliore per operazioni amministrative e dimmi perché l'hai scelta`
3. `quali dataset e layer sono renderizzati al momento?`

Verifica:
- usa `listQMapCloudMaps` / `loadCloudMapAndWait`
- motiva la scelta con campi/capabilities (non con hardcode)

## 2) Record inspection (nuovi tool)
1. `mostrami 5 righe del dataset principale con i campi name, lv, population`
2. `quali valori distinti esistono nel campo lv?`
3. `cerca nel campo name i valori che contengono "appennino"`
4. `quanti record hanno lv = 7?`

Verifica:
- usa `previewQMapDatasetRows`, `distinctQMapFieldValues`, `searchQMapFieldValues`, `countQMapRows`

## 3) Filtri combinati (no overwrite)
1. `applica filtro name = Treviso`
2. `tieni solo la provincia`
3. `dimmi quanti record restano`

Verifica:
- filtri su stesso dataset vengono combinati (es. `name` + `lv`)
- non deve restare un solo filtro attivo

## 4) Tassellazione H3 separata
1. `crea un dataset con i nomi che finiscono per "ano"`
2. `tassella quel dataset a risoluzione 8`
3. `ripeti a risoluzione 7 senza specificare targetDatasetName`

Verifica:
- crea dataset/layer distinti (versionati), non sovrascrive silenziosamente

## 5) Aggregazione H3 con clipping
1. `aggrega tutte le province italiane del dataset principale a risoluzione 5 con sum e avg su population usando area_weighted`

Verifica:
- non usa name contains "provincia"
- risolve prima il livello province
- crea/aggiorna dataset aggregato con campi `sum`, `avg`, `count`

## 6) Layer UX
1. `metti il layer aggregato sopra agli altri`
2. `nascondi il layer sorgente`
3. `mostra solo il layer aggregato`

Verifica:
- usa `setQMapLayerOrder` per z-order
- usa `setQMapLayerVisibility` / `showOnlyQMapLayer` per visibilità

## 7) Anti-replay
1. esegui 2-3 azioni (es. colore + visibilità)
2. chiudi pannello AI
3. riapri pannello AI

Verifica:
- stato layer NON cambia automaticamente alla riapertura

## 8) Provider routing deterministico (q-cumber)
1. `list qcumber providers`
2. `list qcumber datasets`
3. `list qcumber datasets del provider geoapi-q-cumber`

Verifica:
- al punto 2 non resta in pending: se manca providerId deve fallire fast con messaggio guida
- ordine atteso: `listQCumberProviders` -> `listQCumberDatasets(providerId=...)`

## 9) Geometry-first loading (no point layer spurii)
1. `carica il dataset confini amministrativi (provider local-assets-it) con loadToMap=true`
2. `mostrami i layer creati`

Verifica:
- non crea layer `Point` spurii quando il dataset ha geometrie amministrative native
- se non c e geometria renderizzabile, dataset caricato senza auto layer
- i punti da lat/lon vanno creati solo con richiesta esplicita (`inferPointsFromLatLon=true`)

## 10) Chat audit traceability
Prerequisito:
- `Q_ASSISTANT_CHAT_AUDIT_ENABLED=true`
- volume host audit attivo (`./logs/q-assistant`)

1. esegui una richiesta AI con tool-call
2. apri `backends/logs/q-assistant/chat-audit/session-default.jsonl` (o il file `session-<id>.jsonl` della sessione)

Verifica:
- presente `requestId`, `endpoint`, `status`, `durationMs`
- presenti `requestTools` e (quando applicabile) `responseToolCalls`
- payload sanitizzati (niente token/api key in chiaro)

## 11) H3 heavy async flow (worker + fallback)
1. `tassella veneto a ris 6 e colora le celle in base alla popolazione dei comuni sottostanti`

Verifica:
- non resta appeso dopo `populateTassellationFromAdminUnitsAreaWeighted`
- per dataset creati asincroni il flusso completa `waitForQMapDataset` -> `countQMapRows`
- la colorazione finale viene applicata su dataset popolato (non su dataset intermedio)

## 12) Regressione automatica minima (quando tocchi worker/H3)
Esegui:
- `yarn --cwd examples/q-map test:unit:workers`
- `yarn --cwd examples/q-map test:e2e tests/e2e/tools.spec.ts -g "q-map tools functional coverage|q-map base tools smoke coverage|cloud tool coverage"`

## Valutazione continua (prompt + backend)

Per workflow completo con query campione, metriche e report per run usa:

- `AI_EVAL.md`
- `tests/ai-eval/cases.sample.json`
- `scripts/run-ai-eval.mjs`
