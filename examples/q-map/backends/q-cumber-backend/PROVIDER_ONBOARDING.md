# Provider Onboarding (Territoriali e Tematici)

Procedura consigliata per aggiungere provider/dataset senza introdurre logica hardcoded nel prompt AI.

## 1) Crea/aggiorna il descriptor provider

Path:
- `provider-descriptors/<locale>/<provider-id>.json`

Requisiti minimi dataset:
- `source.type`: `postgis` (alias supportati: `postgres`, `postgresql`)
- `source.schema`
- `source.table`
- `source.geometryColumn`

## 2) Definisci metadati AI nel descriptor (`ai.fieldHints` + `ai.profile`)

- `ai.fieldHints`: metadati per-campo (tipo, ruolo semantico, operatori filtro, alias).
- `ai.profile`: metadati dataset-level (routing e workflow).

Esempio amministrativo (territoriale):

```json
{
  "ai": {
    "profile": {
      "datasetClass": "administrative",
      "queryRouting": {
        "preferredTool": "queryQCumberTerritorialUnits",
        "expectedAdminTypeSupported": true
      },
      "adminWorkflows": {
        "levelField": "lv",
        "nameFields": ["name", "name_en"],
        "adminTypeToLevel": {
          "country": 2,
          "region": 4,
          "province": 7,
          "municipality": 9
        },
        "parentIdFields": ["region_id", "province_id"],
        "parentIdPriorityForChildLevel": {
          "9": ["province_id", "region_id"]
        }
      }
    },
    "fieldHints": {
      "lv": {"semanticRole": "admin_level", "type": "number"},
      "name": {"semanticRole": "name"},
      "population": {"semanticRole": "population", "type": "number"},
      "province_id": {"semanticRole": "admin_parent_id"}
    }
  }
}
```

Esempio tematico (land cover):

```json
{
  "ai": {
    "profile": {
      "datasetClass": "land_cover",
      "queryRouting": {
        "preferredTool": "queryQCumberDatasetSpatial",
        "requiresSpatialBbox": true,
        "forbiddenAdminConstraints": ["expectedAdminType", "lv"]
      },
      "h3Workflows": {
        "largeExtentStrategy": "aggregateDatasetToH3",
        "recommendedGroupByFields": ["code_18", "class_name"],
        "recommendedWeightMode": "intersects",
        "avoidTools": ["tassellateDatasetLayer"]
      }
    },
    "fieldHints": {
      "code_18": {"semanticRole": "category_code"},
      "class_name": {"semanticRole": "category_name"}
    }
  }
}
```

Note:
- `ai.profile` viene esposto dal backend come `aiHints.aiProfile`.
- Non viene esposto come campo top-level, quindi la shape API resta compatibile.
- Per dataset amministrativi, dichiarare `ai.profile.adminWorkflows.adminTypeToLevel` evita mapping hardcoded lato runtime e riduce bias geografico.
- Per dataset CLC (`table=clc_2018`) il backend arricchisce automaticamente gli helper AI con:
  - `fieldCatalog.code_18.enumValues` (tutti i codici disponibili dal mapping locale)
  - `aiProfile.thematicCodeHierarchy` (livelli L1/L2/L3 per ampiezza codice + `allCodes`)

## 3) Aggiungi adapter solo se necessario

Usa `src/q_cumber_backend/dataset_adapters.py` solo quando servono:
- alias campi legacy -> canonici
- virtual fields
- enrich righe post-query
- normalizzazione filtri dataset-specific

Se `fieldHints` e `ai.profile` sono sufficienti, evita adapter custom.

## 4) Verifica routing e help API

Controlla che:
- `GET /providers/{provider_id}/datasets` esponga `aiHints.fieldCatalog`, `aiHints.aiProfile`, `routing`
- `GET /providers/{provider_id}/datasets/{dataset_id}/help` esponga hints coerenti
- `routing.queryToolHint.preferredTool` sia coerente con il dataset class e con `ai.profile.queryRouting.preferredTool` (se definito)
- `routing.queryToolHint.requiresSpatialBbox` / `forbiddenAdminConstraints` riflettano i vincoli dichiarati in `ai.profile.queryRouting`

## 5) Aggiungi test di conformita

Minimo consigliato:
- test metadata/routing: `tests/test_api_routing_metadata.py`
- test adapter (se adapter presente): `tests/test_dataset_adapters.py`

Comandi:

```bash
cd examples/q-map/backends/q-cumber-backend
python -m unittest -q tests/test_api_routing_metadata.py tests/test_dataset_adapters.py
```

Container:

```bash
docker exec q-map-q-cumber-backend python -m unittest -q tests/test_api_routing_metadata.py tests/test_dataset_adapters.py
```

## 6) Checklist di produzione

- Evita `parent_id` generico se il dataset usa parent-id specifici per livello.
- Popola `semanticRole` sui campi chiave (`admin_level`, `admin_parent_id`, `population`, `category_code`, `category_name`).
- Definisci `preferredTool` nel profilo invece di affidarti solo a euristiche testuali.
- Per dataset tematici grandi, specifica strategia H3 (`aggregateDatasetToH3`) nel profilo.
- Mantieni il prompt frontend generale: le regole dataset-specific devono stare nei metadati backend.
