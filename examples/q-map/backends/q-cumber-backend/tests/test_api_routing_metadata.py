import json
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from q_cumber_backend.config import Settings
from q_cumber_backend.main import create_app
from q_cumber_backend.models import DatasetHelpResponse, DatasetQueryResponse


class QcumberApiRoutingMetadataTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.providers_dir = self.root / "providers" / "it"
        self.providers_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir = self.root / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        descriptor = {
            "id": "local-assets-it",
            "name": "Local Assets (IT)",
            "locale": "it",
            "datasets": [
                {
                    "id": "kontur-boundaries-italia",
                    "name": "Kontur Boundaries Italia",
                    "description": "Confini amministrativi con livelli/lv e popolazione",
                    "source": {
                        "type": "postgis",
                        "schema": "qvt",
                        "table": "kontur_boundaries",
                        "geometryColumn": "geom",
                    },
                    "format": "geojson",
                    "tags": ["kontur", "boundaries", "italia", "local"],
                    "ai": {
                        "profile": {
                            "datasetClass": "administrative",
                            "queryRouting": {
                                "preferredTool": "queryQCumberTerritorialUnits",
                                "expectedAdminTypeSupported": True,
                            },
                            "analysisMetrics": {
                                "metricSemantic": "administrative_reference",
                                "biasRisk": "none",
                                "numeratorFieldCandidates": [],
                                "denominatorFieldCandidates": [],
                                "preferredRankingFieldCandidates": [],
                                "recommendedDerivedMetrics": [],
                                "analysisCaveats": [],
                            },
                            "adminWorkflows": {
                                "levelField": "lv",
                                "parentIdFields": ["kontur_boundaries__lv4_id", "kontur_boundaries__lv7_id"],
                            },
                        },
                        "fieldHints": {
                            "name": {"description": "Nome unita", "semanticRole": "name", "filterOps": ["eq", "contains"]},
                            "population": {
                                "description": "Popolazione",
                                "semanticRole": "population",
                                "type": "number",
                                "filterOps": ["eq", "gt", "lt"],
                            },
                        }
                    },
                },
                {
                    "id": "test-air-quality-it",
                    "name": "Test Air Quality Measurements IT",
                    "description": "Thematic PM10 observations from monitoring stations",
                    "source": {
                        "type": "postgis",
                        "schema": "qvt",
                        "table": "air_quality_obs",
                        "geometryColumn": "geom",
                    },
                    "format": "geojson",
                    "tags": ["air", "quality", "thematic"],
                },
            ],
        }
        (self.providers_dir / "local-assets-it.json").write_text(
            json.dumps(descriptor),
            encoding="utf-8",
        )

        settings = Settings(
            api_token="",
            user_name="Q-cumber User",
            user_email="qcumber@example.com",
            data_dir=self.data_dir,
            providers_dir=self.root / "providers",
            cors_origins=["http://localhost:8081"],
            ai_hints_cache_ttl_seconds=3600,
            postgis_dsn="",
            postgis_host="localhost",
            postgis_port=5432,
            postgis_db="qvt",
            postgis_user="qvt",
            postgis_password="qvt",
        )
        self.app = create_app(settings)
        self.client = TestClient(self.app)

    def tearDown(self):
        self.tmp.cleanup()

    def test_datasets_endpoint_exposes_backend_routing_metadata(self):
        response = self.client.get("/providers/local-assets-it/datasets")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["providerId"], "local-assets-it")

        by_id = {item["id"]: item for item in payload["items"]}
        self.assertIn("kontur-boundaries-italia", by_id)
        self.assertIn("test-air-quality-it", by_id)

        admin_routing = by_id["kontur-boundaries-italia"]["routing"]
        self.assertEqual(admin_routing["datasetClass"], "administrative")
        self.assertTrue(admin_routing["isAdministrative"])
        self.assertNotIn("preferredQueryTool", admin_routing)
        self.assertNotIn("recommendedQueryTool", admin_routing)
        self.assertEqual(admin_routing["providerId"], "local-assets-it")
        self.assertEqual(admin_routing["datasetId"], "kontur-boundaries-italia")
        self.assertIsInstance(admin_routing.get("queryToolHint"), dict)
        self.assertEqual(admin_routing["queryToolHint"].get("preferredTool"), "queryQCumberTerritorialUnits")
        self.assertEqual(admin_routing["queryToolHint"].get("source"), "descriptor_profile")
        self.assertEqual(admin_routing["queryToolHint"].get("confidence"), "high")
        self.assertTrue(admin_routing["queryToolHint"].get("expectedAdminTypeSupported"))
        self.assertIsInstance(admin_routing.get("metricProfile"), dict)
        self.assertEqual(admin_routing["metricProfile"].get("metricSemantic"), "administrative_reference")
        self.assertEqual(admin_routing["metricProfile"].get("biasRisk"), "none")
        self.assertEqual(admin_routing["metricProfile"].get("source"), "descriptor_profile")
        self.assertEqual(admin_routing["metricProfile"].get("confidence"), "high")

        thematic_routing = by_id["test-air-quality-it"]["routing"]
        self.assertFalse(thematic_routing["isAdministrative"])
        self.assertNotIn("preferredQueryTool", thematic_routing)
        self.assertNotIn("recommendedQueryTool", thematic_routing)
        self.assertEqual(thematic_routing["providerId"], "local-assets-it")
        self.assertEqual(thematic_routing["datasetId"], "test-air-quality-it")
        self.assertIsInstance(thematic_routing.get("queryToolHint"), dict)
        self.assertEqual(thematic_routing["queryToolHint"].get("preferredTool"), "queryQCumberDataset")
        self.assertEqual(thematic_routing["queryToolHint"].get("source"), "inferred_fallback")
        self.assertEqual(thematic_routing["queryToolHint"].get("confidence"), "low")
        self.assertFalse(thematic_routing["queryToolHint"].get("expectedAdminTypeSupported"))
        self.assertIsNone(thematic_routing.get("metricProfile"))

        admin_hints = by_id["kontur-boundaries-italia"]["aiHints"]
        self.assertIsInstance(admin_hints, dict)
        self.assertIn("supportedFilterOps", admin_hints)
        self.assertIn("orderByCandidates", admin_hints)
        field_catalog = admin_hints.get("fieldCatalog")
        self.assertIsInstance(field_catalog, list)
        names = {item.get("name") for item in field_catalog if isinstance(item, dict)}
        self.assertIn("name", names)
        self.assertIn("population", names)
        order_candidates = [str(item) for item in admin_hints.get("orderByCandidates") or []]
        self.assertTrue(order_candidates)
        self.assertEqual(order_candidates[0], "population")
        self.assertIn("aiProfile", admin_hints)
        self.assertEqual(admin_hints["aiProfile"].get("datasetClass"), "administrative")
        self.assertNotIn("aiProfile", by_id["kontur-boundaries-italia"])

    def test_response_models_ignore_removed_top_level_query_tool_aliases(self):
        help_payload = DatasetHelpResponse(
            providerId="local-assets-it",
            datasetId="kontur-boundaries-italia",
            datasetName="Kontur Boundaries Italia",
            recommendedQueryTool="queryQCumberTerritorialUnits",
            queryToolHint={"preferredTool": "queryQCumberTerritorialUnits", "confidence": "high"},
        )
        self.assertNotIn("recommendedQueryTool", help_payload.model_dump())
        self.assertNotIn("queryToolHint", help_payload.model_dump())

        query_payload = DatasetQueryResponse(
            providerId="local-assets-it",
            datasetId="kontur-boundaries-italia",
            totalMatched=1,
            returned=1,
            fields=["name"],
            rows=[{"name": "Treviso"}],
            dataset={
                "info": {"id": "test-ds", "label": "Test DS"},
                "data": {"fields": [{"name": "name", "type": "string"}], "rows": [["Treviso"]]},
            },
            recommendedQueryTool="queryQCumberTerritorialUnits",
            queryToolHint={"preferredTool": "queryQCumberTerritorialUnits", "confidence": "high"},
        )
        self.assertNotIn("recommendedQueryTool", query_payload.model_dump())
        self.assertNotIn("queryToolHint", query_payload.model_dump())


if __name__ == "__main__":
    unittest.main()
