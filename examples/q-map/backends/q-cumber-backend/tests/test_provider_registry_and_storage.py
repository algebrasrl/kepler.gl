import json
import tempfile
import unittest
from pathlib import Path

from q_cumber_backend.provider_registry import ProviderRegistry
from q_cumber_backend.storage import MapStore


class QcumberProviderRegistryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_lists_provider_from_locale_descriptor(self):
        locale_dir = self.root / "it"
        locale_dir.mkdir(parents=True, exist_ok=True)

        descriptor = {
            "id": "test-arpa-it",
            "name": "Test ARPA IT",
            "locale": "it",
            "datasets": [
                {
                    "id": "test-stations",
                    "name": "Test Stations",
                    "source": {"type": "postgis", "schema": "qvt", "table": "stations", "geometryColumn": "geom"},
                    "format": "geojson",
                    "ai": {
                        "fieldHints": {
                            "name": {
                                "description": "Station name",
                                "semanticRole": "name",
                                "filterOps": ["eq", "contains"],
                            }
                        }
                    },
                }
            ],
        }
        (locale_dir / "provider.json").write_text(json.dumps(descriptor), encoding="utf-8")

        registry = ProviderRegistry(self.root)
        locales = registry.list_locales()
        self.assertEqual(locales, ["it"])

        providers = registry.list_providers(locale="it")
        self.assertEqual(len(providers), 1)
        self.assertEqual(providers[0].id, "test-arpa-it")

        datasets = registry.list_provider_datasets("test-arpa-it")
        self.assertEqual(len(datasets), 1)
        self.assertEqual(datasets[0].id, "test-stations")
        self.assertIsNone(datasets[0].url)
        self.assertIsInstance(datasets[0].ai, dict)
        self.assertIn("fieldHints", datasets[0].ai or {})

    def test_ignores_non_locale_directories(self):
        non_locale_dir = self.root / "misc" / "data"
        non_locale_dir.mkdir(parents=True, exist_ok=True)
        (non_locale_dir / "air_quality.csv").write_text("x,y\n1,2\n", encoding="utf-8")

        registry = ProviderRegistry(self.root)
        providers = registry.list_providers()
        self.assertEqual(providers, [])


class QcumberMapStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = MapStore(Path(self.tmp.name))

    def tearDown(self):
        self.tmp.cleanup()

    def test_empty_store_by_default(self):
        maps = self.store.list_maps()
        self.assertEqual(maps, [])


if __name__ == "__main__":
    unittest.main()
