import tempfile
import unittest
from pathlib import Path
import json

from q_cumber_backend.dataset_adapters import DatasetAdapterRegistry


class DatasetAdaptersTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmp.name)
        reference_dir = self.data_dir / "reference"
        reference_dir.mkdir(parents=True, exist_ok=True)
        (reference_dir / "clc_code_18_labels.json").write_text(
            json.dumps({"221": {"name_it": "Vigneti", "name_en": "Vineyards"}}),
            encoding="utf-8",
        )
        self.registry = DatasetAdapterRegistry(data_dir=self.data_dir)

    def tearDown(self):
        self.tmp.cleanup()

    def test_kontur_alias_level_maps_to_lv(self):
        filters = [{"field": "level", "op": "eq", "value": 7}]
        normalized = self.registry.normalize_filters(
            table="kontur_boundaries",
            filters=filters,
            filter_get=lambda item, key, default=None: item.get(key, default),
        )
        self.assertEqual(normalized[0]["field"], "lv")
        self.assertEqual(normalized[0]["value"], 7)

    def test_clc_alias_code_18_maps_to_lowercase(self):
        filters = [{"field": "Code_18", "op": "eq", "value": 221}]
        normalized = self.registry.normalize_filters(
            table="clc_2018",
            filters=filters,
            filter_get=lambda item, key, default=None: item.get(key, default),
        )
        self.assertEqual(normalized[0]["field"], "code_18")
        self.assertEqual(normalized[0]["value"], "221")

    def test_clc_field_hint_overrides_include_all_codes(self):
        overrides = self.registry.field_hint_overrides(table="clc_2018")
        self.assertIn("code_18", overrides)
        enum_values = overrides["code_18"].get("enumValues") or []
        self.assertIn("221", enum_values)

    def test_clc_ai_profile_overrides_include_thematic_levels(self):
        overrides = self.registry.ai_profile_overrides(table="clc_2018")
        hierarchy = overrides.get("thematicCodeHierarchy") or {}
        levels = hierarchy.get("levels") or []
        self.assertTrue(levels)
        level_widths = {(item.get("level"), item.get("width")) for item in levels if isinstance(item, dict)}
        self.assertIn((1, 1), level_widths)
        self.assertIn((2, 2), level_widths)
        self.assertIn((3, 3), level_widths)
        self.assertIn("221", hierarchy.get("allCodes") or [])


if __name__ == "__main__":
    unittest.main()
