from __future__ import annotations

import ast
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dashboard.queries import station_inventory_sql, overview_sql


class StationInventoryTests(unittest.TestCase):
    def test_station_inventory_query_includes_coordinates_and_metrics(self):
        sql = station_inventory_sql(state="Punjab", city="Ludhiana", pollutant="PM2.5")
        self.assertIn("s.latitude", sql)
        self.assertIn("s.longitude", sql)
        self.assertIn("COUNT_IF(f.is_analytically_usable)", sql)
        self.assertIn("s.state = 'Punjab'", sql)
        self.assertIn("s.city = 'Ludhiana'", sql)
        self.assertIn("f.pollutant_id = 'PM2.5'", sql)

    def test_station_inventory_query_supports_unfiltered_scope(self):
        sql = station_inventory_sql()
        self.assertIn("WHERE 1 = 1", sql)
        self.assertIn("LIMIT 2000", sql)

    def test_overview_query_orders_by_selected_alias(self):
        sql = overview_sql()
        self.assertIn("ORDER BY retrieved_at_utc DESC, extraction_id DESC", sql)
        self.assertNotIn("ORDER BY MAX(retrieved_at_utc)", sql)

    def test_app_has_no_external_map_dependency(self):
        app_path = ROOT / "dashboard" / "app.py"
        source = app_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        self.assertFalse(any("folium" in name.lower() for name in imports))
        self.assertFalse(any("map_view" in name.lower() for name in imports))
        self.assertIn("Station inventory", source)
        self.assertNotIn("Geography", source)

    def test_dashboard_dependencies_exclude_folium(self):
        requirements = (ROOT / "requirements-dashboard.txt").read_text(encoding="utf-8").lower()
        self.assertNotIn("folium", requirements)
        self.assertNotIn("streamlit-folium", requirements)


if __name__ == "__main__":
    unittest.main()
