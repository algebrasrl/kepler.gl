# Upload Fixtures

Fixtures used by `tests/e2e/ux-regression.spec.ts`.

- `reg-9` validates successful upload for:
  - `upload_points.csv`
  - `upload_points.json` (row-object JSON)
  - `upload_points.geojson`
- `reg-10` validates handled error UI for an invalid Shapefile ZIP payload.
- `reg-11` validates successful upload/conversion for:
  - `Confini_PANE_2024.shp.zip`
  - `h3_grid_res_3_kon_20230628.gpkg`

`upload_points.arrow` and `upload_points.parquet` are retained as optional fixtures, but those formats are currently hidden from q-map Upload UX.
