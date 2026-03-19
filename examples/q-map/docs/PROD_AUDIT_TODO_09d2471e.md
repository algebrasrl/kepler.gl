# Prod Audit TODO - session `09d2471e-dd96-4b12-a5f7-4f8def6fde22`

Date: 2026-03-15

## Done
- [x] Added runtime loop-limit recovery rule to stop `countQMapRows`/`waitForQMapDataset` retry loops after `dataset_not_found` failures and force dataset materialization first (`saveDataToMap` or `loadData`).
- [x] Added final-text normalization to strip runtime envelope lines leaked to user responses:
  - `[progress] ...`
  - `[executionSummary] ...`
  - `[requestId: ...]`
  - `[guardrail] ...`
- [x] Added regression tests for:
  - dataset-not-found materialization recovery loop-limit behavior
  - progress/executionSummary stripping and no fake coverage line on progress-only payloads

## Next
- [x] Validate the same production trace scenario with a replay harness and ensure no `countQMapRows` burst remains after first `dataset_not_found`.
  - replay check on requestId `334d6fa7a9ce48e6a0af99baec4194c4` confirms `dataset_not_found_materialization_recovery` removes `countQMapRows` and `waitForQMapDataset`.
- [x] Evaluate whether `waitForQMapDataset` should be hard-pruned too when forced recovery tool is `saveDataToMap` (currently pruned only in specific loop pattern).
  - current loop-limit rule already prunes `waitForQMapDataset` together with `countQMapRows` under dataset-not-found recovery.
- [x] Reduce residual `contractResponseMismatch` on `createDatasetWithGeometryArea` / `createDatasetWithNormalizedField` in live traces where wrapped tool payloads still miss canonical output metadata.
  - parser+contract validation replay on this trace now reports `contractResponseMismatch=0` for all extracted tool rows.
