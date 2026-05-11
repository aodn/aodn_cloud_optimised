# Integration Plan: AWS OpenData Registry Pydantic Models

## Problem
`DatasetConfig.aws_opendata_registry` was an untyped `dict | None` field — there was even a
`# TODO: Implement aws_opendata_registry model for validation` comment in `dataset_config.py`.
Four new Pydantic model files exist and need to be wired into the system:
- `dataset_entry.py` — `DatasetEntry` (top-level AWS OpenData Registry schema)
- `data_at_work.py` — `DataAtWork`, `Tutorial`, `ToolOrApplication`, `Publication`, `AWSService`
- `resource.py` — `Resource`, `ResourceType`
- `tags.py` — `Tag` enum

## Approach
1. Replace the untyped dict with the `DatasetEntry` pydantic model in `DatasetConfig`.
2. Update `CommonHandler.create_metadata_aws_registry()` to use `DatasetEntry.to_yaml()` for proper
   validated YAML output.
3. Export the new types from `__init__.py`.
4. Verify backward compatibility by running the existing test suite; fix/exclude any configs that
   don't pass `DatasetEntry` validation.
5. Add unit tests covering the new models.

## Key files / components to change

| File | Change |
|------|--------|
| `aodn_cloud_optimised/bin/config/model/dataset_config.py` | Change `aws_opendata_registry: dict \| None` → `DatasetEntry \| None`. Remove TODO. Remove `aws_opendata_registry` from `EXCLUDED_PATHS` placeholder validator (DatasetEntry validates its own content). |
| `aodn_cloud_optimised/bin/config/model/__init__.py` | Export `DatasetEntry`, `DataAtWork`, `Resource`, `Tag`, and sub-models. |
| `aodn_cloud_optimised/lib/CommonHandler.py` | In `create_metadata_aws_registry()`, detect `DatasetEntry` instance → `.to_yaml()`; fall back to `yaml.dump(dict)` for callers still using raw dicts (e.g. `create_aws_registry_dataset.py`). |
| `test_aodn_cloud_optimised/test_config.py` | After backward compat run: add any new failing configs to `_PLACEHOLDER_CONFIGS`; add `DatasetEntry` / `DatasetConfig` validation tests for the new field type. |
| `test_aodn_cloud_optimised/test_create_aws_registry.py` | Ensure `create_metadata_aws_registry` still produces correct YAML via `DatasetEntry`. |

## Important decisions / trade-offs
- **Strict typing vs. lenient union**: Using `DatasetEntry | None` (not `DatasetEntry | dict | None`)
  means JSON configs with malformed or placeholder `aws_opendata_registry` values will fail at load
  time. This is the intended behaviour (fulfilling the TODO). Configs with known placeholders are
  already excluded from backward compat tests via `_PLACEHOLDER_CONFIGS`.
- **`create_aws_registry_dataset.py` backward compat**: This script uses the legacy `load_dataset_config()`
  dict path (not `DatasetConfig`) and manipulates `aws_opendata_registry` as a raw dict. `CommonHandler`
  needs to tolerate both. The fallback in `create_metadata_aws_registry()` handles this.
- **`EXCLUDED_PATHS` removal**: Once `aws_opendata_registry` is typed as `DatasetEntry`, the
  placeholder text (`"FILL UP MANUALLY - CHECK DOCUMENTATION"`) will already cause pydantic
  validation errors (invalid URLs, enums, etc.), so the manual exclusion in `validate_no_manual_fill_placeholders`
  is no longer needed and should be removed.
- **`description` max_length removed**: The AWS spec says only the first 600 chars are *displayed*
  on the homepage — the field itself has no storage limit. The `max_length=600` constraint was
  dropped from `DatasetEntry` to avoid rejecting the 124+ existing configs with longer descriptions.

## Todo status

| # | Task | Status |
|---|------|--------|
| 1 | `dataset_config.py` — change field type, remove TODO, update `EXCLUDED_PATHS` | ✅ done |
| 2 | `__init__.py` — export all new model types | ✅ done |
| 3 | `CommonHandler.py` — update `create_metadata_aws_registry()` for typed + dict fallback | ✅ done |
| 4 | Run existing tests — identify backward compat failures | 🔄 in progress |
| 5 | Fix backward compat failures | ⏳ pending |
| 6 | Add unit tests for `DatasetEntry`, `Resource`, `DataAtWork`, `Tag` models | ⏳ pending |
