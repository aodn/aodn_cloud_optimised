# Processing summary uses structured events, not log-message parsing

The end-of-run processing summary (which gives operators a digest of batch retries, data-provider bugs, and config hints from a 100K-line log) must be driven by structured events emitted at the call site — not by a `logging.Handler` that reconstructs events by regex-matching free-form log strings.

The regex approach (`SummaryCaptureHandler`) was introduced incrementally and is fragile: any log message wording change silently breaks the summary. The structured approach (`RunSummary` or `ProcessingSummary` object passed into the handler classes) decouples the summary from the log format, is straightforward to test, and keeps `logging.py` as a pure logging utility.

## Considered options

- **Regex log-parsing handler** (`SummaryCaptureHandler`): no changes to call sites; summary logic is centralised but coupled to log message wording. Each change to a log message requires a matching regex update. Hard to unit-test.
- **Structured event object** (chosen): `GenericParquetHandler` / `GenericZarrHandler` call `run_summary.record_batch_retry(...)`, `run_summary.record_bad_file(...)`, etc. directly. Summary renders from structured data. Zero regex patterns. Easy to unit-test. Requires minor refactoring of call sites.

## Consequences

The `SummaryCaptureHandler` class, `_summary_handlers` module-level registry, and `_is_summary_handler` marker on the handler class should be removed. The selective handler-clearing logic in `GenericParquetHandler` (which preserved the summary handler during `handlers.clear()`) can be simplified or removed entirely.
