import logging
import os
import tempfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

# ---------------------------------------------------------------------------
# BatchEvent dataclass
# ---------------------------------------------------------------------------


@dataclass
class BatchEvent:
    """Tracks the retry history and outcome for a single processing batch."""

    batch_num: int
    retry_count: int = 0
    outcome: str = "in_progress"  # "success" | "individual_fallback" | "skipped"
    individual_failed: list = field(default_factory=list)  # (basename, error) tuples


# ---------------------------------------------------------------------------
# BAD_DATA_LABELS
# ---------------------------------------------------------------------------

BAD_DATA_LABELS = {
    "time_overlap": "TIME OVERLAP (overlapping TIME values — excluded from batch)",
    "structural": "STRUCTURALLY INCOMPLETE (excluded from batch)",
    "grid_inconsistency": "INCONSISTENT GRID (excluded from batch)",
    "cant_open": "COULD NOT BE OPENED (excluded from batch)",
    "bad_time": "BAD TIME VALUES (excluded from batch)",
    "other_data": "OTHER DATA PROVIDER ISSUES",
}


# ---------------------------------------------------------------------------
# RunSummary
# ---------------------------------------------------------------------------


class RunSummary:
    """Accumulates structured processing events for an end-of-run digest.

    Call ``render(logger_name, dataset_name)`` after processing to print a
    bordered, colour-highlighted summary to stdout.
    """

    def __init__(self):
        self._batch_events: dict[int, BatchEvent] = {}
        self._bad_data: dict[str, list] = defaultdict(
            list
        )  # cat -> [(file_path, [basenames])]
        self._config_hints: set = set()  # set of (dims_str, vars_str) tuples
        self._grid_mismatches: list = []  # [(batch_num, [file_basenames])]
        self._other: list = []  # [(levelno, levelname, msg)]

    def _get_or_create_batch(self, batch_num: int) -> BatchEvent:
        if batch_num not in self._batch_events:
            self._batch_events[batch_num] = BatchEvent(batch_num=batch_num)
        return self._batch_events[batch_num]

    def record_batch_retry(self, batch_num: int) -> None:
        """Increment retry count for the given batch."""
        self._get_or_create_batch(batch_num).retry_count += 1

    def record_batch_outcome(self, batch_num: int, outcome: str) -> None:
        """Set the outcome for the given batch.

        Args:
            batch_num: 1-based batch number.
            outcome: One of "success", "individual_fallback", "skipped".
        """
        self._get_or_create_batch(batch_num).outcome = outcome

    def record_individual_failure(
        self, batch_num: int, basename: str, error: str
    ) -> None:
        """Record a per-file failure during individual fallback processing.

        Args:
            batch_num: 1-based batch number.
            basename: The file's basename.
            error: The error message string.
        """
        self._get_or_create_batch(batch_num).individual_failed.append((basename, error))

    def record_config_hint(self, dims_str: str, vars_str: str) -> None:
        """Record an auto-drop config hint (deduplicated by set).

        Args:
            dims_str: String representation of region dims.
            vars_str: String representation of auto-dropped variables.
        """
        self._config_hints.add((dims_str, vars_str))

    def record_grid_mismatch(self, batch_num: int, file_basenames: list[str]) -> None:
        """Record a batch rejected due to grid size mismatch.

        Args:
            batch_num: 1-based batch number.
            file_basenames: List of file basenames in the batch.
        """
        self._grid_mismatches.append((batch_num, file_basenames))

    def record_bad_file(
        self, category: str, file_path: str, basenames: list[str]
    ) -> None:
        """Record a bad-data file by category.

        Args:
            category: One of "time_overlap", "structural", "grid_inconsistency",
                "cant_open", "bad_time".
            file_path: The full file path (used for deduplication key).
            basenames: List of basenames to display in the summary.
        """
        self._bad_data[category].append((file_path, basenames))

    def record_other(self, levelno: int, levelname: str, message: str) -> None:
        """Record an unexpected error or warning that doesn't fit other categories.

        Args:
            levelno: The logging level number.
            levelname: The logging level name string.
            message: The log message.
        """
        self._other.append((levelno, levelname, message))

    # ------------------------------------------------------------------
    # Computed properties
    # ------------------------------------------------------------------

    @property
    def batches_with_retries(self) -> list[BatchEvent]:
        return [ev for ev in self._batch_events.values() if ev.retry_count > 0]

    @property
    def n_failed_batches(self) -> int:
        return sum(
            1
            for ev in self._batch_events.values()
            if ev.retry_count > 0 and ev.outcome != "success"
        )

    @property
    def n_recovered_batches(self) -> int:
        return sum(
            1
            for ev in self._batch_events.values()
            if ev.retry_count > 0 and ev.outcome == "success"
        )

    @property
    def total_data_issues(self) -> int:
        return sum(len(v) for v in self._bad_data.values()) + len(self._grid_mismatches)

    def _get_log_file_path(self, logger_name: str) -> str | None:
        """Return the path of the first FileHandler on the named logger, or None."""
        logger = logging.getLogger(logger_name)
        for h in logger.handlers:
            if isinstance(h, logging.FileHandler):
                return h.baseFilename
        return None

    def render(self, logger_name: str, dataset_name: str = "") -> None:
        """Print a concise end-of-run digest to stdout.

        Args:
            logger_name: The logger name used throughout the run.
            dataset_name: Human-readable dataset label for the summary header.
                Defaults to ``logger_name`` if empty.
        """
        log_path = self._get_log_file_path(logger_name)
        if log_path is None:
            # Reconstruct from naming convention as fallback
            date_str = datetime.now().strftime("%Y-%m-%d")
            candidate = os.path.join(
                tempfile.gettempdir(), f"cloud_optimised_{logger_name}_{date_str}.log"
            )
            if os.path.exists(candidate):
                log_path = candidate

        label = dataset_name or logger_name
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

        RED = "\033[91m"
        YELLOW = "\033[93m"
        GREEN = "\033[92m"
        CYAN = "\033[96m"
        BOLD = "\033[1m"
        DIM = "\033[2m"
        RESET = "\033[0m"

        WIDTH = 72

        def hr(char="─"):
            return char * WIDTH

        lines: list[str] = []
        lines.append(f"\n{BOLD}{CYAN}{hr('═')}{RESET}")
        lines.append(f"{BOLD}{CYAN}  PROCESSING SUMMARY — {label} — {timestamp}{RESET}")
        lines.append(f"{BOLD}{CYAN}{hr('═')}{RESET}")

        batches_with_retries = self.batches_with_retries
        n_failed = self.n_failed_batches
        n_recovered = self.n_recovered_batches
        n_data_issues = self.total_data_issues
        n_config_hints = len(self._config_hints)
        n_other = len(self._other)

        all_clear = not batches_with_retries and n_data_issues == 0 and n_other == 0
        if all_clear:
            lines.append(f"{GREEN}  ✅  No issues detected.{RESET}")
        else:
            if n_failed:
                lines.append(
                    f"  {RED}❌  {n_failed} batch{'es' if n_failed != 1 else ''} failed permanently{RESET}"
                )
            if n_recovered:
                lines.append(
                    f"  {GREEN}🔄  {n_recovered} batch{'es' if n_recovered != 1 else ''} recovered after retries{RESET}"
                )
            if n_data_issues:
                lines.append(
                    f"  {YELLOW}⚠   {n_data_issues} data quality issue{'s' if n_data_issues != 1 else ''}{RESET}"
                )
            if n_other:
                lines.append(
                    f"  {RED}❗  {n_other} unexpected error{'s' if n_other != 1 else ''}/warning{'s' if n_other != 1 else ''}{RESET}"
                )

        # ── CLUSTER RETRIES ────────────────────────────────────────────────────
        if batches_with_retries:
            lines.append(f"\n{BOLD}  {hr('─')}{RESET}")
            lines.append(f"{BOLD}  CLUSTER RETRIES{RESET}")
            lines.append(f"{BOLD}  {hr('─')}{RESET}")
            for ev in sorted(batches_with_retries, key=lambda e: e.batch_num):
                retries_str = f"🔄 {ev.retry_count} {'retry' if ev.retry_count == 1 else 'retries'}"
                if ev.outcome == "success":
                    outcome_str = f"{GREEN}→ ✅  recovered{RESET}"
                elif ev.outcome == "individual_fallback":
                    n_ff = len(ev.individual_failed)
                    if n_ff:
                        outcome_str = (
                            f"{RED}→ ❌  fell back to individual "
                            f"({n_ff} file{'s' if n_ff != 1 else ''} failed){RESET}"
                        )
                    else:
                        outcome_str = f"{YELLOW}→ ⚠   fell back to individual (all files succeeded){RESET}"
                elif ev.outcome == "skipped":
                    outcome_str = f"{RED}→ ⏭   skipped (retry limit exceeded){RESET}"
                else:
                    outcome_str = (
                        f"{YELLOW}→ ?   outcome unknown (still in progress?){RESET}"
                    )

                lines.append(
                    f"  Batch {ev.batch_num:>4}  │  {retries_str} {outcome_str}"
                )
                for fname, err in ev.individual_failed:
                    short_err = (err[:80] + "…") if len(err) > 80 else err
                    lines.append(f"{RED}              └─ {fname}: {short_err}{RESET}")

        # ── DATA QUALITY ───────────────────────────────────────────────────────
        has_bad_data = any(v for v in self._bad_data.values())
        if has_bad_data or self._grid_mismatches:
            lines.append(f"\n{BOLD}  {hr('─')}{RESET}")
            lines.append(f"{BOLD}  DATA QUALITY  (contact data provider){RESET}")
            lines.append(f"{BOLD}  {hr('─')}{RESET}")

            for cat_key, cat_label in BAD_DATA_LABELS.items():
                entries = self._bad_data.get(cat_key, [])
                if not entries:
                    continue
                lines.append(f"\n{YELLOW}  {cat_label}:{RESET}")
                for _file_path, basenames in entries:
                    if basenames:
                        for b in basenames:
                            lines.append(f"{DIM}    • {b}{RESET}")
                    else:
                        trimmed = (
                            (_file_path[:160] + "…")
                            if len(_file_path) > 160
                            else _file_path
                        )
                        lines.append(f"{YELLOW}    {trimmed}{RESET}")

            if self._grid_mismatches:
                lines.append(
                    f"\n{YELLOW}  INCOMPATIBLE SPATIAL GRID (batch skipped):{RESET}"
                )
                for batch_num, file_basenames in self._grid_mismatches:
                    lines.append(f"  Batch {batch_num}:")
                    for b in file_basenames:
                        lines.append(f"{DIM}    • {b}{RESET}")

        # ── CONFIG HINTS ───────────────────────────────────────────────────────
        if n_config_hints:
            lines.append(f"\n{BOLD}  {hr('─')}{RESET}")
            lines.append(f"{BOLD}  CONFIG HINT  (fix in dataset config){RESET}")
            lines.append(f"{BOLD}  {hr('─')}{RESET}")
            for dims_str, vars_str in sorted(self._config_hints):
                lines.append(
                    f"{YELLOW}  ⚠  Variables auto-dropped (no dims common with {{{dims_str}}}): [{vars_str}]{RESET}"
                )
                lines.append(
                    f"{DIM}     → Add to 'vars_incompatible_with_region' in dataset config to suppress{RESET}"
                )

        # ── OTHER ERRORS / WARNINGS ────────────────────────────────────────────
        if n_other:
            lines.append(f"\n{BOLD}  {hr('─')}{RESET}")
            lines.append(f"{BOLD}  OTHER ERRORS / WARNINGS{RESET}")
            lines.append(f"{BOLD}  {hr('─')}{RESET}")
            for lvl, lvlname, msg in self._other:
                colour = RED if lvl >= logging.ERROR else YELLOW
                display = (msg[:300] + "…") if len(msg) > 300 else msg
                lines.append(f"{colour}  [{lvlname}] {display}{RESET}")

        # ── Footer ────────────────────────────────────────────────────────────
        lines.append(f"\n{BOLD}{CYAN}{hr('─')}{RESET}")
        if log_path:
            lines.append(f"  Full log → {log_path}")
        lines.append(f"{BOLD}{CYAN}{hr('═')}{RESET}\n")

        print("\n".join(lines))  # noqa: T201
