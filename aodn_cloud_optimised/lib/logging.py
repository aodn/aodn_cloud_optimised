import inspect
import logging
import os
import re
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime


class ExitOnErrorLogger(logging.Logger):
    """Custom logger that exits on error if raise_error is set."""

    def __init__(self, name, raise_error=True):
        super().__init__(name)

        self.raise_error = raise_error

    def error(self, msg, *args, **kwargs):

        super(ExitOnErrorLogger, self).error(msg, *args, **kwargs)
        if self.raise_error:
            raise Exception("Error in Cloud Optimised process. Forcing script exit")


class CustomFormatter(logging.Formatter):
    """Formatter to add colors, line numbers, and error types to logs."""

    FORMATS = {
        logging.DEBUG: "\033[94m%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s\033[0m",
        logging.INFO: "\033[92m%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s\033[0m",
        logging.WARNING: "\033[93m%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s\033[0m",
        logging.ERROR: "\033[91m%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s\033[0m",
        logging.CRITICAL: "\033[35m%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s\033[0m",
    }

    def __init__(self, use_color=True):
        """
        Initialize the custom formatter.

        Args:
            use_color (bool, optional): Whether to use colored output (default is True).
        """
        super().__init__()
        self.use_color = use_color

    def format(self, record):
        """
        Format the log record.

        Args:
            record (logging.LogRecord): The log record to format.

        Returns:
            str: Formatted log message.
        """
        # Get line number from caller
        frame = inspect.stack()[2]
        lineno = frame.lineno

        # Include error type for errors (handle potential missing exc_info)
        if record.levelno >= logging.ERROR and record.__dict__.get("exc_info"):
            filename = frame.filename
            error_type = (
                ": " + record.exc_info[0].__name__
            )  # Access first element of exc_info
            record.msg = f"{record.msg} ({filename}:{lineno}{error_type})"

        if self.use_color:
            log_fmt = self.FORMATS.get(record.levelno)
        else:
            log_fmt = "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s"

        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


# ---------------------------------------------------------------------------
# Module-level registry: survives logger.handlers.clear() calls
# ---------------------------------------------------------------------------

_summary_handlers: dict[str, "SummaryCaptureHandler"] = {}


def get_logger(
    logger_name: str, log_level: int = logging.DEBUG, raise_error: bool = False
) -> logging.Logger:
    """
    Get or create a logger with colored output for console, and non-colored output for file.
    Preserves existing handlers like Prefect's APILogHandler.

    Args:
        logger_name (str): Name of the logger.
        log_level (int, optional): Logging level for the logger (default is logging.DEBUG).
        raise_error (bool, optional): if True, raise exception on any log.error

    Returns:
        logging.Logger: The logger object.
    """
    logger = logging.getLogger(logger_name)

    if raise_error and not isinstance(logger, ExitOnErrorLogger):
        logger.__class__ = ExitOnErrorLogger
        logger.raise_error = True

    logger.setLevel(log_level)

    # Console handler with color
    color_formatter = CustomFormatter(use_color=True)

    # Check for handler types to avoid duplicates
    existing_handler_types = {type(h) for h in logger.handlers}

    # Add StreamHandler only if it doesn't already exist
    if logging.StreamHandler not in existing_handler_types:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(CustomFormatter(color_formatter))
        stream_handler.setLevel(log_level)
        logger.addHandler(stream_handler)

    # Add FileHandler only if it doesn't already exist
    if logging.FileHandler not in existing_handler_types:
        date_str = datetime.now().strftime("%Y-%m-%d")
        temp_file_name = f"cloud_optimised_{logger_name}_{date_str}.log"
        temp_path = os.path.join(tempfile.gettempdir(), temp_file_name)

        file_handler = logging.FileHandler(temp_path)
        file_handler.setFormatter(CustomFormatter(use_color=False))
        file_handler.setLevel(log_level)
        logger.addHandler(file_handler)

    # SummaryCaptureHandler: use module-level registry so it survives handlers.clear()
    if logger_name not in _summary_handlers:
        summary_handler = SummaryCaptureHandler()
        _summary_handlers[logger_name] = summary_handler
        logger.addHandler(summary_handler)
    elif SummaryCaptureHandler not in existing_handler_types:
        # Handler exists in registry but was cleared from logger — re-attach it
        logger.addHandler(_summary_handlers[logger_name])

    return logger


# ---------------------------------------------------------------------------
# Summary capture handler — patterns
# ---------------------------------------------------------------------------

# Noise: suppress entirely from summary
_NOISE_PATTERNS = [
    re.compile(r"Option 'clear_existing_data' is True", re.I),
    re.compile(r"Resetting Coiled cluster after scheduler failure", re.I),
    re.compile(r"^\s*Dropping variables from the dataset:", re.I),
]

# Retry attempt messages (ERROR level)
_ZARR_RETRY_PAT = re.compile(
    r"Scheduler/worker/shuffle failure during batch (\d+)", re.I
)
_PARQUET_RETRY_PAT = re.compile(r"Scheduler connection lost during batch (\d+)", re.I)

# Retry limit exceeded messages (ERROR level)
_ZARR_RETRY_EXCEEDED_PAT = re.compile(
    r"Batch (\d+) has exceeded retry limit.*Falling back to individual processing",
    re.I | re.DOTALL,
)
_PARQUET_RETRY_EXCEEDED_PAT = re.compile(
    r"Batch (\d+) exceeded retry limit.*Skipping to next batch",
    re.I | re.DOTALL,
)

# Batch success messages (INFO level — captured only to detect recovery after retries)
_ZARR_SUCCESS_PAT = re.compile(
    r"Batch (\d+) successfully published to Zarr store", re.I
)
_PARQUET_SUCCESS_PAT = re.compile(r"batch (\d+) processing completed\.", re.I)

# Individual fallback failure summary
_INDIVIDUAL_FALLBACK_PAT = re.compile(
    r"(\d+)/(\d+) file\(s\) failed during individual fallback for batch (\d+)", re.I
)

# Config hint: auto-drop warning
_CONFIG_HINT_AUTODROP_PAT = re.compile(
    r"Auto-dropping variables/coordinates with no dimensions in common with region dims "
    r"\{([^}]+)\}: \[([^\]]+)\]",
    re.I,
)

# Grid size mismatch (entire batch rejected)
_GRID_MISMATCH_PAT = re.compile(r"Batch (\d+) rejected", re.I)

# Bad data: contact the data provider — ordered from most specific to least
_BAD_DATA_PATTERNS: list[tuple[str, re.Pattern]] = [
    (
        "time_overlap",
        re.compile(r"Contact the data provider.*has.*overlap", re.I | re.DOTALL),
    ),
    (
        "structural",
        re.compile(r"Contact the data provider.*structurally incomplete", re.I),
    ),
    (
        "grid_inconsistency",
        re.compile(r"Contact the data provider.*inconsistent grid", re.I | re.DOTALL),
    ),
    ("cant_open", re.compile(r"Contact the data provider.*could not be opened", re.I)),
    (
        "bad_time",
        re.compile(r"Contact the data provider.*(?:bad.*time|time issues)", re.I),
    ),
    ("other_data", re.compile(r"Contact the data provider", re.I)),
]

# NC filename extractor (works on S3FileSystem repr, S3 URIs, and plain paths)
_NC_FILENAME_PAT = re.compile(r"([\w][\w\-\.]*\.nc(?:\.gz)?)")

_BAD_DATA_LABELS = {
    "time_overlap": "TIME OVERLAP (overlapping TIME values — excluded from batch)",
    "structural": "STRUCTURALLY INCOMPLETE (excluded from batch)",
    "grid_inconsistency": "INCONSISTENT GRID (excluded from batch)",
    "cant_open": "COULD NOT BE OPENED (excluded from batch)",
    "bad_time": "BAD TIME VALUES (excluded from batch)",
    "other_data": "OTHER DATA PROVIDER ISSUES",
}


def _extract_nc_basenames(msg: str) -> list[str]:
    """Extract unique NetCDF basenames from a log message (preserves order)."""
    seen: set[str] = set()
    out: list[str] = []
    for match in _NC_FILENAME_PAT.finditer(msg):
        b = os.path.basename(match.group(1))
        if b not in seen:
            seen.add(b)
            out.append(b)
    return out


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
# SummaryCaptureHandler
# ---------------------------------------------------------------------------


class SummaryCaptureHandler(logging.Handler):
    """Silently collects log records for an end-of-run digest.

    Tracks per-batch retry sequences and outcomes, bad-data files by category,
    config hints (deduplicated), and unexpected errors.  Call
    :func:`print_processing_summary` to render the digest after processing.
    """

    #: Marker used by GenericParquetHandler to preserve this handler during handlers.clear()
    _is_summary_handler = True

    def __init__(self):
        super().__init__(level=logging.DEBUG)  # Filter manually in emit()
        self._batch_events: dict[int, BatchEvent] = {}
        self._bad_data: dict[str, list] = defaultdict(
            list
        )  # cat -> [(msg, [basenames])]
        self._config_hints: set = set()  # set of (dims_str, vars_str) tuples
        self._grid_mismatches: list = []  # [(batch_num, msg)]
        self._other: list = []  # [(levelno, levelname, msg)]

    def _get_or_create_batch(self, batch_num: int) -> BatchEvent:
        if batch_num not in self._batch_events:
            self._batch_events[batch_num] = BatchEvent(batch_num=batch_num)
        return self._batch_events[batch_num]

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        lvl = record.levelno

        # --- Noise: suppress entirely ---
        for pat in _NOISE_PATTERNS:
            if pat.search(msg):
                return

        # --- INFO: capture only specific success signals, discard the rest ---
        if lvl == logging.INFO:
            m = _ZARR_SUCCESS_PAT.search(msg)
            if m:
                self._get_or_create_batch(int(m.group(1))).outcome = "success"
            else:
                m = _PARQUET_SUCCESS_PAT.search(msg)
                if m:
                    self._get_or_create_batch(int(m.group(1))).outcome = "success"
            return

        # --- DEBUG: always discard ---
        if lvl < logging.WARNING:
            return

        # === WARNING or ERROR from here ===

        # --- Retry attempts ---
        m = _ZARR_RETRY_PAT.search(msg) or _PARQUET_RETRY_PAT.search(msg)
        if m:
            self._get_or_create_batch(int(m.group(1))).retry_count += 1
            return  # Absorbed — don't show separately

        # --- Retry limit exceeded ---
        m = _ZARR_RETRY_EXCEEDED_PAT.search(msg)
        if m:
            self._get_or_create_batch(int(m.group(1))).outcome = "individual_fallback"
            return
        m = _PARQUET_RETRY_EXCEEDED_PAT.search(msg)
        if m:
            self._get_or_create_batch(int(m.group(1))).outcome = "skipped"
            return

        # --- Individual fallback failure summary ---
        m = _INDIVIDUAL_FALLBACK_PAT.search(msg)
        if m:
            batch_num = int(m.group(3))
            ev = self._get_or_create_batch(batch_num)
            for line in msg.split("\n")[1:]:
                line = line.strip()
                if not line:
                    continue
                # Format: "file_repr: error"
                parts = line.split(": ", 1)
                raw_path = (
                    re.sub(r"<File-like object \S+,?\s*", "", parts[0])
                    .strip()
                    .rstrip(">")
                )
                basename = os.path.basename(raw_path)
                err = parts[1] if len(parts) > 1 else ""
                ev.individual_failed.append((basename, err))
            return

        # --- Config hint: auto-drop ---
        m = _CONFIG_HINT_AUTODROP_PAT.search(msg)
        if m:
            self._config_hints.add((m.group(1), m.group(2)))
            return

        # --- Grid size mismatch (batch rejected) ---
        m = _GRID_MISMATCH_PAT.search(msg)
        if m:
            self._grid_mismatches.append((int(m.group(1)), msg))
            return

        # --- Bad data: contact the data provider ---
        for cat_key, pat in _BAD_DATA_PATTERNS:
            if pat.search(msg):
                self._bad_data[cat_key].append((msg, _extract_nc_basenames(msg)))
                return

        # --- Catch-all: unexpected errors and warnings ---
        self._other.append((lvl, record.levelname, msg))

    # ------------------------------------------------------------------
    # Computed properties used by print_processing_summary
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

    def get_log_file_path(self, logger: logging.Logger) -> str | None:
        """Return the path of the first FileHandler on *logger*, or None."""
        for h in logger.handlers:
            if isinstance(h, logging.FileHandler):
                return h.baseFilename
        return None


# ---------------------------------------------------------------------------
# print_processing_summary
# ---------------------------------------------------------------------------


def print_processing_summary(logger_name: str, dataset_name: str = "") -> None:
    """Print a concise end-of-run digest of all WARNING/ERROR log records.

    Retrieves the :class:`SummaryCaptureHandler` from the module-level registry
    (so it works even if ``logger.handlers`` was cleared) and renders a
    bordered, colour-highlighted summary to stdout.

    Args:
        logger_name: The logger name used throughout the run (matches the
            ``logger_name`` field in the dataset config).
        dataset_name: Human-readable dataset label for the summary header.
            Defaults to ``logger_name`` if empty.
    """
    # Prefer the module-level registry so the handler survives handlers.clear()
    handler: SummaryCaptureHandler | None = _summary_handlers.get(logger_name)
    if handler is None:
        logger = logging.getLogger(logger_name)
        handler = next(
            (h for h in logger.handlers if isinstance(h, SummaryCaptureHandler)), None
        )
    if handler is None:
        return

    logger = logging.getLogger(logger_name)
    log_path = handler.get_log_file_path(logger)
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

    batches_with_retries = handler.batches_with_retries
    n_failed = handler.n_failed_batches
    n_recovered = handler.n_recovered_batches
    n_data_issues = handler.total_data_issues
    n_config_hints = len(handler._config_hints)
    n_other = len(handler._other)

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
            retries_str = (
                f"🔄 {ev.retry_count} {'retry' if ev.retry_count == 1 else 'retries'}"
            )
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

            lines.append(f"  Batch {ev.batch_num:>4}  │  {retries_str} {outcome_str}")
            for fname, err in ev.individual_failed:
                short_err = (err[:80] + "…") if len(err) > 80 else err
                lines.append(f"{RED}              └─ {fname}: {short_err}{RESET}")

    # ── DATA QUALITY ───────────────────────────────────────────────────────
    has_bad_data = any(v for v in handler._bad_data.values())
    if has_bad_data or handler._grid_mismatches:
        lines.append(f"\n{BOLD}  {hr('─')}{RESET}")
        lines.append(f"{BOLD}  DATA QUALITY  (contact data provider){RESET}")
        lines.append(f"{BOLD}  {hr('─')}{RESET}")

        for cat_key, cat_label in _BAD_DATA_LABELS.items():
            entries = handler._bad_data.get(cat_key, [])
            if not entries:
                continue
            lines.append(f"\n{YELLOW}  {cat_label}:{RESET}")
            for _msg, basenames in entries:
                if basenames:
                    for b in basenames:
                        lines.append(f"{DIM}    • {b}{RESET}")
                else:
                    trimmed = (_msg[:160] + "…") if len(_msg) > 160 else _msg
                    lines.append(f"{YELLOW}    {trimmed}{RESET}")

        if handler._grid_mismatches:
            lines.append(
                f"\n{YELLOW}  INCOMPATIBLE SPATIAL GRID (batch skipped):{RESET}"
            )
            for batch_num, msg in handler._grid_mismatches:
                basenames = _extract_nc_basenames(msg)
                lines.append(f"  Batch {batch_num}:")
                for b in basenames:
                    lines.append(f"{DIM}    • {b}{RESET}")

    # ── CONFIG HINTS ───────────────────────────────────────────────────────
    if n_config_hints:
        lines.append(f"\n{BOLD}  {hr('─')}{RESET}")
        lines.append(f"{BOLD}  CONFIG HINT  (fix in dataset config){RESET}")
        lines.append(f"{BOLD}  {hr('─')}{RESET}")
        for dims_str, vars_str in sorted(handler._config_hints):
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
        for lvl, lvlname, msg in handler._other:
            colour = RED if lvl >= logging.ERROR else YELLOW
            display = (msg[:300] + "…") if len(msg) > 300 else msg
            lines.append(f"{colour}  [{lvlname}] {display}{RESET}")

    # ── Footer ────────────────────────────────────────────────────────────
    lines.append(f"\n{BOLD}{CYAN}{hr('─')}{RESET}")
    if log_path:
        lines.append(f"  Full log → {log_path}")
    lines.append(f"{BOLD}{CYAN}{hr('═')}{RESET}\n")

    print("\n".join(lines), file=sys.stdout, flush=True)
