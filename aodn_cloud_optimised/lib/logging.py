import inspect
import logging
import os
import re
import sys
import tempfile
from collections import defaultdict
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

    # Add SummaryCaptureHandler only if it doesn't already exist
    if SummaryCaptureHandler not in existing_handler_types:
        logger.addHandler(SummaryCaptureHandler())

    return logger


# ---------------------------------------------------------------------------
# Summary capture handler
# ---------------------------------------------------------------------------


class SummaryCaptureHandler(logging.Handler):
    """Silently collects WARNING and ERROR records for an end-of-run digest.

    Records are grouped into named categories based on regex patterns applied
    to the log message.  Call :func:`print_processing_summary` to render the
    digest after all processing is complete.
    """

    #: Ordered list of ``(category_key, display_label, compiled_pattern)``.
    #: The first matching category wins; unmatched records go to the catch-all.
    CATEGORIES = [
        (
            "data_quality",
            "DATA QUALITY  (contact data provider)",
            re.compile(r"contact the data provider", re.I),
        ),
        (
            "structural",
            "STRUCTURALLY INCOMPLETE FILES",
            re.compile(r"structurally incomplete", re.I),
        ),
        (
            "time_overlap",
            "OVERLAPPING TIME VALUES",
            re.compile(
                r"overlap.*TIME|TIME.*overlap|overlapping.*append|append.*overlap"
                r"|TIME values that overlap|overlapping.*time|time.*overlapping",
                re.I,
            ),
        ),
        (
            "grid_inconsistency",
            "GRID INCONSISTENCIES",
            re.compile(r"inconsistent grid|not globally monotonic", re.I),
        ),
        (
            "file_open_failure",
            "FILE OPEN / TIME FAILURES",
            re.compile(r"failed to open|time issues|bad time values", re.I),
        ),
    ]

    def __init__(self):
        super().__init__(level=logging.WARNING)
        # category -> list of (levelno, levelname, message)
        self._records: dict[str, list] = defaultdict(list)

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno < logging.WARNING:
            return
        msg = record.getMessage()
        category = "other"
        for cat_key, _label, pattern in self.CATEGORIES:
            if pattern.search(msg):
                category = cat_key
                break
        self._records[category].append((record.levelno, record.levelname, msg))

    @property
    def total_errors(self) -> int:
        return sum(
            1
            for entries in self._records.values()
            for lvl, *_ in entries
            if lvl >= logging.ERROR
        )

    @property
    def total_warnings(self) -> int:
        return sum(
            1
            for entries in self._records.values()
            for lvl, *_ in entries
            if lvl == logging.WARNING
        )

    def get_log_file_path(self, logger: logging.Logger) -> str | None:
        """Return the path of the first FileHandler on *logger*, or None."""
        for h in logger.handlers:
            if isinstance(h, logging.FileHandler):
                return h.baseFilename
        return None


def print_processing_summary(logger_name: str, dataset_name: str = "") -> None:
    """Print a concise end-of-run digest of all WARNING/ERROR log records.

    Retrieves the :class:`SummaryCaptureHandler` attached to the named logger
    and renders a bordered, colour-highlighted summary to stdout.  The full
    log file path is shown at the bottom so users can drill into details.

    Args:
        logger_name: The logger name used throughout the run (matches the
            ``logger_name`` field in the dataset config).
        dataset_name: Human-readable dataset label for the summary header.
            Defaults to ``logger_name`` if empty.
    """
    logger = logging.getLogger(logger_name)
    handler: SummaryCaptureHandler | None = next(
        (h for h in logger.handlers if isinstance(h, SummaryCaptureHandler)), None
    )
    if handler is None:
        return

    label = dataset_name or logger_name
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    log_path = handler.get_log_file_path(logger)

    # ANSI colours (match CustomFormatter palette)
    RED = "\033[91m"
    YELLOW = "\033[93m"
    GREEN = "\033[92m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    width = 72

    def hr(char="─"):
        return char * width

    def box_line(text="", fill=" "):
        """Left-aligned text padded to *width* chars."""
        return f"  {text}"

    lines = []
    lines.append(f"\n{BOLD}{CYAN}{hr('═')}{RESET}")
    lines.append(f"{BOLD}{CYAN}  PROCESSING SUMMARY — {label} — {timestamp}{RESET}")
    lines.append(f"{BOLD}{CYAN}{hr('═')}{RESET}")

    n_errors = handler.total_errors
    n_warnings = handler.total_warnings

    if n_errors == 0 and n_warnings == 0:
        lines.append(f"{GREEN}  ✅  No warnings or errors.{RESET}")
    else:
        parts = []
        if n_errors:
            parts.append(
                f"{RED}❌  {n_errors} error{'s' if n_errors != 1 else ''}{RESET}"
            )
        if n_warnings:
            parts.append(
                f"{YELLOW}⚠   {n_warnings} warning{'s' if n_warnings != 1 else ''}{RESET}"
            )
        lines.append("  " + "   ".join(parts))

        # Print each non-empty category
        for cat_key, cat_label, _pat in SummaryCaptureHandler.CATEGORIES:
            entries = handler._records.get(cat_key, [])
            if not entries:
                continue
            lines.append(f"\n{BOLD}  {hr('─')}{RESET}")
            lines.append(f"{BOLD}  {cat_label}{RESET}")
            lines.append(f"{BOLD}  {hr('─')}{RESET}")
            for lvl, lvlname, msg in entries:
                colour = RED if lvl >= logging.ERROR else YELLOW
                prefix = f"[{lvlname}]"
                # Truncate very long messages to keep the summary readable
                display = msg if len(msg) <= 200 else msg[:197] + "…"
                lines.append(f"{colour}  {prefix} {display}{RESET}")

        # Other catch-all
        other = handler._records.get("other", [])
        if other:
            lines.append(f"\n{BOLD}  {hr('─')}{RESET}")
            lines.append(f"{BOLD}  OTHER WARNINGS / ERRORS{RESET}")
            lines.append(f"{BOLD}  {hr('─')}{RESET}")
            for lvl, lvlname, msg in other:
                colour = RED if lvl >= logging.ERROR else YELLOW
                display = msg if len(msg) <= 200 else msg[:197] + "…"
                lines.append(f"{colour}  [{lvlname}] {display}{RESET}")

    lines.append(f"\n{BOLD}{CYAN}{hr('─')}{RESET}")
    if log_path:
        lines.append(f"  Full log → {log_path}")
    lines.append(f"{BOLD}{CYAN}{hr('═')}{RESET}\n")

    print("\n".join(lines), file=sys.stdout, flush=True)
