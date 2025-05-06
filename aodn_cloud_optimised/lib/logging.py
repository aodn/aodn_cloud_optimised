import inspect
import logging
import os
import sys
import tempfile
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
    print(f"[DEBUG] Initializing logger: {logger_name}")

    logger = logging.getLogger(logger_name)

    if raise_error and not isinstance(logger, ExitOnErrorLogger):
        print(f"[DEBUG] Monkey-patching logger class for: {logger_name}")
        logger.__class__ = ExitOnErrorLogger
        logger.raise_error = True

    logger.setLevel(log_level)

    # Console handler with color
    color_formatter = CustomFormatter(use_color=True)
    
    # Check for handler types to avoid duplicates
    existing_handler_types = {type(h) for h in logger.handlers}

    # Add StreamHandler only if it doesn't already exist
    if logging.StreamHandler not in existing_handler_types:
        print("[DEBUG] Adding StreamHandler...")
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(CustomFormatter(color_formatter))
        stream_handler.setLevel(log_level)
        logger.addHandler(stream_handler)
        print("[DEBUG] StreamHandler added.")
    else:
        print("[DEBUG] StreamHandler already present.")

    # Add FileHandler only if it doesn't already exist
    if logging.FileHandler not in existing_handler_types:
        print("[DEBUG] Adding FileHandler...")
        date_str = datetime.now().strftime("%Y-%m-%d")
        temp_file_name = f"cloud_optimised_{logger_name}_{date_str}.log"
        temp_path = os.path.join(tempfile.gettempdir(), temp_file_name)

        file_handler = logging.FileHandler(temp_path)
        file_handler.setFormatter(CustomFormatter(use_color=False))
        file_handler.setLevel(log_level)
        logger.addHandler(file_handler)

        print(f"[DEBUG] FileHandler added. Log file path: {temp_path}")
    else:
        print("[DEBUG] FileHandler already present.")

    return logger
