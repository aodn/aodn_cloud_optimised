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

    Args:
        logger_name (str): Name of the logger.
        log_level (int, optional): Logging level for the logger (default is logging.DEBUG).
        raise_error (bool, optional): if True, sys.exit(1) on any log.error

    Returns:
        logging.Logger: The logger object.
    """

    existing_logger = logging.getLogger(logger_name)

    if not existing_logger.handlers:
        if raise_error:
            # Use the custom logger
            logger = ExitOnErrorLogger(logger_name)
        else:
            logger = logging.getLogger(logger_name)

        logger.setLevel(log_level)

        # Console handler with color
        color_formatter = CustomFormatter(use_color=True)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(color_formatter)
        stream_handler.setLevel(log_level)
        logger.addHandler(stream_handler)

        # Create a temporary file for logging with prefix and date
        date_str = datetime.now().strftime("%Y-%m-%d")
        temp_file_name = f"cloud_optimised_{logger_name}_{date_str}.log"
        temp_path = os.path.join(tempfile.gettempdir(), temp_file_name)

        # File handler without color
        file_formatter = CustomFormatter(use_color=False)
        file_handler = logging.FileHandler(temp_path)
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(log_level)
        logger.addHandler(file_handler)

        return logger
    else:
        return existing_logger
