import inspect
import logging
import os
import tempfile
from datetime import datetime


class CustomFormatter(logging.Formatter):
    """Formatter to add colors, line numbers, and error types to logs."""

    FORMATS = {
        logging.DEBUG:    '\033[94m%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s\033[0m',
        logging.INFO:     '\033[92m%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s\033[0m',
        logging.WARNING:  '\033[93m%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s\033[0m',
        logging.ERROR:    '\033[91m%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s\033[0m',
        logging.CRITICAL: '\033[35m%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s\033[0m',
    }

    def format(self, record):
        # Get line number from caller
        frame = inspect.stack()[2]
        lineno = frame.lineno

        # Include error type for errors (handle potential missing exc_info)
        if record.levelno >= logging.ERROR and record.__dict__.get('exc_info'):
            filename = frame.filename
            error_type = ': ' + record.exc_info[0].__name__  # Access first element of exc_info
            record.msg = f'{record.msg} ({filename}:{lineno}{error_type})'

        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def get_logger(logger_name: str, log_level: int = logging.DEBUG) -> logging.Logger:
    """
    Get or create a logger with colored output, line numbers, and error types.

    Args:
        logger_name (str): Name of the logger.
        log_level (int, optional): Logging level for the logger (default is logging.DEBUG).

    Returns:
        logging.Logger: The logger object.
    """

    existing_logger = logging.getLogger(logger_name)

    if not existing_logger.handlers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)

        formatter = CustomFormatter()
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(log_level)
        logger.addHandler(stream_handler)

        # Create a temporary file for logging with prefix and date
        #date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        date_str = datetime.now().strftime("%Y-%m-%d")
        temp_file_name = f"{logger_name}_{date_str}.log"

        temp_path = os.path.join(tempfile.gettempdir(),
                                 temp_file_name)

        file_handler = logging.FileHandler(temp_path)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)

        logger.addHandler(file_handler)

        return logger
    else:
        return existing_logger
