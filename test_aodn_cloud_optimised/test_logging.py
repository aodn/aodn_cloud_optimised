import logging
import os
import sys
import unittest
from io import StringIO
from unittest.mock import patch

from aodn_cloud_optimised.lib.logging import (
    SummaryCaptureHandler,
    get_logger,
    print_processing_summary,
)


class TestGetLogger(unittest.TestCase):
    @patch("aodn_cloud_optimised.lib.logging.tempfile.mkstemp")
    def test_get_logger(self, mock_mkstemp):
        mock_temp_path = "/tmp/tempfile.log"
        # Mock the return value of tempfile.mkstemp to return the desired temp_path
        mock_mkstemp.return_value = (0, mock_temp_path)  # random 0 value

        # Test logger creation
        logger = get_logger("test_logger")
        self.assertIsInstance(logger, logging.Logger)

        # Test logging to a file
        log_file_path = None
        try:
            logger.info("Testing logging to a file")
            handlers = logger.handlers
            for handler in handlers:
                if isinstance(handler, logging.FileHandler):
                    log_file_path = handler.baseFilename
                    self.assertTrue(os.path.exists(log_file_path))
                    break
        finally:
            if log_file_path:
                os.remove(log_file_path)

    def test_get_logger_attaches_summary_handler(self):
        """get_logger() must attach exactly one SummaryCaptureHandler."""
        logger = get_logger("test_summary_handler_attached")
        summary_handlers = [
            h for h in logger.handlers if isinstance(h, SummaryCaptureHandler)
        ]
        self.assertEqual(
            len(summary_handlers),
            1,
            "Expected exactly one SummaryCaptureHandler on the logger",
        )

    def test_summary_capture_handler_no_duplicates(self):
        """Calling get_logger() twice must not add a second SummaryCaptureHandler."""
        get_logger("test_no_dup_summary")
        get_logger("test_no_dup_summary")
        logger = logging.getLogger("test_no_dup_summary")
        summary_handlers = [
            h for h in logger.handlers if isinstance(h, SummaryCaptureHandler)
        ]
        self.assertEqual(len(summary_handlers), 1)


class TestSummaryCaptureHandler(unittest.TestCase):
    def _make_handler(self):
        return SummaryCaptureHandler()

    def _emit(self, handler, level, msg):
        record = logging.LogRecord(
            name="test",
            level=level,
            pathname="",
            lineno=0,
            msg=msg,
            args=(),
            exc_info=None,
        )
        handler.emit(record)

    def test_categorises_data_quality(self):
        h = self._make_handler()
        self._emit(h, logging.ERROR, "Contact the data provider. File foo.nc is bad.")
        self.assertIn("data_quality", h._records)
        self.assertEqual(len(h._records["data_quality"]), 1)

    def test_categorises_structural(self):
        h = self._make_handler()
        self._emit(
            h, logging.ERROR, "Excluding structurally incomplete file from batch."
        )
        self.assertIn("structural", h._records)

    def test_categorises_time_overlap(self):
        h = self._make_handler()
        self._emit(
            h, logging.ERROR, "File has TIME values that overlap with a preceding file."
        )
        self.assertIn("time_overlap", h._records)

    def test_categorises_grid_inconsistency(self):
        h = self._make_handler()
        self._emit(
            h, logging.WARNING, "dimension 'LONGITUDE' is not globally monotonic."
        )
        self.assertIn("grid_inconsistency", h._records)

    def test_categorises_file_open_failure(self):
        h = self._make_handler()
        self._emit(
            h, logging.ERROR, "Failed to open s3://bucket/foo.nc: connection error"
        )
        self.assertIn("file_open_failure", h._records)

    def test_catch_all_other(self):
        h = self._make_handler()
        self._emit(h, logging.WARNING, "Some unknown warning message.")
        self.assertIn("other", h._records)

    def test_total_counts(self):
        h = self._make_handler()
        self._emit(h, logging.ERROR, "Contact the data provider.")
        self._emit(h, logging.ERROR, "Contact the data provider again.")
        self._emit(h, logging.WARNING, "Some warning.")
        self.assertEqual(h.total_errors, 2)
        self.assertEqual(h.total_warnings, 1)

    def test_below_warning_not_captured(self):
        """INFO and DEBUG records must not be captured by SummaryCaptureHandler."""
        h = self._make_handler()
        self._emit(h, logging.INFO, "This is informational.")
        self._emit(h, logging.DEBUG, "This is debug.")
        self.assertEqual(sum(len(v) for v in h._records.values()), 0)


class TestPrintProcessingSummary(unittest.TestCase):
    def test_prints_no_issues(self):
        """print_processing_summary prints a ✅ line when there are no issues."""
        get_logger("test_print_summary_clean")
        captured = StringIO()
        with patch("sys.stdout", captured):
            print_processing_summary("test_print_summary_clean", "My Dataset")
        output = captured.getvalue()
        self.assertIn("PROCESSING SUMMARY", output)
        self.assertIn("My Dataset", output)
        self.assertIn("✅", output)

    def test_prints_errors_and_warnings(self):
        """print_processing_summary shows error/warning sections when records exist."""
        logger = get_logger("test_print_summary_errors")
        # Inject records directly into the handler
        handler = next(
            h for h in logger.handlers if isinstance(h, SummaryCaptureHandler)
        )
        record_err = logging.LogRecord(
            name="test_print_summary_errors",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="Contact the data provider. foo.nc is bad.",
            args=(),
            exc_info=None,
        )
        record_warn = logging.LogRecord(
            name="test_print_summary_errors",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="dimension 'LONGITUDE' is not globally monotonic.",
            args=(),
            exc_info=None,
        )
        handler.emit(record_err)
        handler.emit(record_warn)

        captured = StringIO()
        with patch("sys.stdout", captured):
            print_processing_summary("test_print_summary_errors", "Test Dataset")
        output = captured.getvalue()
        self.assertIn("❌", output)
        self.assertIn("⚠", output)
        self.assertIn("DATA QUALITY", output)
        self.assertIn("GRID INCONSISTENCIES", output)

    def test_no_crash_when_no_handler(self):
        """print_processing_summary must not crash if no SummaryCaptureHandler is found."""
        bare_logger_name = "test_bare_no_summary_handler"
        logging.getLogger(bare_logger_name)  # never passed through get_logger
        # Should silently return without raising
        print_processing_summary(bare_logger_name, "Bare")


if __name__ == "__main__":
    unittest.main()
