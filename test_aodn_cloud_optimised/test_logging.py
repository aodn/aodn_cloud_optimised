import logging
import os
import sys
import unittest
from io import StringIO
from unittest.mock import patch

from aodn_cloud_optimised.lib.logging import get_logger
from aodn_cloud_optimised.lib.run_summary import BAD_DATA_LABELS, BatchEvent, RunSummary

# ---------------------------------------------------------------------------
# get_logger tests
# ---------------------------------------------------------------------------


class TestGetLogger(unittest.TestCase):
    @patch("aodn_cloud_optimised.lib.logging.tempfile.mkstemp")
    def test_get_logger(self, mock_mkstemp):
        mock_temp_path = "/tmp/tempfile.log"
        mock_mkstemp.return_value = (0, mock_temp_path)

        logger = get_logger("test_logger")
        self.assertIsInstance(logger, logging.Logger)

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

    def test_get_logger_no_summary_handler(self):
        """get_logger() must not attach a SummaryCaptureHandler (removed)."""
        logger = get_logger("test_no_summary_handler")
        # Only StreamHandler and FileHandler expected — no SummaryCaptureHandler
        from aodn_cloud_optimised.lib import logging as lib_logging

        self.assertFalse(
            hasattr(lib_logging, "SummaryCaptureHandler"),
            "SummaryCaptureHandler should be gone from logging.py",
        )


# ---------------------------------------------------------------------------
# RunSummary — batch retry tracking
# ---------------------------------------------------------------------------


class TestBatchRetryTracking(unittest.TestCase):
    def test_record_batch_retry_increments(self):
        rs = RunSummary()
        rs.record_batch_retry(5)
        rs.record_batch_retry(5)
        rs.record_batch_retry(5)
        self.assertEqual(rs._batch_events[5].retry_count, 3)

    def test_record_batch_outcome_success(self):
        rs = RunSummary()
        rs.record_batch_retry(2)
        rs.record_batch_outcome(2, "success")
        self.assertEqual(rs._batch_events[2].outcome, "success")

    def test_record_batch_outcome_individual_fallback(self):
        rs = RunSummary()
        rs.record_batch_retry(7)
        rs.record_batch_outcome(7, "individual_fallback")
        self.assertEqual(rs._batch_events[7].outcome, "individual_fallback")

    def test_record_batch_outcome_skipped(self):
        rs = RunSummary()
        rs.record_batch_retry(3)
        rs.record_batch_outcome(3, "skipped")
        self.assertEqual(rs._batch_events[3].outcome, "skipped")

    def test_batches_with_retries_only_includes_retried(self):
        rs = RunSummary()
        rs.record_batch_outcome(1, "success")  # no retries — should not appear
        rs.record_batch_retry(2)
        rs.record_batch_outcome(2, "success")
        bwr = rs.batches_with_retries
        nums = [e.batch_num for e in bwr]
        self.assertNotIn(1, nums)
        self.assertIn(2, nums)

    def test_n_recovered_batches(self):
        rs = RunSummary()
        rs.record_batch_retry(1)
        rs.record_batch_outcome(1, "success")
        rs.record_batch_retry(2)
        rs.record_batch_outcome(2, "individual_fallback")
        self.assertEqual(rs.n_recovered_batches, 1)
        self.assertEqual(rs.n_failed_batches, 1)

    def test_record_individual_failure(self):
        rs = RunSummary()
        rs.record_individual_failure(9, "IMOS_foo_20200101.nc", "error A")
        rs.record_individual_failure(9, "IMOS_bar_20200102.nc", "error B")
        self.assertIn(9, rs._batch_events)
        ev = rs._batch_events[9]
        self.assertEqual(len(ev.individual_failed), 2)
        self.assertEqual(ev.individual_failed[0], ("IMOS_foo_20200101.nc", "error A"))


# ---------------------------------------------------------------------------
# RunSummary — bad data tracking
# ---------------------------------------------------------------------------


class TestBadDataTracking(unittest.TestCase):
    def test_record_bad_file_time_overlap(self):
        rs = RunSummary()
        rs.record_bad_file("time_overlap", "s3://bucket/IMOS_foo.nc", ["IMOS_foo.nc"])
        self.assertIn("time_overlap", rs._bad_data)
        self.assertEqual(len(rs._bad_data["time_overlap"]), 1)
        self.assertEqual(rs._bad_data["time_overlap"][0][1], ["IMOS_foo.nc"])

    def test_record_bad_file_structural(self):
        rs = RunSummary()
        rs.record_bad_file("structural", "s3://bucket/IMOS_bad.nc", ["IMOS_bad.nc"])
        self.assertIn("structural", rs._bad_data)

    def test_record_bad_file_grid_inconsistency(self):
        rs = RunSummary()
        rs.record_bad_file("grid_inconsistency", "s3://bucket/IMOS_x.nc", ["IMOS_x.nc"])
        self.assertIn("grid_inconsistency", rs._bad_data)

    def test_record_bad_file_cant_open(self):
        rs = RunSummary()
        rs.record_bad_file("cant_open", "s3://bucket/IMOS_y.nc", ["IMOS_y.nc"])
        self.assertIn("cant_open", rs._bad_data)

    def test_total_data_issues_counts_bad_files_and_mismatches(self):
        rs = RunSummary()
        rs.record_bad_file("time_overlap", "a.nc", ["a.nc"])
        rs.record_bad_file("structural", "b.nc", ["b.nc"])
        rs.record_grid_mismatch(5, ["c.nc", "d.nc"])
        self.assertEqual(rs.total_data_issues, 3)


# ---------------------------------------------------------------------------
# RunSummary — config hints
# ---------------------------------------------------------------------------


class TestConfigHints(unittest.TestCase):
    def test_record_config_hint_deduplicated(self):
        rs = RunSummary()
        rs.record_config_hint("'TIME'", "'LATITUDE', 'LONGITUDE'")
        rs.record_config_hint("'TIME'", "'LATITUDE', 'LONGITUDE'")  # duplicate
        self.assertEqual(len(rs._config_hints), 1)

    def test_record_config_hint_multiple_unique(self):
        rs = RunSummary()
        rs.record_config_hint("'TIME'", "'LATITUDE'")
        rs.record_config_hint("'TIME'", "'LONGITUDE'")
        self.assertEqual(len(rs._config_hints), 2)


# ---------------------------------------------------------------------------
# RunSummary — grid mismatch
# ---------------------------------------------------------------------------


class TestGridMismatch(unittest.TestCase):
    def test_record_grid_mismatch(self):
        rs = RunSummary()
        rs.record_grid_mismatch(15, ["IMOS_foo_20200101.nc", "IMOS_bar_20200102.nc"])
        self.assertEqual(len(rs._grid_mismatches), 1)
        self.assertEqual(rs._grid_mismatches[0][0], 15)
        self.assertIn("IMOS_foo_20200101.nc", rs._grid_mismatches[0][1])


# ---------------------------------------------------------------------------
# RunSummary — other
# ---------------------------------------------------------------------------


class TestOther(unittest.TestCase):
    def test_record_other_error(self):
        rs = RunSummary()
        rs.record_other(logging.ERROR, "ERROR", "Unexpected internal error.")
        self.assertEqual(len(rs._other), 1)
        self.assertEqual(rs._other[0][1], "ERROR")

    def test_record_other_warning(self):
        rs = RunSummary()
        rs.record_other(logging.WARNING, "WARNING", "Some unknown warning.")
        self.assertEqual(len(rs._other), 1)
        self.assertEqual(rs._other[0][1], "WARNING")


# ---------------------------------------------------------------------------
# RunSummary — render()
# ---------------------------------------------------------------------------


class TestRender(unittest.TestCase):
    def test_renders_no_issues(self):
        """render() prints ✅ when there are no issues."""
        rs = RunSummary()
        captured = StringIO()
        with patch("sys.stdout", captured):
            rs.render("nonexistent_logger_for_test", "My Dataset")
        output = captured.getvalue()
        self.assertIn("PROCESSING SUMMARY", output)
        self.assertIn("My Dataset", output)
        self.assertIn("✅", output)

    def test_renders_retry_section(self):
        """render() shows CLUSTER RETRIES section when retries occurred."""
        rs = RunSummary()
        rs.record_batch_retry(3)
        rs.record_batch_retry(3)
        rs.record_batch_outcome(3, "success")
        rs.record_batch_retry(7)
        rs.record_batch_outcome(7, "individual_fallback")
        rs.record_individual_failure(7, "IMOS_bad.nc", "Error reading file")
        captured = StringIO()
        with patch("sys.stdout", captured):
            rs.render("nonexistent_logger_for_test", "Test Dataset")
        output = captured.getvalue()
        self.assertIn("CLUSTER RETRIES", output)
        self.assertIn("✅", output)  # recovered
        self.assertIn("❌", output)  # failed
        self.assertIn("IMOS_bad.nc", output)
        self.assertIn("Batch    3", output)
        self.assertIn("Batch    7", output)

    def test_renders_data_quality_section(self):
        """render() shows DATA QUALITY section with filenames."""
        rs = RunSummary()
        rs.record_bad_file(
            "time_overlap",
            "s3://bucket/IMOS_foo_20200101.nc",
            ["IMOS_foo_20200101.nc"],
        )
        captured = StringIO()
        with patch("sys.stdout", captured):
            rs.render("nonexistent_logger_for_test", "Test Dataset")
        output = captured.getvalue()
        self.assertIn("DATA QUALITY", output)
        self.assertIn("TIME OVERLAP", output)
        self.assertIn("IMOS_foo_20200101.nc", output)

    def test_renders_config_hint_section(self):
        """render() shows CONFIG HINT section."""
        rs = RunSummary()
        rs.record_config_hint("'TIME'", "'LATITUDE', 'LONGITUDE'")
        captured = StringIO()
        with patch("sys.stdout", captured):
            rs.render("nonexistent_logger_for_test", "Test Dataset")
        output = captured.getvalue()
        self.assertIn("CONFIG HINT", output)
        self.assertIn("vars_incompatible_with_region", output)

    def test_renders_other_section(self):
        """render() shows OTHER ERRORS section."""
        rs = RunSummary()
        rs.record_other(logging.ERROR, "ERROR", "Something went wrong.")
        captured = StringIO()
        with patch("sys.stdout", captured):
            rs.render("nonexistent_logger_for_test", "Test Dataset")
        output = captured.getvalue()
        self.assertIn("OTHER ERRORS", output)
        self.assertIn("Something went wrong", output)

    def test_renders_grid_mismatch_section(self):
        """render() shows grid mismatch in DATA QUALITY section."""
        rs = RunSummary()
        rs.record_grid_mismatch(15, ["IMOS_foo.nc", "IMOS_bar.nc"])
        captured = StringIO()
        with patch("sys.stdout", captured):
            rs.render("nonexistent_logger_for_test", "Test Dataset")
        output = captured.getvalue()
        self.assertIn("INCOMPATIBLE SPATIAL GRID", output)
        self.assertIn("IMOS_foo.nc", output)

    def test_uses_logger_name_as_fallback_label(self):
        """render() uses logger_name as label when dataset_name is empty."""
        rs = RunSummary()
        captured = StringIO()
        with patch("sys.stdout", captured):
            rs.render("my_logger_name", "")
        output = captured.getvalue()
        self.assertIn("my_logger_name", output)


if __name__ == "__main__":
    unittest.main()
