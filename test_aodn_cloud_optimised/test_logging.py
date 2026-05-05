import logging
import os
import sys
import unittest
from io import StringIO
from unittest.mock import patch

from aodn_cloud_optimised.lib.logging import (
    BatchEvent,
    SummaryCaptureHandler,
    _summary_handlers,
    get_logger,
    print_processing_summary,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_handler() -> SummaryCaptureHandler:
    return SummaryCaptureHandler()


def _emit(handler: SummaryCaptureHandler, level: int, msg: str) -> None:
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

    def test_get_logger_attaches_summary_handler(self):
        """get_logger() must attach exactly one SummaryCaptureHandler."""
        logger = get_logger("test_summary_handler_attached")
        summary_handlers = [
            h for h in logger.handlers if isinstance(h, SummaryCaptureHandler)
        ]
        self.assertEqual(len(summary_handlers), 1)

    def test_summary_capture_handler_no_duplicates(self):
        """Calling get_logger() twice must not add a second SummaryCaptureHandler."""
        get_logger("test_no_dup_summary")
        get_logger("test_no_dup_summary")
        logger = logging.getLogger("test_no_dup_summary")
        summary_handlers = [
            h for h in logger.handlers if isinstance(h, SummaryCaptureHandler)
        ]
        self.assertEqual(len(summary_handlers), 1)

    def test_summary_handler_in_registry(self):
        """get_logger() must add the handler to _summary_handlers registry."""
        logger_name = "test_registry_check"
        get_logger(logger_name)
        self.assertIn(logger_name, _summary_handlers)
        self.assertIsInstance(_summary_handlers[logger_name], SummaryCaptureHandler)


# ---------------------------------------------------------------------------
# SummaryCaptureHandler — noise suppression
# ---------------------------------------------------------------------------


class TestNoiseSuppression(unittest.TestCase):
    def test_clear_existing_data_suppressed(self):
        h = _make_handler()
        _emit(
            h,
            logging.WARNING,
            "Option 'clear_existing_data' is True. DELETING all existing Zarr objects.",
        )
        self.assertEqual(len(h._other), 0)
        self.assertEqual(h.total_data_issues, 0)

    def test_resetting_coiled_suppressed(self):
        h = _make_handler()
        _emit(
            h,
            logging.WARNING,
            "uuid: Resetting Coiled cluster after scheduler failure…",
        )
        self.assertEqual(len(h._other), 0)

    def test_info_records_discarded_unless_success_signal(self):
        h = _make_handler()
        _emit(h, logging.INFO, "Some informational message about processing.")
        self.assertEqual(len(h._other), 0)
        self.assertEqual(len(h._batch_events), 0)

    def test_debug_records_discarded(self):
        h = _make_handler()
        _emit(h, logging.DEBUG, "Detailed debug info.")
        self.assertEqual(len(h._other), 0)


# ---------------------------------------------------------------------------
# SummaryCaptureHandler — retry tracking
# ---------------------------------------------------------------------------


class TestRetryTracking(unittest.TestCase):
    def test_zarr_retry_counted(self):
        h = _make_handler()
        _emit(
            h,
            logging.ERROR,
            "uuid: Scheduler/worker/shuffle failure during batch 5: some error. "
            "Recreating Dask cluster and retrying batch…\nRetry number for failing batch:1/5",
        )
        self.assertIn(5, h._batch_events)
        self.assertEqual(h._batch_events[5].retry_count, 1)
        # Must NOT appear in other
        self.assertEqual(len(h._other), 0)

    def test_parquet_retry_counted(self):
        h = _make_handler()
        _emit(
            h,
            logging.ERROR,
            "uuid: Scheduler connection lost during batch 12 (attempt 2/3): stream closed. "
            "Recreating Dask cluster and retrying batch...",
        )
        self.assertIn(12, h._batch_events)
        self.assertEqual(h._batch_events[12].retry_count, 1)

    def test_multiple_retries_accumulated(self):
        h = _make_handler()
        for i in range(3):
            _emit(
                h,
                logging.ERROR,
                f"uuid: Scheduler/worker/shuffle failure during batch 7: err. "
                f"Recreating Dask cluster and retrying batch…\nRetry number:{i+1}/5",
            )
        self.assertEqual(h._batch_events[7].retry_count, 3)

    def test_zarr_retry_exceeded_sets_outcome_individual_fallback(self):
        h = _make_handler()
        _emit(
            h,
            logging.ERROR,
            "uuid: Batch 7 has exceeded retry limit (5). Falling back to individual processing.",
        )
        self.assertIn(7, h._batch_events)
        self.assertEqual(h._batch_events[7].outcome, "individual_fallback")

    def test_parquet_retry_exceeded_sets_outcome_skipped(self):
        h = _make_handler()
        _emit(
            h,
            logging.ERROR,
            "uuid: Batch 3 exceeded retry limit (3). Skipping to next batch.",
        )
        self.assertIn(3, h._batch_events)
        self.assertEqual(h._batch_events[3].outcome, "skipped")

    def test_zarr_success_after_retry_marks_recovered(self):
        h = _make_handler()
        # First a retry
        _emit(
            h,
            logging.ERROR,
            "uuid: Scheduler/worker/shuffle failure during batch 2: err. "
            "Recreating Dask cluster and retrying batch…\nRetry number:1/5",
        )
        # Then success
        _emit(
            h,
            logging.INFO,
            "uuid: Batch 2 successfully published to Zarr store: s3://bucket/store.zarr",
        )
        self.assertEqual(h._batch_events[2].retry_count, 1)
        self.assertEqual(h._batch_events[2].outcome, "success")

    def test_parquet_success_after_retry_marks_recovered(self):
        h = _make_handler()
        _emit(
            h,
            logging.ERROR,
            "uuid: Scheduler connection lost during batch 8 (attempt 1/3): err. Recreating...",
        )
        _emit(h, logging.INFO, "uuid: batch 8 processing completed.")
        self.assertEqual(h._batch_events[8].retry_count, 1)
        self.assertEqual(h._batch_events[8].outcome, "success")

    def test_success_without_retries_not_tracked(self):
        """Success messages for batches with no retries should NOT create batch events."""
        h = _make_handler()
        _emit(
            h,
            logging.INFO,
            "uuid: Batch 1 successfully published to Zarr store: s3://x",
        )
        # This creates a batch event with outcome "success" but 0 retries
        # batches_with_retries should be empty
        self.assertEqual(h.batches_with_retries, [])

    def test_n_recovered_batches(self):
        h = _make_handler()
        # Batch 1: retried + recovered
        _emit(
            h,
            logging.ERROR,
            "uuid: Scheduler/worker/shuffle failure during batch 1: e. Recreating…\nRetry:1/5",
        )
        _emit(
            h,
            logging.INFO,
            "uuid: Batch 1 successfully published to Zarr store: s3://x",
        )
        # Batch 2: retried + failed permanently
        _emit(
            h,
            logging.ERROR,
            "uuid: Scheduler/worker/shuffle failure during batch 2: e. Recreating…\nRetry:1/5",
        )
        _emit(
            h,
            logging.ERROR,
            "uuid: Batch 2 has exceeded retry limit (5). Falling back to individual processing.",
        )
        self.assertEqual(h.n_recovered_batches, 1)
        self.assertEqual(h.n_failed_batches, 1)

    def test_individual_fallback_failed_files_extracted(self):
        h = _make_handler()
        _emit(
            h,
            logging.ERROR,
            "uuid: 2/3 file(s) failed during individual fallback for batch 9:\n"
            "  <File-like object S3FileSystem, bucket/path/IMOS_foo_20200101.nc>: error A\n"
            "  <File-like object S3FileSystem, bucket/path/IMOS_bar_20200102.nc>: error B",
        )
        self.assertIn(9, h._batch_events)
        ev = h._batch_events[9]
        self.assertEqual(len(ev.individual_failed), 2)
        self.assertIn("IMOS_foo_20200101.nc", ev.individual_failed[0][0])
        self.assertIn("IMOS_bar_20200102.nc", ev.individual_failed[1][0])


# ---------------------------------------------------------------------------
# SummaryCaptureHandler — bad data categorisation
# ---------------------------------------------------------------------------


class TestBadDataCategorisation(unittest.TestCase):
    def test_time_overlap(self):
        h = _make_handler()
        _emit(
            h,
            logging.ERROR,
            "uuid: Contact the data provider. File 'IMOS_foo_20200101.nc' has TIME values "
            "that overlap with a preceding file in the dataset.",
        )
        self.assertIn("time_overlap", h._bad_data)
        self.assertEqual(len(h._bad_data["time_overlap"]), 1)

    def test_structurally_incomplete(self):
        h = _make_handler()
        _emit(
            h,
            logging.ERROR,
            "uuid: Contact the data provider. Excluding structurally incomplete file "
            "from batch: IMOS_foo_20200101.nc",
        )
        self.assertIn("structural", h._bad_data)

    def test_inconsistent_grid(self):
        h = _make_handler()
        _emit(
            h,
            logging.ERROR,
            "uuid: Contact the data provider. The following files have an inconsistent grid "
            "with the rest of the dataset for variable 'WAVELENGTH_a':\n['IMOS_foo.nc', 'IMOS_bar.nc']",
        )
        self.assertIn("grid_inconsistency", h._bad_data)

    def test_cant_open(self):
        h = _make_handler()
        _emit(
            h,
            logging.ERROR,
            "uuid: Contact the data provider. 2/5 file(s) could not be opened and were "
            "excluded from the batch:\n  IMOS_bad.nc: NetCDF error",
        )
        self.assertIn("cant_open", h._bad_data)

    def test_nc_basenames_extracted(self):
        h = _make_handler()
        _emit(
            h,
            logging.ERROR,
            "uuid: Contact the data provider. Excluding structurally incomplete file from batch: "
            "<File-like object S3FileSystem, imos-data/IMOS/path/IMOS_SRS_20200101_fv02.nc>",
        )
        _msg, basenames = h._bad_data["structural"][0]
        self.assertIn("IMOS_SRS_20200101_fv02.nc", basenames)

    def test_grid_size_mismatch_batch_rejected(self):
        h = _make_handler()
        _emit(
            h,
            logging.ERROR,
            "uuid: Batch 15 rejected — grid mismatch\n"
            "The following 2 file(s) have an incompatible spatial grid and will be skipped:\n"
            "  - IMOS_foo_20200101.nc\n  - IMOS_bar_20200102.nc",
        )
        self.assertEqual(len(h._grid_mismatches), 1)
        self.assertEqual(h._grid_mismatches[0][0], 15)


# ---------------------------------------------------------------------------
# SummaryCaptureHandler — config hints
# ---------------------------------------------------------------------------


class TestConfigHints(unittest.TestCase):
    def test_autodrop_captured_deduplicated(self):
        h = _make_handler()
        msg = (
            "uuid: Auto-dropping variables/coordinates with no dimensions in common "
            "with region dims {'TIME'}: ['LATITUDE', 'LONGITUDE']. Consider adding these "
            "to 'vars_incompatible_with_region' in the dataset config."
        )
        _emit(h, logging.WARNING, msg)
        _emit(h, logging.WARNING, msg)  # duplicate
        self.assertEqual(len(h._config_hints), 1)

    def test_autodrop_not_in_other(self):
        h = _make_handler()
        _emit(
            h,
            logging.WARNING,
            "uuid: Auto-dropping variables/coordinates with no dimensions in common "
            "with region dims {'TIME'}: ['LATITUDE', 'LONGITUDE']. Consider adding...",
        )
        self.assertEqual(len(h._other), 0)


# ---------------------------------------------------------------------------
# SummaryCaptureHandler — catch-all other
# ---------------------------------------------------------------------------


class TestCatchAll(unittest.TestCase):
    def test_unknown_error_in_other(self):
        h = _make_handler()
        _emit(h, logging.ERROR, "Unexpected internal error: something went wrong.")
        self.assertEqual(len(h._other), 1)
        self.assertEqual(h._other[0][1], "ERROR")

    def test_unknown_warning_in_other(self):
        h = _make_handler()
        _emit(h, logging.WARNING, "Some unknown warning we did not anticipate.")
        self.assertEqual(len(h._other), 1)
        self.assertEqual(h._other[0][1], "WARNING")


# ---------------------------------------------------------------------------
# GenericParquetHandler handlers.clear() fix: _is_summary_handler marker
# ---------------------------------------------------------------------------


class TestSummaryHandlerMarker(unittest.TestCase):
    def test_is_summary_handler_attribute(self):
        h = _make_handler()
        self.assertTrue(getattr(h, "_is_summary_handler", False))

    def test_survives_selective_clear(self):
        """Simulates the GenericParquetHandler selective clear logic."""
        logger_name = "test_selective_clear"
        logger = get_logger(logger_name)
        # Simulate the fixed handlers.clear() from GenericParquetHandler
        for h in list(logger.handlers):
            if not getattr(h, "_is_summary_handler", False):
                h.close()
                logger.removeHandler(h)
        # SummaryCaptureHandler must still be present
        summary_handlers = [
            h for h in logger.handlers if isinstance(h, SummaryCaptureHandler)
        ]
        self.assertEqual(len(summary_handlers), 1)


# ---------------------------------------------------------------------------
# print_processing_summary
# ---------------------------------------------------------------------------


class TestPrintProcessingSummary(unittest.TestCase):
    def test_prints_no_issues(self):
        """print_processing_summary prints a ✅ line when there are no issues."""
        logger_name = "test_print_summary_clean"
        # Ensure clean handler
        _summary_handlers.pop(logger_name, None)
        get_logger(logger_name)
        captured = StringIO()
        with patch("sys.stdout", captured):
            print_processing_summary(logger_name, "My Dataset")
        output = captured.getvalue()
        self.assertIn("PROCESSING SUMMARY", output)
        self.assertIn("My Dataset", output)
        self.assertIn("✅", output)

    def test_prints_retry_section(self):
        """print_processing_summary shows CLUSTER RETRIES section when retries occurred."""
        logger_name = "test_print_summary_retries"
        _summary_handlers.pop(logger_name, None)
        logger = get_logger(logger_name)
        h = _summary_handlers[logger_name]
        # Batch 3: retried 2 times, recovered
        h._batch_events[3] = BatchEvent(batch_num=3, retry_count=2, outcome="success")
        # Batch 7: retried 5 times, fell back
        h._batch_events[7] = BatchEvent(
            batch_num=7,
            retry_count=5,
            outcome="individual_fallback",
            individual_failed=[("IMOS_bad.nc", "Error reading file")],
        )
        captured = StringIO()
        with patch("sys.stdout", captured):
            print_processing_summary(logger_name, "Test Dataset")
        output = captured.getvalue()
        self.assertIn("CLUSTER RETRIES", output)
        self.assertIn("✅", output)  # recovered
        self.assertIn("❌", output)  # failed
        self.assertIn("IMOS_bad.nc", output)
        self.assertIn("Batch    3", output)
        self.assertIn("Batch    7", output)

    def test_prints_data_quality_section(self):
        """print_processing_summary shows DATA QUALITY section with filenames."""
        logger_name = "test_print_summary_data_quality"
        _summary_handlers.pop(logger_name, None)
        get_logger(logger_name)
        h = _summary_handlers[logger_name]
        h._bad_data["time_overlap"].append(
            (
                "Contact the data provider. File 'IMOS_foo_20200101.nc' has TIME overlap.",
                ["IMOS_foo_20200101.nc"],
            )
        )
        captured = StringIO()
        with patch("sys.stdout", captured):
            print_processing_summary(logger_name, "Test Dataset")
        output = captured.getvalue()
        self.assertIn("DATA QUALITY", output)
        self.assertIn("TIME OVERLAP", output)
        self.assertIn("IMOS_foo_20200101.nc", output)

    def test_prints_config_hint_section(self):
        """print_processing_summary shows CONFIG HINT section with dedup."""
        logger_name = "test_print_summary_config"
        _summary_handlers.pop(logger_name, None)
        get_logger(logger_name)
        h = _summary_handlers[logger_name]
        h._config_hints.add(("'TIME'", "'LATITUDE', 'LONGITUDE'"))
        captured = StringIO()
        with patch("sys.stdout", captured):
            print_processing_summary(logger_name, "Test Dataset")
        output = captured.getvalue()
        self.assertIn("CONFIG HINT", output)
        self.assertIn("vars_incompatible_with_region", output)

    def test_uses_registry_when_handlers_cleared(self):
        """print_processing_summary must work even after logger.handlers.clear()."""
        logger_name = "test_print_summary_after_clear"
        _summary_handlers.pop(logger_name, None)
        logger = get_logger(logger_name)
        h = _summary_handlers[logger_name]
        h._other.append((logging.ERROR, "ERROR", "Something went wrong."))
        # Simulate handlers.clear()
        logger.handlers.clear()
        captured = StringIO()
        with patch("sys.stdout", captured):
            print_processing_summary(logger_name, "Test Dataset")
        output = captured.getvalue()
        self.assertIn("OTHER ERRORS", output)
        self.assertIn("Something went wrong", output)

    def test_no_crash_when_no_handler(self):
        """print_processing_summary must not crash if no SummaryCaptureHandler is found."""
        bare_logger_name = "test_bare_no_summary_handler_v2"
        _summary_handlers.pop(bare_logger_name, None)
        logging.getLogger(bare_logger_name)  # never passed through get_logger
        # Should silently return without raising
        print_processing_summary(bare_logger_name, "Bare")


if __name__ == "__main__":
    unittest.main()
