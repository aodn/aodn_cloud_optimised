import logging
import os
import unittest
from unittest.mock import patch

from aodn_cloud_optimised.lib.logging import get_logger


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


if __name__ == "__main__":
    unittest.main()
