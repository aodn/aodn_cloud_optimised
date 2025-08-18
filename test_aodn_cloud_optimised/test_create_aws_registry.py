import logging
import os
import shutil
import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

from aodn_cloud_optimised.bin.create_aws_registry_dataset import main

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


DATASET_CONFIG_NC_ACORN_JSON = os.path.join(
    ROOT_DIR,
    "resources",
    "radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.json",
)
DATASET_CONFIG_NC_ACORN_JSON = Path(DATASET_CONFIG_NC_ACORN_JSON)

CSV_EXTRA_INFO = os.path.join(ROOT_DIR, "resources", "IMOSPortalCollections.csv")

CSV_EXTRA_INFO = Path(CSV_EXTRA_INFO)


class TestGenericCloudOptimisedCreation(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        # Delete all objects and buckets in the mock S3
        shutil.rmtree(self.tempdir)

    # @patch("argparse.ArgumentParser.parse_args")
    @patch(
        "aodn_cloud_optimised.bin.create_aws_registry_dataset.argparse.ArgumentParser.parse_args"
    )
    def test_main(self, mock_parse_args):
        # Prepare mock arguments
        mock_parse_args.return_value = MagicMock(
            file=DATASET_CONFIG_NC_ACORN_JSON,
            directory=self.tempdir,
            all=False,
            csv_path=CSV_EXTRA_INFO,
            geonetwork=False,
            # csv_path=None,
        )

        # Capture logs
        log_stream = StringIO()
        log_handler = logging.StreamHandler(log_stream)
        logger = logging.getLogger()
        logger.addHandler(log_handler)

        try:
            # Run main function
            main()

            # Get captured logs
            log_handler.flush()
            captured_logs = log_stream.getvalue().strip().split("\n")

            # Validate logs
            self.assertTrue(
                any("Created AWS Registry file at" in log for log in captured_logs)
            )
            self.assertTrue(
                any(
                    "radar_TurquoiseCoast_velocity_hourly_averaged_delayed_qc.yaml"
                    in log
                    for log in captured_logs
                )
            )

        finally:
            sys.stdout = sys.__stdout__


if __name__ == "__main__":
    unittest.main()
