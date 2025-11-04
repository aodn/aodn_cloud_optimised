import copy
import json
import logging
import os
import unittest
from io import StringIO
from unittest.mock import patch

import boto3
import pandas as pd
import pyarrow as pa
import s3fs
from moto import mock_aws
from moto.moto_server.threaded_moto_server import ThreadedMotoServer
from shapely import wkb
from shapely.geometry import Polygon

from aodn_cloud_optimised.lib.config import load_dataset_config
from aodn_cloud_optimised.lib.GenericParquetHandler import GenericHandler
from aodn_cloud_optimised.lib.s3Tools import get_free_local_port, s3_ls

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Specify the filename relative to the current directory
TEST_FILE_NC_ANMN = os.path.join(
    ROOT_DIR,
    "resources",
    "IMOS_ANMN-NSW_CDSTZ_20210429T015500Z_SYD140_FV01_SYD140-2104-SBE37SM-RS232-128_END-20210812T011500Z_C-20210827T074819Z.nc",
)

TEST_FILE_NC_ARDC = os.path.join(
    ROOT_DIR, "resources", "BOM_20240301_CAPE-SORELL_RT_WAVE-PARAMETERS_monthly.nc"
)

TEST_FILE_NC_SOOP_SST = os.path.join(
    ROOT_DIR,
    "resources",
    "IMOS_SOOP-SST_MT_20110101T000000Z_9HA2479_FV01_C-20120528T071958Z.nc",
)

TEST_FILE_NC_SOOP_SST_BAD_NO_TIME = os.path.join(
    ROOT_DIR,
    "resources",
    "IMOS_SOOP-SST_ST_20130612T000000Z_VMQ9273_FV01_C-20130613T005515Z.nc",
)

TEST_FILE_NC_SOOP_SST_BAD_OUTOFRANGE_TIME = os.path.join(
    ROOT_DIR,
    "resources",
    "IMOS_SOOP-SST_ST_20131229T235800Z_VMQ9273_FV01_C-20131230T005523Z.nc",
)

TEST_FILE_NC_FISHSOOP = os.path.join(
    ROOT_DIR,
    "resources",
    "IMOS_SOOP-FishSOOP_TP_20250708T032737Z_FV01_1195.nc",
)


DUMMY_FILE = os.path.join(ROOT_DIR, "resources", "DUMMY.nan")
DUMMY_NC_FILE = os.path.join(ROOT_DIR, "resources", "DUMMY.nc")
TEST_CSV_FILE = os.path.join(
    ROOT_DIR, "resources", "A69-1105-135_107799906_130722039.csv"
)
DATASET_CONFIG_CSV_AATAMS_JSON = os.path.join(
    ROOT_DIR, "resources", "animal_acoustic_tracking_delayed_qc.json"
)

DATASET_CONFIG_NC_ANMN_JSON = os.path.join(
    ROOT_DIR, "resources", "mooring_ctd_delayed_qc.json"
)

DATASET_CONFIG_NC_ARDC_JSON = os.path.join(
    ROOT_DIR, "resources", "wave_buoy_realtime_nonqc.json"
)

DATASET_CONFIG_NC_SOOP_SST_JSON = os.path.join(
    ROOT_DIR, "resources", "vessel_sst_delayed_qc.json"
)

DATASET_CONFIG_NC_FISHSOOP_JSON = os.path.join(
    ROOT_DIR, "resources", "vessel_fishsoop_realtime_qc.json"
)


@mock_aws
class TestGenericHandler(unittest.TestCase):
    def setUp(self):
        # Create a mock S3 service
        self.BUCKET_OPTIMISED_NAME = "imos-data-lab-optimised"
        self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH = "testing"

        self.port = get_free_local_port()
        self.endpoint_ip = "127.0.0.1"
        self.server = ThreadedMotoServer(ip_address=self.endpoint_ip, port=self.port)
        self.server.start()

        self.s3_client_opts_common = {
            "service_name": "s3",
            "region_name": "us-east-1",
            "endpoint_url": f"http://{self.endpoint_ip}:{self.port}",
        }
        self.s3 = boto3.client(**self.s3_client_opts_common)

        self.s3.create_bucket(Bucket="imos-data")
        self.s3.create_bucket(Bucket=self.BUCKET_OPTIMISED_NAME)

        # create moto server; needed for s3fs and parquet
        self.s3_fs = s3fs.S3FileSystem(
            anon=False,
            client_kwargs={
                "endpoint_url": f"http://{self.endpoint_ip}:{self.port}/",
                "region_name": "us-east-1",
            },
        )

        # Make the "imos-data" bucket public
        public_policy_imos_data = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::imos-data/*",
                }
            ],
        }

        public_policy_cloud_optimised_data = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{self.BUCKET_OPTIMISED_NAME}/*",
                }
            ],
        }

        self.s3.put_bucket_policy(
            Bucket="imos-data", Policy=json.dumps(public_policy_imos_data)
        )

        self.s3.put_bucket_policy(
            Bucket=self.BUCKET_OPTIMISED_NAME,
            Policy=json.dumps(public_policy_cloud_optimised_data),
        )

        # Copy files to the mock S3 bucket
        self.s3.put_object(
            Bucket=self.BUCKET_OPTIMISED_NAME, Key="testing", Body=""
        )  # empty file
        self._upload_to_s3(
            "imos-data",
            f"good_nc_anmn/{os.path.basename(TEST_FILE_NC_ANMN)}",
            TEST_FILE_NC_ANMN,
        )
        self._upload_to_s3(
            "imos-data", f"dummy/{os.path.basename(DUMMY_FILE)}", DUMMY_FILE
        )
        self._upload_to_s3(
            "imos-data", f"dummy_nc/{os.path.basename(DUMMY_NC_FILE)}", DUMMY_NC_FILE
        )
        self._upload_to_s3(
            "imos-data", f"good_csv/{os.path.basename(TEST_CSV_FILE)}", TEST_CSV_FILE
        )
        self._upload_to_s3(
            "imos-data",
            f"good_nc_ardc/{os.path.basename(TEST_FILE_NC_ARDC)}",
            TEST_FILE_NC_ARDC,
        )

        self._upload_to_s3(
            "imos-data",
            f"good_nc_soop_sst/{os.path.basename(TEST_FILE_NC_SOOP_SST)}",
            TEST_FILE_NC_SOOP_SST,
        )

        self._upload_to_s3(
            "imos-data",
            f"bad_nc_soop_sst/{os.path.basename(TEST_FILE_NC_SOOP_SST_BAD_NO_TIME)}",
            TEST_FILE_NC_SOOP_SST_BAD_NO_TIME,
        )
        self._upload_to_s3(
            "imos-data",
            f"bad_nc_soop_sst/{os.path.basename(TEST_FILE_NC_SOOP_SST_BAD_OUTOFRANGE_TIME)}",
            TEST_FILE_NC_SOOP_SST_BAD_OUTOFRANGE_TIME,
        )

        self._upload_to_s3(
            "imos-data",
            f"fishsoop/{os.path.basename(TEST_FILE_NC_FISHSOOP)}",
            TEST_FILE_NC_FISHSOOP,
        )

        dataset_anmn_netcdf_config = load_dataset_config(DATASET_CONFIG_NC_ANMN_JSON)
        self.handler_nc_anmn_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_anmn_netcdf_config,
            clear_existing_data=True,
            force_previous_parquet_deletion=True,
            cluster_mode="local",
            s3_client_opts_common=self.s3_client_opts_common,
            s3_fs_common_session=self.s3_fs,
        )

        dataset_ardc_netcdf_config = load_dataset_config(DATASET_CONFIG_NC_ARDC_JSON)
        self.handler_nc_ardc_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_ardc_netcdf_config,
            clear_existing_data=True,
            force_previous_parquet_deletion=True,
            cluster_mode="local",
            s3_client_opts_common=self.s3_client_opts_common,
            s3_fs_common_session=self.s3_fs,
        )

        dataset_ardc_netcdf_config_mod = copy.deepcopy(dataset_ardc_netcdf_config)
        self.mod_title = "modified_title"
        dataset_ardc_netcdf_config_mod["schema_transformation"]["global_attributes"][
            "set"
        ]["title"] = self.mod_title
        self.handler_nc_ardc_mod_metadata = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_ardc_netcdf_config_mod,
            s3_client_opts_common=self.s3_client_opts_common,
            s3_fs_common_session=self.s3_fs,
        )

        dataset_soop_sst_netcdf_config = load_dataset_config(
            DATASET_CONFIG_NC_SOOP_SST_JSON
        )
        self.handler_nc_soop_sst_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_soop_sst_netcdf_config,
            clear_existing_data=True,
            force_previous_parquet_deletion=True,
            cluster_mode="local",
            s3_client_opts_common=self.s3_client_opts_common,
            s3_fs_common_session=self.s3_fs,
        )

        dataset_aatams_csv_config = load_dataset_config(DATASET_CONFIG_CSV_AATAMS_JSON)
        self.handler_csv_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_aatams_csv_config,
            clear_existing_data=True,
            s3_client_opts_common=self.s3_client_opts_common,
            s3_fs_common_session=self.s3_fs,
            # cluster_mode="local",  # TEST without the localcluster
        )

        dataset_fishoop_netcdf_config = load_dataset_config(
            DATASET_CONFIG_NC_FISHSOOP_JSON
        )
        self.handler_nc_fishoop_file = GenericHandler(
            optimised_bucket_name=self.BUCKET_OPTIMISED_NAME,
            root_prefix_cloud_optimised_path=self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH,
            dataset_config=dataset_fishoop_netcdf_config,
            clear_existing_data=True,
            force_previous_parquet_deletion=True,
            cluster_mode=None,
            s3_client_opts_common=self.s3_client_opts_common,
            s3_fs_common_session=self.s3_fs,
        )

    def _upload_to_s3(self, bucket_name, key, file_path):
        with open(file_path, "rb") as f:
            self.s3.upload_fileobj(f, bucket_name, key)

    def tearDown(self):
        # Delete all objects and buckets in the mock S3
        bucket_list = self.s3.list_buckets()["Buckets"]
        for bucket in bucket_list:
            bucket_name = bucket["Name"]
            objects = self.s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
            for obj in objects:
                self.s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
            self.s3.delete_bucket(Bucket=bucket_name)

        self.server.stop()

    def test_parquet_nc_fishsoop(self):
        nc_obj_ls = s3_ls("imos-data", "fishsoop")

        # Capture logs
        log_stream = StringIO()
        log_handler = logging.StreamHandler(log_stream)
        logger = logging.getLogger()
        logger.addHandler(log_handler)

        # 1st pass
        self.handler_nc_fishoop_file.to_cloud_optimised([nc_obj_ls[0]])

        log_handler.flush()
        captured_logs = log_stream.getvalue().strip().split("\n")

        # Validate logs
        self.assertTrue(
            any(
                "The dataframe is now empty after removing out of range" in log
                for log in captured_logs
            )
        )

    def test_parquet_nc_anmn_handler(self):
        nc_obj_ls = s3_ls("imos-data", "good_nc_anmn")

        # 1st pass
        self.handler_nc_anmn_file.to_cloud_optimised([nc_obj_ls[0]])

        # 2nd pass, process the same file a second time. Should be deleted
        # TODO: Not a big big deal breaker, but got an issue which should be fixed in the try except only for the unittest
        #       2024-07-01 16:04:54,721 - INFO - GenericParquetHandler.py:824 - delete_existing_matching_parquet - No files to delete: GetFileInfo() yielded path 'imos-data-lab-optimised/testing/anmn_ctd_ts_fv01.parquet/site_code=SYD140/timestamp=1625097600/polygon=01030000000100000005000000000000000020624000000000008041C0000000000060634000000000008041C0000000000060634000000000000039C0000000000020624000000000000039C0000000000020624000000000008041C0/IMOS_ANMN-NSW_CDSTZ_20210429T015500Z_SYD140_FV01_SYD140-2104-SBE37SM-RS232-128_END-20210812T011500Z_C-20210827T074819Z.nc-0.parquet', which is outside base dir 's3://imos-data-lab-optimised/testing/anmn_ctd_ts_fv01.parquet/'
        self.handler_nc_anmn_file.to_cloud_optimised_single(nc_obj_ls[0])

        # read parquet
        dataset_config = load_dataset_config(DATASET_CONFIG_NC_ANMN_JSON)
        dataset_name = dataset_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.parquet/"

        parquet_dataset = pd.read_parquet(
            dname,
            engine="pyarrow",
            storage_options={
                "client_kwargs": {
                    "endpoint_url": f"http://{self.endpoint_ip}:{self.port}"
                }
            },
        )

        self.assertNotIn("station_name", parquet_dataset.columns)
        self.assertAlmostEqual(parquet_dataset["TEMP"][0], 13.2773, delta=1e-2)

        # Check if the column are added with the correct values
        self.assertIn("site_code", parquet_dataset.columns)
        self.assertEqual(parquet_dataset["site_code"].iloc[0], "SYD140")

        self.assertEqual(
            parquet_dataset["filename"].iloc[0], os.path.basename(nc_obj_ls[0])
        )
        self.assertEqual(parquet_dataset["timestamp"].iloc[0], 1617235200.0)

        # The following section shows how the created polygon variable can be used to perform data queries. this adds significant overload, but is worth it
        parquet_dataset["converted_polygon"] = parquet_dataset["polygon"].apply(
            lambda x: wkb.loads(bytes.fromhex(x))
        )

        # Define the predefined polygon
        predefined_polygon_coords_out = [(150, -40), (155, -40), (155, -45), (150, -45)]
        predefined_polygon_coords_in = [(150, -32), (155, -32), (155, -45), (150, -45)]

        predefined_polygon_out = Polygon(predefined_polygon_coords_out)
        predefined_polygon_in = Polygon(predefined_polygon_coords_in)

        df_unique_polygon = parquet_dataset["converted_polygon"].unique()[0]
        self.assertFalse(df_unique_polygon.intersects(predefined_polygon_out))
        self.assertTrue(df_unique_polygon.intersects(predefined_polygon_in))

        # Testing the metadata sidecar file
        # Reading the metadata file of the dataset (at the root)
        parquet_meta_file_path = os.path.join(
            self.handler_nc_anmn_file.cloud_optimised_output_path, "_common_metadata"
        )
        parquet_meta = pa.parquet.read_schema(
            parquet_meta_file_path, filesystem=self.s3_fs
        )

        # horrible ... but got to be done. The dictionary of metadata has to be a dictionnary with byte keys and byte values.
        # meaning that we can't have nested dictionaries ...
        decoded_meta = {
            key.decode("utf-8"): json.loads(value.decode("utf-8").replace("'", '"'))
            for key, value in parquet_meta.metadata.items()
        }

        self.assertEqual(decoded_meta["LONGITUDE"]["axis"], "X")
        self.assertEqual(decoded_meta["NOMINAL_DEPTH"]["standard_name"], "depth")

        # alternative way to access the metadata
        # Create a dictionary where keys are the names and values are the elements
        schema_dict = {obj.name: obj for obj in parquet_meta}
        self.assertEqual(
            schema_dict["TEMP"].metadata.get(b"standard_name"), b"sea_water_temperature"
        )
        # other way to access the metadata
        schema_dict = {obj.name: obj.metadata for obj in parquet_meta}
        self.assertEqual(
            schema_dict["TEMP"][b"standard_name"], b"sea_water_temperature"
        )

    def test_parquet_nc_generic_handler_h5netcdf(self):
        # test with the h5netcdf engine
        nc_obj_ls = s3_ls("imos-data", "good_nc_ardc")

        # 1st pass
        self.handler_nc_ardc_file.to_cloud_optimised([nc_obj_ls[0]])

        # 2nd pass, process the same file a second time. Should be deleted
        # TODO: Not a big big deal breaker, but got an issue which should be fixed in the try except only for the unittest
        #       2024-07-01 16:04:54,721 - INFO - GenericParquetHandler.py:824 - delete_existing_matching_parquet - No files to delete: GetFileInfo() yielded path 'imos-data-lab-optimised/testing/anmn_ctd_ts_fv01.parquet/site_code=SYD140/timestamp=1625097600/polygon=01030000000100000005000000000000000020624000000000008041C0000000000060634000000000008041C0000000000060634000000000000039C0000000000020624000000000000039C0000000000020624000000000008041C0/IMOS_ANMN-NSW_CDSTZ_20210429T015500Z_SYD140_FV01_SYD140-2104-SBE37SM-RS232-128_END-20210812T011500Z_C-20210827T074819Z.nc-0.parquet', which is outside base dir 's3://imos-data-lab-optimised/testing/anmn_ctd_ts_fv01.parquet/'
        self.handler_nc_ardc_file.to_cloud_optimised_single(nc_obj_ls[0])

        # read parquet
        dataset_config = load_dataset_config(DATASET_CONFIG_NC_ARDC_JSON)
        dataset_name = dataset_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.parquet/"

        parquet_dataset = pd.read_parquet(
            dname,
            engine="pyarrow",
            storage_options={
                "client_kwargs": {
                    "endpoint_url": f"http://{self.endpoint_ip}:{self.port}"
                }
            },
        )

        self.assertEqual(parquet_dataset["timestamp"][0], 1709251200.0)

        self.assertNotIn(
            "DUMMY_VAR_NOT_IN", parquet_dataset.columns
        )  # make sure the variable is removed
        self.assertNotIn(
            "WHTH", parquet_dataset.columns
        )  # removed on purpose to trigger "missing variable from provided pyarrow_schema config, please add to dataset config"
        self.assertIn("WPMH", parquet_dataset.columns)

        self.assertEqual(parquet_dataset["timestamp"][0], 1709251200.0)
        self.assertEqual(
            parquet_dataset["TIME"][0], pd.Timestamp("2024-03-01 01:30:00")
        )

        parquet_meta_file_path = os.path.join(
            self.handler_nc_ardc_file.cloud_optimised_output_path, "_common_metadata"
        )
        parquet_meta = pa.parquet.read_schema(
            parquet_meta_file_path, filesystem=self.s3_fs
        )

        # horrible ... but got to be done. The dictionary of metadata has to be a dictionnary with byte keys and byte values.
        # meaning that we can't have nested dictionaries ...
        decoded_meta = {
            key.decode("utf-8"): json.loads(value.decode("utf-8").replace("'", '"'))
            for key, value in parquet_meta.metadata.items()
        }

        self.assertEqual(
            decoded_meta["global_attributes"]["metadata_uuid"],
            "b299cdcd-3dee-48aa-abdd-e0fcdbb9cadc",
        )
        self.assertEqual(decoded_meta["global_attributes"]["title"], "ARDC")

        #################################################################################
        # another test to modify the global_attributes and making sure this works as expected
        self.handler_nc_ardc_mod_metadata._add_metadata_sidecar()

        parquet_meta_file_path = os.path.join(
            self.handler_nc_ardc_mod_metadata.cloud_optimised_output_path,
            "_common_metadata",
        )
        parquet_meta = pa.parquet.read_schema(
            parquet_meta_file_path, filesystem=self.s3_fs
        )

        decoded_meta = {
            key.decode("utf-8"): json.loads(value.decode("utf-8").replace("'", '"'))
            for key, value in parquet_meta.metadata.items()
        }

        self.assertEqual(decoded_meta["global_attributes"]["title"], self.mod_title)

    def test_parquet_nc_generic_handler_scipy(self):
        # test with the scipy engine
        nc_obj_ls = s3_ls("imos-data", "good_nc_soop_sst")

        # 1st pass
        self.handler_nc_soop_sst_file.to_cloud_optimised([nc_obj_ls[0]])

        # 2nd pass, process the same file a second time. Should be deleted
        self.handler_nc_soop_sst_file.to_cloud_optimised_single(nc_obj_ls[0])

        # read parquet
        dataset_config = load_dataset_config(DATASET_CONFIG_NC_SOOP_SST_JSON)
        dataset_name = dataset_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.parquet/"

        parquet_dataset = pd.read_parquet(
            dname,
            engine="pyarrow",
            storage_options={
                "client_kwargs": {
                    "endpoint_url": f"http://{self.endpoint_ip}:{self.port}"
                }
            },
        )

        self.assertEqual(parquet_dataset["timestamp"][0], 1293840000)

        self.assertEqual(
            parquet_dataset["TIME"][0], pd.Timestamp("2011-01-01 00:00:00")
        )

    def test_parquet_nc_generic_handler_bad_time_values(self):
        # test with the scipy engine
        nc_obj_ls = s3_ls("imos-data", "bad_nc_soop_sst")

        # Capture logs
        log_stream = StringIO()
        log_handler = logging.StreamHandler(log_stream)
        logger = logging.getLogger()
        logger.addHandler(log_handler)

        self.handler_nc_soop_sst_file.to_cloud_optimised([nc_obj_ls[0]])

        log_handler.flush()
        captured_logs = log_stream.getvalue().strip().split("\n")

        # Validate logs
        self.assertTrue(
            any(
                "All values of the time variable were bad" in log
                for log in captured_logs
            )
        )
        ####################3

        # Capture logs
        log_stream = StringIO()
        log_handler = logging.StreamHandler(log_stream)
        logger = logging.getLogger()
        logger.addHandler(log_handler)

        self.handler_nc_soop_sst_file.to_cloud_optimised([nc_obj_ls[1]])

        log_handler.flush()
        captured_logs = log_stream.getvalue().strip().split("\n")
        # Validate logs
        self.assertTrue(
            any(
                "time issues with the input file. File not processed" in log
                for log in captured_logs
            )
        )

    def test_parquet_csv_generic_handler(self):  # , MockS3FileSystem):
        csv_obj_ls = s3_ls("imos-data", "good_csv", suffix=".csv")
        # MockS3FileSystem.return_value = s3fs.S3FileSystem(anon=False, client_kwargs={"endpoint_url": f"http://127.0.0.1:{self.port}"})

        # with mock_aws(aws_credentials):
        # 1st pass, could have some errors distributed.worker - ERROR - Failed to communicate with scheduler during heartbeat.
        # Solution is the rerun the unittest
        self.handler_csv_file.to_cloud_optimised([csv_obj_ls[0]])

        # 2nd pass
        self.handler_csv_file.to_cloud_optimised_single(csv_obj_ls[0])

        # Read parquet dataset and check data is good!
        dataset_config = load_dataset_config(DATASET_CONFIG_CSV_AATAMS_JSON)
        dataset_name = dataset_config["dataset_name"]
        dname = f"s3://{self.BUCKET_OPTIMISED_NAME}/{self.ROOT_PREFIX_CLOUD_OPTIMISED_PATH}/{dataset_name}.parquet/"

        parquet_dataset = pd.read_parquet(
            dname,
            engine="pyarrow",
            storage_options={
                "client_kwargs": {
                    "endpoint_url": f"http://{self.endpoint_ip}:{self.port}"
                }
            },
        )

        self.assertIn("station_name", parquet_dataset.columns)


if __name__ == "__main__":
    unittest.main()
